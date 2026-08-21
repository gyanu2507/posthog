"""Delivering a run's batches to BigQuery.

BigQuery loads Arrow natively, so batches are loaded as parquet rather than turned into INSERT
statements. The shape is otherwise the same as the other SQL destinations:

- A full refresh loads into a per-run staging table and copies it over the live table on the
  final batch. `WRITE_TRUNCATE` on a copy job is atomic, so readers never see a partial run.
- An incremental run loads the batch into a temporary table and runs `MERGE` on the schema's
  primary keys, which is what makes re-applying a batch harmless.
"""

from __future__ import annotations

import io
from collections.abc import AsyncIterator
from typing import ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
from asgiref.sync import sync_to_async
from google.cloud import bigquery
from google.oauth2 import service_account

from products.warehouse_sources.backend.temporal.data_imports.destinations.contracts import (
    BatchWriteOutcome,
    DestinationBatchContext,
    DestinationRunContext,
)

BATCH_INDEX_COLUMN = "_ph_batch_index"


def staging_table_name(ctx: DestinationRunContext) -> str:
    return f"{ctx.table_name}__ph_stage_{ctx.run_uuid.replace('-', '')[:12]}"


def _backtick(name: str) -> str:
    escaped = name.replace("`", "")
    return f"`{escaped}`"


class BigQueryDestinationWriter:
    holds_sync_lock: ClassVar[bool] = False
    runs_post_load: ClassVar[bool] = False

    def __init__(self, ctx: DestinationRunContext) -> None:
        self._ctx = ctx
        config = ctx.config or {}
        self._dataset = config.get("dataset") or config.get("dataset_id") or ""
        self._client: bigquery.Client | None = None
        self._project: str = config.get("project") or config.get("project_id") or ""

    def _get_client(self) -> bigquery.Client:
        if self._client is not None:
            return self._client

        if self._ctx.integration_id is None:
            raise ValueError(f"Destination {self._ctx.destination_name} has no integration to connect with")

        from posthog.models.integration import Integration  # noqa: PLC0415 — avoids a model import cycle

        integration = Integration.objects.get(id=self._ctx.integration_id, team_id=self._ctx.team_id)
        info = {
            "type": "service_account",
            "project_id": integration.config.get("project_id"),
            "private_key": integration.sensitive_config.get("private_key"),
            "private_key_id": integration.sensitive_config.get("private_key_id"),
            "client_email": integration.config.get("service_account_email"),
            "token_uri": integration.sensitive_config.get("token_uri") or "https://oauth2.googleapis.com/token",
        }
        credentials = service_account.Credentials.from_service_account_info(info)
        self._project = self._project or (integration.config.get("project_id") or "")
        self._client = bigquery.Client(project=self._project, credentials=credentials)
        return self._client

    def _table_ref(self, table: str) -> str:
        return f"{self._project}.{self._dataset}.{table}"

    def _quoted_table(self, table: str) -> str:
        return _backtick(self._table_ref(table))

    async def prepare_run(self, ctx: DestinationRunContext) -> None:
        def ensure_dataset() -> None:
            client = self._get_client()
            client.create_dataset(f"{self._project}.{self._dataset}", exists_ok=True)

        await sync_to_async(ensure_dataset, thread_sensitive=False)()

    async def write_batch(
        self, batches: AsyncIterator[pa.RecordBatch], ctx: DestinationBatchContext
    ) -> BatchWriteOutcome:
        run = ctx.run
        full_refresh = run.is_full_refresh

        collected: list[pa.RecordBatch] = []
        async for batch in batches:
            collected.append(batch)
        if not collected:
            return BatchWriteOutcome(rows_written=0)

        table = pa.Table.from_batches(collected)

        def write() -> int:
            client = self._get_client()

            if full_refresh:
                staging = staging_table_name(run)
                # Batch 0 truncates so a re-run of the whole batch sequence starts clean; later
                # batches append. Re-applying one batch is covered by the apply marker.
                disposition = (
                    bigquery.WriteDisposition.WRITE_TRUNCATE
                    if ctx.batch_index == 0
                    else bigquery.WriteDisposition.WRITE_APPEND
                )
                self._load(client, staging, table, disposition)
                return table.num_rows

            if run.is_incremental and run.primary_keys:
                temp = f"{run.table_name}__ph_tmp_{run.run_uuid.replace('-', '')[:8]}_{ctx.batch_index}"
                self._load(client, temp, table, bigquery.WriteDisposition.WRITE_TRUNCATE)
                self._merge(client, run.table_name, temp, list(table.schema.names), list(run.primary_keys))
                client.delete_table(self._table_ref(temp), not_found_ok=True)
                return table.num_rows

            self._load(client, run.table_name, table, bigquery.WriteDisposition.WRITE_APPEND)
            return table.num_rows

        rows_written = await sync_to_async(write, thread_sensitive=False)()
        return BatchWriteOutcome(rows_written=rows_written)

    def _load(self, client: bigquery.Client, table: str, data: pa.Table, disposition: str) -> None:
        """Load an Arrow table as parquet, letting BigQuery derive and evolve the schema."""
        buffer = io.BytesIO()
        pq.write_table(data, buffer)
        buffer.seek(0)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=disposition,
            autodetect=True,
            # Additive evolution: a column the source grew is added rather than rejected.
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )
        client.load_table_from_file(buffer, self._table_ref(table), job_config=job_config).result()

    def _merge(
        self, client: bigquery.Client, target: str, source: str, column_names: list[str], primary_keys: list[str]
    ) -> None:
        on_clause = " AND ".join(f"T.{_backtick(k)} = S.{_backtick(k)}" for k in primary_keys)
        updates = ", ".join(f"T.{_backtick(c)} = S.{_backtick(c)}" for c in column_names if c not in primary_keys)
        columns = ", ".join(_backtick(c) for c in column_names)
        values = ", ".join(f"S.{_backtick(c)}" for c in column_names)
        update_clause = f"WHEN MATCHED THEN UPDATE SET {updates} " if updates else ""
        client.query(
            f"MERGE {self._quoted_table(target)} T USING {self._quoted_table(source)} S ON {on_clause} "
            f"{update_clause}"
            f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})"
        ).result()

    async def finalize_run(self, ctx: DestinationRunContext) -> None:
        """Copy a completed full refresh over the live table. Idempotent: no staging table means done."""
        if not ctx.is_full_refresh:
            return

        def publish() -> None:
            client = self._get_client()
            staging = staging_table_name(ctx)
            try:
                client.get_table(self._table_ref(staging))
            except Exception:
                return
            client.copy_table(
                self._table_ref(staging),
                self._table_ref(ctx.table_name),
                job_config=bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE),
            ).result()
            client.delete_table(self._table_ref(staging), not_found_ok=True)

        await sync_to_async(publish, thread_sensitive=False)()

    async def abort_run(self, ctx: DestinationRunContext) -> None:
        if not ctx.is_full_refresh or self._client is None:
            return

        def drop() -> None:
            try:
                self._get_client().delete_table(self._table_ref(staging_table_name(ctx)), not_found_ok=True)
            except Exception:
                pass

        await sync_to_async(drop, thread_sensitive=False)()
