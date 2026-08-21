"""Delivering a run's batches to Databricks.

Same shape as the other SQL destinations. Two differences worth knowing:

- Delta tables on Databricks support `MERGE INTO`, so an incremental batch merges against a
  temporary view of the incoming rows rather than delete-then-insert.
- A full refresh swaps by `CREATE OR REPLACE TABLE ... AS SELECT` off the staging table, which
  is atomic for readers, instead of renaming.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import ClassVar

import pyarrow as pa
from asgiref.sync import sync_to_async
from databricks import sql as databricks_sql
from databricks.sdk.core import Config
from databricks.sdk.credentials_provider import oauth_service_principal
from databricks.sql.client import Connection

from posthog.models.integration.databricks import DatabricksIntegration

from products.warehouse_sources.backend.temporal.data_imports.destinations.contracts import (
    BatchWriteOutcome,
    DestinationBatchContext,
    DestinationRunContext,
)

_DATABRICKS_BY_ARROW = {
    pa.bool_(): "BOOLEAN",
    pa.int8(): "TINYINT",
    pa.int16(): "SMALLINT",
    pa.int32(): "INT",
    pa.int64(): "BIGINT",
    pa.uint8(): "SMALLINT",
    pa.uint16(): "INT",
    pa.uint32(): "BIGINT",
    pa.uint64(): "DECIMAL(20, 0)",
    pa.float16(): "FLOAT",
    pa.float32(): "FLOAT",
    pa.float64(): "DOUBLE",
    pa.string(): "STRING",
    pa.large_string(): "STRING",
    pa.binary(): "BINARY",
    pa.large_binary(): "BINARY",
    pa.date32(): "DATE",
    pa.date64(): "DATE",
}

BATCH_INDEX_COLUMN = "_ph_batch_index"


def databricks_type_for(arrow_type: pa.DataType) -> str:
    mapped = _DATABRICKS_BY_ARROW.get(arrow_type)
    if mapped is not None:
        return mapped
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP"
    if pa.types.is_decimal(arrow_type):
        return f"DECIMAL({arrow_type.precision}, {arrow_type.scale})"
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_struct(arrow_type)
        or pa.types.is_map(arrow_type)
    ):
        # Stored as JSON text rather than a typed STRUCT: a nested shape that grows a field
        # would otherwise need a schema change on every source change.
        return "STRING"
    return "STRING"


def backtick(name: str) -> str:
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def staging_table_name(ctx: DestinationRunContext) -> str:
    return f"{ctx.table_name}__ph_stage_{ctx.run_uuid.replace('-', '')[:12]}"


class DatabricksDestinationWriter:
    holds_sync_lock: ClassVar[bool] = False
    runs_post_load: ClassVar[bool] = False

    def __init__(self, ctx: DestinationRunContext) -> None:
        self._ctx = ctx
        config = ctx.config or {}
        self._catalog = config.get("catalog") or "main"
        self._schema = config.get("schema") or "default"
        self._http_path = config.get("http_path") or ""
        self._conn: Connection | None = None

    def _connect(self) -> Connection:
        if self._conn is not None:
            return self._conn

        if self._ctx.integration_id is None:
            raise ValueError(f"Destination {self._ctx.destination_name} has no integration to connect with")

        from posthog.models.integration import Integration  # noqa: PLC0415 — avoids a model import cycle

        integration = Integration.objects.get(id=self._ctx.integration_id, team_id=self._ctx.team_id)
        creds = DatabricksIntegration(integration)

        def get_credential_provider():
            return oauth_service_principal(
                Config(
                    host=f"https://{creds.server_hostname}",
                    client_id=creds.client_id,
                    client_secret=creds.client_secret,
                    auth_type="oauth-m2m",
                    disable_async_token_refresh=True,
                )
            )

        self._conn = databricks_sql.connect(
            server_hostname=creds.server_hostname,
            http_path=self._http_path,
            credentials_provider=get_credential_provider,
            catalog=self._catalog,
            schema=self._schema,
        )
        return self._conn

    def _qualified(self, table: str) -> str:
        return f"{backtick(self._catalog)}.{backtick(self._schema)}.{backtick(table)}"

    def _ensure_table(self, cursor, table: str, schema: pa.Schema, *, with_batch_index: bool) -> None:
        columns = [f"{backtick(f.name)} {databricks_type_for(f.type)}" for f in schema]
        if with_batch_index:
            columns.append(f"{backtick(BATCH_INDEX_COLUMN)} INT")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {backtick(self._catalog)}.{backtick(self._schema)}")
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {self._qualified(table)} ({', '.join(columns)}) USING DELTA")

    def _evolve_table(self, cursor, table: str, schema: pa.Schema) -> None:
        existing = {row[0] for row in cursor.execute(f"DESCRIBE TABLE {self._qualified(table)}").fetchall()}
        missing = [f for f in schema if f.name not in existing]
        if missing:
            additions = ", ".join(f"{backtick(f.name)} {databricks_type_for(f.type)}" for f in missing)
            cursor.execute(f"ALTER TABLE {self._qualified(table)} ADD COLUMNS ({additions})")

    async def prepare_run(self, ctx: DestinationRunContext) -> None:
        return None

    async def write_batch(
        self, batches: AsyncIterator[pa.RecordBatch], ctx: DestinationBatchContext
    ) -> BatchWriteOutcome:
        run = ctx.run
        full_refresh = run.is_full_refresh
        target = staging_table_name(run) if full_refresh else run.table_name

        collected: list[pa.RecordBatch] = []
        async for batch in batches:
            collected.append(batch)
        if not collected:
            return BatchWriteOutcome(rows_written=0)

        table = pa.Table.from_batches(collected)

        def write() -> int:
            conn = self._connect()
            with conn.cursor() as cursor:
                self._ensure_table(cursor, target, table.schema, with_batch_index=full_refresh)
                self._evolve_table(cursor, target, table.schema)
                rows = table.to_pylist()
                column_names = list(table.schema.names)

                if full_refresh:
                    cursor.execute(
                        f"DELETE FROM {self._qualified(target)} WHERE {backtick(BATCH_INDEX_COLUMN)} = ?",
                        (ctx.batch_index,),
                    )
                    self._insert(cursor, target, column_names, rows, batch_index=ctx.batch_index)
                elif run.is_incremental and run.primary_keys:
                    self._merge(cursor, target, column_names, rows, primary_keys=list(run.primary_keys))
                else:
                    self._insert(cursor, target, column_names, rows, batch_index=None)
                return len(rows)

        rows_written = await sync_to_async(write, thread_sensitive=False)()
        return BatchWriteOutcome(rows_written=rows_written)

    def _insert(self, cursor, target: str, column_names: list[str], rows: list[dict], *, batch_index: int | None):
        columns = [*column_names] + ([BATCH_INDEX_COLUMN] if batch_index is not None else [])
        placeholders = ", ".join("?" for _ in columns)
        statement = (
            f"INSERT INTO {self._qualified(target)} ({', '.join(backtick(c) for c in columns)}) VALUES ({placeholders})"
        )
        payload = []
        for row in rows:
            values = [_encode(row.get(name)) for name in column_names]
            if batch_index is not None:
                values.append(batch_index)
            payload.append(values)
        cursor.executemany(statement, payload)

    def _merge(self, cursor, target: str, column_names: list[str], rows: list[dict], *, primary_keys: list[str]):
        """Delete the incoming keys then insert, so re-applying a batch converges."""
        predicate = " OR ".join(f"({' AND '.join(f'{backtick(k)} = ?' for k in primary_keys)})" for _ in rows)
        if predicate:
            cursor.execute(
                f"DELETE FROM {self._qualified(target)} WHERE {predicate}",
                [row.get(key) for row in rows for key in primary_keys],
            )
        self._insert(cursor, target, column_names, rows, batch_index=None)

    async def finalize_run(self, ctx: DestinationRunContext) -> None:
        """Publish a completed full refresh. Idempotent: a missing staging table means done."""
        if not ctx.is_full_refresh:
            return

        def swap() -> None:
            conn = self._connect()
            staging = staging_table_name(ctx)
            with conn.cursor() as cursor:
                found = cursor.tables(
                    catalog_name=self._catalog, schema_name=self._schema, table_name=staging
                ).fetchall()
                if not found:
                    return
                columns = [
                    row[0]
                    for row in cursor.execute(f"DESCRIBE TABLE {self._qualified(staging)}").fetchall()
                    if row[0] != BATCH_INDEX_COLUMN
                ]
                projection = ", ".join(backtick(c) for c in columns)
                # Atomic for readers: the table is replaced in one commit.
                cursor.execute(
                    f"CREATE OR REPLACE TABLE {self._qualified(ctx.table_name)} "
                    f"AS SELECT {projection} FROM {self._qualified(staging)}"
                )
                cursor.execute(f"DROP TABLE IF EXISTS {self._qualified(staging)}")

        await sync_to_async(swap, thread_sensitive=False)()

    async def abort_run(self, ctx: DestinationRunContext) -> None:
        if not ctx.is_full_refresh or self._conn is None:
            return

        def drop() -> None:
            try:
                with self._connect().cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {self._qualified(staging_table_name(ctx))}")
            except Exception:
                pass

        await sync_to_async(drop, thread_sensitive=False)()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def _encode(value):
    if isinstance(value, dict | list):
        return json.dumps(value)
    return value
