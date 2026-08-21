"""Delivering a run's batches to a Postgres database.

The shape here is the one every SQL destination follows, so read it before adding Redshift,
Snowflake, or BigQuery:

- A full refresh writes into a per-run staging table and swaps it into place on the final
  batch, so the destination table holds the previous run's data right up to the moment the
  new one is complete. A run that dies half way leaves the live table untouched.
- An incremental run merges each batch straight into the destination table on the schema's
  primary keys, which is what makes re-applying a batch after a crash harmless.
- An append run inserts. Without primary keys there is nothing to merge on, so the apply
  marker is the only thing standing between a crash and a duplicated batch.

Every write is idempotent per batch index, because the consumer re-claims any batch whose
outcome it could not confirm.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import ClassVar

import psycopg
import pyarrow as pa
from psycopg import sql

from posthog.models.integration.postgres import PostgreSQLServerIntegration

from products.warehouse_sources.backend.temporal.data_imports.destinations.contracts import (
    BatchWriteOutcome,
    DestinationBatchContext,
    DestinationRunContext,
)
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.destinations_load.writers.sql_types import (
    postgres_type_for,
    quote_identifier,
)

# Marks which batch of a run wrote a staged row, so re-applying a batch can delete exactly
# what its previous attempt wrote instead of the whole staging table.
BATCH_INDEX_COLUMN = "_ph_batch_index"


def staging_table_name(ctx: DestinationRunContext) -> str:
    # Run-scoped, so two runs of the same table never share a staging table. Postgres caps
    # identifiers at 63 bytes, hence the truncated run id.
    return f"{ctx.table_name}__ph_stage_{ctx.run_uuid.replace('-', '')[:12]}"


class PostgresDestinationWriter:
    """Writes a run's batches into a Postgres table."""

    holds_sync_lock: ClassVar[bool] = False
    runs_post_load: ClassVar[bool] = False

    def __init__(self, ctx: DestinationRunContext) -> None:
        self._ctx = ctx
        self._schema = (ctx.config or {}).get("schema") or "public"
        self._conn: psycopg.Connection | None = None
        self._table_columns: list[tuple[str, str]] = []

    # --- connection -------------------------------------------------------------------

    def _connect(self) -> psycopg.Connection:
        if self._conn is not None and not self._conn.closed:
            return self._conn

        if self._ctx.integration_id is None:
            raise ValueError(f"Destination {self._ctx.destination_name} has no integration to connect with")

        from posthog.models.integration import Integration  # noqa: PLC0415 — avoids a model import cycle

        integration = Integration.objects.get(id=self._ctx.integration_id, team_id=self._ctx.team_id)
        server = PostgreSQLServerIntegration(integration)
        credentials = server.credentials()
        authority = server.authority()
        tls = server.tls()

        self._conn = psycopg.connect(
            host=authority.host,
            port=authority.port,
            user=credentials.user,
            password=credentials.password,
            dbname=(self._ctx.config or {}).get("database") or "postgres",
            sslmode=tls.ssl_mode,
            autocommit=True,
        )
        return self._conn

    # --- dialect seams ------------------------------------------------------------------
    # Overridden by the SQL destinations that share this writer's shape but not its types.

    @property
    def _batch_index_column(self) -> str:
        return BATCH_INDEX_COLUMN

    def _column_type(self, arrow_type: pa.DataType) -> str:
        return postgres_type_for(arrow_type)

    # --- schema -----------------------------------------------------------------------

    def _ensure_table(self, conn: psycopg.Connection, table: str, schema: pa.Schema, *, with_batch_index: bool) -> None:
        columns = [(field.name, self._column_type(field.type)) for field in schema]
        if with_batch_index:
            columns.append((self._batch_index_column, "INTEGER"))

        column_sql = sql.SQL(", ").join(
            sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(type_name)) for name, type_name in columns
        )
        conn.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self._schema)),
        )
        conn.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                sql.Identifier(self._schema), sql.Identifier(table), column_sql
            )
        )
        self._table_columns = columns

    def _evolve_table(self, conn: psycopg.Connection, table: str, schema: pa.Schema) -> None:
        """Add columns the source has grown since the destination table was created.

        Deliberately additive. Batch exports filter the incoming data down to the columns the
        destination already has, which silently drops new fields; a synced table is supposed to
        mirror its source, so the column is added instead.
        """
        existing = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                (self._schema, table),
            ).fetchall()
        }
        for field in schema:
            if field.name not in existing:
                conn.execute(
                    sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}").format(
                        sql.Identifier(self._schema),
                        sql.Identifier(table),
                        sql.Identifier(field.name),
                        sql.SQL(self._column_type(field.type)),
                    )
                )

    # --- writer protocol ----------------------------------------------------------------

    async def prepare_run(self, ctx: DestinationRunContext) -> None:
        # Tables are created from the first batch's schema, once it is known.
        return None

    async def write_batch(
        self, batches: AsyncIterator[pa.RecordBatch], ctx: DestinationBatchContext
    ) -> BatchWriteOutcome:
        run = ctx.run
        conn = self._connect()
        full_refresh = run.is_full_refresh
        target = staging_table_name(run) if full_refresh else run.table_name

        rows_written = 0
        first = True
        async for batch in batches:
            if first:
                self._ensure_table(conn, target, batch.schema, with_batch_index=full_refresh)
                self._evolve_table(conn, target, batch.schema)
                if full_refresh:
                    # This batch may be a re-apply after a crash, so clear whatever its previous
                    # attempt wrote before writing it again.
                    conn.execute(
                        sql.SQL("DELETE FROM {}.{} WHERE {} = %s").format(
                            sql.Identifier(self._schema),
                            sql.Identifier(target),
                            sql.Identifier(self._batch_index_column),
                        ),
                        (ctx.batch_index,),
                    )
                first = False

            rows_written += self._write_record_batch(conn, target, batch, ctx, full_refresh=full_refresh)

        return BatchWriteOutcome(rows_written=rows_written)

    def _write_record_batch(
        self,
        conn: psycopg.Connection,
        target: str,
        batch: pa.RecordBatch,
        ctx: DestinationBatchContext,
        *,
        full_refresh: bool,
    ) -> int:
        run = ctx.run
        column_names = list(batch.schema.names)
        rows = batch.to_pylist()
        if not rows:
            return 0

        if full_refresh:
            self._copy_rows(conn, target, column_names, rows, batch_index=ctx.batch_index)
            return len(rows)

        if run.is_incremental and run.primary_keys:
            self._merge_rows(conn, target, column_names, rows, primary_keys=list(run.primary_keys))
        else:
            self._copy_rows(conn, target, column_names, rows, batch_index=None)
        return len(rows)

    def _copy_rows(
        self,
        conn: psycopg.Connection,
        target: str,
        column_names: list[str],
        rows: list[dict],
        *,
        batch_index: int | None,
    ) -> None:
        columns = [*column_names] + ([self._batch_index_column] if batch_index is not None else [])
        statement = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
            sql.Identifier(self._schema),
            sql.Identifier(target),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        )
        with conn.cursor().copy(statement) as copy:
            for row in rows:
                values = [row.get(name) for name in column_names]
                if batch_index is not None:
                    values.append(batch_index)
                copy.write_row(values)

    def _merge_rows(
        self,
        conn: psycopg.Connection,
        target: str,
        column_names: list[str],
        rows: list[dict],
        *,
        primary_keys: list[str],
    ) -> None:
        """Upsert on the schema's primary keys, so re-applying a batch is a no-op."""
        updatable = [c for c in column_names if c not in primary_keys]
        insert = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO {}").format(
            sql.Identifier(self._schema),
            sql.Identifier(target),
            sql.SQL(", ").join(sql.Identifier(c) for c in column_names),
            sql.SQL(", ").join(sql.Placeholder() for _ in column_names),
            sql.SQL(", ").join(sql.Identifier(c) for c in primary_keys),
            sql.SQL("UPDATE SET {}").format(
                sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in updatable
                )
            )
            if updatable
            else sql.SQL("NOTHING"),
        )
        self._ensure_unique_index(conn, target, primary_keys)
        with conn.cursor() as cursor:
            cursor.executemany(insert, [[row.get(name) for name in column_names] for row in rows])

    def _ensure_unique_index(self, conn: psycopg.Connection, target: str, primary_keys: list[str]) -> None:
        """ON CONFLICT needs a unique index on the merge keys; create it once."""
        index_name = f"{target}__ph_pk"[:63]
        conn.execute(
            sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
                sql.Identifier(index_name),
                sql.Identifier(self._schema),
                sql.Identifier(target),
                sql.SQL(", ").join(sql.Identifier(c) for c in primary_keys),
            )
        )

    async def finalize_run(self, ctx: DestinationRunContext) -> None:
        """Swap a completed full refresh into place. Other sync types wrote in place already.

        Idempotent: the consumer re-claims a final batch whose outcome it could not confirm, so
        this can be called again after the swap already happened. A missing staging table is
        exactly that case, and means the run is already live.
        """
        if not ctx.is_full_refresh:
            return

        conn = self._connect()
        staging = staging_table_name(ctx)
        live = ctx.table_name
        old = f"{live}__ph_old_{ctx.run_uuid.replace('-', '')[:12]}"

        staging_exists = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (self._schema, staging),
        ).fetchone()
        if not staging_exists:
            return

        with conn.transaction():
            conn.execute(
                sql.SQL("ALTER TABLE {}.{} DROP COLUMN IF EXISTS {}").format(
                    sql.Identifier(self._schema), sql.Identifier(staging), sql.Identifier(self._batch_index_column)
                )
            )
            live_exists = conn.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (self._schema, live),
            ).fetchone()
            if live_exists:
                conn.execute(
                    sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                        sql.Identifier(self._schema), sql.Identifier(live), sql.Identifier(old)
                    )
                )
            conn.execute(
                sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                    sql.Identifier(self._schema), sql.Identifier(staging), sql.Identifier(live)
                )
            )
        if live_exists:
            conn.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{}").format(sql.Identifier(self._schema), sql.Identifier(old))
            )

    async def abort_run(self, ctx: DestinationRunContext) -> None:
        """Drop the staging table a failed full refresh left behind. Best effort."""
        if not ctx.is_full_refresh or self._conn is None or self._conn.closed:
            return
        try:
            self._conn.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier(self._schema), sql.Identifier(staging_table_name(ctx))
                )
            )
        except Exception:
            # The next run of this table creates its own staging table, so a leftover one costs
            # storage rather than correctness.
            pass

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()


__all__ = ["BATCH_INDEX_COLUMN", "PostgresDestinationWriter", "quote_identifier", "staging_table_name"]
