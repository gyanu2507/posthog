"""Delivering a run's batches to Snowflake.

Follows the same shape as the Postgres writer (staging table plus swap for a full refresh,
merge on primary keys for an incremental run), but Snowflake is not Postgres-wire, so it brings
its own connection and its own statements rather than subclassing.

Snowflake identifiers are case-sensitive once quoted, and unquoted ones fold to upper case.
Everything here is quoted, so a source column named `id` stays `id` rather than becoming `ID`
and breaking a later merge.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any, ClassVar

import pyarrow as pa
import snowflake.connector
from asgiref.sync import sync_to_async
from snowflake.connector.connection import SnowflakeConnection

from posthog.models.integration.snowflake import SnowflakeIntegration

from products.warehouse_sources.backend.temporal.data_imports.destinations.contracts import (
    BatchWriteOutcome,
    DestinationBatchContext,
    DestinationRunContext,
)
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.destinations_load.writers.sql_types import (
    quote_identifier,
)

_SNOWFLAKE_BY_ARROW = {
    pa.bool_(): "BOOLEAN",
    pa.int8(): "NUMBER(38, 0)",
    pa.int16(): "NUMBER(38, 0)",
    pa.int32(): "NUMBER(38, 0)",
    pa.int64(): "NUMBER(38, 0)",
    pa.uint8(): "NUMBER(38, 0)",
    pa.uint16(): "NUMBER(38, 0)",
    pa.uint32(): "NUMBER(38, 0)",
    pa.uint64(): "NUMBER(38, 0)",
    pa.float16(): "FLOAT",
    pa.float32(): "FLOAT",
    pa.float64(): "FLOAT",
    pa.string(): "VARCHAR",
    pa.large_string(): "VARCHAR",
    pa.binary(): "BINARY",
    pa.large_binary(): "BINARY",
    pa.date32(): "DATE",
    pa.date64(): "DATE",
}

BATCH_INDEX_COLUMN = "_ph_batch_index"


def snowflake_type_for(arrow_type: pa.DataType) -> str:
    mapped = _SNOWFLAKE_BY_ARROW.get(arrow_type)
    if mapped is not None:
        return mapped
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP_TZ" if arrow_type.tz else "TIMESTAMP_NTZ"
    if pa.types.is_time(arrow_type):
        return "TIME"
    if pa.types.is_decimal(arrow_type):
        return f"NUMBER({arrow_type.precision}, {arrow_type.scale})"
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_struct(arrow_type)
        or pa.types.is_map(arrow_type)
    ):
        # VARIANT keeps semi-structured values whole, so a struct that grows a field keeps
        # loading rather than needing a schema change.
        return "VARIANT"
    return "VARCHAR"


def staging_table_name(ctx: DestinationRunContext) -> str:
    return f"{ctx.table_name}__PH_STAGE_{ctx.run_uuid.replace('-', '')[:12]}"


class SnowflakeDestinationWriter:
    holds_sync_lock: ClassVar[bool] = False
    runs_post_load: ClassVar[bool] = False

    def __init__(self, ctx: DestinationRunContext) -> None:
        self._ctx = ctx
        config = ctx.config or {}
        self._database = config.get("database") or ""
        self._schema = config.get("schema") or "PUBLIC"
        self._warehouse = config.get("warehouse") or ""
        self._role = config.get("role")
        self._conn: SnowflakeConnection | None = None

    def _connect(self) -> SnowflakeConnection:
        if self._conn is not None:
            return self._conn

        if self._ctx.integration_id is None:
            raise ValueError(f"Destination {self._ctx.destination_name} has no integration to connect with")

        from posthog.models.integration import Integration  # noqa: PLC0415 — avoids a model import cycle

        integration = Integration.objects.get(id=self._ctx.integration_id, team_id=self._ctx.team_id)
        creds = SnowflakeIntegration(integration)

        kwargs: dict[str, Any] = {
            "account": creds.account,
            "user": creds.user,
            "database": self._database,
            "schema": self._schema,
            "warehouse": self._warehouse,
        }
        if self._role:
            kwargs["role"] = self._role
        if creds.authentication_type == "keypair":
            kwargs["private_key"] = creds.private_key
            if creds.private_key_passphrase:
                kwargs["private_key_passphrase"] = creds.private_key_passphrase
        else:
            kwargs["password"] = creds.password

        self._conn = snowflake.connector.connect(**kwargs)
        return self._conn

    def _qualified(self, table: str) -> str:
        return f"{quote_identifier(self._schema)}.{quote_identifier(table)}"

    def _ensure_table(self, conn: SnowflakeConnection, table: str, schema: pa.Schema, *, with_batch_index: bool):
        columns = [f"{quote_identifier(f.name)} {snowflake_type_for(f.type)}" for f in schema]
        if with_batch_index:
            columns.append(f"{quote_identifier(BATCH_INDEX_COLUMN)} NUMBER(38, 0)")
        conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(self._schema)}")
        conn.cursor().execute(f"CREATE TABLE IF NOT EXISTS {self._qualified(table)} ({', '.join(columns)})")

    def _evolve_table(self, conn: SnowflakeConnection, table: str, schema: pa.Schema) -> None:
        """Add columns the source has grown. Additive only, same as the other SQL writers."""
        existing = {
            row[0]
            for row in _fetch_all(
                conn,
                "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                (self._schema, table),
            )
        }
        for field in schema:
            if field.name not in existing and field.name.upper() not in existing:
                conn.cursor().execute(
                    f"ALTER TABLE {self._qualified(table)} "
                    f"ADD COLUMN IF NOT EXISTS {quote_identifier(field.name)} {snowflake_type_for(field.type)}"
                )

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
            self._ensure_table(conn, target, table.schema, with_batch_index=full_refresh)
            self._evolve_table(conn, target, table.schema)
            rows = table.to_pylist()
            column_names = list(table.schema.names)

            if full_refresh:
                # This batch may be a re-apply, so clear what its previous attempt wrote first.
                conn.cursor().execute(
                    f"DELETE FROM {self._qualified(target)} WHERE {quote_identifier(BATCH_INDEX_COLUMN)} = %s",
                    (ctx.batch_index,),
                )
                self._insert(conn, target, column_names, rows, batch_index=ctx.batch_index)
            elif run.is_incremental and run.primary_keys:
                self._merge(conn, target, column_names, rows, primary_keys=list(run.primary_keys))
            else:
                self._insert(conn, target, column_names, rows, batch_index=None)
            return len(rows)

        rows_written = await sync_to_async(write, thread_sensitive=False)()
        return BatchWriteOutcome(rows_written=rows_written)

    def _insert(
        self,
        conn: SnowflakeConnection,
        target: str,
        column_names: list[str],
        rows: list[dict],
        *,
        batch_index: int | None,
    ) -> None:
        columns = [*column_names] + ([BATCH_INDEX_COLUMN] if batch_index is not None else [])
        placeholders = ", ".join("%s" for _ in columns)
        statement = (
            f"INSERT INTO {self._qualified(target)} "
            f"({', '.join(quote_identifier(c) for c in columns)}) VALUES ({placeholders})"
        )
        payload = []
        for row in rows:
            values = [_encode(row.get(name)) for name in column_names]
            if batch_index is not None:
                values.append(batch_index)
            payload.append(values)
        conn.cursor().executemany(statement, payload)

    def _merge(
        self,
        conn: SnowflakeConnection,
        target: str,
        column_names: list[str],
        rows: list[dict],
        *,
        primary_keys: list[str],
    ) -> None:
        """Delete the incoming keys then insert, in one transaction.

        Snowflake has MERGE, but it needs the incoming rows as a queryable source; a
        delete-then-insert over the same batch reaches the same state and keeps the statement
        shape identical to Redshift's.
        """
        conn.cursor().execute("BEGIN")
        try:
            predicate = " OR ".join(
                f"({' AND '.join(f'{quote_identifier(k)} = %s' for k in primary_keys)})" for _ in rows
            )
            if predicate:
                conn.cursor().execute(
                    f"DELETE FROM {self._qualified(target)} WHERE {predicate}",
                    [row.get(key) for row in rows for key in primary_keys],
                )
            self._insert(conn, target, column_names, rows, batch_index=None)
            conn.cursor().execute("COMMIT")
        except Exception:
            conn.cursor().execute("ROLLBACK")
            raise

    async def finalize_run(self, ctx: DestinationRunContext) -> None:
        """Swap a completed full refresh into place. Idempotent: a missing staging table means done."""
        if not ctx.is_full_refresh:
            return

        def swap() -> None:
            conn = self._connect()
            staging = staging_table_name(ctx)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (self._schema, staging),
            )
            exists = cursor.fetchone()
            if not exists:
                return
            conn.cursor().execute(
                f"ALTER TABLE {self._qualified(staging)} DROP COLUMN IF EXISTS {quote_identifier(BATCH_INDEX_COLUMN)}"
            )
            # SWAP exchanges the two tables atomically, then the old contents are dropped.
            live_cursor = conn.cursor()
            live_cursor.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (self._schema, ctx.table_name),
            )
            live_exists = live_cursor.fetchone()
            if live_exists:
                conn.cursor().execute(
                    f"ALTER TABLE {self._qualified(staging)} SWAP WITH {self._qualified(ctx.table_name)}"
                )
                conn.cursor().execute(f"DROP TABLE IF EXISTS {self._qualified(staging)}")
            else:
                conn.cursor().execute(
                    f"ALTER TABLE {self._qualified(staging)} RENAME TO {self._qualified(ctx.table_name)}"
                )

        await sync_to_async(swap, thread_sensitive=False)()

    async def abort_run(self, ctx: DestinationRunContext) -> None:
        if not ctx.is_full_refresh or self._conn is None:
            return

        def drop() -> None:
            try:
                self._connect().cursor().execute(f"DROP TABLE IF EXISTS {self._qualified(staging_table_name(ctx))}")
            except Exception:
                # The next run stages under its own id, so a leftover table costs storage only.
                pass

        await sync_to_async(drop, thread_sensitive=False)()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def _fetch_all(conn: SnowflakeConnection, statement: str, params: tuple) -> list:
    cursor = conn.cursor()
    cursor.execute(statement, params)
    return cursor.fetchall()


def _encode(value):
    if isinstance(value, dict | list):
        return json.dumps(value)
    return value
