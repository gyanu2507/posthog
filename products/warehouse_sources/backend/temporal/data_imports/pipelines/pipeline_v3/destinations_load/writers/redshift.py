"""Delivering a run's batches to Redshift.

Redshift speaks the Postgres wire protocol, so the connection and the full-refresh swap are
shared with the Postgres writer, and a Redshift destination is backed by a `postgresql`
integration. (The `aws-redshift` integration kind holds AWS keys for COPY-from-S3, not the
cluster's host and login, so it cannot back the connection.) Three things genuinely differ and
are overridden here:

- No `ON CONFLICT`. Upserts are a staging table plus a delete-then-insert inside one
  transaction, which is the portable form and works on clusters predating Redshift's `MERGE`.
- No unbounded `TEXT`. Columns are `VARCHAR(MAX)`, and nested values go in `SUPER` rather than
  `JSONB`.
- No `CREATE UNIQUE INDEX`. Redshift has no unique indexes to create, so the Postgres writer's
  index step is skipped entirely.
"""

from __future__ import annotations

import json
from typing import ClassVar

import psycopg
import pyarrow as pa
from psycopg import sql

from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.destinations_load.writers.postgres import (
    PostgresDestinationWriter,
)

# Redshift's widest string type. Anything longer than this is truncated by the cluster, which
# is preferable to failing the sync outright.
VARCHAR_MAX = "VARCHAR(MAX)"

_REDSHIFT_BY_ARROW = {
    pa.bool_(): "BOOLEAN",
    pa.int8(): "SMALLINT",
    pa.int16(): "SMALLINT",
    pa.int32(): "INTEGER",
    pa.int64(): "BIGINT",
    pa.uint8(): "SMALLINT",
    pa.uint16(): "INTEGER",
    pa.uint32(): "BIGINT",
    pa.uint64(): "NUMERIC(20, 0)",
    pa.float16(): "REAL",
    pa.float32(): "REAL",
    pa.float64(): "DOUBLE PRECISION",
    pa.string(): VARCHAR_MAX,
    pa.large_string(): VARCHAR_MAX,
    pa.binary(): VARCHAR_MAX,
    pa.large_binary(): VARCHAR_MAX,
    pa.date32(): "DATE",
    pa.date64(): "DATE",
}


def redshift_type_for(arrow_type: pa.DataType) -> str:
    mapped = _REDSHIFT_BY_ARROW.get(arrow_type)
    if mapped is not None:
        return mapped
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMPTZ" if arrow_type.tz else "TIMESTAMP"
    if pa.types.is_time(arrow_type):
        return "TIME"
    if pa.types.is_decimal(arrow_type):
        return f"NUMERIC({arrow_type.precision}, {arrow_type.scale})"
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_struct(arrow_type)
        or pa.types.is_map(arrow_type)
    ):
        # SUPER holds semi-structured values without flattening them, so a struct that grows a
        # field keeps loading instead of needing a schema change.
        return "SUPER"
    return VARCHAR_MAX


class RedshiftDestinationWriter(PostgresDestinationWriter):
    holds_sync_lock: ClassVar[bool] = False
    runs_post_load: ClassVar[bool] = False

    def _column_type(self, arrow_type: pa.DataType) -> str:
        return redshift_type_for(arrow_type)

    def _ensure_unique_index(self, conn: psycopg.Connection, target: str, primary_keys: list[str]) -> None:
        # Redshift has no unique indexes; uniqueness is enforced by the delete-then-insert below.
        return None

    def _merge_rows(
        self,
        conn: psycopg.Connection,
        target: str,
        column_names: list[str],
        rows: list[dict],
        *,
        primary_keys: list[str],
    ) -> None:
        """Upsert by deleting the incoming keys and inserting the batch, in one transaction.

        Both halves run together so a reader never sees the deleted rows missing, and so a
        re-applied batch converges on the same result.
        """
        with conn.transaction():
            key_tuples = [tuple(row.get(key) for key in primary_keys) for row in rows]
            delete = sql.SQL("DELETE FROM {}.{} WHERE ({}) IN ({})").format(
                sql.Identifier(self._schema),
                sql.Identifier(target),
                sql.SQL(", ").join(sql.Identifier(key) for key in primary_keys),
                sql.SQL(", ").join(
                    sql.SQL("({})").format(sql.SQL(", ").join(sql.Placeholder() for _ in primary_keys))
                    for _ in key_tuples
                ),
            )
            conn.execute(delete, [value for key_tuple in key_tuples for value in key_tuple])

            insert = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                sql.Identifier(self._schema),
                sql.Identifier(target),
                sql.SQL(", ").join(sql.Identifier(c) for c in column_names),
                sql.SQL(", ").join(sql.Placeholder() for _ in column_names),
            )
            with conn.cursor() as cursor:
                cursor.executemany(insert, [[_encode(row.get(name)) for name in column_names] for row in rows])

    def _copy_rows(
        self,
        conn: psycopg.Connection,
        target: str,
        column_names: list[str],
        rows: list[dict],
        *,
        batch_index: int | None,
    ) -> None:
        """Insert rather than COPY FROM STDIN, which Redshift does not support."""
        columns = [*column_names] + ([self._batch_index_column] if batch_index is not None else [])
        insert = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(self._schema),
            sql.Identifier(target),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )
        payload = []
        for row in rows:
            values = [_encode(row.get(name)) for name in column_names]
            if batch_index is not None:
                values.append(batch_index)
            payload.append(values)
        with conn.cursor() as cursor:
            cursor.executemany(insert, payload)


def _encode(value):
    """Send nested values as JSON text, which Redshift parses into SUPER."""
    if isinstance(value, dict | list):
        return json.dumps(value)
    return value
