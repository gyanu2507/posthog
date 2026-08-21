"""Claiming and settling destination work items.

The delta path's queries in ``postgres_queue/jobs_db.py`` are the model for these: candidates
come off denormalized state columns behind partial indexes rather than a per-row lookup into
the append-only status log, because that lookup melted down under failure storms. State moves
through the same dual-write shape, so ``sourcebatchdestinationstatus`` stays the source of
truth while readers stay cheap.

Every gate here is scoped to one destination. A destination that fails or falls behind holds
up only its own deliveries; the warehouse and its peers keep draining.
"""

from __future__ import annotations

from typing import Any

import psycopg

from posthog.dataclasses import frozen
from posthog.models.utils import uuid7

BATCH_TABLE = "sourcebatch"
WORK_ITEM_TABLE = "sourcebatchdestination"
STATUS_TABLE = "sourcebatchdestinationstatus"
APPLY_TABLE = "sourcebatchdestinationapply"
LEASE_TABLE = "sourcedestinationgrouplease"
RUN_DESTINATION_TABLE = "sourcerundestination"

# Matches the delta path: a work item older than this may have lost its staged parquet to
# retention, so claiming it would read a prefix that no longer exists.
CLAIM_ELIGIBILITY_INTERVAL = "6 days 12 hours"

# States that mean the item is still owed to its destination.
NON_TERMINAL_STATES = ("pending", "waiting", "executing", "waiting_retry")


@frozen
class ClaimedWorkItem:
    """One batch to deliver to one destination, with everything the writer needs."""

    work_item_id: str
    batch_id: str
    team_id: int
    schema_id: str
    run_uuid: str
    destination_job_id: str
    destination_type: str
    destination_id: str
    config_snapshot: dict[str, Any]
    batch_index: int
    is_final_batch: bool
    sync_type: str
    s3_path: str
    row_count: int
    attempt: int
    metadata: dict[str, Any]

    @property
    def group_key(self) -> tuple[int, str, str]:
        return (self.team_id, self.schema_id, self.destination_job_id)


def _claim_sql(destination_type_filter: str) -> str:
    return f"""
    WITH candidates AS MATERIALIZED (
        SELECT
            w.id AS work_item_id, w.batch_id, w.team_id, w.schema_id, w.run_uuid,
            w.destination_job_id, w.destination_type, w.batch_index, w.is_final_batch,
            w.sync_type, w.latest_attempt, w.created_at,
            b.s3_path, b.row_count, b.metadata,
            rd.destination_id, rd.config_snapshot
        FROM {WORK_ITEM_TABLE} w
        JOIN {BATCH_TABLE} b ON b.id = w.batch_id
        JOIN {RUN_DESTINATION_TABLE} rd
            ON rd.run_uuid = w.run_uuid AND rd.destination_job_id = w.destination_job_id
        WHERE w.created_at > now() - interval '{CLAIM_ELIGIBILITY_INTERVAL}'
          {destination_type_filter}
          -- CDC ticks a final batch continuously, which has no run-scoped commit for a
          -- destination to swap into. Out of scope until destinations model that.
          AND w.sync_type <> 'cdc'
          -- Own state only. A warehouse failure must not stop Redshift, and vice versa.
          AND (
                w.latest_state = 'pending'
                OR (
                    w.latest_state = 'waiting_retry'
                    AND w.state_changed_at <= now() - make_interval(
                        secs => %(backoff)s * GREATEST(w.latest_attempt, 1)
                    )
                )
          )
          -- Head of line for this destination: an earlier batch of the run still in flight.
          AND NOT EXISTS (
                SELECT 1 FROM {WORK_ITEM_TABLE} earlier
                WHERE earlier.run_uuid = w.run_uuid
                  AND earlier.destination_job_id = w.destination_job_id
                  AND earlier.batch_index < w.batch_index
                  AND earlier.latest_state IN ('executing', 'waiting_retry', 'pending', 'waiting')
          )
          -- 'failed' is absorbing per destination: once a run fails for one destination,
          -- its remaining batches stop rather than landing a partial run.
          AND NOT EXISTS (
                SELECT 1 FROM {WORK_ITEM_TABLE} dead
                WHERE dead.run_uuid = w.run_uuid
                  AND dead.destination_job_id = w.destination_job_id
                  AND dead.latest_state = 'failed'
          )
          -- One executing batch per (schema, destination).
          AND NOT EXISTS (
                SELECT 1 FROM {WORK_ITEM_TABLE} busy
                WHERE busy.team_id = w.team_id
                  AND busy.schema_id = w.schema_id
                  AND busy.destination_job_id = w.destination_job_id
                  AND busy.latest_state = 'executing'
          )
          -- Cross-run serialization. The sync lock does not cover external drains, so
          -- without this a newer run's full-refresh swap could land while an older run's
          -- stragglers are still applying on top of it.
          AND NOT EXISTS (
                SELECT 1 FROM {WORK_ITEM_TABLE} older
                WHERE older.team_id = w.team_id
                  AND older.schema_id = w.schema_id
                  AND older.destination_job_id = w.destination_job_id
                  AND older.run_uuid <> w.run_uuid
                  AND older.created_at < w.created_at
                  AND older.latest_state IN ('pending', 'waiting', 'executing', 'waiting_retry')
          )
          -- Groups another pod holds a live lease on are not ours to take.
          AND NOT EXISTS (
                SELECT 1 FROM {LEASE_TABLE} l
                WHERE l.team_id = w.team_id
                  AND l.schema_id = w.schema_id
                  AND l.destination_job_id = w.destination_job_id
                  AND l.expires_at > now()
                  AND l.owner_token <> %(owner)s
          )
        ORDER BY
            row_number() OVER (PARTITION BY w.team_id ORDER BY w.created_at, w.batch_index),
            w.created_at,
            w.batch_index
        LIMIT %(limit)s
    ),
    claimed AS (
        INSERT INTO {LEASE_TABLE} (
            team_id, schema_id, destination_job_id, owner_token, expires_at, acquired_at, updated_at
        )
        SELECT DISTINCT team_id, schema_id, destination_job_id, %(owner)s,
               now() + make_interval(secs => %(ttl)s), now(), now()
        FROM candidates
        ON CONFLICT (team_id, schema_id, destination_job_id) DO UPDATE
            SET owner_token = excluded.owner_token,
                expires_at = excluded.expires_at,
                acquired_at = CASE
                    WHEN {LEASE_TABLE}.owner_token = excluded.owner_token THEN {LEASE_TABLE}.acquired_at
                    ELSE now()
                END,
                updated_at = now()
            WHERE {LEASE_TABLE}.expires_at < now()
               OR {LEASE_TABLE}.owner_token = excluded.owner_token
        RETURNING team_id, schema_id, destination_job_id
    )
    SELECT c.*
    FROM candidates c
    JOIN claimed USING (team_id, schema_id, destination_job_id)
    """


class DestinationQueue:
    """Queries the destination consumers run against the queue database."""

    @staticmethod
    def claim(
        conn: psycopg.Connection,
        *,
        owner_token: str,
        limit: int,
        lease_ttl_seconds: int,
        retry_backoff_seconds: int,
        destination_types: list[str] | None = None,
        exclude_destination_types: list[str] | None = None,
    ) -> list[ClaimedWorkItem]:
        """Claim work items, leasing each (team, schema, destination) group they belong to.

        `destination_types` / `exclude_destination_types` partition the fleet: the warehouse
        deployment claims its own type and the external deployment claims the rest, off the
        same tables with no second code path.
        """
        params: dict[str, Any] = {
            "owner": owner_token,
            "limit": limit,
            "ttl": lease_ttl_seconds,
            "backoff": retry_backoff_seconds,
        }
        if destination_types:
            type_filter = "AND w.destination_type = ANY(%(types)s)"
            params["types"] = destination_types
        elif exclude_destination_types:
            type_filter = "AND NOT (w.destination_type = ANY(%(excluded_types)s))"
            params["excluded_types"] = exclude_destination_types
        else:
            type_filter = ""

        rows = conn.execute(_claim_sql(type_filter), params).fetchall()
        return [
            ClaimedWorkItem(
                work_item_id=str(row[0]),
                batch_id=str(row[1]),
                team_id=row[2],
                schema_id=row[3],
                run_uuid=row[4],
                destination_job_id=row[5],
                destination_type=row[6],
                batch_index=row[7],
                is_final_batch=row[8],
                sync_type=row[9],
                attempt=row[10],
                s3_path=row[12],
                row_count=row[13],
                metadata=row[14] or {},
                destination_id=str(row[15]),
                config_snapshot=row[16] or {},
            )
            for row in rows
        ]

    @staticmethod
    def set_state(
        conn: psycopg.Connection,
        *,
        work_item_id: str,
        state: str,
        attempt: int,
        error: dict[str, Any] | None = None,
    ) -> None:
        """Append a status row and mirror it onto the work item, in one statement.

        The append-only log stays the source of truth; the denormalized columns are what the
        claim reads. Writing them apart would let a crash leave the two disagreeing.
        """
        conn.execute(
            f"""
            WITH logged AS (
                INSERT INTO {STATUS_TABLE} (id, work_item_id, job_state, attempt, error_response, created_at)
                VALUES (%(status_id)s, %(work_item_id)s, %(state)s, %(attempt)s, %(error)s, now())
                RETURNING work_item_id
            )
            UPDATE {WORK_ITEM_TABLE}
               SET latest_state = %(state)s,
                   latest_attempt = %(attempt)s,
                   state_changed_at = now()
             WHERE id = (SELECT work_item_id FROM logged)
            """,
            {
                "status_id": str(uuid7()),
                "work_item_id": work_item_id,
                "state": state,
                "attempt": attempt,
                "error": psycopg.types.json.Json(error) if error is not None else None,
            },
        )

    @staticmethod
    def fail_run(
        conn: psycopg.Connection,
        *,
        run_uuid: str,
        destination_job_id: str,
        error: dict[str, Any],
    ) -> int:
        """Fail every non-terminal item of a run for one destination.

        Scoped to this destination on purpose: it never touches the delta path's statuses, the
        parent job, or the sync lock. A destination failing is not the run's other destinations
        failing.
        """
        rows = conn.execute(
            f"""
            WITH doomed AS (
                SELECT id FROM {WORK_ITEM_TABLE}
                WHERE run_uuid = %(run_uuid)s
                  AND destination_job_id = %(destination_job_id)s
                  AND latest_state IN {NON_TERMINAL_STATES}
                FOR UPDATE
            ),
            logged AS (
                INSERT INTO {STATUS_TABLE} (id, work_item_id, job_state, attempt, error_response, created_at)
                SELECT gen_random_uuid(), id, 'failed', 0, %(error)s, now() FROM doomed
                RETURNING work_item_id
            )
            UPDATE {WORK_ITEM_TABLE}
               SET latest_state = 'failed', state_changed_at = now()
             WHERE id IN (SELECT work_item_id FROM logged)
            """,
            {
                "run_uuid": run_uuid,
                "destination_job_id": destination_job_id,
                "error": psycopg.types.json.Json(error),
            },
        ).rowcount
        return rows

    @staticmethod
    def mark_applied(
        conn: psycopg.Connection,
        *,
        team_id: int,
        schema_id: str,
        run_uuid: str,
        batch_index: int,
        destination_job_id: str,
        row_count: int,
    ) -> None:
        """Record that this batch reached this destination.

        The unique row is what makes a re-claimed batch safe to skip after a crash between the
        destination's own commit and the state write.
        """
        conn.execute(
            f"""
            INSERT INTO {APPLY_TABLE} (
                id, team_id, schema_id, run_uuid, batch_index, destination_job_id, row_count, created_at
            ) VALUES (
                %(id)s, %(team_id)s, %(schema_id)s, %(run_uuid)s, %(batch_index)s,
                %(destination_job_id)s, %(row_count)s, now()
            )
            ON CONFLICT (team_id, schema_id, run_uuid, batch_index, destination_job_id) DO NOTHING
            """,
            {
                "id": str(uuid7()),
                "team_id": team_id,
                "schema_id": schema_id,
                "run_uuid": run_uuid,
                "batch_index": batch_index,
                "destination_job_id": destination_job_id,
                "row_count": row_count,
            },
        )

    @staticmethod
    def has_been_applied(
        conn: psycopg.Connection,
        *,
        team_id: int,
        schema_id: str,
        run_uuid: str,
        batch_index: int,
        destination_job_id: str,
    ) -> bool:
        row = conn.execute(
            f"""
            SELECT 1 FROM {APPLY_TABLE}
            WHERE team_id = %(team_id)s AND schema_id = %(schema_id)s AND run_uuid = %(run_uuid)s
              AND batch_index = %(batch_index)s AND destination_job_id = %(destination_job_id)s
            """,
            {
                "team_id": team_id,
                "schema_id": schema_id,
                "run_uuid": run_uuid,
                "batch_index": batch_index,
                "destination_job_id": destination_job_id,
            },
        ).fetchone()
        return row is not None

    @staticmethod
    def release_lease(conn: psycopg.Connection, *, group: tuple[int, str, str], owner_token: str) -> None:
        team_id, schema_id, destination_job_id = group
        conn.execute(
            f"""
            DELETE FROM {LEASE_TABLE}
            WHERE team_id = %(team_id)s AND schema_id = %(schema_id)s
              AND destination_job_id = %(destination_job_id)s AND owner_token = %(owner)s
            """,
            {
                "team_id": team_id,
                "schema_id": schema_id,
                "destination_job_id": destination_job_id,
                "owner": owner_token,
            },
        )

    @staticmethod
    def renew_lease(
        conn: psycopg.Connection, *, group: tuple[int, str, str], owner_token: str, ttl_seconds: int
    ) -> bool:
        """Extend our lease. False means we no longer hold it, so the caller must stop writing."""
        team_id, schema_id, destination_job_id = group
        rows = conn.execute(
            f"""
            UPDATE {LEASE_TABLE}
               SET expires_at = now() + make_interval(secs => %(ttl)s), updated_at = now()
             WHERE team_id = %(team_id)s AND schema_id = %(schema_id)s
               AND destination_job_id = %(destination_job_id)s AND owner_token = %(owner)s
            """,
            {
                "team_id": team_id,
                "schema_id": schema_id,
                "destination_job_id": destination_job_id,
                "owner": owner_token,
                "ttl": ttl_seconds,
            },
        ).rowcount
        return rows > 0

    @staticmethod
    def oldest_eligible_age_seconds(conn: psycopg.Connection) -> float | None:
        """Age of the oldest claimable work item, for the backlog alert.

        Retention drops staged parquet at seven days, so a destination that lets this climb
        loses those batches permanently. Alert well before the cliff.
        """
        row = conn.execute(
            f"""
            SELECT EXTRACT(EPOCH FROM (now() - MIN(created_at)))
            FROM {WORK_ITEM_TABLE}
            WHERE latest_state IN ('pending', 'waiting_retry')
            """
        ).fetchone()
        return float(row[0]) if row and row[0] is not None else None
