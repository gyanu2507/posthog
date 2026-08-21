"""Writing a run's destination work items alongside its batches.

The queue database cannot read the app database, so a run's resolved destination set is
snapshotted here when it enqueues its first batch, and every batch then gets one claimable
work item per destination. Both writes ride the batch insert's transaction: a batch that
exists with no work items would be delivered nowhere and never retried.
"""

from __future__ import annotations

import json
from typing import Any

import psycopg

from posthog.dataclasses import frozen
from posthog.models.utils import uuid7

RUN_DESTINATION_TABLE = "sourcerundestination"
BATCH_DESTINATION_TABLE = "sourcebatchdestination"


@frozen
class RunDestination:
    """One destination a run delivers to, as the queue needs to see it."""

    destination_job_id: str
    destination_id: str
    destination_type: str
    config: dict[str, Any]


def insert_run_destinations(
    conn: psycopg.Connection,
    *,
    team_id: int,
    schema_id: str,
    source_id: str,
    job_id: str,
    run_uuid: str,
    destinations: list[RunDestination],
) -> None:
    """Snapshot the run's destination set. Idempotent, so a retried first batch is harmless."""
    if not destinations:
        return

    for destination in destinations:
        conn.execute(
            f"""
            INSERT INTO {RUN_DESTINATION_TABLE} (
                id, team_id, schema_id, source_id, job_id, run_uuid,
                destination_job_id, destination_id, destination_type, config_snapshot, created_at
            ) VALUES (
                %(id)s, %(team_id)s, %(schema_id)s, %(source_id)s, %(job_id)s, %(run_uuid)s,
                %(destination_job_id)s, %(destination_id)s, %(destination_type)s, %(config_snapshot)s, now()
            )
            ON CONFLICT (run_uuid, destination_job_id) DO NOTHING
            """,
            {
                # These tables are written by raw SQL, and Django's UUID default is applied in
                # Python, so the id has to be supplied here rather than by the column.
                "id": str(uuid7()),
                "team_id": team_id,
                "schema_id": schema_id,
                "source_id": source_id,
                "job_id": job_id,
                "run_uuid": run_uuid,
                "destination_job_id": destination.destination_job_id,
                "destination_id": destination.destination_id,
                "destination_type": destination.destination_type,
                "config_snapshot": json.dumps(destination.config or {}),
            },
        )


def insert_batch_destinations(
    conn: psycopg.Connection,
    *,
    batch_id: str,
    team_id: int,
    schema_id: str,
    run_uuid: str,
    batch_index: int,
    is_final_batch: bool,
    sync_type: str,
    destinations: list[RunDestination],
) -> None:
    """Create one claimable work item per destination for a batch.

    `batch_index`, `is_final_batch` and `sync_type` are copied off the batch so the claim's
    ordering and run gates never join back to the partitioned batch table.
    """
    if not destinations:
        return

    for destination in destinations:
        conn.execute(
            f"""
            INSERT INTO {BATCH_DESTINATION_TABLE} (
                id, team_id, batch_id, schema_id, run_uuid, destination_job_id, destination_type,
                batch_index, is_final_batch, sync_type, latest_state, latest_attempt, created_at
            ) VALUES (
                %(id)s, %(team_id)s, %(batch_id)s, %(schema_id)s, %(run_uuid)s, %(destination_job_id)s,
                %(destination_type)s, %(batch_index)s, %(is_final_batch)s, %(sync_type)s,
                'pending', 0, now()
            )
            """,
            {
                "id": str(uuid7()),
                "team_id": team_id,
                "batch_id": batch_id,
                "schema_id": schema_id,
                "run_uuid": run_uuid,
                "destination_job_id": destination.destination_job_id,
                "destination_type": destination.destination_type,
                "batch_index": batch_index,
                "is_final_batch": is_final_batch,
                "sync_type": sync_type,
            },
        )
