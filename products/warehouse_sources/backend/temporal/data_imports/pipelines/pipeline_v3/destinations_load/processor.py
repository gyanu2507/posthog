"""Applying one claimed batch to one destination.

The consumer engine hands over a claimed work item; everything destination-specific happens
behind the `DestinationWriter` it resolves, so this file never names a destination type.

Ordering matters on the final batch. The writer commits the run first, then the child job is
finalized, then the apply marker is written. A crash between any two of those leaves the batch
re-claimable, and each step is idempotent, so replaying is safe. Writing the marker first would
make a crash look like a completed delivery.
"""

from __future__ import annotations

from django.db import close_old_connections
from django.db.models import F

import psycopg
import structlog
from asgiref.sync import sync_to_async

from products.warehouse_sources.backend.models.external_data_destination import (
    TERMINAL_DESTINATION_JOB_STATUSES,
    ExternalDataDestinationJob,
)
from products.warehouse_sources.backend.models.external_data_job import ExternalDataJob
from products.warehouse_sources.backend.temporal.data_imports.destination_finalization import (
    finalize_destination_job_and_maybe_close_parent,
)
from products.warehouse_sources.backend.temporal.data_imports.destinations.contracts import (
    DestinationBatchContext,
    DestinationRunContext,
)
from products.warehouse_sources.backend.temporal.data_imports.destinations.registry import resolve_destination_writer
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.destinations_load.parquet_source import (
    aiter_record_batches,
)
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.destinations_queue.jobs_db import (
    ClaimedWorkItem,
    DestinationQueue,
)

logger = structlog.get_logger(__name__)


class DestinationJobGoneError(Exception):
    """The child job is terminal, so this batch must not be delivered."""


def build_run_context(item: ClaimedWorkItem, child: ExternalDataDestinationJob) -> DestinationRunContext:
    config = item.config_snapshot or {}
    return DestinationRunContext(
        team_id=item.team_id,
        schema_id=item.schema_id,
        source_id=str(child.job.pipeline_id),
        job_id=str(child.job_id),
        destination_job_id=item.destination_job_id,
        run_uuid=item.run_uuid,
        destination_id=item.destination_id,
        destination_type=item.destination_type,
        destination_name=child.destination_name,
        table_name=config.get("table_name") or (item.metadata or {}).get("resource_name") or item.schema_id,
        sync_type=item.sync_type,
        primary_keys=tuple((item.metadata or {}).get("primary_keys") or ()),
        config=config,
        integration_id=child.destination.integration_id if child.destination else None,
    )


def _load_child(item: ClaimedWorkItem) -> ExternalDataDestinationJob:
    return (
        ExternalDataDestinationJob.objects.for_team(item.team_id, canonical=True)
        .select_related("job", "destination")
        .get(id=item.destination_job_id)
    )


async def process_work_item(item: ClaimedWorkItem, queue_conn: psycopg.Connection) -> int:
    """Deliver one staged batch to one destination. Returns the rows written.

    Raises `DestinationJobGoneError` when the child is already terminal, which is how a
    cancelled run or a swept one stops its still-claimable batches.
    """
    close_old_connections()

    child = await _aload_child(item)
    if child.status in TERMINAL_DESTINATION_JOB_STATUSES:
        raise DestinationJobGoneError(
            f"Destination job {item.destination_job_id} is {child.status}; dropping batch {item.batch_index}"
        )

    run_ctx = build_run_context(item, child)
    batch_ctx = DestinationBatchContext(
        run=run_ctx,
        batch_index=item.batch_index,
        is_final_batch=item.is_final_batch,
        expected_row_count=item.row_count,
    )

    already_applied = DestinationQueue.has_been_applied(
        queue_conn,
        team_id=item.team_id,
        schema_id=item.schema_id,
        run_uuid=item.run_uuid,
        batch_index=item.batch_index,
        destination_job_id=item.destination_job_id,
    )

    writer = resolve_destination_writer(run_ctx)
    rows_written = 0

    if not already_applied:
        await writer.prepare_run(run_ctx)
        outcome = await writer.write_batch(aiter_record_batches(item.s3_path), batch_ctx)
        rows_written = outcome.rows_written

    if item.is_final_batch:
        # Commit the run at the destination before anything records it as delivered.
        await writer.finalize_run(run_ctx)
        await _afinalize_child(item, rows_written)

    DestinationQueue.mark_applied(
        queue_conn,
        team_id=item.team_id,
        schema_id=item.schema_id,
        run_uuid=item.run_uuid,
        batch_index=item.batch_index,
        destination_job_id=item.destination_job_id,
        row_count=rows_written,
    )

    logger.debug(
        "destination_batch_applied",
        destination_type=item.destination_type,
        batch_index=item.batch_index,
        rows_written=rows_written,
        skipped=already_applied,
    )
    return rows_written


async def abort_run_for_item(item: ClaimedWorkItem) -> None:
    """Let a writer drop whatever a run that will not finish left staged."""
    try:
        child = await _aload_child(item)
        writer = resolve_destination_writer(build_run_context(item, child))
        await writer.abort_run(build_run_context(item, child))
    except Exception as e:
        # Cleanup is best effort: the next run stages under its own run id, so a leftover
        # staging table costs storage rather than correctness.
        logger.warning("destination_abort_failed", destination_job_id=item.destination_job_id, error=str(e))


def _increment_child_rows(destination_job_id: str, team_id: int, rows: int) -> None:
    if rows <= 0:
        return
    ExternalDataDestinationJob.objects.for_team(team_id, canonical=True).filter(id=destination_job_id).update(
        rows_synced=F("rows_synced") + rows
    )


def _finalize_child(item: ClaimedWorkItem, rows_written: int) -> None:
    _increment_child_rows(item.destination_job_id, item.team_id, rows_written)
    finalize_destination_job_and_maybe_close_parent(
        destination_job_id=item.destination_job_id,
        team_id=item.team_id,
        status=ExternalDataJob.Status.COMPLETED,
        logger=logger,
        run_uuid=item.run_uuid,
    )


def fail_destination_job(item: ClaimedWorkItem, error: str) -> None:
    """Record that this destination could not take the run."""
    close_old_connections()
    finalize_destination_job_and_maybe_close_parent(
        destination_job_id=item.destination_job_id,
        team_id=item.team_id,
        status=ExternalDataJob.Status.FAILED,
        logger=logger,
        latest_error=error,
        run_uuid=item.run_uuid,
    )


# The writers are async because destinations are network-bound; the ORM calls around them are
# not, so they run off the event loop.
_aload_child = sync_to_async(_load_child, thread_sensitive=False)
_afinalize_child = sync_to_async(_finalize_child, thread_sensitive=False)
