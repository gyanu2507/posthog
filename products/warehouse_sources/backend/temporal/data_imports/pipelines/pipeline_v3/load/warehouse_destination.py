"""The warehouse's side of being one destination among several.

The Delta loader still owns how a batch is written; what changes when a run fans out is who
its completion belongs to. Instead of writing the run's terminal status directly, the loader
finalizes the warehouse's own child job and lets the last destination to finish close the run.

Runs with no children keep the original path exactly, so nothing changes for a team the
feature is off for.
"""

from __future__ import annotations

import structlog

from products.warehouse_sources.backend.models.external_data_destination import (
    ExternalDataDestination,
    ExternalDataDestinationJob,
)
from products.warehouse_sources.backend.models.external_data_job import ExternalDataJob
from products.warehouse_sources.backend.temporal.data_imports.destination_finalization import (
    finalize_destination_job_and_maybe_close_parent,
)

logger = structlog.get_logger(__name__)


def warehouse_child_for_job(job_id: str, team_id: int) -> ExternalDataDestinationJob | None:
    """The run's PostHog warehouse child, or None when the run does not fan out.

    None is the signal to keep the original single-owner behavior: the loader writes the run's
    status itself, releases the sync lock, and starts post-import, exactly as before.

    Both call sites refresh stale connections before calling in, so this does not do it again:
    dropping the connection here would cut the transaction the caller is working inside.
    """
    try:
        return (
            ExternalDataDestinationJob.objects.for_team(team_id, canonical=True)
            .filter(job_id=job_id, destination_type=ExternalDataDestination.Type.POSTHOG_WAREHOUSE)
            .first()
        )
    except Exception as e:
        # Never let destination bookkeeping decide whether a warehouse load can finish. Falling
        # back to the original path completes the run; the worst case is that a sibling
        # destination's child is closed by a sweep instead of by this loader.
        logger.warning("warehouse_destination_child_lookup_failed", job_id=job_id, error=str(e))
        return None


def complete_warehouse_child(
    child: ExternalDataDestinationJob,
    *,
    team_id: int,
    run_uuid: str,
    rows_synced: int | None = None,
) -> bool:
    """Record that the warehouse took the run, and close the run if it was the last destination.

    Returns whether this call closed the run, which is what the loader keys its post-import
    trigger off: those steps describe a warehouse table, so they run when the warehouse lands
    rather than when the slowest destination does.
    """
    parent = finalize_destination_job_and_maybe_close_parent(
        destination_job_id=str(child.id),
        team_id=team_id,
        status=ExternalDataJob.Status.COMPLETED,
        logger=logger,
        rows_synced=rows_synced,
        run_uuid=run_uuid,
    )
    logger.info(
        "warehouse_destination_completed",
        destination_job_id=str(child.id),
        closed_run=parent is not None,
    )
    return parent is not None


def fail_warehouse_child(
    child: ExternalDataDestinationJob,
    *,
    team_id: int,
    run_uuid: str,
    error: str,
) -> None:
    """Record that the warehouse could not take the run.

    The run's other destinations are untouched: one that already delivered stays completed and
    is billed, and one still draining keeps going.
    """
    finalize_destination_job_and_maybe_close_parent(
        destination_job_id=str(child.id),
        team_id=team_id,
        status=ExternalDataJob.Status.FAILED,
        logger=logger,
        latest_error=error,
        run_uuid=run_uuid,
    )
