"""Who closes a run when its destinations finish at different times.

Before destinations, exactly one party wrote a run's terminal status — the load consumer
when it applied the final batch, the workflow otherwise (`PipelineResult`'s
`consumer_manages_job_status` contract). With N destinations there are N finishers, so the
rule becomes: each finisher closes only its own child, and whichever child lands last closes
the parent by aggregating them.

The parent stays `Running` until every child is terminal. That is load-bearing: a terminal
FAILED parent makes `update_external_job_status` sweep the run's still-claimable queue
batches, which would cut off a sibling destination that is still mid-run. Paths that *do*
want that sweep — extraction failed, the user cancelled, a lock takeover — fail the parent
directly and cascade to the children, which is what `cascade_destination_jobs` is for.
"""

from __future__ import annotations

import datetime as dt

from django.db import transaction

from structlog.types import FilteringBoundLogger

from products.data_warehouse.backend.facade.api import update_external_job_status
from products.warehouse_sources.backend.models.external_data_destination import (
    TERMINAL_DESTINATION_JOB_STATUSES,
    ExternalDataDestinationJob,
)
from products.warehouse_sources.backend.models.external_data_job import ExternalDataJob
from products.warehouse_sources.backend.models.external_data_schema import ExternalDataSchema
from products.warehouse_sources.backend.temporal.data_imports.metrics import (
    LOCK_TAKEOVER_LATEST_ERROR,
    TERMINAL_JOB_STATUSES,
)


def _is_child_takeover_recovery(child: ExternalDataDestinationJob, requested: str) -> bool:
    """Mirror of the parent's unseal rule: only the takeover sentinel reopens Failed -> Completed."""
    return (
        child.status == ExternalDataDestinationJob.Status.FAILED
        and requested == ExternalDataDestinationJob.Status.COMPLETED
        and child.latest_error == LOCK_TAKEOVER_LATEST_ERROR
    )


def _write_child_status(
    child: ExternalDataDestinationJob,
    status: str,
    latest_error: str | None,
    rows_synced: int | None = None,
) -> None:
    if child.status in TERMINAL_DESTINATION_JOB_STATUSES and not _is_child_takeover_recovery(child, status):
        # Terminal is absorbing, and the first recorded failure reason is the actionable one.
        return

    child.status = status
    child.latest_error = latest_error
    update_fields = ["status", "latest_error", "updated_at"]

    if rows_synced is not None:
        child.rows_synced = rows_synced
        update_fields.append("rows_synced")

    if status in TERMINAL_DESTINATION_JOB_STATUSES:
        child.finished_at = dt.datetime.now(dt.UTC)
        update_fields.append("finished_at")

    # Scoped save so a concurrent F-update to rows_synced from the writer is not clobbered.
    child.save(update_fields=update_fields)


def finalize_destination_job_and_maybe_close_parent(
    destination_job_id: str,
    team_id: int,
    status: str,
    logger: FilteringBoundLogger,
    latest_error: str | None = None,
    rows_synced: int | None = None,
) -> ExternalDataJob | None:
    """Record one destination's outcome, then close the parent run if it was the last one.

    Safe to call from any consumer, sweep, or management command, and safe to call twice for
    the same child. Returns the parent when this call closed it, else None.

    Callers running outside a request (the consumers, sweeps, management commands) own
    connection hygiene and must have called `close_old_connections()` for this thread.
    """
    with transaction.atomic():
        # Lock order is parent first, then child, for every caller. Two consumers finishing
        # different children of one run therefore serialize on the parent row instead of
        # interleaving their reads of the sibling set.
        child_probe = ExternalDataDestinationJob.objects.for_team(team_id, canonical=True).get(id=destination_job_id)
        parent = ExternalDataJob.objects.select_for_update().get(id=child_probe.job_id, team_id=team_id)
        child = (
            ExternalDataDestinationJob.objects.for_team(team_id, canonical=True)
            .select_for_update()
            .get(id=destination_job_id)
        )

        _write_child_status(child, status, latest_error, rows_synced)

        parent_takeover_window = (
            parent.status == ExternalDataJob.Status.FAILED and parent.latest_error == LOCK_TAKEOVER_LATEST_ERROR
        )
        if parent.status in TERMINAL_JOB_STATUSES and not parent_takeover_window:
            # The workflow, the cancel endpoint, or a takeover already owned this run. The child
            # status is still recorded above so the run's history shows what each destination did.
            return None

        children = list(ExternalDataDestinationJob.objects.for_team(team_id, canonical=True).filter(job_id=parent.id))
        if any(c.status not in TERMINAL_DESTINATION_JOB_STATUSES for c in children):
            return None

        failed = [c for c in children if c.status != ExternalDataDestinationJob.Status.COMPLETED]
        if failed:
            first = failed[0]
            update_external_job_status(
                job_id=str(parent.id),
                team_id=team_id,
                status=ExternalDataJob.Status.FAILED,
                logger=logger,
                latest_error=f"{first.destination_name}: {first.latest_error or 'failed'}",
            )
            logger.info(
                "dwh_run_closed_with_failed_destinations",
                job_id=str(parent.id),
                failed_destinations=[c.destination_name for c in failed],
            )
            # The sweep update_external_job_status fires here is a no-op by construction: every
            # child is terminal, so no sibling is still owed batches.
            return parent

        model = update_external_job_status(
            job_id=str(parent.id),
            team_id=team_id,
            status=ExternalDataJob.Status.COMPLETED,
            logger=logger,
            latest_error=None,
        )
        if model.status == ExternalDataJob.Status.COMPLETED:
            # Promotion commits with the parent's COMPLETED write, so a failure here rolls the
            # completion back and the batch retries — the guarantee the warehouse-only path had.
            _promote_cursor_for_run(parent, team_id, logger)
            _stamp_parent_rows_if_unset(parent, children)

        return parent


def _promote_cursor_for_run(parent: ExternalDataJob, team_id: int, logger: FilteringBoundLogger) -> None:
    """Advance the incremental cursor, but only now that every destination has the window.

    Holding it until the whole run succeeds is what stops a destination developing a permanent
    gap: a run repeated because one destination failed re-extracts the same window, and the
    destinations that already applied it merge idempotently.
    """
    if parent.schema_id is None:
        return

    run_uuid = _run_uuid_for(parent)
    if run_uuid is None:
        return

    schema = ExternalDataSchema.objects.get(id=parent.schema_id, team_id=team_id)
    if schema.promote_staged_incremental_values(run_uuid):
        logger.info("staged_cursor_promoted", run_uuid=run_uuid, external_data_schema_id=str(parent.schema_id))


def _stamp_parent_rows_if_unset(parent: ExternalDataJob, children: list[ExternalDataDestinationJob]) -> None:
    """Give warehouse-less runs a parent row count.

    The warehouse writer increments the parent as it goes, so runs that include it already have
    one. Runs without it would otherwise report null rows in the UI, so take the largest child
    count — destinations all receive the same rows, and a partial child would understate it.
    """
    if parent.rows_synced:
        return

    counts = [c.rows_synced or 0 for c in children]
    if not counts:
        return

    parent.rows_synced = max(counts)
    parent.save(update_fields=["rows_synced", "updated_at"])


def _run_uuid_for(parent: ExternalDataJob) -> str | None:
    # The extractor stamps the run uuid on the job's schema snapshot; the workflow run id is the
    # fallback for runs recorded before that.
    snapshot = parent.schema_snapshot or {}
    return snapshot.get("run_uuid") or parent.workflow_run_id


def cascade_destination_jobs(
    job_id: str | None,
    team_id: int,
    status: str,
    latest_error: str | None = None,
) -> int:
    """Force every non-terminal child of a run to `status`.

    For the paths that own the whole run rather than one destination: extraction failed, the
    billing limit stopped it, the user cancelled, a lock takeover stole it. Those write the
    parent directly and use this to make the children agree, so a later consumer finishing a
    straggler batch finds its child already terminal and stops.

    Returns how many children were changed.
    """
    if job_id is None:
        return 0

    changed = 0
    with transaction.atomic():
        children = (
            ExternalDataDestinationJob.objects.for_team(team_id, canonical=True)
            .select_for_update()
            .filter(job_id=job_id)
            .exclude(status__in=TERMINAL_DESTINATION_JOB_STATUSES)
        )
        for child in children:
            _write_child_status(child, status, latest_error)
            changed += 1

    return changed
