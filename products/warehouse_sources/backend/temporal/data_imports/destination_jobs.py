"""Creating a run's per-destination child jobs, and deciding which of them bill.

A run fans out to whatever `resolve_destinations` returns for its schema, one child job each.
Children are created up front, before extraction, so every later party — the consumers, the
sweeps, the cancel endpoint — has a row to write against even if extraction dies immediately.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django.db import transaction

import posthoganalytics

from posthog.exceptions_capture import capture_exception
from posthog.models.team.team import Team

from products.warehouse_sources.backend.models.external_data_destination import (
    ExternalDataDestination,
    ExternalDataDestinationJob,
    resolve_destinations,
)
from products.warehouse_sources.backend.models.external_data_job import ExternalDataJob

if TYPE_CHECKING:
    from products.warehouse_sources.backend.models.external_data_schema import ExternalDataSchema

WAREHOUSE_MULTI_DESTINATION_FLAG = "warehouse-multi-destination"

# Sync types whose runs read a moving window, so repeating one re-delivers rows a destination
# may already have. A full refresh is a fresh snapshot every time and always bills.
_WATERMARKED_SYNC_TYPES = frozenset({"incremental", "append"})


def is_multi_destination_enabled(team_id: int, source_type: str) -> bool:
    """Whether this team's syncs of this source type fan out to configured destinations.

    Evaluated in an activity and carried into the workflow as recorded history, the same way
    the v3 rollout flag is — a workflow must never read a flag directly.
    """
    try:
        team = Team.objects.only("uuid", "organization_id").get(id=team_id)
    except Team.DoesNotExist:
        return False

    try:
        return bool(
            posthoganalytics.feature_enabled(
                WAREHOUSE_MULTI_DESTINATION_FLAG,
                str(team.uuid),
                groups={
                    "organization": str(team.organization_id),
                    "project": str(team.id),
                },
                group_properties={
                    "organization": {"id": str(team.organization_id), "source_type": source_type},
                    "project": {"id": str(team.id), "source_type": source_type},
                },
                only_evaluate_locally=False,
                send_feature_flag_events=False,
            )
        )
    except Exception as e:
        capture_exception(e)
        return False


def watermark_start_for(schema: ExternalDataSchema) -> str | None:
    """The incremental cursor this run will read from, as a comparable string.

    None for full refresh and for a first incremental run, which have no window to repeat.
    """
    if schema.sync_type not in _WATERMARKED_SYNC_TYPES:
        return None

    value = (schema.sync_type_config or {}).get("incremental_field_last_value")
    return None if value is None else str(value)


def _already_delivered_this_window(
    team_id: int,
    schema_id: Any,
    destination: ExternalDataDestination,
    watermark_start: str | None,
) -> bool:
    """Has this destination already been billed for a completed run of this same window?

    The cursor only advances when every destination succeeds, so a run repeated because one
    destination failed re-extracts a window the healthy ones already delivered. They still
    re-deliver it for consistency, but charging for it twice would be wrong.
    """
    if watermark_start is None:
        return False

    return (
        ExternalDataDestinationJob.objects.for_team(team_id, canonical=True)
        .filter(
            destination_id=destination.id,
            status=ExternalDataDestinationJob.Status.COMPLETED,
            billable=True,
            job__schema_id=schema_id,
            job__watermark_start=watermark_start,
        )
        .exists()
    )


def create_destination_jobs_for_run(
    job: ExternalDataJob,
    schema: ExternalDataSchema,
) -> list[ExternalDataDestinationJob]:
    """Create one child job per destination this schema syncs to.

    Idempotent per (job, destination): a retried activity reuses the existing children rather
    than duplicating them.
    """
    destinations = resolve_destinations(schema)
    if not destinations:
        return []

    watermark_start = job.watermark_start
    children: list[ExternalDataDestinationJob] = []

    with transaction.atomic():
        for destination in destinations:
            billable = bool(job.billable) and not _already_delivered_this_window(
                job.team_id, schema.id, destination, watermark_start
            )
            child, _ = ExternalDataDestinationJob.objects.for_team(job.team_id, canonical=True).get_or_create(
                job_id=job.id,
                destination_id=destination.id,
                defaults={
                    "team_id": job.team_id,
                    "destination_type": destination.type,
                    "destination_name": destination.name,
                    "config_snapshot": destination.config or {},
                    "status": ExternalDataJob.Status.RUNNING,
                    "rows_synced": 0,
                    "billable": billable,
                },
            )
            children.append(child)

    return children


def has_warehouse_destination(children: list[ExternalDataDestinationJob]) -> bool:
    """Whether the PostHog warehouse is one of this run's destinations.

    The workflow needs this to know who releases the v3 sync lock: the warehouse writer does it
    when its child goes terminal, so a run without one must release it itself.
    """
    return any(c.destination_type == ExternalDataDestination.Type.POSTHOG_WAREHOUSE for c in children)
