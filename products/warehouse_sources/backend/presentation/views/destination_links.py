"""Setting which destinations a source or one of its tables syncs to.

Kept out of the source and schema viewsets so the destination set is edited through one
shared, tested path rather than two near-copies.
"""

from typing import Any

from rest_framework import serializers
from rest_framework.exceptions import ValidationError

from products.warehouse_sources.backend.facade.models import ExternalDataDestination
from products.warehouse_sources.backend.models.external_data_destination import (
    ExternalDataSchemaDestination,
    ExternalDataSourceDestination,
)

EMPTY_SET_MESSAGE = (
    "Pick at least one destination. To stop syncing this data, turn off syncing instead of removing every destination."
)


class DestinationLinkSerializer(serializers.Serializer):
    destination_ids = serializers.ListField(
        child=serializers.UUIDField(),
        allow_empty=True,
        allow_null=True,
        help_text=(
            "Destinations to sync to. On a table, null clears the override so the table follows its source again."
        ),
    )


def _resolve_destinations(team_id: int, destination_ids: list) -> list[ExternalDataDestination]:
    destinations = list(
        ExternalDataDestination.objects.for_team(team_id).filter(id__in=destination_ids).exclude(deleted=True)
    )
    missing = {str(i) for i in destination_ids} - {str(d.id) for d in destinations}
    if missing:
        raise ValidationError({"destination_ids": f"Unknown destinations: {', '.join(sorted(missing))}"})
    return destinations


def set_source_destinations(*, team_id: int, source_id: Any, destination_ids: list | None) -> list[str]:
    """Replace a source's destination set. Returns the ids now attached."""
    if not destination_ids:
        raise ValidationError({"destination_ids": EMPTY_SET_MESSAGE})

    destinations = _resolve_destinations(team_id, destination_ids)

    ExternalDataSourceDestination.objects.for_team(team_id).filter(source_id=source_id).delete()
    for destination in destinations:
        ExternalDataSourceDestination.objects.for_team(team_id).create(
            team_id=team_id, source_id=source_id, destination=destination
        )
    return [str(d.id) for d in destinations]


def set_schema_destinations(*, team_id: int, schema_id: Any, destination_ids: list | None) -> list[str] | None:
    """Replace a table's destination override, or clear it so the table follows its source.

    Null clears the override. An empty list is rejected rather than treated as "sync nowhere":
    with no rows left there is nothing to distinguish it from having no override, so the table
    would quietly fall back to its source's destinations. Turning syncing off is the supported
    way to stop a table.
    """
    if destination_ids is not None and not destination_ids:
        raise ValidationError({"destination_ids": EMPTY_SET_MESSAGE})

    ExternalDataSchemaDestination.objects.for_team(team_id).filter(schema_id=schema_id).delete()

    if destination_ids is None:
        return None

    destinations = _resolve_destinations(team_id, destination_ids)
    for destination in destinations:
        ExternalDataSchemaDestination.objects.for_team(team_id).create(
            team_id=team_id, schema_id=schema_id, destination=destination
        )
    return [str(d.id) for d in destinations]
