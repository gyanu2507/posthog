"""Keeps `Team.session_recording_linked_flag` in step with the flag key it points at.

The stored dict carries both the flag `id` and its `key`, but the SDK payload that
`RemoteConfig._build_session_recording_config` builds is derived from the key alone. Both the
browser and React Native SDKs treat a linked flag they can't resolve as "do not record", so a
stale key silently turns replay off for the team rather than surfacing an error anywhere.
"""

from typing import Any

from django.db import transaction
from django.db.models import Exists, OuterRef, QuerySet
from django.db.models.functions import JSONObject

import structlog

from posthog.exceptions_capture import capture_exception
from posthog.models import Team
from posthog.models.signals import model_activity_signal, mutable_receiver

from products.feature_flags.backend.models.feature_flag import FeatureFlag

logger = structlog.get_logger(__name__)


def linked_flag_id(linked_flag: Any) -> int | None:
    """The flag id a stored replay link points at, or None when it holds no usable one."""
    if not isinstance(linked_flag, dict):
        return None
    stored_id = linked_flag.get("id")
    # The column is schemaless, so anything an API client or the admin's JSON widget sent can be
    # here. Only an int is usable, and every other shape is left for the caller to handle rather
    # than coerced. `bool` is excluded explicitly because it subclasses `int`, so `{"id": true}`
    # would otherwise read as a link to flag 1.
    if isinstance(stored_id, bool) or not isinstance(stored_id, int):
        return None
    return stored_id


def teams_linking_flag(feature_flag: FeatureFlag) -> QuerySet[Team]:
    """Every team gating session recording on this flag."""
    # Scoped by project rather than by team: any team in the project can gate recording on a flag
    # owned by a sibling team.
    return Team.objects.filter(
        project_id=feature_flag.team.project_id,
        session_recording_linked_flag__contains={"id": feature_flag.id},
    )


def teams_linking_flag_subquery() -> Exists:
    """`teams_linking_flag` as a correlated subquery, for annotating a `FeatureFlag` queryset."""
    # Containment never casts, so a non-integer id in the JSON yields False rather than erroring
    # the query.
    return Exists(
        Team.objects.filter(
            project_id=OuterRef("team__project_id"),
            session_recording_linked_flag__contains=JSONObject(id=OuterRef("id")),
        )
    )


def replay_linked_flag_ids(project_id: int) -> set[int]:
    """Every flag id a team in this project gates session recording on.

    One query for the whole project, for callers checking many flags at once. `teams_linking_flag`
    is the per-flag equivalent; calling it in a loop is a query per flag.
    """
    stored = (
        Team.objects.filter(project_id=project_id)
        .exclude(session_recording_linked_flag__isnull=True)
        .values_list("session_recording_linked_flag", flat=True)
    )
    return {flag_id for linked_flag in stored if (flag_id := linked_flag_id(linked_flag)) is not None}


def update_linked_flag_key(team: Team, new_key: str) -> None:
    """Rewrite the stored key on a team's replay link, leaving teams that already match alone."""
    linked_flag = team.session_recording_linked_flag
    if not isinstance(linked_flag, dict) or linked_flag.get("key") == new_key:
        # A no-op save would still spend a write, a Celery task, and a RemoteConfig rebuild.
        return

    team.session_recording_linked_flag = {**linked_flag, "key": new_key}
    # Saving the instance rather than issuing a queryset `update()` is what fires the `post_save`
    # receiver that refreshes the team's RemoteConfig; a bulk update would leave the cached SDK
    # payload holding the old key.
    team.save(update_fields=["session_recording_linked_flag"])


def relink_teams(feature_flag: FeatureFlag) -> None:
    """Point every team gating replay on this flag at its current key."""
    for team in teams_linking_flag(feature_flag):
        try:
            update_linked_flag_key(team, feature_flag.key)
        except Exception:
            # This runs after the rename has committed, so raising would fail a request that
            # already succeeded, and the retry would find the key unchanged and skip the relink
            # entirely. Report instead and leave the row for `repair_replay_linked_flag_keys`.
            # Per team, so one unwritable row doesn't strand the others.
            logger.exception("replay_relink_failed", flag_id=feature_flag.pk, team_id=team.pk)
            capture_exception()


@mutable_receiver(model_activity_signal, sender=FeatureFlag)
def relink_teams_on_key_change(
    sender: Any, before_update: FeatureFlag | None, after_update: FeatureFlag | None, **kwargs: Any
) -> None:
    # Wired to the model signal rather than to FeatureFlagSerializer so renames from the Django
    # admin, a shell, or a Celery task keep the link intact too. `before_update` is None on create;
    # `after_update` is None on delete.
    if before_update is None or after_update is None or before_update.key == after_update.key:
        return

    # Unlike `repair_replay_linked_flag_keys`, this has no `after_update.deleted` guard, including
    # for the tombstone rename `_free_key_held_by_soft_deleted_flags` does when freeing a
    # soft-deleted flag's key for reuse. That's intentional: relinking still rewrites a team's
    # stored key to the flag's new, id-suffixed tombstone, which no live flag's key can equal.
    # Skipping the rewrite would leave the team pointing at the now-freed original key, which a
    # new flag could reuse next, silently gating replay on a flag the team never linked.

    # Deferred to commit because the serializer renames inside a transaction that holds
    # `select_for_update` on the flag row, and taking team locks in that window invites deadlocks.
    # Outside a transaction (admin, shell) `on_commit` runs the callback immediately.
    transaction.on_commit(lambda: relink_teams(after_update))
