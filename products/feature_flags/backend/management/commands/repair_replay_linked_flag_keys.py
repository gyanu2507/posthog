"""Repair `Team.session_recording_linked_flag` rows whose stored key no longer matches the flag.

The SDKs resolve the linked flag by key rather than by id, so a stored key that no longer matches
its flag silently stops the team recording. `relink_teams_on_key_change` in
`session_recording_links` covers every rename that goes through `FeatureFlag.save()`; this command
repairs the rows that predate that receiver, plus any left behind by a writer that bypasses
`save()`, such as the `bulk_update` in `bulk_delete`.

Only rows whose stored id resolves to a live flag in the team's own project are rewritten. Rows
pointing at a soft-deleted flag or at a flag in another project are counted and reported but
left alone, because neither has a safe new key to adopt: a human has to decide whether the team
still wants a recording gate at all.
"""

import json
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db.models import QuerySet

from posthog.models import Team

from products.feature_flags.backend.models.feature_flag import FeatureFlag
from products.feature_flags.backend.session_recording_links import linked_flag_id, update_linked_flag_key


class Outcome(StrEnum):
    REPAIRED = "repaired"
    ALREADY_CORRECT = "already_correct"
    FLAG_SOFT_DELETED = "flag_soft_deleted"
    FLAG_IN_OTHER_PROJECT = "flag_in_other_project"
    FLAG_MISSING = "flag_missing"
    MALFORMED = "malformed"


@dataclass(frozen=True, kw_only=True)
class _FlagRow:
    key: str
    deleted: bool
    project_id: int


def _iter_team_chunks(queryset: QuerySet[Team], chunk_size: int) -> Iterator[list[Team]]:
    # `Team` is a wide model with several large JSONFields, and the scan reads three columns.
    base = queryset.only("id", "project_id", "session_recording_linked_flag").order_by("id")
    # Keyset pagination instead of .iterator(): prod runs behind PgBouncer with server-side
    # cursors disabled, so .iterator() buffers the whole result set client-side on execute.
    last_id = 0
    while True:
        chunk = list(base.filter(id__gt=last_id)[:chunk_size])
        if not chunk:
            return
        yield chunk
        last_id = chunk[-1].id


class Command(BaseCommand):
    help = "Rewrite stale flag keys in teams' session replay linked flag settings"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--live-run", action="store_true", help="Apply changes (default is dry-run)")
        parser.add_argument("--team-id", type=int, nargs="+", help="Only scan these teams (defaults to every team)")
        parser.add_argument("--chunk-size", type=int, default=500, help="Teams to load per query (default: 500)")
        parser.add_argument("--json", action="store_true", help="Emit the report as JSON instead of prose")

    def handle(self, *args: Any, **options: Any) -> None:
        # Dry-run by default, matching the other flags repair commands: a bare invocation rewrites
        # every team's replay config and enqueues a RemoteConfig rebuild per row, so writing has
        # to be asked for.
        dry_run: bool = not options["live_run"]
        as_json: bool = options["json"]
        chunk_size: int = options["chunk_size"]
        if chunk_size < 1:
            # A zero chunk slices to an empty list, so the scan would report a clean run having
            # looked at nothing.
            raise CommandError("--chunk-size must be at least 1")

        queryset = Team.objects.exclude(session_recording_linked_flag__isnull=True)
        if options["team_id"]:
            queryset = queryset.filter(id__in=options["team_id"])

        outcomes: Counter[Outcome] = Counter()
        repairs: list[dict[str, Any]] = []
        unrepairable: list[dict[str, Any]] = []

        for teams in _iter_team_chunks(queryset, chunk_size):
            flags_by_id = self._load_flags(teams)
            for team in teams:
                outcome, detail = self._repair_team(team, flags_by_id, dry_run=dry_run)
                outcomes[outcome] += 1
                if outcome == Outcome.REPAIRED:
                    repairs.append({"outcome": outcome, **detail})
                elif outcome != Outcome.ALREADY_CORRECT:
                    # already_correct rows need no follow-up and would dwarf the report, so only
                    # their count is kept.
                    unrepairable.append({"outcome": outcome, **detail})

        report = {
            "dry_run": dry_run,
            "scanned": outcomes.total(),
            "outcomes": dict(outcomes),
            "repairs": repairs,
            "unrepairable": unrepairable,
        }
        self._report(report, as_json=as_json)

    def _load_flags(self, teams: list[Team]) -> dict[int, _FlagRow]:
        flag_ids = {
            flag_id for team in teams if (flag_id := linked_flag_id(team.session_recording_linked_flag)) is not None
        }
        if not flag_ids:
            return {}
        rows = FeatureFlag.objects_including_soft_deleted.filter(id__in=flag_ids).values_list(
            "id", "key", "deleted", "team__project_id"
        )
        return {
            flag_id: _FlagRow(key=key, deleted=deleted, project_id=project_id)
            for flag_id, key, deleted, project_id in rows
        }

    def _repair_team(
        self, team: Team, flags_by_id: dict[int, _FlagRow], *, dry_run: bool
    ) -> tuple[Outcome, dict[str, Any]]:
        linked_flag = team.session_recording_linked_flag
        detail: dict[str, Any] = {"team_id": team.id, "project_id": team.project_id, "linked_flag": linked_flag}

        stored_id = linked_flag_id(linked_flag)
        if stored_id is None:
            return Outcome.MALFORMED, detail

        detail["flag_id"] = stored_id
        flag = flags_by_id.get(stored_id)
        if flag is None:
            return Outcome.FLAG_MISSING, detail
        if flag.project_id != team.project_id:
            return Outcome.FLAG_IN_OTHER_PROJECT, detail
        if flag.deleted:
            return Outcome.FLAG_SOFT_DELETED, detail
        if linked_flag.get("key") == flag.key:
            return Outcome.ALREADY_CORRECT, detail

        detail["old_key"] = linked_flag.get("key")
        detail["new_key"] = flag.key
        if not dry_run:
            update_linked_flag_key(team, stored_id, flag.key)
        return Outcome.REPAIRED, detail

    def _report(self, report: dict[str, Any], *, as_json: bool) -> None:
        if as_json:
            self.stdout.write(json.dumps(report, indent=2, default=str))
            return

        verb = "Would repair" if report["dry_run"] else "Repaired"
        self.stdout.write(f"Scanned {report['scanned']} team(s) with a session replay linked flag.")
        self.stdout.write(f"{verb} {len(report['repairs'])} team(s):")
        for repair in report["repairs"]:
            self.stdout.write(
                f"  team {repair['team_id']} (project {repair['project_id']}): "
                f"flag {repair['flag_id']} {repair['old_key']!r} -> {repair['new_key']!r}"
            )

        for outcome, count in sorted(report["outcomes"].items()):
            if outcome != Outcome.REPAIRED:
                self.stdout.write(f"{outcome}: {count}")

        if report["unrepairable"]:
            self.stdout.write("Not repaired:")
            for row in report["unrepairable"]:
                self.stdout.write(
                    f"  team {row['team_id']} (project {row['project_id']}) [{row['outcome']}]: {row['linked_flag']!r}"
                )
