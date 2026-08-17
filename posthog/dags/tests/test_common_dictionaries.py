import itertools
from collections.abc import Iterable
from datetime import datetime
from typing import Any, cast

import pytest

from clickhouse_driver.client import Client

from posthog.clickhouse.client.connection import ClickHouseCredentials, ClickHouseUser
from posthog.clickhouse.cluster import ClickhouseCluster
from posthog.dags import clickhouse_cleanup
from posthog.dags.common import dictionaries
from posthog.dags.common.dictionaries import ClusterDictionary, load_and_verify
from posthog.dags.deletes import (
    AdhocEventDeletesDictionary,
    AdhocEventDeletesTable,
    PendingDeletesDictionary,
    PendingDeletesTable,
)


class _StubDictionary(ClusterDictionary):
    @property
    def name(self) -> str:
        return "stub_dictionary"

    @property
    def schema(self) -> str:
        return "team_id Int64, key String"

    @property
    def primary_key(self) -> str:
        return "team_id, key"

    @property
    def query(self) -> str:
        return "SELECT team_id, key FROM stub_source"


class _ScriptedClient:
    """Answers dictionary-status polls from a script and records every statement."""

    def __init__(self, statuses: Iterable[tuple[str, str] | None] = (), checksum: int = 0) -> None:
        self.statuses = iter(statuses)
        self.checksum = checksum
        self.executed: list[tuple[str, dict[str, Any] | None]] = []

    def execute(self, query: str, params: dict[str, Any] | None = None) -> list[list[Any]]:
        self.executed.append((query, params))
        if "system.dictionaries" in query:
            status = next(self.statuses)
            return [] if status is None else [list(status)]
        if "groupBitXor" in query:
            return [[self.checksum]]
        return []


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    clock = _Clock()
    monkeypatch.setattr(dictionaries.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(dictionaries.time, "sleep", clock.sleep)
    return clock


def test_load_returns_the_checksum_once_loaded(clock: _Clock) -> None:
    client = _ScriptedClient(statuses=[("LOADING", ""), ("LOADED", "")], checksum=42)
    assert _StubDictionary().load(cast(Client, client), timeout_seconds=600) == 42


def test_load_fails_instead_of_waiting_forever_on_a_wedged_dictionary(clock: _Clock) -> None:
    client = _ScriptedClient(statuses=itertools.repeat(("LOADING", "")))
    with pytest.raises(Exception, match="still not loaded after 600"):
        _StubDictionary().load(cast(Client, client), timeout_seconds=600)


@pytest.mark.parametrize(
    "status,match",
    [
        (("FAILED", "grant missing"), "failed to load: grant missing"),
        (("MYSTERY", ""), "unexpected status: MYSTERY"),
        (None, "does not exist"),
    ],
)
def test_load_surfaces_terminal_statuses(clock: _Clock, status: tuple[str, str] | None, match: str) -> None:
    client = _ScriptedClient(statuses=[status])
    with pytest.raises(Exception, match=match):
        _StubDictionary().load(cast(Client, client), timeout_seconds=600)


class _FakeFuture:
    def __init__(self, value: dict[str, int]) -> None:
        self.value = value

    def result(self) -> dict[str, int]:
        return self.value


class _FakeCluster:
    def __init__(self, checksums: dict[str, int]) -> None:
        self.checksums = checksums

    def map_all_hosts(self, fn: Any, concurrency: int | None = None) -> _FakeFuture:
        return _FakeFuture(self.checksums)


def test_load_and_verify_raises_when_hosts_disagree() -> None:
    cluster = cast(ClickhouseCluster, _FakeCluster({"host-a": 1, "host-b": 2}))
    with pytest.raises(Exception, match="stub_dictionary differs across hosts"):
        load_and_verify(cluster, _StubDictionary(), timeout_seconds=600)


def test_load_and_verify_passes_when_hosts_agree() -> None:
    cluster = cast(ClickhouseCluster, _FakeCluster({"host-a": 7, "host-b": 7}))
    load_and_verify(cluster, _StubDictionary(), timeout_seconds=600)


def _pending_deletes_dictionary() -> PendingDeletesDictionary:
    return PendingDeletesDictionary(source=PendingDeletesTable(timestamp=datetime(2026, 1, 1)))


def _adhoc_event_deletes_dictionary() -> AdhocEventDeletesDictionary:
    return AdhocEventDeletesDictionary(source=AdhocEventDeletesTable())


def _snapshot_dictionary() -> clickhouse_cleanup.SnapshotDictionary:
    return clickhouse_cleanup.SnapshotDictionary(
        source=clickhouse_cleanup.DeletedPersonsTable(run_id="run_1"),
        excluded=clickhouse_cleanup.RevivedPersonsTable(run_id="run_1"),
    )


@pytest.mark.parametrize(
    "make_dictionary,expected_user",
    [
        (_pending_deletes_dictionary, "app_default"),
        (_adhoc_event_deletes_dictionary, "app_default"),
        (_snapshot_dictionary, "reader"),
    ],
)
def test_create_reads_the_source_as_each_callers_credentials(
    monkeypatch: pytest.MonkeyPatch, make_dictionary: Any, expected_user: str
) -> None:
    # The GDPR job's dictionaries must keep reading as the default user until dict_reader's
    # SELECT grants on its per-run tables are proven in every environment; the sweep already
    # reads as dict_reader. A shared-lifecycle change that flips either breaks a weekly prod job.
    creds = {
        ClickHouseUser.DEFAULT: ClickHouseCredentials(user="app_default", password="p1"),
        ClickHouseUser.DICT_READER: ClickHouseCredentials(user="reader", password="p2"),
    }
    monkeypatch.setattr(dictionaries, "get_clickhouse_creds", creds.__getitem__)
    monkeypatch.setattr(clickhouse_cleanup, "get_clickhouse_creds", creds.__getitem__)

    client = _ScriptedClient()
    make_dictionary().create(cast(Client, client), shards=1, max_execution_time=0, max_memory_usage=0)

    [(query, params)] = client.executed
    assert "CREATE DICTIONARY" in query
    assert params is not None and params["user"] == expected_user
