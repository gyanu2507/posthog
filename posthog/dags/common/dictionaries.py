import abc
import time
from functools import partial

from django.conf import settings

from clickhouse_driver.client import Client

from posthog.clickhouse.client.connection import ClickHouseCredentials, ClickHouseUser, get_clickhouse_creds
from posthog.clickhouse.cluster import ClickhouseCluster

DEFAULT_DICTIONARY_LOAD_TIMEOUT: float = 1800.0


class ClusterDictionary(abc.ABC):
    """One cluster-wide ClickHouse dictionary: the same definition created on every host, loaded
    and checksummed until all hosts agree, then dropped when its job is done.

    Subclasses supply the identity (name, declared columns, key, source query, credentials);
    this base owns the lifecycle so every dictionary-driven deletion job shares one
    implementation of create, load-with-deadline, checksum, and drop.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Unqualified dictionary name; the caller owns the naming scheme."""

    @property
    @abc.abstractmethod
    def schema(self) -> str:
        """Column declarations, e.g. "team_id Int64, key String"."""

    @property
    @abc.abstractmethod
    def primary_key(self) -> str:
        """Comma-separated key columns."""

    @property
    @abc.abstractmethod
    def query(self) -> str:
        """The SELECT the dictionary source runs against ClickHouse."""

    @property
    def credentials(self) -> ClickHouseCredentials:
        """Credentials the dictionary source reads as.

        Defaults to the default user. Override only when the role's SELECT grants on the source
        tables are known to exist in every environment; grants live in infra, not this repo.
        """
        return get_clickhouse_creds(ClickHouseUser.DEFAULT)

    @property
    def qualified_name(self) -> str:
        return f"{settings.CLICKHOUSE_DATABASE}.{self.name}"

    def create(self, client: Client, shards: int, max_execution_time: int, max_memory_usage: int) -> None:
        # Credentials are query parameters so they stay out of the traced statement.
        creds = self.credentials
        client.execute(
            f"""
            CREATE DICTIONARY IF NOT EXISTS {self.qualified_name} ({self.schema})
            PRIMARY KEY {self.primary_key}
            SOURCE(CLICKHOUSE(DB %(database)s USER %(user)s PASSWORD %(password)s QUERY %(query)s))
            LAYOUT(COMPLEX_KEY_HASHED(SHARDS {shards}))
            LIFETIME(0)
            SETTINGS(max_execution_time={max_execution_time}, max_memory_usage={max_memory_usage})
            """,
            {
                "database": settings.CLICKHOUSE_DATABASE,
                "user": creds.user,
                "password": creds.password,
                "query": self.query,
            },
        )

    def exists(self, client: Client) -> bool:
        [[count]] = client.execute(
            "SELECT count() FROM system.dictionaries WHERE database = %(database)s AND name = %(name)s",
            {"database": settings.CLICKHOUSE_DATABASE, "name": self.name},
        )
        return count > 0

    def drop(self, client: Client) -> None:
        client.execute(f"DROP DICTIONARY IF EXISTS {self.qualified_name} SYNC")

    def is_loaded(self, client: Client) -> bool:
        results = client.execute(
            "SELECT status, last_exception FROM system.dictionaries WHERE database = %(database)s AND name = %(name)s",
            {"database": settings.CLICKHOUSE_DATABASE, "name": self.name},
        )
        if not results:
            raise Exception(f"{self.qualified_name} does not exist")
        [[status, last_exception]] = results
        if status == "LOADED":
            return True
        if status in {"LOADING", "FAILED_AND_RELOADING", "LOADED_AND_RELOADING"}:
            return False
        if status == "FAILED":
            raise Exception(f"{self.qualified_name} failed to load: {last_exception}")
        raise Exception(f"{self.qualified_name} in unexpected status: {status}")

    def load(self, client: Client, timeout_seconds: float = DEFAULT_DICTIONARY_LOAD_TIMEOUT) -> int:
        client.execute(f"SYSTEM RELOAD DICTIONARY {self.qualified_name}")

        # The reload is asynchronous, so a consumer would read a half-populated dictionary
        # without this wait. The deadline matters because a dictionary wedged in LOADING would
        # otherwise hold the run open forever and silently: a run that hangs raises no failure
        # alert, while one that times out does.
        deadline = time.monotonic() + timeout_seconds
        while not self.is_loaded(client):
            if time.monotonic() > deadline:
                raise Exception(f"{self.qualified_name} still not loaded after {timeout_seconds}s")
            time.sleep(5.0)

        return self.checksum(client)

    def checksum(self, client: Client) -> int:
        # XOR of per-row hashes is order independent, so hosts holding the same entries agree
        # regardless of read order. cityHash64(*) covers every declared column, attributes
        # included: consumers read attributes from whichever host they land on, so hosts must
        # agree on more than the key set.
        [[checksum]] = client.execute(f"SELECT groupBitXor(cityHash64(*)) FROM {self.qualified_name}")
        return checksum


def load_and_verify(
    cluster: ClickhouseCluster,
    dictionary: ClusterDictionary,
    timeout_seconds: float = DEFAULT_DICTIONARY_LOAD_TIMEOUT,
) -> None:
    """Load the dictionary on every host and require them to agree.

    Whatever probes the dictionary reads whichever host it executes on, so hosts holding
    different contents would produce different results per replica. Loads run one host at a
    time because each load re-executes the source query, and running them all at once points
    every host's read at the source table simultaneously.
    """
    checksums = cluster.map_all_hosts(partial(dictionary.load, timeout_seconds=timeout_seconds), concurrency=1).result()
    if len(set(checksums.values())) != 1:
        raise Exception(f"{dictionary.name} differs across hosts: {checksums}")
