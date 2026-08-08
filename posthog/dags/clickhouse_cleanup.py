"""Weekly ClickHouse cleanup sweeps: deleted persons, then deleted cohorts.

ClickHouse cannot cheaply delete individual rows, so deletions are marked and swept later.
Both sweeps issue cluster-wide mutations, and running those at the same time overloads the
cluster, so the ops below are chained on each other's output to force them into sequence.
"""

import time
from dataclasses import dataclass
from functools import partial

from django.conf import settings

import dagster
import pydantic
from clickhouse_driver.client import Client

from posthog.clickhouse.cluster import ClickhouseCluster, LightweightDeleteMutationRunner, NodeRole
from posthog.dags.common import JobOwners
from posthog.models.async_deletion.delete_cohorts import (
    COHORT_DELETION_MARK_FAILURE_COUNTER,
    COHORT_DELETION_RUN_FAILURE_COUNTER,
    AsyncCohortDeletion,
)
from posthog.models.person.sql import PERSONS_TABLE

# The failure hook rebuilds the per-run asset names from this and the run id alone.
SNAPSHOT_TABLE_PREFIX = "deleted_persons"


class CleanupConfig(dagster.Config):
    dry_run: bool = pydantic.Field(
        default=True,
        description="Build the snapshot and report what would be removed, without deleting anything.",
    )
    cleanup: bool = pydantic.Field(
        default=True,
        description="Drop the dictionary and snapshot table when the run finishes.",
    )
    shards: int = pydantic.Field(default=16, description="Dictionary SHARDS, which parallelize loading.")
    max_execution_time: int = pydantic.Field(default=0, description="Dictionary load timeout, 0 for no limit.")
    max_memory_usage: int = pydantic.Field(default=0, description="Dictionary load memory cap, 0 for no limit.")


@dataclass(frozen=True, kw_only=True)
class SweepResult:
    """What the person sweep hands to the ops after it.

    dry_run travels here rather than being read from config by each op, so it is set in one
    place. Declaring it on every op would let a launch set it on one and miss another, and
    half a sweep is worse than none.
    """

    dictionary: "DeletedPersonsDictionary"
    dry_run: bool


@dataclass
class DeletedPersonsTable:
    """Snapshot of the persons whose latest ClickHouse version is deleted.

    The sweep removes those rows, which destroys the tombstones the set was derived from, so
    the set is captured here first and every later step reads it from here rather than
    recomputing it.
    """

    run_id: str

    @property
    def table_name(self) -> str:
        return f"{SNAPSHOT_TABLE_PREFIX}_{self.run_id}"

    @property
    def qualified_name(self) -> str:
        return f"{settings.CLICKHOUSE_DATABASE}.{self.table_name}"

    @property
    def zk_path(self) -> str:
        # table_name carries the run id, so replicas of one run agree and separate runs never collide.
        testing = "testing/" if settings.TEST else ""
        return f"/clickhouse/tables/{testing}noshard/{self.table_name}"

    def create(self, client: Client) -> None:
        client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.qualified_name} (
                team_id Int64,
                person_id UUID
            )
            ENGINE = ReplicatedReplacingMergeTree('{self.zk_path}', '{{shard}}-{{replica}}')
            ORDER BY (team_id, person_id)
            """
        )

    def populate(self, client: Client) -> None:
        # A person can be soft-deleted and later revived by a higher version, so membership is
        # decided by the latest version rather than by any version having is_deleted set. The
        # inner IN narrows the aggregation to persons with at least one deleted version.
        client.execute(
            f"""
            INSERT INTO {self.qualified_name} (team_id, person_id)
            SELECT team_id, id
            FROM {PERSONS_TABLE}
            WHERE (team_id, id) IN (SELECT team_id, id FROM {PERSONS_TABLE} WHERE is_deleted > 0)
            GROUP BY team_id, id
            HAVING argMax(is_deleted, version) > 0
            """
        )

    def count(self, client: Client) -> int:
        [[count]] = client.execute(f"SELECT count() FROM {self.qualified_name}")
        return count

    def drop(self, client: Client) -> None:
        client.execute(f"DROP TABLE IF EXISTS {self.qualified_name} SYNC")


@dataclass
class DeletedPersonsDictionary:
    source: DeletedPersonsTable

    @property
    def name(self) -> str:
        return f"{self.source.table_name}_dictionary"

    @property
    def qualified_name(self) -> str:
        return f"{settings.CLICKHOUSE_DATABASE}.{self.name}"

    @property
    def query(self) -> str:
        return f"SELECT team_id, person_id FROM {self.source.qualified_name}"

    def create(self, client: Client, shards: int, max_execution_time: int, max_memory_usage: int) -> None:
        # The source authenticates as the default user, like the other dictionaries in this code
        # location. get_clickhouse_creds(DICT_READER) would silently fall back to this same user
        # rather than fail, so naming it here keeps the choice visible instead of implicit.
        # Credentials are query parameters so they stay out of the traced statement.
        client.execute(
            f"""
            CREATE DICTIONARY IF NOT EXISTS {self.qualified_name} (
                team_id Int64,
                person_id UUID
            )
            PRIMARY KEY team_id, person_id
            SOURCE(CLICKHOUSE(DB %(database)s USER %(user)s PASSWORD %(password)s QUERY %(query)s))
            LAYOUT(COMPLEX_KEY_HASHED(SHARDS {shards}))
            LIFETIME(0)
            SETTINGS(max_execution_time={max_execution_time}, max_memory_usage={max_memory_usage})
            """,
            {
                "database": settings.CLICKHOUSE_DATABASE,
                "user": settings.CLICKHOUSE_USER,
                "password": settings.CLICKHOUSE_PASSWORD,
                "query": self.query,
            },
        )

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

    def load(self, client: Client) -> int:
        client.execute(f"SYSTEM RELOAD DICTIONARY {self.qualified_name}")

        # The reload is asynchronous, so the mutation would read a half-populated dictionary
        # without this wait.
        while not self.is_loaded(client):
            time.sleep(5.0)

        return self.checksum(client)

    def checksum(self, client: Client) -> int:
        # XOR is order independent, so hosts that hold the same keys agree regardless of read order.
        [[checksum]] = client.execute(f"SELECT groupBitXor(cityHash64(team_id, person_id)) FROM {self.qualified_name}")
        return checksum


@dagster.op
def snapshot_deleted_persons(
    context: dagster.OpExecutionContext,
    cluster: dagster.ResourceParam[ClickhouseCluster],
) -> DeletedPersonsTable:
    """Capture the persons whose latest version is deleted into a per-run table."""
    table = DeletedPersonsTable(run_id=context.run_id.replace("-", "_"))

    cluster.map_all_hosts(table.create).result()
    cluster.any_host_by_role(table.populate, NodeRole.DATA).result()

    # The insert lands on one host, but every host reads this table when the dictionary loads.
    def sync_replica(client: Client) -> None:
        client.execute(f"SYSTEM SYNC REPLICA {table.qualified_name} STRICT")

    cluster.map_all_hosts(sync_replica).result()

    count = cluster.any_host_by_role(table.count, NodeRole.DATA).result()
    context.add_output_metadata(
        {
            "deleted_persons": dagster.MetadataValue.int(count),
            "table_name": dagster.MetadataValue.text(table.table_name),
        }
    )
    return table


@dagster.op
def build_deleted_persons_dictionary(
    config: CleanupConfig,
    cluster: dagster.ResourceParam[ClickhouseCluster],
    snapshot_deleted_persons: DeletedPersonsTable,
) -> DeletedPersonsDictionary:
    """Create the dictionary the delete predicate probes, on every host."""
    dictionary = DeletedPersonsDictionary(source=snapshot_deleted_persons)
    cluster.map_all_hosts(
        partial(
            dictionary.create,
            shards=config.shards,
            max_execution_time=config.max_execution_time,
            max_memory_usage=config.max_memory_usage,
        )
    ).result()
    return dictionary


@dagster.op
def load_and_verify_deleted_persons_dictionary(
    cluster: dagster.ResourceParam[ClickhouseCluster],
    dictionary: DeletedPersonsDictionary,
) -> DeletedPersonsDictionary:
    """Load the dictionary on all hosts and confirm they hold identical keys."""
    checksums = cluster.map_all_hosts(dictionary.load, concurrency=1).result()
    assert len(set(checksums.values())) == 1
    return dictionary


@dagster.op
def delete_persons(
    context: dagster.OpExecutionContext,
    config: CleanupConfig,
    cluster: dagster.ResourceParam[ClickhouseCluster],
    load_and_verify_deleted_persons_dictionary: DeletedPersonsDictionary,
) -> SweepResult:
    """Remove the snapshotted persons from the ClickHouse person table."""
    dictionary = load_and_verify_deleted_persons_dictionary
    context.add_output_metadata({"dry_run": dagster.MetadataValue.bool(config.dry_run)})

    if config.dry_run:
        context.log.info("dry run: skipping the delete from %s", PERSONS_TABLE)
        return SweepResult(dictionary=dictionary, dry_run=True)

    runner = LightweightDeleteMutationRunner(
        table=PERSONS_TABLE,
        predicate="dictHas(%(dictionary)s, (team_id, id))",
        parameters={"dictionary": dictionary.qualified_name},
    )

    # person is replicated and not sharded, so a mutation started on one host reaches all of them.
    mutation = cluster.any_host(runner).result()
    cluster.map_all_hosts(mutation.wait).result()

    return SweepResult(dictionary=dictionary, dry_run=False)


@dagster.op
def clear_removed_cohort_data(
    context: dagster.OpExecutionContext,
    delete_persons: SweepResult,
) -> SweepResult:
    """Remove cohort membership rows for cohorts that were deleted or recalculated."""
    if delete_persons.dry_run:
        context.log.info("dry run: skipping the cohort sweep")
        return delete_persons

    runner = AsyncCohortDeletion()
    failures = []

    # Each pass is guarded on its own: failing to tick off already-deleted cohorts must not stop
    # the pass that actually removes rows.
    try:
        runner.mark_deletions_done()
    except Exception:
        context.log.exception("failed to mark cohort deletions done")
        COHORT_DELETION_MARK_FAILURE_COUNTER.inc()
        failures.append("mark")

    try:
        runner.run()
    except Exception:
        context.log.exception("failed to run cohort deletions")
        COHORT_DELETION_RUN_FAILURE_COUNTER.inc()
        failures.append("run")

    context.add_output_metadata({"failed_passes": dagster.MetadataValue.text(", ".join(failures) or "none")})
    return delete_persons


@dagster.op
def drop_snapshot_assets(
    context: dagster.OpExecutionContext,
    config: CleanupConfig,
    cluster: dagster.ResourceParam[ClickhouseCluster],
    clear_removed_cohort_data: SweepResult,
) -> None:
    """Drop the per-run dictionary and snapshot table."""
    dictionary = clear_removed_cohort_data.dictionary

    if not config.cleanup:
        context.log.info("cleanup disabled, leaving %s in place", dictionary.qualified_name)
        return

    # The dictionary reads from the table, so it has to go first.
    cluster.map_all_hosts(dictionary.drop).result()
    cluster.map_all_hosts(dictionary.source.drop).result()


@dagster.failure_hook(required_resource_keys={"cluster"})
def drop_assets_on_failure(context: dagster.HookContext) -> None:
    """Drop this run's assets when an op fails.

    Dagster skips downstream ops after a failure, so drop_snapshot_assets never runs and the
    per-run table and dictionary would survive on the cluster and accumulate across failures.
    The names come from the run id alone, so this needs nothing from the failed op.

    This ignores the cleanup flag on purpose. A stranded dictionary holds its whole key set in
    memory on every host, which costs more than the ability to inspect it after a failure.
    """
    run_id = context.run_id.replace("-", "_")
    qualified = f"{settings.CLICKHOUSE_DATABASE}.{SNAPSHOT_TABLE_PREFIX}_{run_id}"
    cluster = context.resources.cluster

    # The dictionary reads from the table, so it has to go first.
    cluster.map_all_hosts(
        lambda client: client.execute(f"DROP DICTIONARY IF EXISTS {qualified}_dictionary SYNC")
    ).result()
    cluster.map_all_hosts(lambda client: client.execute(f"DROP TABLE IF EXISTS {qualified} SYNC")).result()


@dagster.job(hooks={drop_assets_on_failure}, tags={"owner": JobOwners.TEAM_CLICKHOUSE.value})
def clickhouse_cleanup_job():
    """Sweep deleted persons out of ClickHouse, then deleted cohort memberships."""
    dictionary = load_and_verify_deleted_persons_dictionary(
        build_deleted_persons_dictionary(snapshot_deleted_persons())
    )
    # Each op takes the previous op's output, which is what keeps the two sweeps in sequence.
    drop_snapshot_assets(clear_removed_cohort_data(delete_persons(dictionary)))
