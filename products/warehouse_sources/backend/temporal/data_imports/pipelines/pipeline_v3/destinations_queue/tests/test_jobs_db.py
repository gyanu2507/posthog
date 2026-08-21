import json
import uuid

import pytest

from django.conf import settings

import psycopg

from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.destinations_queue.jobs_db import (
    DestinationQueue,
)
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.postgres_queue.destination_queue import (
    RunDestination,
    insert_batch_destinations,
    insert_run_destinations,
)

OWNER = "test-owner"
OTHER_OWNER = "other-owner"
TTL = 60
BACKOFF = 15

WAREHOUSE = "PostHogWarehouse"
REDSHIFT = "Redshift"


@pytest.fixture
def conn():
    connection = psycopg.connect(settings.WAREHOUSE_SOURCES_DATABASE_URL, autocommit=True)
    yield connection
    connection.close()


class QueueFixture:
    """Writes batches and their destination work items the way the producer does."""

    def __init__(self, conn: psycopg.Connection) -> None:
        self.conn = conn
        self.team_id = 90_000_000 + int(uuid.uuid4().int % 1_000_000)
        self.schema_id = str(uuid.uuid4())
        self.source_id = str(uuid.uuid4())

    def destination(self, destination_type: str = REDSHIFT) -> RunDestination:
        return RunDestination(
            destination_job_id=str(uuid.uuid4()),
            destination_id=str(uuid.uuid4()),
            destination_type=destination_type,
            config={"table": "target"},
        )

    def run(
        self,
        destinations: list[RunDestination],
        *,
        batches: int = 1,
        sync_type: str = "incremental",
        run_uuid: str | None = None,
        age_interval: str = "0 seconds",
    ) -> str:
        run_uuid = run_uuid or str(uuid.uuid4())
        job_id = str(uuid.uuid4())
        insert_run_destinations(
            self.conn,
            team_id=self.team_id,
            schema_id=self.schema_id,
            source_id=self.source_id,
            job_id=job_id,
            run_uuid=run_uuid,
            destinations=destinations,
        )
        for index in range(batches):
            batch_row = self.conn.execute(
                """
                INSERT INTO sourcebatch (
                    team_id, schema_id, source_id, job_id, run_uuid, batch_index, s3_path,
                    row_count, byte_size, is_final_batch, sync_type, resource_name, metadata, created_at
                ) VALUES (
                    %(team_id)s, %(schema_id)s, %(source_id)s, %(job_id)s, %(run_uuid)s, %(batch_index)s,
                    %(s3_path)s, 10, 100, %(is_final)s, %(sync_type)s, 'table',
                    %(metadata)s, now()
                ) RETURNING id
                """,
                {
                    "team_id": self.team_id,
                    "schema_id": self.schema_id,
                    "source_id": self.source_id,
                    "job_id": job_id,
                    "run_uuid": run_uuid,
                    "batch_index": index,
                    "s3_path": f"s3://bucket/{run_uuid}/part-{index}.parquet",
                    "is_final": index == batches - 1,
                    "sync_type": sync_type,
                    "metadata": json.dumps({"primary_keys": ["id"]}),
                },
            ).fetchone()
            assert batch_row is not None
            batch_id = batch_row[0]
            self.conn.execute(
                f"UPDATE sourcebatch SET created_at = now() - interval '{age_interval}' WHERE id = %s",
                (batch_id,),
            )
            insert_batch_destinations(
                self.conn,
                batch_id=str(batch_id),
                team_id=self.team_id,
                schema_id=self.schema_id,
                run_uuid=run_uuid,
                batch_index=index,
                is_final_batch=index == batches - 1,
                sync_type=sync_type,
                destinations=destinations,
            )
            self.conn.execute(
                f"UPDATE sourcebatchdestination SET created_at = now() - interval '{age_interval}' WHERE batch_id = %s",
                (batch_id,),
            )
        return run_uuid

    def claim(self, limit: int = 10, **kwargs):
        return DestinationQueue.claim(
            self.conn,
            owner_token=kwargs.pop("owner_token", OWNER),
            limit=limit,
            lease_ttl_seconds=TTL,
            retry_backoff_seconds=BACKOFF,
            **kwargs,
        )

    def cleanup(self) -> None:
        for table in (
            "sourcebatchdestinationstatus",
            "sourcebatchdestinationapply",
            "sourcebatchdestination",
            "sourcerundestination",
            "sourcedestinationgrouplease",
            "sourcebatch",
        ):
            column = "work_item_id" if table == "sourcebatchdestinationstatus" else "team_id"
            if column == "team_id":
                self.conn.execute(f"DELETE FROM {table} WHERE team_id = %s", (self.team_id,))
            else:
                self.conn.execute(
                    "DELETE FROM sourcebatchdestinationstatus WHERE work_item_id IN "
                    "(SELECT id FROM sourcebatchdestination WHERE team_id = %s)",
                    (self.team_id,),
                )


@pytest.fixture
def queue(conn):
    fixture = QueueFixture(conn)
    yield fixture
    fixture.cleanup()


class TestClaim:
    def test_each_destination_gets_its_own_work_item(self, queue) -> None:
        warehouse, redshift = queue.destination(WAREHOUSE), queue.destination(REDSHIFT)
        queue.run([warehouse, redshift])

        claimed = queue.claim()

        assert {c.destination_type for c in claimed} == {WAREHOUSE, REDSHIFT}
        assert all(c.s3_path.endswith("part-0.parquet") for c in claimed)
        assert all(c.metadata["primary_keys"] == ["id"] for c in claimed)

    def test_a_fleet_only_claims_the_types_it_owns(self, queue) -> None:
        queue.run([queue.destination(WAREHOUSE), queue.destination(REDSHIFT)])

        warehouse_only = queue.claim(destination_types=[WAREHOUSE])

        assert [c.destination_type for c in warehouse_only] == [WAREHOUSE]

    def test_a_fleet_can_claim_everything_except_the_types_it_excludes(self, queue) -> None:
        queue.run([queue.destination(WAREHOUSE), queue.destination(REDSHIFT)])

        external_only = queue.claim(exclude_destination_types=[WAREHOUSE])

        assert [c.destination_type for c in external_only] == [REDSHIFT]

    def test_one_destination_failing_does_not_block_the_others(self, queue) -> None:
        warehouse, redshift = queue.destination(WAREHOUSE), queue.destination(REDSHIFT)
        run_uuid = queue.run([warehouse, redshift], batches=2)

        DestinationQueue.fail_run(
            queue.conn,
            run_uuid=run_uuid,
            destination_job_id=redshift.destination_job_id,
            error={"message": "connection refused"},
        )

        claimed = queue.claim()
        assert [c.destination_type for c in claimed] == [WAREHOUSE]

    def test_batches_of_a_run_are_claimed_in_order(self, queue) -> None:
        redshift = queue.destination(REDSHIFT)
        queue.run([redshift], batches=3)

        first = queue.claim()

        assert [c.batch_index for c in first] == [0]

    def test_a_later_batch_unlocks_once_the_earlier_one_succeeds(self, queue) -> None:
        redshift = queue.destination(REDSHIFT)
        queue.run([redshift], batches=2)
        first = queue.claim()[0]

        DestinationQueue.set_state(queue.conn, work_item_id=first.work_item_id, state="succeeded", attempt=1)

        assert [c.batch_index for c in queue.claim()] == [1]

    def test_a_group_leased_by_another_pod_is_not_claimable(self, queue) -> None:
        queue.run([queue.destination(REDSHIFT)])
        queue.claim(owner_token=OTHER_OWNER)

        assert queue.claim(owner_token=OWNER) == []

    def test_cdc_runs_are_left_alone(self, queue) -> None:
        queue.run([queue.destination(REDSHIFT)], sync_type="cdc")

        assert queue.claim() == []

    def test_work_older_than_retention_is_not_claimed(self, queue) -> None:
        queue.run([queue.destination(REDSHIFT)], age_interval="8 days")

        assert queue.claim() == []

    def test_an_older_unfinished_run_blocks_a_newer_one(self, queue) -> None:
        redshift = queue.destination(REDSHIFT)
        queue.run([redshift], run_uuid="older-run", age_interval="1 hour")
        queue.run([redshift], run_uuid="newer-run")

        claimed = queue.claim()

        assert [c.run_uuid for c in claimed] == ["older-run"]


class TestIdempotencyMarkers:
    def test_applying_is_recorded_once(self, queue) -> None:
        redshift = queue.destination(REDSHIFT)
        run_uuid = queue.run([redshift])
        args = {
            "team_id": queue.team_id,
            "schema_id": queue.schema_id,
            "run_uuid": run_uuid,
            "batch_index": 0,
            "destination_job_id": redshift.destination_job_id,
        }

        assert DestinationQueue.has_been_applied(queue.conn, **args) is False
        DestinationQueue.mark_applied(queue.conn, **args, row_count=10)
        DestinationQueue.mark_applied(queue.conn, **args, row_count=10)

        assert DestinationQueue.has_been_applied(queue.conn, **args) is True


class TestLeases:
    def test_renewing_a_lease_we_hold_succeeds(self, queue) -> None:
        queue.run([queue.destination(REDSHIFT)])
        claimed = queue.claim()[0]

        assert DestinationQueue.renew_lease(queue.conn, group=claimed.group_key, owner_token=OWNER, ttl_seconds=TTL)

    def test_renewing_a_lease_another_pod_took_fails(self, queue) -> None:
        queue.run([queue.destination(REDSHIFT)])
        claimed = queue.claim()[0]
        DestinationQueue.release_lease(queue.conn, group=claimed.group_key, owner_token=OWNER)
        queue.claim(owner_token=OTHER_OWNER)

        assert not DestinationQueue.renew_lease(queue.conn, group=claimed.group_key, owner_token=OWNER, ttl_seconds=TTL)
