"""Covers the seams between the pieces the delivery path is assembled from.

Each part has its own tests; what this file checks is that they agree with each other, because
every bug that made it through so far lived in a handoff rather than inside a component.

`transaction=True` is load-bearing: the path spans the app database and the queue database, and
the queue side is reached over its own connection. Inside a rolled-back test transaction the two
connections deadlock against each other rather than seeing each other's rows.
"""

import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from unittest import mock

from django.conf import settings

import psycopg
import pyarrow as pa
from asgiref.sync import async_to_sync

from posthog.models import Organization, Team

from products.warehouse_sources.backend.models.external_data_destination import (
    ExternalDataDestination,
    ExternalDataDestinationJob,
)
from products.warehouse_sources.backend.models.external_data_job import ExternalDataJob
from products.warehouse_sources.backend.models.external_data_schema import ExternalDataSchema
from products.warehouse_sources.backend.models.external_data_source import ExternalDataSource
from products.warehouse_sources.backend.temporal.data_imports.destination_jobs import run_destinations_for_job
from products.warehouse_sources.backend.temporal.data_imports.destinations.contracts import BatchWriteOutcome
from products.warehouse_sources.backend.temporal.data_imports.destinations.registry import register_destination_writer
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.destinations_load import processor
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.destinations_queue.jobs_db import (
    DestinationQueue,
)
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.postgres_queue.destination_queue import (
    insert_batch_destinations,
    insert_run_destinations,
)
from products.warehouse_sources.backend.types import ExternalDataSourceType

pytestmark = pytest.mark.django_db(transaction=True)

OWNER = "delivery-flow-test"
REDSHIFT = ExternalDataDestination.Type.REDSHIFT


class RecordingWriter:
    """Stands in for a real destination so the test can assert on what it was handed."""

    holds_sync_lock = False
    runs_post_load = False
    calls: list[tuple[str, Any]] = []

    def __init__(self, ctx) -> None:
        self._ctx = ctx

    async def prepare_run(self, ctx) -> None:
        RecordingWriter.calls.append(("prepare", ctx))

    async def write_batch(self, batches: AsyncIterator[pa.RecordBatch], ctx) -> BatchWriteOutcome:
        rows = 0
        async for batch in batches:
            rows += batch.num_rows
        RecordingWriter.calls.append(("write", ctx))
        return BatchWriteOutcome(rows_written=rows)

    async def finalize_run(self, ctx) -> None:
        RecordingWriter.calls.append(("finalize", ctx))

    async def abort_run(self, ctx) -> None:
        RecordingWriter.calls.append(("abort", ctx))


async def _fake_batches(_s3_path, batch_rows=None) -> AsyncIterator[pa.RecordBatch]:
    yield pa.RecordBatch.from_pydict({"id": [1, 2, 3]})


class Fixture:
    def __init__(self) -> None:
        RecordingWriter.calls = []
        register_destination_writer(REDSHIFT, RecordingWriter)

        self.org = Organization.objects.create(name="delivery flow")
        self.team = Team.objects.create(organization=self.org, name="delivery flow")
        self.source = ExternalDataSource.objects.create(
            team=self.team,
            source_id="src",
            connection_id="conn",
            status="Running",
            source_type=ExternalDataSourceType.STRIPE,
        )
        self.schema = ExternalDataSchema.objects.create(team=self.team, source=self.source, name="charges")
        self.job = ExternalDataJob.objects.create(
            team=self.team,
            pipeline=self.source,
            schema=self.schema,
            status=ExternalDataJob.Status.RUNNING,
            rows_synced=0,
            pipeline_version=ExternalDataJob.PipelineVersion.V3,
        )
        self.destination = ExternalDataDestination.objects.for_team(self.team.pk).create(
            team_id=self.team.pk, type=REDSHIFT, name="analytics", config={"table_name": "charges"}
        )
        self.child = ExternalDataDestinationJob.objects.for_team(self.team.pk).create(
            team_id=self.team.pk,
            job=self.job,
            destination=self.destination,
            destination_type=REDSHIFT,
            destination_name="analytics",
            config_snapshot={"table_name": "charges"},
            status=ExternalDataJob.Status.RUNNING,
            rows_synced=0,
        )
        self.run_uuid = str(uuid.uuid4())
        self.conn = psycopg.connect(settings.WAREHOUSE_SOURCES_DATABASE_URL, autocommit=True)

    def enqueue(self, batch_index: int, is_final: bool) -> None:
        destinations = run_destinations_for_job(self.job)
        assert destinations, "the run's children should resolve into queue destinations"
        insert_run_destinations(
            self.conn,
            team_id=self.team.pk,
            schema_id=str(self.schema.id),
            source_id=str(self.source.id),
            job_id=str(self.job.id),
            run_uuid=self.run_uuid,
            destinations=destinations,
        )
        batch_row = self.conn.execute(
            """
            INSERT INTO sourcebatch (
                team_id, schema_id, source_id, job_id, run_uuid, batch_index, s3_path,
                row_count, byte_size, is_final_batch, sync_type, resource_name, metadata, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, 3, 100, %s, 'incremental', 'charges',
                '{"primary_keys": ["id"]}', now()
            ) RETURNING id
            """,
            (
                self.team.pk,
                str(self.schema.id),
                str(self.source.id),
                str(self.job.id),
                self.run_uuid,
                batch_index,
                f"s3://bucket/{self.run_uuid}/part-{batch_index}.parquet",
                is_final,
            ),
        ).fetchone()
        assert batch_row is not None
        insert_batch_destinations(
            self.conn,
            batch_id=str(batch_row[0]),
            team_id=self.team.pk,
            schema_id=str(self.schema.id),
            run_uuid=self.run_uuid,
            batch_index=batch_index,
            is_final_batch=is_final,
            sync_type="incremental",
            destinations=destinations,
        )

    def claim_one(self):
        claimed = DestinationQueue.claim(
            self.conn, owner_token=OWNER, limit=5, lease_ttl_seconds=60, retry_backoff_seconds=15
        )
        assert len(claimed) == 1, f"expected exactly one claimable item, got {len(claimed)}"
        return claimed[0]

    def process(self, item) -> int:
        async def run() -> int:
            with mock.patch.object(processor, "aiter_record_batches", _fake_batches):
                return await processor.process_work_item(item, self.conn)

        return async_to_sync(run)()

    def cleanup(self) -> None:
        self.conn.execute(
            "DELETE FROM sourcebatchdestinationstatus WHERE work_item_id IN "
            "(SELECT id FROM sourcebatchdestination WHERE team_id = %s)",
            (self.team.pk,),
        )
        for table in (
            "sourcebatchdestinationapply",
            "sourcebatchdestination",
            "sourcerundestination",
            "sourcedestinationgrouplease",
            "sourcebatch",
        ):
            self.conn.execute(f"DELETE FROM {table} WHERE team_id = %s", (self.team.pk,))
        self.conn.close()
        self.org.delete()


@pytest.fixture
def flow():
    fixture = Fixture()
    yield fixture
    fixture.cleanup()


def test_a_run_reaches_its_destination_and_closes(flow) -> None:
    flow.enqueue(batch_index=0, is_final=True)
    item = flow.claim_one()

    rows = flow.process(item)

    assert rows == 3
    # The writer was handed the run's real coordinates, not placeholders.
    _, write_ctx = next(call for call in RecordingWriter.calls if call[0] == "write")
    assert write_ctx.run.table_name == "charges"
    assert write_ctx.run.primary_keys == ("id",)
    assert write_ctx.run.sync_type == "incremental"
    # The run is committed at the destination before anything records it as delivered.
    assert [name for name, _ in RecordingWriter.calls] == ["prepare", "write", "finalize"]

    flow.child.refresh_from_db()
    flow.job.refresh_from_db()
    assert flow.child.status == ExternalDataJob.Status.COMPLETED
    assert flow.child.rows_synced == 3
    assert flow.job.status == ExternalDataJob.Status.COMPLETED


def test_a_batch_already_delivered_is_not_written_again(flow) -> None:
    flow.enqueue(batch_index=0, is_final=False)
    item = flow.claim_one()
    flow.process(item)
    RecordingWriter.calls = []

    # A crash between the destination's commit and the state write re-claims the batch.
    flow.process(item)

    assert [name for name, _ in RecordingWriter.calls] == []


def test_a_cancelled_run_stops_its_queued_batches(flow) -> None:
    flow.enqueue(batch_index=0, is_final=True)
    item = flow.claim_one()
    ExternalDataDestinationJob.objects.for_team(flow.team.pk).filter(id=flow.child.id).update(
        status=ExternalDataJob.Status.FAILED, latest_error="Cancelled"
    )

    with pytest.raises(processor.DestinationJobGoneError):
        flow.process(item)

    assert RecordingWriter.calls == []
