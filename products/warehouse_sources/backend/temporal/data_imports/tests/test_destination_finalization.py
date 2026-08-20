from posthog.test.base import BaseTest

import structlog

from products.warehouse_sources.backend.models.external_data_destination import (
    ExternalDataDestination,
    ExternalDataDestinationJob,
)
from products.warehouse_sources.backend.models.external_data_job import ExternalDataJob
from products.warehouse_sources.backend.models.external_data_schema import ExternalDataSchema
from products.warehouse_sources.backend.models.external_data_source import ExternalDataSource
from products.warehouse_sources.backend.temporal.data_imports.destination_finalization import (
    cascade_destination_jobs,
    finalize_destination_job_and_maybe_close_parent,
)
from products.warehouse_sources.backend.temporal.data_imports.metrics import LOCK_TAKEOVER_LATEST_ERROR
from products.warehouse_sources.backend.types import ExternalDataSourceType

logger = structlog.get_logger(__name__)

COMPLETED = ExternalDataJob.Status.COMPLETED
FAILED = ExternalDataJob.Status.FAILED
RUNNING = ExternalDataJob.Status.RUNNING


class DestinationFinalizationTestBase(BaseTest):
    def setUp(self) -> None:
        super().setUp()
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
            status=RUNNING,
            rows_synced=0,
            pipeline_version=ExternalDataJob.PipelineVersion.V3,
        )

    def _child(self, name: str, type_: str = ExternalDataDestination.Type.REDSHIFT) -> ExternalDataDestinationJob:
        destination = ExternalDataDestination.objects.for_team(self.team.pk).create(
            team_id=self.team.pk, type=type_, name=name
        )
        return ExternalDataDestinationJob.objects.for_team(self.team.pk).create(
            team_id=self.team.pk,
            job=self.job,
            destination=destination,
            destination_type=type_,
            destination_name=name,
            status=RUNNING,
        )

    def _finalize(self, child, status, error=None, rows=None, run_uuid=None):
        return finalize_destination_job_and_maybe_close_parent(
            destination_job_id=str(child.id),
            team_id=self.team.pk,
            status=status,
            logger=logger,
            latest_error=error,
            rows_synced=rows,
            run_uuid=run_uuid,
        )

    def _parent_status(self) -> str:
        self.job.refresh_from_db()
        return self.job.status


class TestParentClose(DestinationFinalizationTestBase):
    def test_parent_stays_running_until_every_child_is_terminal(self) -> None:
        warehouse = self._child("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)
        self._child("redshift")

        self._finalize(warehouse, COMPLETED)

        assert self._parent_status() == RUNNING

    def test_parent_completes_when_all_children_complete(self) -> None:
        warehouse = self._child("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)
        redshift = self._child("redshift")

        self._finalize(warehouse, COMPLETED)
        self._finalize(redshift, COMPLETED)

        assert self._parent_status() == COMPLETED

    def test_one_failed_destination_fails_the_run_and_names_it(self) -> None:
        warehouse = self._child("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)
        redshift = self._child("redshift")

        self._finalize(warehouse, COMPLETED)
        self._finalize(redshift, FAILED, error="connection refused")

        self.job.refresh_from_db()
        assert self.job.status == FAILED
        assert "redshift" in (self.job.latest_error or "")
        assert "connection refused" in (self.job.latest_error or "")

    def test_a_succeeded_sibling_keeps_its_completed_status_when_the_run_fails(self) -> None:
        warehouse = self._child("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)
        redshift = self._child("redshift")

        self._finalize(warehouse, COMPLETED)
        self._finalize(redshift, FAILED, error="boom")

        warehouse.refresh_from_db()
        assert warehouse.status == COMPLETED
        assert warehouse.finished_at is not None

    def test_the_run_closes_whichever_order_children_land_in(self) -> None:
        warehouse = self._child("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)
        redshift = self._child("redshift")

        self._finalize(redshift, FAILED, error="boom")
        assert self._parent_status() == RUNNING

        self._finalize(warehouse, COMPLETED)
        assert self._parent_status() == FAILED

    def test_finalizing_the_same_child_twice_is_idempotent(self) -> None:
        warehouse = self._child("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)

        self._finalize(warehouse, COMPLETED, rows=10)
        first_finished_at = ExternalDataDestinationJob.objects.for_team(self.team.pk).get(id=warehouse.id).finished_at

        self._finalize(warehouse, COMPLETED, rows=10)

        warehouse.refresh_from_db()
        assert warehouse.finished_at == first_finished_at
        assert self._parent_status() == COMPLETED

    def test_a_childs_first_failure_reason_survives_a_later_write(self) -> None:
        redshift = self._child("redshift")

        self._finalize(redshift, FAILED, error="the real reason")
        self._finalize(redshift, FAILED, error="Cancelled")

        redshift.refresh_from_db()
        assert redshift.latest_error == "the real reason"

    def test_a_terminal_parent_is_not_reopened_by_a_late_child(self) -> None:
        redshift = self._child("redshift")
        self.job.status = FAILED
        self.job.latest_error = "extraction failed"
        self.job.save()

        self._finalize(redshift, COMPLETED)

        self.job.refresh_from_db()
        assert self.job.status == FAILED
        assert self.job.latest_error == "extraction failed"
        redshift.refresh_from_db()
        assert redshift.status == COMPLETED

    def test_a_takeover_failed_parent_can_still_be_completed(self) -> None:
        warehouse = self._child("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)
        self.job.status = FAILED
        self.job.latest_error = LOCK_TAKEOVER_LATEST_ERROR
        self.job.save()
        warehouse.status = FAILED
        warehouse.latest_error = LOCK_TAKEOVER_LATEST_ERROR
        warehouse.save()

        self._finalize(warehouse, COMPLETED)

        assert self._parent_status() == COMPLETED

    def test_a_warehouse_less_run_gets_a_parent_row_count(self) -> None:
        redshift = self._child("redshift")

        self._finalize(redshift, COMPLETED, rows=42)

        self.job.refresh_from_db()
        assert self.job.rows_synced == 42


class TestCursorPromotion(DestinationFinalizationTestBase):
    # The staging id is attempt-scoped and only the consumer holding the batch knows it; the job
    # row does not carry it. Deriving it from the job instead would silently never promote, so
    # every incremental sync would re-extract the same window forever.
    STAGED_RUN_UUID = "run-abc-a1"

    def setUp(self) -> None:
        super().setUp()
        self.schema.sync_type = ExternalDataSchema.SyncType.INCREMENTAL
        self.schema.sync_type_config = {
            "incremental_field": "created",
            "incremental_field_last_value": 100,
            "incremental_staged": {"run_uuid": self.STAGED_RUN_UUID, "last_value": 200},
        }
        self.schema.save()

    def _cursor(self) -> int | None:
        self.schema.refresh_from_db()
        return self.schema.sync_type_config.get("incremental_field_last_value")

    def test_the_cursor_advances_once_every_destination_succeeded(self) -> None:
        warehouse = self._child("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)
        redshift = self._child("redshift")

        self._finalize(warehouse, COMPLETED, run_uuid=self.STAGED_RUN_UUID)
        self._finalize(redshift, COMPLETED, run_uuid=self.STAGED_RUN_UUID)

        assert self._cursor() == 200

    def test_the_cursor_is_held_when_a_destination_failed(self) -> None:
        warehouse = self._child("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)
        redshift = self._child("redshift")

        self._finalize(warehouse, COMPLETED, run_uuid=self.STAGED_RUN_UUID)
        self._finalize(redshift, FAILED, error="boom", run_uuid=self.STAGED_RUN_UUID)

        assert self._cursor() == 100

    def test_a_stale_run_uuid_does_not_promote_another_runs_window(self) -> None:
        warehouse = self._child("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)

        self._finalize(warehouse, COMPLETED, run_uuid="some-other-run-a1")

        assert self._cursor() == 100


class TestCascadeDestinationJobs(DestinationFinalizationTestBase):
    def test_non_terminal_children_are_forced_terminal(self) -> None:
        self._child("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)
        self._child("redshift")

        changed = cascade_destination_jobs(
            job_id=str(self.job.id), team_id=self.team.pk, status=FAILED, latest_error="Extraction failed"
        )

        assert changed == 2
        statuses = {
            c.status for c in ExternalDataDestinationJob.objects.for_team(self.team.pk).filter(job_id=self.job.id)
        }
        assert statuses == {FAILED}

    def test_already_terminal_children_keep_their_outcome(self) -> None:
        warehouse = self._child("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)
        self._finalize(warehouse, COMPLETED)
        self._child("redshift")

        changed = cascade_destination_jobs(
            job_id=str(self.job.id), team_id=self.team.pk, status=FAILED, latest_error="Cancelled"
        )

        assert changed == 1
        warehouse.refresh_from_db()
        assert warehouse.status == COMPLETED
