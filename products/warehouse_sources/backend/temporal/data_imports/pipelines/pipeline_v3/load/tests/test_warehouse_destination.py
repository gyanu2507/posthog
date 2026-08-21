from posthog.test.base import BaseTest

from products.warehouse_sources.backend.models.external_data_destination import (
    ExternalDataDestination,
    ExternalDataDestinationJob,
)
from products.warehouse_sources.backend.models.external_data_job import ExternalDataJob
from products.warehouse_sources.backend.models.external_data_schema import ExternalDataSchema
from products.warehouse_sources.backend.models.external_data_source import ExternalDataSource
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.load.warehouse_destination import (
    complete_warehouse_child,
    fail_warehouse_child,
    warehouse_child_for_job,
)
from products.warehouse_sources.backend.types import ExternalDataSourceType

COMPLETED = ExternalDataJob.Status.COMPLETED
FAILED = ExternalDataJob.Status.FAILED
RUNNING = ExternalDataJob.Status.RUNNING


class WarehouseDestinationTestBase(BaseTest):
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

    def _child(self, type_: str, name: str) -> ExternalDataDestinationJob:
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


class TestWarehouseChildLookup(WarehouseDestinationTestBase):
    def test_a_run_that_does_not_fan_out_has_no_child(self) -> None:
        # None is what keeps the original single-owner path for unflagged runs.
        assert warehouse_child_for_job(str(self.job.id), self.team.pk) is None

    def test_the_warehouse_child_is_found_among_its_peers(self) -> None:
        self._child(ExternalDataDestination.Type.REDSHIFT, "redshift")
        warehouse = self._child(ExternalDataDestination.Type.POSTHOG_WAREHOUSE, "warehouse")

        found = warehouse_child_for_job(str(self.job.id), self.team.pk)

        assert found is not None
        assert found.id == warehouse.id

    def test_a_run_with_only_external_destinations_has_no_warehouse_child(self) -> None:
        self._child(ExternalDataDestination.Type.REDSHIFT, "redshift")

        assert warehouse_child_for_job(str(self.job.id), self.team.pk) is None


class TestWarehouseCompletion(WarehouseDestinationTestBase):
    def test_the_run_stays_open_while_a_peer_is_still_draining(self) -> None:
        warehouse = self._child(ExternalDataDestination.Type.POSTHOG_WAREHOUSE, "warehouse")
        self._child(ExternalDataDestination.Type.REDSHIFT, "redshift")

        closed = complete_warehouse_child(warehouse, team_id=self.team.pk, run_uuid="run-a1")

        assert closed is False
        self.job.refresh_from_db()
        assert self.job.status == RUNNING

    def test_the_warehouse_closes_a_run_it_is_the_only_destination_of(self) -> None:
        warehouse = self._child(ExternalDataDestination.Type.POSTHOG_WAREHOUSE, "warehouse")

        closed = complete_warehouse_child(warehouse, team_id=self.team.pk, run_uuid="run-a1")

        assert closed is True
        self.job.refresh_from_db()
        assert self.job.status == COMPLETED

    def test_a_warehouse_failure_leaves_a_peer_free_to_keep_going(self) -> None:
        warehouse = self._child(ExternalDataDestination.Type.POSTHOG_WAREHOUSE, "warehouse")
        redshift = self._child(ExternalDataDestination.Type.REDSHIFT, "redshift")

        fail_warehouse_child(warehouse, team_id=self.team.pk, run_uuid="run-a1", error="delta write failed")

        self.job.refresh_from_db()
        redshift.refresh_from_db()
        assert self.job.status == RUNNING
        assert redshift.status == RUNNING
        warehouse.refresh_from_db()
        assert warehouse.status == FAILED
