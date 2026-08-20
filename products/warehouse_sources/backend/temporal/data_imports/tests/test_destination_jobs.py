from posthog.test.base import BaseTest

from products.warehouse_sources.backend.models.external_data_destination import (
    ExternalDataDestination,
    ExternalDataDestinationJob,
    ExternalDataSourceDestination,
)
from products.warehouse_sources.backend.models.external_data_job import ExternalDataJob
from products.warehouse_sources.backend.models.external_data_schema import ExternalDataSchema
from products.warehouse_sources.backend.models.external_data_source import ExternalDataSource
from products.warehouse_sources.backend.temporal.data_imports.destination_jobs import (
    create_destination_jobs_for_run,
    has_warehouse_destination,
    watermark_start_for,
)
from products.warehouse_sources.backend.types import ExternalDataSourceType


class DestinationJobsTestBase(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.source = ExternalDataSource.objects.create(
            team=self.team,
            source_id="src",
            connection_id="conn",
            status="Running",
            source_type=ExternalDataSourceType.STRIPE,
        )
        self.schema = ExternalDataSchema.objects.create(
            team=self.team,
            source=self.source,
            name="charges",
            sync_type=ExternalDataSchema.SyncType.INCREMENTAL,
            sync_type_config={"incremental_field": "created", "incremental_field_last_value": 100},
        )

    def _destination(self, name: str, type_: str = ExternalDataDestination.Type.REDSHIFT) -> ExternalDataDestination:
        destination = ExternalDataDestination.objects.for_team(self.team.pk).create(
            team_id=self.team.pk, type=type_, name=name
        )
        ExternalDataSourceDestination.objects.for_team(self.team.pk).create(
            team_id=self.team.pk, source=self.source, destination=destination
        )
        return destination

    def _job(self, watermark_start: str | None, billable: bool = True) -> ExternalDataJob:
        return ExternalDataJob.objects.create(
            team=self.team,
            pipeline=self.source,
            schema=self.schema,
            status=ExternalDataJob.Status.RUNNING,
            rows_synced=0,
            billable=billable,
            watermark_start=watermark_start,
            pipeline_version=ExternalDataJob.PipelineVersion.V3,
        )


class TestCreateDestinationJobsForRun(DestinationJobsTestBase):
    def test_one_child_per_resolved_destination(self) -> None:
        self._destination("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)
        self._destination("redshift")

        children = create_destination_jobs_for_run(self._job("100"), self.schema)

        assert {c.destination_type for c in children} == {
            ExternalDataDestination.Type.POSTHOG_WAREHOUSE,
            ExternalDataDestination.Type.REDSHIFT,
        }
        assert has_warehouse_destination(children)

    def test_an_unconfigured_schema_still_syncs_to_the_warehouse(self) -> None:
        children = create_destination_jobs_for_run(self._job(None), self.schema)

        assert [c.destination_type for c in children] == [ExternalDataDestination.Type.POSTHOG_WAREHOUSE]

    def test_calling_twice_reuses_the_children(self) -> None:
        self._destination("redshift")
        job = self._job("100")

        first = create_destination_jobs_for_run(job, self.schema)
        second = create_destination_jobs_for_run(job, self.schema)

        assert [c.id for c in first] == [c.id for c in second]
        assert ExternalDataDestinationJob.objects.for_team(self.team.pk).filter(job_id=job.id).count() == 1

    def test_a_destination_that_already_delivered_this_window_does_not_bill_again(self) -> None:
        redshift = self._destination("redshift")
        warehouse = self._destination("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)

        # First run: the warehouse lands, Redshift fails, so the cursor never advanced.
        first_run = self._job("100")
        first_children = create_destination_jobs_for_run(first_run, self.schema)
        by_destination = {c.destination_id: c for c in first_children}
        by_destination[warehouse.id].status = ExternalDataJob.Status.COMPLETED
        by_destination[warehouse.id].save()
        by_destination[redshift.id].status = ExternalDataJob.Status.FAILED
        by_destination[redshift.id].save()

        # The retry re-extracts the same window.
        retry_children = create_destination_jobs_for_run(self._job("100"), self.schema)

        billable = {c.destination_id: c.billable for c in retry_children}
        assert billable[warehouse.id] is False
        assert billable[redshift.id] is True

    def test_a_later_window_bills_every_destination_again(self) -> None:
        warehouse = self._destination("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)

        first = create_destination_jobs_for_run(self._job("100"), self.schema)
        first[0].status = ExternalDataJob.Status.COMPLETED
        first[0].save()

        later = create_destination_jobs_for_run(self._job("200"), self.schema)

        assert {c.destination_id: c.billable for c in later}[warehouse.id] is True

    def test_a_non_billable_run_makes_every_child_non_billable(self) -> None:
        self._destination("redshift")
        self._destination("warehouse", ExternalDataDestination.Type.POSTHOG_WAREHOUSE)

        children = create_destination_jobs_for_run(self._job("100", billable=False), self.schema)

        assert all(c.billable is False for c in children)


class TestWatermarkStartFor(DestinationJobsTestBase):
    def test_incremental_schemas_snapshot_their_cursor(self) -> None:
        assert watermark_start_for(self.schema) == "100"

    def test_full_refresh_has_no_window_to_repeat(self) -> None:
        self.schema.sync_type = ExternalDataSchema.SyncType.FULL_REFRESH
        self.schema.save()

        assert watermark_start_for(self.schema) is None

    def test_a_first_incremental_run_has_no_cursor_yet(self) -> None:
        self.schema.sync_type_config = {"incremental_field": "created"}
        self.schema.save()

        assert watermark_start_for(self.schema) is None
