from datetime import UTC, datetime, timedelta

from posthog.test.base import BaseTest

from posthog.tasks.usage_report import (
    get_teams_with_free_historical_rows_synced_in_period,
    get_teams_with_rows_synced_in_period,
)

from products.warehouse_sources.backend.models.external_data_destination import (
    ExternalDataDestination,
    ExternalDataDestinationJob,
)
from products.warehouse_sources.backend.models.external_data_job import ExternalDataJob
from products.warehouse_sources.backend.models.external_data_schema import ExternalDataSchema
from products.warehouse_sources.backend.models.external_data_source import ExternalDataSource
from products.warehouse_sources.backend.types import ExternalDataSourceType

# Well clear of the DWH free pricing window, so these exercise the normal billing path.
PERIOD_START = datetime(2026, 8, 1, tzinfo=UTC)
PERIOD_END = datetime(2026, 8, 31, tzinfo=UTC)
FINISHED_AT = datetime(2026, 8, 15, tzinfo=UTC)


class TestRowsSyncedBilling(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.source = ExternalDataSource.objects.create(
            team=self.team,
            source_id="src",
            connection_id="conn",
            status="Running",
            source_type=ExternalDataSourceType.STRIPE,
        )
        # Sources younger than 7 days sync free, so age this one out of that window.
        ExternalDataSource.objects.filter(id=self.source.id).update(
            created_at=PERIOD_END - timedelta(days=30),
        )
        self.schema = ExternalDataSchema.objects.create(team=self.team, source=self.source, name="charges")

    def _run(self, rows: int, billable: bool = True, status: str = ExternalDataJob.Status.COMPLETED):
        job = ExternalDataJob.objects.create(
            team=self.team,
            pipeline=self.source,
            schema=self.schema,
            status=status,
            rows_synced=rows,
            billable=billable,
            pipeline_version=ExternalDataJob.PipelineVersion.V3,
        )
        ExternalDataJob.objects.filter(id=job.id).update(finished_at=FINISHED_AT)
        return job

    def _child(self, job, rows: int, name: str, billable: bool = True, status: str = ExternalDataJob.Status.COMPLETED):
        destination = ExternalDataDestination.objects.for_team(self.team.pk).create(
            team_id=self.team.pk, type=ExternalDataDestination.Type.REDSHIFT, name=name
        )
        child = ExternalDataDestinationJob.objects.for_team(self.team.pk).create(
            team_id=self.team.pk,
            job=job,
            destination=destination,
            destination_type=destination.type,
            destination_name=name,
            status=status,
            rows_synced=rows,
            billable=billable,
        )
        ExternalDataDestinationJob.objects.for_team(self.team.pk).filter(id=child.id).update(finished_at=FINISHED_AT)
        return child

    def _billed(self) -> int:
        rows = get_teams_with_rows_synced_in_period(PERIOD_START, PERIOD_END)
        return next((r["total"] for r in rows if r["team_id"] == self.team.pk), 0)

    def test_a_run_with_no_children_bills_from_the_parent(self) -> None:
        self._run(rows=100)

        assert self._billed() == 100

    def test_a_run_with_children_bills_per_destination_not_from_the_parent(self) -> None:
        job = self._run(rows=100)
        self._child(job, rows=100, name="warehouse")
        self._child(job, rows=100, name="redshift")

        # 2 x 100, and the parent's own 100 must not be added on top.
        assert self._billed() == 200

    def test_a_failed_destination_is_not_billed(self) -> None:
        job = self._run(rows=100)
        self._child(job, rows=100, name="warehouse")
        self._child(job, rows=0, name="redshift", status=ExternalDataJob.Status.FAILED)

        assert self._billed() == 100

    def test_a_non_billable_child_is_not_billed(self) -> None:
        job = self._run(rows=100)
        self._child(job, rows=100, name="warehouse", billable=False)
        self._child(job, rows=100, name="redshift")

        assert self._billed() == 100

    def test_a_failed_run_still_bills_the_destinations_that_succeeded(self) -> None:
        job = self._run(rows=100, status=ExternalDataJob.Status.FAILED)
        self._child(job, rows=100, name="warehouse")
        self._child(job, rows=0, name="redshift", status=ExternalDataJob.Status.FAILED)

        assert self._billed() == 100

    def test_legacy_and_destination_runs_add_up(self) -> None:
        self._run(rows=50)
        job = self._run(rows=100)
        self._child(job, rows=100, name="warehouse")

        assert self._billed() == 150

    def test_new_sources_stay_out_of_billed_rows_and_in_free_historical(self) -> None:
        new_source = ExternalDataSource.objects.create(
            team=self.team,
            source_id="new",
            connection_id="conn2",
            status="Running",
            source_type=ExternalDataSourceType.STRIPE,
        )
        ExternalDataSource.objects.filter(id=new_source.id).update(created_at=PERIOD_END - timedelta(days=1))
        new_schema = ExternalDataSchema.objects.create(team=self.team, source=new_source, name="new")
        job = ExternalDataJob.objects.create(
            team=self.team,
            pipeline=new_source,
            schema=new_schema,
            status=ExternalDataJob.Status.COMPLETED,
            rows_synced=70,
            billable=True,
            pipeline_version=ExternalDataJob.PipelineVersion.V3,
        )
        ExternalDataJob.objects.filter(id=job.id).update(finished_at=FINISHED_AT)
        self._child(job, rows=70, name="redshift")

        free = get_teams_with_free_historical_rows_synced_in_period(PERIOD_START, PERIOD_END)

        assert self._billed() == 0
        assert next(r["total"] for r in free if r["team_id"] == self.team.pk) == 70
