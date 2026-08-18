from posthog.test.base import BaseTest
from unittest.mock import MagicMock, patch

from django.apps import apps

from parameterized import parameterized

from products.signals.backend.models import SignalReport, SignalScoutRun
from products.signals.backend.scout_harness.slack_charts import MAX_SLACK_REPORT_CHARTS, build_scout_report_chart_blocks
from products.signals.backend.scout_harness.slack_delivery import build_scout_report_slack_message

_TRENDS = {"kind": "InsightVizNode", "source": {"kind": "TrendsQuery", "series": [{"event": "$pageview"}]}}
_SAVED = {"kind": "SavedInsightNode", "shortId": "abc123xy"}
_SQL = {"kind": "DataVisualizationNode", "source": {"kind": "HogQLQuery", "query": "select 1"}}


def _chart(chart_id: str, query: dict, **extra: object) -> dict:
    return {"chart_id": chart_id, "title": f"Chart {chart_id}", "query": query, **extra}


class TestScoutSlackReportCharts(BaseTest):
    def _make_run(self, *, created_by=None) -> SignalScoutRun:
        Task = apps.get_model("tasks", "Task")
        TaskRun = apps.get_model("tasks", "TaskRun")
        task = Task.objects.create(
            team=self.team,
            created_by=created_by,
            title="scout run",
            description="scout run",
            origin_product=Task.OriginProduct.SIGNALS_SCOUT,
        )
        task_run = TaskRun.objects.create(task=task, team=self.team)
        return SignalScoutRun.all_teams.create(
            task_run=task_run, team=self.team, skill_name="signals-scout-product-analytics", skill_version=1
        )

    def _make_report(self, charts: list[dict], summary: str = "Signups dropped.") -> SignalReport:
        return SignalReport.objects.create(
            team=self.team, status=SignalReport.Status.READY, title="Signups", summary=summary, charts=charts
        )

    def _patched_render(self):
        render = patch("products.signals.backend.scout_harness.slack_charts.render_png_export")
        url = patch("products.signals.backend.scout_harness.slack_charts.get_delivery_image_url")
        return render, url

    def test_renders_supported_charts_and_skips_the_rest_without_failing(self) -> None:
        run = self._make_run(created_by=self.user)
        report = self._make_report(
            [
                _chart("trend", _TRENDS, caption="Daily signups, last 30 days"),
                _chart("sql", _SQL),
                _chart("saved", _SAVED),
                _chart("broken", _TRENDS),
            ]
        )
        assets = iter(
            [(MagicMock(id=11), b"png"), (MagicMock(id=12), b"png"), (MagicMock(id=13, exception="boom"), None)]
        )
        render, url = self._patched_render()
        with render as render_mock, url as url_mock:
            render_mock.side_effect = lambda **_: next(assets)
            url_mock.side_effect = lambda *, team_id, asset_id, expiry_delta: f"https://img/{asset_id}"
            blocks = build_scout_report_chart_blocks(report, run)

        assert [call.kwargs.get("insight_short_id") for call in render_mock.call_args_list] == [None, "abc123xy", None]
        assert render_mock.call_args_list[0].kwargs["export_context"] == {"source": _TRENDS}
        assert render_mock.call_args_list[0].kwargs["created_by"] == self.user
        assert [b for b in blocks if b["type"] == "image"] == [
            {"type": "image", "image_url": "https://img/11", "alt_text": "Chart trend"},
            {"type": "image", "image_url": "https://img/12", "alt_text": "Chart saved"},
        ]
        assert blocks[2] == {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "Daily signups, last 30 days"}],
        }

    def test_referenced_charts_render_first_and_the_cap_holds(self) -> None:
        run = self._make_run(created_by=self.user)
        charts = [_chart(f"c{i}", _TRENDS) for i in range(MAX_SLACK_REPORT_CHARTS + 2)]
        report = self._make_report(charts, summary="See [the last one](chart:c4) and [c1](chart:c1).")
        render, url = self._patched_render()
        with render as render_mock, url as url_mock:
            render_mock.return_value = (MagicMock(id=1), b"png")
            url_mock.return_value = "https://img/1"
            blocks = build_scout_report_chart_blocks(report, run)

        titles = [b["text"]["text"] for b in blocks if b["type"] == "section"]
        assert titles == ["*Chart c4*", "*Chart c1*", "*Chart c0*"]
        assert render_mock.call_count == MAX_SLACK_REPORT_CHARTS

    @parameterized.expand([("no_acting_user", None), ("no_charts", "user")])
    def test_returns_nothing_without_a_principal_or_charts(self, _name, actor) -> None:
        run = self._make_run(created_by=self.user if actor else None)
        report = self._make_report([_chart("trend", _TRENDS)] if actor is None else [])
        render, url = self._patched_render()
        with render as render_mock, url:
            assert build_scout_report_chart_blocks(report, run) == []
        render_mock.assert_not_called()

    def test_render_budget_stops_further_charts(self) -> None:
        run = self._make_run(created_by=self.user)
        report = self._make_report([_chart("a", _TRENDS), _chart("b", _TRENDS)])
        clock = iter([0.0, 0.0, 10_000.0])
        render, url = self._patched_render()
        with render as render_mock, url as url_mock:
            render_mock.return_value = (MagicMock(id=1), b"png")
            url_mock.return_value = "https://img/1"
            blocks = build_scout_report_chart_blocks(report, run, clock=lambda: next(clock))

        assert render_mock.call_count == 1
        assert len([b for b in blocks if b["type"] == "image"]) == 1

    def test_report_message_places_charts_between_prose_and_link(self) -> None:
        run = self._make_run(created_by=self.user)
        report = self._make_report([_chart("trend", _TRENDS)], summary="Look at [signups](chart:trend).")
        render, url = self._patched_render()
        with render as render_mock, url as url_mock:
            render_mock.return_value = (MagicMock(id=1), b"png")
            url_mock.return_value = "https://img/1"
            blocks, _ = build_scout_report_slack_message(report, run)

        assert [b["type"] for b in blocks] == ["context", "header", "section", "section", "image", "actions"]
        assert blocks[2]["text"]["text"] == "Look at signups."
