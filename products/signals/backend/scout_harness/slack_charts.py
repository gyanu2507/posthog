from __future__ import annotations

import re
from collections.abc import Callable
from datetime import timedelta
from time import monotonic

from django.core.cache import cache

import structlog

from posthog.models import Team, User

from products.exports.backend.facade.api import RENDER_TIMEOUT, get_delivery_image_url, render_png_export
from products.signals.backend.models import SignalReport, SignalScoutRun
from products.signals.backend.slack_formatting import escape_slack_mrkdwn

logger = structlog.get_logger(__name__)

# Each render can hold the Celery worker for the facade's RENDER_TIMEOUT, and the delivery task
# retries the whole message on transient failure, so both the count and the total time are bounded.
# A render only starts if it can finish inside the budget, so the budget is the worst case, not the
# point after which no more start. Charts past either bound still show in the inbox; the Slack
# message just links there.
MAX_SLACK_REPORT_CHARTS = 3
SLACK_REPORT_CHART_RENDER_BUDGET_SECONDS = 240

# Slack re-fetches image_url after the message is posted, so the token has to outlive the post
# by a comfortable margin; matches what task-run chart delivery uses.
SLACK_REPORT_CHART_URL_TTL = timedelta(days=30)

# The delivery task retries the whole message when Slack fails, and its backoff can span hours.
# Rendered asset ids are remembered per delivery for longer than that, so a retry re-posts the
# same PNGs instead of launching every export workflow again.
_RENDERED_ASSETS_CACHE_TTL_SECONDS = 24 * 60 * 60

# The exporter renders an InsightVizNode-wrapped query; a SavedInsightNode is rendered through the
# insight it points at. DataVisualizationNode (SQL) has no PNG render path yet, so it is left to
# the inbox.
_RENDERABLE_CHART_KINDS = frozenset({"InsightVizNode", "SavedInsightNode"})

# The chart ids the summary points at, in the order the prose reaches them. Deliberately looser
# than the inbox's link parse: this only decides which charts go first when the cap bites, and
# a false positive costs nothing more than ordering.
_CHART_REF_ID_RE = re.compile(r"chart:([a-z0-9][a-z0-9_-]*)")


def _referenced_chart_ids(summary: str) -> list[str]:
    seen: dict[str, None] = {}
    for match in _CHART_REF_ID_RE.finditer(summary):
        seen.setdefault(match.group(1), None)
    return list(seen)


def _ordered_charts(report: SignalReport) -> list[dict]:
    charts = [chart for chart in (report.charts or []) if isinstance(chart, dict)]
    by_id = {chart.get("chart_id"): chart for chart in charts if isinstance(chart.get("chart_id"), str)}
    referenced = [by_id[chart_id] for chart_id in _referenced_chart_ids(report.summary or "") if chart_id in by_id]
    referenced_ids = {chart["chart_id"] for chart in referenced}
    unreferenced = [chart for chart in charts if chart.get("chart_id") not in referenced_ids]
    return referenced + unreferenced


def _render_chart_asset_id(*, team: Team, created_by: User, query: dict) -> int | None:
    kind = query.get("kind")
    if kind == "SavedInsightNode":
        short_id = query.get("shortId")
        if not isinstance(short_id, str) or not short_id:
            return None
        asset, png = render_png_export(team=team, created_by=created_by, insight_short_id=short_id)
    else:
        asset, png = render_png_export(team=team, created_by=created_by, export_context={"source": query})
    if png is None:
        logger.warning("signals_scout.slack_report_chart_render_failed", asset_id=asset.id, error=asset.exception)
        return None
    return asset.id


def _chart_blocks(chart: dict, image_url: str) -> list[dict]:
    title = " ".join(str(chart.get("title") or "Chart").split())
    blocks: list[dict] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*{escape_slack_mrkdwn(title)}*"}},
        {"type": "image", "image_url": image_url, "alt_text": title[:2000]},
    ]
    caption = chart.get("caption")
    if isinstance(caption, str) and caption.strip():
        blocks.append(
            {"type": "context", "elements": [{"type": "mrkdwn", "text": escape_slack_mrkdwn(caption.strip())}]}
        )
    return blocks


def _acting_user(run: SignalScoutRun) -> User | None:
    """The user the scout ran as; the render is attributed to and access-checked against them."""
    task_run = getattr(run, "task_run", None)
    task = getattr(task_run, "task", None)
    return getattr(task, "created_by", None)


def _rendered_assets_cache_key(delivery_id: str) -> str:
    return f"signals_scout:slack_report_chart_assets:{delivery_id}"


# The cache only saves re-renders on retry; if it is down the message must still go out, so both
# sides degrade to "no reuse" rather than raising into the delivery task.
def _load_rendered_assets(cache_key: str | None) -> dict[str, int]:
    if cache_key is None:
        return {}
    try:
        cached = cache.get(cache_key)
    except Exception:
        logger.warning("signals_scout.slack_report_chart_cache_read_failed", exc_info=True)
        return {}
    return dict(cached) if isinstance(cached, dict) else {}


def _store_rendered_assets(cache_key: str | None, rendered_assets: dict[str, int]) -> None:
    if cache_key is None or not rendered_assets:
        return
    try:
        cache.set(cache_key, rendered_assets, timeout=_RENDERED_ASSETS_CACHE_TTL_SECONDS)
    except Exception:
        logger.warning("signals_scout.slack_report_chart_cache_write_failed", exc_info=True)


def build_scout_report_chart_blocks(
    report: SignalReport,
    run: SignalScoutRun,
    *,
    delivery_id: str | None = None,
    clock: Callable[[], float] = monotonic,
) -> list[dict]:
    """Render the report's charts to PNGs and return Slack blocks that show them.

    Best effort by design: any chart that cannot be rendered — an unsupported query kind, a
    render failure, no acting user, the cap or the time budget — is skipped rather than failing the
    delivery, since the message still links to the report where the inbox draws every chart.

    The cap counts attempts, not successes: a report full of failing charts must not launch an
    export workflow per chart. With a `delivery_id`, successful renders are remembered so a retry
    of the same delivery reuses them."""
    charts = _ordered_charts(report)
    if not charts:
        return []
    created_by = _acting_user(run)
    if created_by is None:
        logger.info("signals_scout.slack_report_chart_no_acting_user", report_id=str(report.id), run_id=str(run.id))
        return []

    cache_key = _rendered_assets_cache_key(delivery_id) if delivery_id else None
    rendered_assets = _load_rendered_assets(cache_key)
    started = clock()
    blocks: list[dict] = []
    attempts = 0
    for chart in charts:
        if attempts >= MAX_SLACK_REPORT_CHARTS:
            break
        query = chart.get("query")
        if not isinstance(query, dict) or query.get("kind") not in _RENDERABLE_CHART_KINDS:
            continue
        chart_id = str(chart.get("chart_id"))
        asset_id = rendered_assets.get(chart_id)
        if (
            asset_id is None
            and clock() - started + RENDER_TIMEOUT.total_seconds() > SLACK_REPORT_CHART_RENDER_BUDGET_SECONDS
        ):
            logger.info("signals_scout.slack_report_chart_budget_exhausted", report_id=str(report.id))
            break
        attempts += 1
        try:
            if asset_id is None:
                asset_id = _render_chart_asset_id(team=report.team, created_by=created_by, query=query)
                if asset_id is None:
                    continue
            image_url = get_delivery_image_url(
                team_id=report.team_id, asset_id=asset_id, expiry_delta=SLACK_REPORT_CHART_URL_TTL
            )
        except Exception:
            logger.warning(
                "signals_scout.slack_report_chart_render_error",
                report_id=str(report.id),
                chart_id=chart_id,
                exc_info=True,
            )
            continue
        if image_url is None:
            continue
        rendered_assets[chart_id] = asset_id
        blocks.extend(_chart_blocks(chart, image_url))
    _store_rendered_assets(cache_key, rendered_assets)
    return blocks
