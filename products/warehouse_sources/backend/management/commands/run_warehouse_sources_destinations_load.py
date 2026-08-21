"""Drains destination work items into the destinations a run syncs to.

Runs beside `run_warehouse_sources_load` off the same queue tables. The two are separated by
claim scope, not by code: this one claims the destination types it can write, that one claims
the PostHog warehouse. Splitting them keeps a slow external destination off the loader that
owns the warehouse, and lets each keep its own retry budget and memory profile.
"""

from __future__ import annotations

import signal
import asyncio
from typing import Any

from django.conf import settings
from django.core.management.base import BaseCommand

import structlog

from posthog.temporal.common.logger import configure_logger

from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.destinations_load.builtin_writers import (
    builtin_destination_types,
    register_builtin_destination_writers,
)
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.destinations_load.consumer import (
    DestinationConsumer,
    DestinationConsumerConfig,
)
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.load.health import (
    HealthState,
    start_health_server,
)

logger = structlog.get_logger(__name__)


def _parse_types(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [part.strip() for part in raw.split(",") if part.strip()]


class Command(BaseCommand):
    help = "Deliver staged warehouse-source batches to their configured destinations"

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("--poll-interval", type=float, default=2.0, help="Seconds between claim polls.")
        parser.add_argument("--poll-limit", type=int, default=25, help="Work items claimed per poll.")
        parser.add_argument(
            "--max-attempts",
            type=int,
            default=8,
            help=(
                "Attempts before a destination's run is given up on. Higher than the warehouse "
                "loader's, so a destination's maintenance window does not gap a customer's table."
            ),
        )
        parser.add_argument(
            "--retry-backoff",
            type=int,
            default=300,
            help="Base seconds between attempts, multiplied by the attempt number.",
        )
        parser.add_argument("--lease-ttl", type=int, default=900, help="Seconds a claimed group stays leased.")
        parser.add_argument(
            "--claim-destination-types",
            type=str,
            default=None,
            help="Comma-separated destination types this consumer claims. Defaults to the ones it can write.",
        )
        parser.add_argument(
            "--claim-exclude-destination-types",
            type=str,
            default=None,
            help="Comma-separated destination types this consumer does NOT claim. Mutually exclusive with the above.",
        )
        parser.add_argument("--health-port", type=int, default=8081)
        parser.add_argument("--health-timeout", type=int, default=60)

    def handle(self, *args: Any, **options: Any) -> None:
        claim_types = _parse_types(options.get("claim_destination_types"))
        exclude_types = _parse_types(options.get("claim_exclude_destination_types"))
        if claim_types and exclude_types:
            raise SystemExit("--claim-destination-types and --claim-exclude-destination-types are mutually exclusive")

        register_builtin_destination_writers()
        # Without an explicit scope, claim only what this deployment has a writer for. Claiming a
        # type it cannot write would lease the group and then fail every batch in it.
        if not claim_types and not exclude_types:
            claim_types = builtin_destination_types()

        config = DestinationConsumerConfig(
            database_url=settings.WAREHOUSE_SOURCES_DATABASE_URL,
            poll_interval_seconds=options["poll_interval"],
            poll_limit=options["poll_limit"],
            max_attempts=options["max_attempts"],
            retry_backoff_seconds=options["retry_backoff"],
            lease_ttl_seconds=options["lease_ttl"],
            destination_types=claim_types,
            exclude_destination_types=exclude_types,
        )

        health = HealthState(timeout_seconds=options["health_timeout"])
        start_health_server(port=options["health_port"], health_state=health)

        asyncio.run(self._run(config, health))

    async def _run(self, config: DestinationConsumerConfig, health: HealthState) -> None:
        configure_logger(loop=asyncio.get_running_loop())
        consumer = DestinationConsumer(config, heartbeat=health.report_healthy)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, consumer.stop)

        await consumer.run()
        logger.info("destination_consumer_stopped")
