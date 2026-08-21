"""The loop that drains destination work items.

Deliberately softer than the warehouse loader it runs beside. That loader retries a batch
three times over about a minute, because a failing Delta write is usually our problem and
failing fast surfaces it. A destination is someone else's database: it goes down for
maintenance, rejects connections under load, and comes back. Retrying for hours is the
difference between riding out a maintenance window and gapping a customer's table.

Falling behind is bounded by retention, not by patience. Staged parquet is pruned after seven
days, so `oldest_eligible_age_seconds` is the number to alert on: past that, the batches are
gone and the run has to be re-extracted.
"""

from __future__ import annotations

import uuid
import asyncio
from collections.abc import Callable
from dataclasses import dataclass

import psycopg
import structlog

from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.destinations_load.processor import (
    DestinationJobGoneError,
    abort_run_for_item,
    fail_destination_job,
    process_work_item,
)
from products.warehouse_sources.backend.temporal.data_imports.pipelines.pipeline_v3.destinations_queue.jobs_db import (
    ClaimedWorkItem,
    DestinationQueue,
)

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class DestinationConsumerConfig:
    database_url: str
    poll_interval_seconds: float = 2.0
    poll_limit: int = 25
    # Roughly forty minutes of active retrying before a destination's run is given up on,
    # against the warehouse loader's ~45 seconds.
    max_attempts: int = 8
    retry_backoff_seconds: int = 300
    lease_ttl_seconds: int = 900
    destination_types: list[str] | None = None
    exclude_destination_types: list[str] | None = None


class DestinationConsumer:
    def __init__(self, config: DestinationConsumerConfig, heartbeat: Callable[[], None] = lambda: None) -> None:
        self._config = config
        self._owner_token = str(uuid.uuid4())
        self._heartbeat = heartbeat
        # Set from the signal handler, which runs on the event loop, so an Event is enough.
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        logger.info(
            "destination_consumer_started",
            owner=self._owner_token,
            destination_types=self._config.destination_types,
            exclude_destination_types=self._config.exclude_destination_types,
        )
        with psycopg.connect(self._config.database_url, autocommit=True) as conn:
            while not self._stopping.is_set():
                try:
                    claimed = await asyncio.to_thread(self._claim, conn)
                except Exception as e:
                    # A failed poll is usually the queue database blipping. Keep looping so the
                    # consumer recovers on its own rather than needing a restart.
                    logger.exception("destination_claim_failed", error=str(e))
                    await asyncio.sleep(self._config.poll_interval_seconds)
                    continue

                # A poll that returned, empty or not, is proof the loop is alive.
                self._heartbeat()

                if not claimed:
                    await asyncio.sleep(self._config.poll_interval_seconds)
                    continue

                for item in claimed:
                    if self._stopping.is_set():
                        break
                    await self._process_one(conn, item)

    def _claim(self, conn: psycopg.Connection) -> list[ClaimedWorkItem]:
        return DestinationQueue.claim(
            conn,
            owner_token=self._owner_token,
            limit=self._config.poll_limit,
            lease_ttl_seconds=self._config.lease_ttl_seconds,
            retry_backoff_seconds=self._config.retry_backoff_seconds,
            destination_types=self._config.destination_types,
            exclude_destination_types=self._config.exclude_destination_types,
        )

    async def _process_one(self, conn: psycopg.Connection, item: ClaimedWorkItem) -> None:
        attempt = item.attempt + 1
        await asyncio.to_thread(
            DestinationQueue.set_state, conn, work_item_id=item.work_item_id, state="executing", attempt=attempt
        )

        try:
            await process_work_item(item, conn)
        except DestinationJobGoneError as e:
            # The run was cancelled or swept while this batch sat in the queue. Stop the rest of
            # it for this destination; the child job is already terminal.
            logger.info("destination_batch_dropped", reason=str(e))
            await asyncio.to_thread(
                DestinationQueue.fail_run,
                conn,
                run_uuid=item.run_uuid,
                destination_job_id=item.destination_job_id,
                error={"message": str(e)},
            )
            return
        except Exception as e:
            await self._handle_failure(conn, item, attempt, e)
            return

        await asyncio.to_thread(
            DestinationQueue.set_state, conn, work_item_id=item.work_item_id, state="succeeded", attempt=attempt
        )

    async def _handle_failure(
        self, conn: psycopg.Connection, item: ClaimedWorkItem, attempt: int, error: Exception
    ) -> None:
        payload = {"message": str(error), "type": type(error).__name__}

        if attempt < self._config.max_attempts:
            logger.warning(
                "destination_batch_retrying",
                destination_type=item.destination_type,
                batch_index=item.batch_index,
                attempt=attempt,
                error=str(error),
            )
            await asyncio.to_thread(
                DestinationQueue.set_state,
                conn,
                work_item_id=item.work_item_id,
                state="waiting_retry",
                attempt=attempt,
                error=payload,
            )
            return

        logger.error(
            "destination_run_failed",
            destination_type=item.destination_type,
            batch_index=item.batch_index,
            attempts=attempt,
            error=str(error),
        )
        # Fail this destination's remaining batches, drop whatever it had staged, and record the
        # outcome on its child job. Nothing here touches the warehouse path or the run's other
        # destinations.
        await asyncio.to_thread(
            DestinationQueue.fail_run,
            conn,
            run_uuid=item.run_uuid,
            destination_job_id=item.destination_job_id,
            error=payload,
        )
        await abort_run_for_item(item)
        await asyncio.to_thread(fail_destination_job, item, str(error))
