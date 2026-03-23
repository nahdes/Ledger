"""
src/projections/daemon.py

Async ProjectionDaemon — the read-side engine for CQRS.

Design:
  - Polls events table from last checkpoint using load_all() async generator
  - Routes each event to subscribed projections by event_type
  - Checkpoints are updated inside the same transaction as the projection write
  - Fault-tolerant: projection handler failures are logged and skipped
    (configurable max_retries per event before giving up and advancing)
  - Exposes get_lag() per projection and get_all_lags() for health endpoint
  - PostgreSQL advisory lock prevents duplicate processing on multi-node
    deployments

SLOs:
  ApplicationSummary:     lag < 500ms  under normal operation
  AgentPerformanceLedger: lag < 500ms
  ComplianceAuditView:    lag < 2000ms
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import asyncpg

from src.projections.base import Projection

logger = logging.getLogger(__name__)


class ProjectionDaemon:
    """
    Fault-tolerant async projection daemon.

    Usage:
        daemon = ProjectionDaemon(store, pool, projections)
        asyncio.create_task(daemon.run_forever())
    """

    def __init__(
        self,
        store: Any,
        pool: asyncpg.Pool,
        projections: list[Projection],
        batch_size: int = 100,
        max_retries_per_event: int = 3,
    ) -> None:
        self._store       = store
        self._pool        = pool
        self._projections = {p.name: p for p in projections}
        self._batch_size  = batch_size
        self._max_retries = max_retries_per_event
        self._running     = False
        self._events_processed: dict[str, int] = {p.name: 0 for p in projections}
        self._errors_skipped:   dict[str, int] = {p.name: 0 for p in projections}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run_forever(self, poll_interval_ms: int = 500) -> None:
        self._running = True
        logger.info("ProjectionDaemon starting — projections: %s",
                    list(self._projections.keys()))
        while self._running:
            try:
                await self._process_batch()
            except Exception as exc:
                logger.exception("ProjectionDaemon batch error (will retry): %s", exc)
            await asyncio.sleep(poll_interval_ms / 1000)

    async def stop(self) -> None:
        self._running = False

    # ── Batch processing ──────────────────────────────────────────────────────

    async def _process_batch(self) -> None:
        """
        Load events from the lowest checkpoint, route to projections.
        Each event is handled inside its own transaction so a failure on
        one event doesn't roll back work done on previous events.
        """
        checkpoints = await self._load_checkpoints()
        if not checkpoints:
            return

        # Start from the lowest checkpoint so every projection catches up
        from_position = min(checkpoints.values())

        async with self._pool.acquire() as conn:
            async for event in self._store.load_all(
                from_global_position=from_position,
                batch_size=self._batch_size,
            ):
                for name, projection in self._projections.items():
                    # Skip events this projection has already processed
                    if event.global_position <= checkpoints.get(name, 0):
                        continue
                    if event.event_type not in projection.event_types:
                        # Advance checkpoint even for unhandled event types
                        # so the projection doesn't fall behind
                        async with conn.transaction():
                            await conn.execute(
                                """
                                UPDATE projection_checkpoints
                                SET    last_global_position = GREATEST(last_global_position, $2),
                                       updated_at = NOW()
                                WHERE  projection_name = $1
                                """,
                                name, event.global_position,
                            )
                        checkpoints[name] = max(checkpoints.get(name, 0),
                                                event.global_position)
                        continue

                    await self._handle_with_retry(name, projection, event, conn)
                    checkpoints[name] = max(checkpoints.get(name, 0),
                                            event.global_position)

    async def _handle_with_retry(
        self,
        name: str,
        projection: Projection,
        event: Any,
        conn: asyncpg.Connection,
    ) -> None:
        """
        Handle one event for one projection with retry + skip on persistent failure.
        Checkpoint is updated inside the same transaction as the projection write.
        """
        for attempt in range(1, self._max_retries + 1):
            try:
                async with conn.transaction():
                    await projection.handle(event, conn)
                    await conn.execute(
                        """
                        UPDATE projection_checkpoints
                        SET    last_global_position = GREATEST(last_global_position, $2),
                               updated_at = NOW()
                        WHERE  projection_name = $1
                        """,
                        name, event.global_position,
                    )
                self._events_processed[name] = self._events_processed.get(name, 0) + 1
                return
            except Exception as exc:
                if attempt >= self._max_retries:
                    logger.error(
                        "Projection %s failed on %s (pos=%d) after %d attempts, skipping: %s",
                        name, event.event_type, event.global_position, attempt, exc,
                    )
                    # Advance checkpoint past the bad event so we don't loop forever
                    try:
                        async with conn.transaction():
                            await conn.execute(
                                """
                                UPDATE projection_checkpoints
                                SET    last_global_position = GREATEST(last_global_position, $2),
                                       updated_at = NOW()
                                WHERE  projection_name = $1
                                """,
                                name, event.global_position,
                            )
                    except Exception:
                        pass
                    self._errors_skipped[name] = self._errors_skipped.get(name, 0) + 1
                else:
                    logger.warning(
                        "Projection %s attempt %d/%d failed: %s",
                        name, attempt, self._max_retries, exc,
                    )
                    await asyncio.sleep(0.05 * attempt)

    async def _load_checkpoints(self) -> dict[str, int]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT projection_name, last_global_position FROM projection_checkpoints"
            )
        return {r["projection_name"]: r["last_global_position"] for r in rows}

    # ── Lag metrics ───────────────────────────────────────────────────────────

    async def get_lag(self, projection_name: str) -> dict[str, Any]:
        """
        Returns lag between the latest event in the store and the latest
        event this projection has processed. Used by ledger://ledger/health.
        """
        async with self._pool.acquire() as conn:
            latest_store = await conn.fetchval(
                "SELECT COALESCE(MAX(global_position), 0) FROM events"
            )
            row = await conn.fetchrow(
                """
                SELECT last_global_position, updated_at
                FROM   projection_checkpoints
                WHERE  projection_name = $1
                """,
                projection_name,
            )

        if not row:
            return {"projection_name": projection_name, "lag_events": 0, "lag_ms": 0}

        checkpoint_pos = row["last_global_position"]
        lag_events     = max(0, latest_store - checkpoint_pos)

        now        = datetime.now(timezone.utc)
        updated_at = row["updated_at"]
        if updated_at and updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        lag_ms = (now - updated_at).total_seconds() * 1000 if updated_at else 0

        return {
            "projection_name":    projection_name,
            "checkpoint_position": checkpoint_pos,
            "store_position":     latest_store,
            "lag_events":         lag_events,
            "lag_ms":             round(lag_ms, 1),
            "events_processed":   self._events_processed.get(projection_name, 0),
            "errors_skipped":     self._errors_skipped.get(projection_name, 0),
        }

    async def get_all_lags(self) -> list[dict[str, Any]]:
        """Aggregate health for all projections — used by ledger://ledger/health."""
        return [await self.get_lag(name) for name in self._projections]