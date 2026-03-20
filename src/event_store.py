"""
src/event_store.py

Async EventStore backed by PostgreSQL via asyncpg.

Key design decisions (from DESIGN.md):
  - OCC via SELECT ... FOR UPDATE on event_streams row (no advisory locks)
  - Outbox written in the SAME transaction as events (at-least-once delivery)
  - Upcasting applied transparently on load — raw payload in DB is never touched
  - load_all() is an async generator for projection daemon backpressure
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import asyncpg

from src.models.events import (
    OptimisticConcurrencyError,
    PreconditionFailedError,
    StoredEvent,
    StreamMetadata,
    StreamNotFoundError,
)
from src.upcasting.registry import UpcasterRegistry


# ── helpers ────────────────────────────────────────────────────────────────────

def _infer_aggregate_type(stream_id: str) -> str:
    prefix = stream_id.split("-")[0]
    return {
        "loan":       "LoanApplication",
        "docpkg":     "DocumentPackage",
        "agent":      "AgentSession",
        "credit":     "CreditRecord",
        "fraud":      "FraudRecord",
        "compliance": "ComplianceRecord",
    }.get(prefix, "Unknown")


def _parse_jsonb(value: Any) -> dict:
    """asyncpg returns JSONB as dict on Linux/Mac but as str on Windows. Handle both."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    return json.loads(value)


def _to_stored(row: asyncpg.Record, registry: UpcasterRegistry) -> StoredEvent:
    raw = StoredEvent(
        event_id        = row["event_id"],
        stream_id       = row["stream_id"],
        stream_position = row["stream_position"],
        global_position = row["global_position"],
        event_type      = row["event_type"],
        event_version   = row["event_version"],
        payload         = _parse_jsonb(row["payload"]),
        metadata        = _parse_jsonb(row["metadata"]),
        recorded_at     = row["recorded_at"],
    )
    return registry.upcast(raw)


# =============================================================================
# EventStore
# =============================================================================

class EventStore:
    """
    Async append-only event store.

    Fixture for unit tests: replace with FakeEventStore (tests/unit/conftest.py)
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        upcaster_registry: UpcasterRegistry,
        outbox_destinations: list[str] | None = None,
    ) -> None:
        self._pool        = pool
        self._registry    = upcaster_registry
        self._destinations = outbox_destinations or ["redis-streams"]

    # ── append ────────────────────────────────────────────────────────────────

    async def append(
        self,
        stream_id: str,
        events: list[Any],        # list[BaseEvent]
        expected_version: int,    # -1 = new stream
        correlation_id: str | None = None,
        causation_id: str | None = None,
        actor: str | None = None,
    ) -> int:
        """
        Atomically append events to a stream.

        Raises OptimisticConcurrencyError if expected_version != current_version.
        Raises PreconditionFailedError if the stream is archived.
        Returns the new stream version.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():

                if expected_version == -1:
                    # ── New stream ─────────────────────────────────────────────
                    try:
                        await conn.execute(
                            """
                            INSERT INTO event_streams (stream_id, aggregate_type, current_version)
                            VALUES ($1, $2, 0)
                            """,
                            stream_id,
                            _infer_aggregate_type(stream_id),
                        )
                    except asyncpg.UniqueViolationError:
                        # Race: another writer created it first → treat as OCC
                        row = await conn.fetchrow(
                            "SELECT current_version FROM event_streams WHERE stream_id = $1",
                            stream_id,
                        )
                        raise OptimisticConcurrencyError(stream_id, -1, row["current_version"])

                    current_version = 0

                else:
                    # ── Existing stream — SELECT FOR UPDATE ────────────────────
                    row = await conn.fetchrow(
                        """
                        SELECT current_version, archived_at
                        FROM   event_streams
                        WHERE  stream_id = $1
                        FOR UPDATE
                        """,
                        stream_id,
                    )
                    if row is None:
                        raise StreamNotFoundError(stream_id)
                    if row["archived_at"] is not None:
                        raise PreconditionFailedError(
                            f"Stream {stream_id} is archived — no further appends allowed"
                        )
                    current_version = row["current_version"]
                    if current_version != expected_version:
                        raise OptimisticConcurrencyError(
                            stream_id, expected_version, current_version
                        )

                # ── Write events ───────────────────────────────────────────────
                metadata: dict[str, Any] = {}
                if correlation_id:
                    metadata["correlation_id"] = correlation_id
                if causation_id:
                    metadata["causation_id"] = causation_id
                if actor:
                    metadata["actor"] = actor

                new_version = current_version
                inserted_event_ids: list[uuid.UUID] = []

                for event in events:
                    new_version += 1
                    payload = event.model_dump(
                        exclude={"event_type", "event_version"}, mode="json"
                    )
                    eid = uuid.uuid4()
                    await conn.execute(
                        """
                        INSERT INTO events
                          (event_id, stream_id, stream_position,
                           event_type, event_version, payload, metadata, recorded_at)
                        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8)
                        """,
                        eid,
                        stream_id,
                        new_version,
                        event.event_type,
                        event.event_version,
                        json.dumps(payload),
                        json.dumps(metadata),
                        datetime.now(timezone.utc),
                    )
                    inserted_event_ids.append(eid)

                # ── Update stream version ──────────────────────────────────────
                await conn.execute(
                    """
                    UPDATE event_streams
                    SET    current_version = $2
                    WHERE  stream_id       = $1
                    """,
                    stream_id,
                    new_version,
                )

                # ── Write outbox (same transaction) ────────────────────────────
                for eid, event in zip(inserted_event_ids, events):
                    payload = event.model_dump(mode="json")
                    payload["stream_id"]   = stream_id
                    payload["event_type"]  = event.event_type
                    for dest in self._destinations:
                        await conn.execute(
                            """
                            INSERT INTO outbox (event_id, destination, payload)
                            VALUES ($1, $2, $3::jsonb)
                            """,
                            eid,
                            dest,
                            json.dumps(payload),
                        )

                return new_version

    # ── load_stream ───────────────────────────────────────────────────────────

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        """Load events from a stream, optionally slice by position."""
        async with self._pool.acquire() as conn:
            if to_position is not None:
                rows = await conn.fetch(
                    """
                    SELECT * FROM events
                    WHERE  stream_id       = $1
                    AND    stream_position  > $2
                    AND    stream_position <= $3
                    ORDER  BY stream_position
                    """,
                    stream_id, from_position, to_position,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM events
                    WHERE  stream_id      = $1
                    AND    stream_position > $2
                    ORDER  BY stream_position
                    """,
                    stream_id, from_position,
                )
        return [_to_stored(r, self._registry) for r in rows]

    # ── load_all (async generator for ProjectionDaemon) ───────────────────────

    async def load_all(
        self,
        from_global_position: int = 0,
        batch_size: int = 100,
    ) -> AsyncIterator[StoredEvent]:
        """Yield all events globally ordered — used for projection replay."""
        last_pos = from_global_position
        async with self._pool.acquire() as conn:
            while True:
                rows = await conn.fetch(
                    """
                    SELECT * FROM events
                    WHERE  global_position > $1
                    ORDER  BY global_position
                    LIMIT  $2
                    """,
                    last_pos, batch_size,
                )
                if not rows:
                    break
                for row in rows:
                    stored = _to_stored(row, self._registry)
                    last_pos = stored.global_position
                    yield stored

    # ── stream_version ────────────────────────────────────────────────────────

    async def stream_version(self, stream_id: str) -> int:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
        if row is None:
            raise StreamNotFoundError(stream_id)
        return row["current_version"]

    # ── get_stream_metadata ───────────────────────────────────────────────────

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
        if row is None:
            raise StreamNotFoundError(stream_id)
        return StreamMetadata(
            stream_id       = row["stream_id"],
            aggregate_type  = row["aggregate_type"],
            current_version = row["current_version"],
            created_at      = row["created_at"],
            archived_at     = row["archived_at"],
            metadata        = _parse_jsonb(row["metadata"]),
        )

    # ── archive_stream ────────────────────────────────────────────────────────

    async def archive_stream(self, stream_id: str) -> None:
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE event_streams
                SET    archived_at = NOW()
                WHERE  stream_id   = $1
                AND    archived_at IS NULL
                """,
                stream_id,
            )
        if result == "UPDATE 0":
            raise PreconditionFailedError(
                f"Stream {stream_id} not found or already archived"
            )