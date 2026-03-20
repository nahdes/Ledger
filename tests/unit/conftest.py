"""
tests/unit/conftest.py

Unit test infrastructure — zero database dependencies.

FakeEventStore: in-memory drop-in replacement for EventStore.
  - seed() pre-loads events into a stream
  - tracks all appended events for assertion
  - will_raise() simulates OCC failures on demand

All unit tests follow: Given (seed events) → When (command/assertion) → Then (check)
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

from src.models.events import (
    AgentSessionStarted,
    ApplicationSubmitted,
    StoredEvent,
    StreamMetadata,
    StreamNotFoundError,
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def stored(event: Any, stream_id: str, position: int) -> StoredEvent:
    """Convert any BaseEvent into a StoredEvent at a given stream position."""
    payload = event.model_dump(
        exclude={"event_type", "event_version"}, mode="json"
    )
    return StoredEvent(
        event_id        = uuid.uuid4(),
        stream_id       = stream_id,
        stream_position = position,
        global_position = position,
        event_type      = event.event_type,
        event_version   = event.event_version,
        payload         = payload,
        metadata        = {},
        recorded_at     = utcnow(),
    )


# =============================================================================
# FakeEventStore
# =============================================================================

class FakeEventStore:
    """
    In-memory EventStore for unit tests.
    API is identical to src.event_store.EventStore.
    """

    def __init__(self) -> None:
        self._streams: dict[str, list[StoredEvent]] = {}
        self.appended: list[tuple[str, list[Any]]]  = []
        self._raise_on_append: Exception | None      = None

    # ── setup helpers ──────────────────────────────────────────────────────────

    def seed(self, stream_id: str, events: list[StoredEvent]) -> None:
        self._streams[stream_id] = list(events)

    def will_raise(self, exc: Exception) -> None:
        self._raise_on_append = exc

    # ── EventStore interface ───────────────────────────────────────────────────

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        all_events = self._streams.get(stream_id, [])
        result = [e for e in all_events if e.stream_position > from_position]
        if to_position is not None:
            result = [e for e in result if e.stream_position <= to_position]
        return result

    async def append(
        self,
        stream_id: str,
        events: list[Any],
        expected_version: int,
        **kwargs: Any,
    ) -> int:
        if self._raise_on_append:
            exc = self._raise_on_append
            self._raise_on_append = None
            raise exc

        self.appended.append((stream_id, list(events)))
        existing = self._streams.get(stream_id, [])
        version  = expected_version if expected_version >= 0 else 0
        new_rows: list[StoredEvent] = []
        for evt in events:
            version += 1
            new_rows.append(stored(evt, stream_id, version))
        self._streams[stream_id] = existing + new_rows
        return version

    async def stream_version(self, stream_id: str) -> int:
        if stream_id not in self._streams:
            raise StreamNotFoundError(stream_id)
        rows = self._streams[stream_id]
        return rows[-1].stream_position if rows else 0

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        if stream_id not in self._streams:
            raise StreamNotFoundError(stream_id)
        return StreamMetadata(
            stream_id       = stream_id,
            aggregate_type  = "Test",
            current_version = len(self._streams[stream_id]),
            created_at      = utcnow(),
            archived_at     = None,
            metadata        = {},
        )

    async def archive_stream(self, stream_id: str) -> None:
        pass  # no-op in tests

    # ── assertion helpers ──────────────────────────────────────────────────────

    @property
    def last_appended_events(self) -> list[Any]:
        return self.appended[-1][1] if self.appended else []

    @property
    def last_appended_stream(self) -> str:
        return self.appended[-1][0] if self.appended else ""

    def all_appended_event_types(self) -> list[str]:
        return [e.event_type for _, evts in self.appended for e in evts]


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def fake_store() -> FakeEventStore:
    return FakeEventStore()


# ── Pre-built starting states ──────────────────────────────────────────────────

@pytest.fixture
def submitted_application():
    """(app_id, [StoredEvent]) — freshly submitted loan."""
    app_id    = "APEX-TEST-001"
    stream_id = f"loan-{app_id}"
    events    = [
        stored(
            ApplicationSubmitted(
                application_id       = app_id,
                applicant_id         = "COMP-001",
                requested_amount_usd = 500_000.0,
                loan_purpose         = "expansion",
                submission_channel   = "web",
                submitted_at         = utcnow(),
            ),
            stream_id, 1,
        )
    ]
    return app_id, events


@pytest.fixture
def started_agent_session():
    """(agent_type, session_id, stream_id, [StoredEvent])"""
    agent_type = "credit_analysis"
    session_id = "sess-cre-test001"
    stream_id  = f"agent-{agent_type}-{session_id}"
    events     = [
        stored(
            AgentSessionStarted(
                session_id          = session_id,
                agent_type          = agent_type,
                agent_id            = "credit-agent-1",
                application_id      = "APEX-TEST-001",
                model_version       = "claude-sonnet-4-20250514",
                context_source      = "fresh",
                context_token_count = 1500,
                started_at          = utcnow(),
            ),
            stream_id, 1,
        )
    ]
    return agent_type, session_id, stream_id, events
