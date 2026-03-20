"""
tests/integration/test_event_store.py

Integration tests for EventStore against real PostgreSQL.

Includes THE DOUBLE-DECISION TEST (graded):
  two concurrent tasks both try to append at expected_version=3.
  exactly one must win; the other must receive OptimisticConcurrencyError.
"""
from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone

import pytest

from src.event_store import EventStore
from src.models.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    DomainError,
    OptimisticConcurrencyError,
    PackageCreated,
    PreconditionFailedError,
    StreamNotFoundError,
)
from tests.conftest import utcnow


def new_app_id():
    return f"APEX-{uuid.uuid4().hex[:8].upper()}"


def make_submitted(app_id: str) -> ApplicationSubmitted:
    return ApplicationSubmitted(
        application_id       = app_id,
        applicant_id         = "COMP-001",
        requested_amount_usd = 500_000.0,
        loan_purpose         = "expansion",
        submission_channel   = "web",
        submitted_at         = utcnow(),
    )


def make_requested(app_id: str) -> CreditAnalysisRequested:
    return CreditAnalysisRequested(
        application_id = app_id,
        requested_at   = utcnow(),
        priority       = "NORMAL",
    )


def make_completed(app_id: str) -> CreditAnalysisCompleted:
    return CreditAnalysisCompleted(
        application_id       = app_id,
        session_id           = "sess-cre-test",
        decision             = {"risk_tier": "MEDIUM", "recommended_limit_usd": "450000"},
        model_version        = "claude-sonnet-4-20250514",
        input_data_hash      = "abc123",
        analysis_duration_ms = 1200,
        completed_at         = utcnow(),
    )


# =============================================================================
# Basic append and load
# =============================================================================

class TestAppendAndLoad:

    async def test_new_stream_returns_version_1(self, store: EventStore):
        app_id = new_app_id()
        version = await store.append(f"loan-{app_id}", [make_submitted(app_id)], expected_version=-1)
        assert version == 1

    async def test_appended_event_is_loadable(self, store: EventStore):
        app_id    = new_app_id()
        stream_id = f"loan-{app_id}"
        await store.append(stream_id, [make_submitted(app_id)], expected_version=-1)
        events = await store.load_stream(stream_id)
        assert len(events) == 1
        assert events[0].event_type == "ApplicationSubmitted"

    async def test_sequential_positions(self, store: EventStore):
        app_id    = new_app_id()
        stream_id = f"loan-{app_id}"
        await store.append(stream_id, [make_submitted(app_id)], expected_version=-1)
        await store.append(stream_id, [make_requested(app_id)], expected_version=1)
        await store.append(stream_id, [make_requested(app_id)], expected_version=2)
        events = await store.load_stream(stream_id)
        assert [e.stream_position for e in events] == [1, 2, 3]

    async def test_payload_roundtrips(self, store: EventStore):
        app_id = new_app_id()
        await store.append(f"loan-{app_id}", [make_submitted(app_id)], expected_version=-1)
        events = await store.load_stream(f"loan-{app_id}")
        assert events[0].payload["application_id"] == app_id
        assert events[0].payload["requested_amount_usd"] == 500_000.0

    async def test_correlation_id_in_metadata(self, store: EventStore):
        app_id  = new_app_id()
        corr_id = str(uuid.uuid4())
        await store.append(
            f"loan-{app_id}", [make_submitted(app_id)],
            expected_version=-1, correlation_id=corr_id
        )
        events = await store.load_stream(f"loan-{app_id}")
        assert events[0].metadata.get("correlation_id") == corr_id

    async def test_load_stream_from_position(self, store: EventStore):
        app_id    = new_app_id()
        stream_id = f"loan-{app_id}"
        await store.append(stream_id, [make_submitted(app_id)], expected_version=-1)
        await store.append(stream_id, [make_requested(app_id)], expected_version=1)
        await store.append(stream_id, [make_requested(app_id)], expected_version=2)
        events = await store.load_stream(stream_id, from_position=1)
        assert len(events) == 2
        assert events[0].stream_position == 2

    async def test_load_all_global_order(self, store: EventStore):
        for _ in range(3):
            app_id = new_app_id()
            await store.append(f"loan-{app_id}", [make_submitted(app_id)], expected_version=-1)
        positions = []
        async for evt in store.load_all(from_global_position=0):
            positions.append(evt.global_position)
        assert positions == sorted(positions)
        assert len(positions) == 3


# =============================================================================
# Optimistic Concurrency Control
# =============================================================================

class TestOptimisticConcurrencyControl:

    async def test_wrong_version_raises(self, store: EventStore):
        app_id    = new_app_id()
        stream_id = f"loan-{app_id}"
        await store.append(stream_id, [make_submitted(app_id)], expected_version=-1)
        with pytest.raises(OptimisticConcurrencyError) as exc:
            await store.append(stream_id, [make_requested(app_id)], expected_version=99)
        assert exc.value.expected_version == 99
        assert exc.value.actual_version   == 1

    async def test_occ_error_has_required_fields(self, store: EventStore):
        app_id    = new_app_id()
        stream_id = f"loan-{app_id}"
        await store.append(stream_id, [make_submitted(app_id)], expected_version=-1)
        try:
            await store.append(stream_id, [make_submitted(app_id)], expected_version=99)
        except OptimisticConcurrencyError as e:
            d = e.to_dict()
            for key in ("error_type", "stream_id", "expected_version",
                        "actual_version", "suggested_action"):
                assert key in d, f"Missing key: {key}"
            assert d["suggested_action"] == "reload_stream_and_retry"

    @pytest.mark.graded
    async def test_concurrent_appends_exactly_one_wins(self, store: EventStore):
        """
        THE DOUBLE-DECISION TEST (graded).

        Seed the stream to version 3.
        Two concurrent tasks both try expected_version=3.
        Exactly one must win; one must lose.
        The stream must end at version 4 (not 5).
        """
        app_id    = new_app_id()
        stream_id = f"loan-{app_id}"

        # Seed to version 3
        await store.append(stream_id, [make_submitted(app_id)], expected_version=-1)
        await store.append(stream_id, [make_requested(app_id)], expected_version=1)
        await store.append(stream_id, [make_requested(app_id)], expected_version=2)

        results: dict[str, int]                   = {}
        errors:  dict[str, OptimisticConcurrencyError] = {}

        async def agent(name: str) -> None:
            try:
                v = await store.append(
                    stream_id, [make_completed(app_id)], expected_version=3
                )
                results[name] = v
            except OptimisticConcurrencyError as e:
                errors[name] = e

        await asyncio.gather(agent("A"), agent("B"))

        # (a) Exactly one winner
        assert len(results) == 1, f"Expected 1 winner, got {len(results)}: {results}"
        assert len(errors)  == 1, f"Expected 1 error, got {len(errors)}"

        # (b) Winner is at position 4
        assert list(results.values())[0] == 4

        # (c) Loser has correct error context
        err = list(errors.values())[0]
        assert err.expected_version == 3
        assert err.actual_version   == 4

        # (d) Stream has exactly 4 events — no split-brain
        events = await store.load_stream(stream_id)
        assert len(events) == 4, f"Expected 4 events, got {len(events)}"


# =============================================================================
# Outbox atomicity
# =============================================================================

class TestOutboxAtomicity:

    async def test_outbox_row_written_with_event(self, store: EventStore, raw_conn):
        app_id = new_app_id()
        await store.append(f"loan-{app_id}", [make_submitted(app_id)], expected_version=-1)
        count = await raw_conn.fetchval(
            "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"
        )
        assert count >= 1

    async def test_failed_occ_does_not_write_outbox(self, store: EventStore, raw_conn):
        """Rolled-back transaction must not leave outbox rows."""
        app_id    = new_app_id()
        stream_id = f"loan-{app_id}"
        await store.append(stream_id, [make_submitted(app_id)], expected_version=-1)
        before = await raw_conn.fetchval("SELECT COUNT(*) FROM outbox")

        with pytest.raises(OptimisticConcurrencyError):
            await store.append(stream_id, [make_submitted(app_id)], expected_version=99)

        after = await raw_conn.fetchval("SELECT COUNT(*) FROM outbox")
        assert after == before, "Failed OCC must not write any outbox rows"

    async def test_outbox_payload_has_event_type(self, store: EventStore, raw_conn):
        app_id = new_app_id()
        await store.append(f"loan-{app_id}", [make_submitted(app_id)], expected_version=-1)
        row = await raw_conn.fetchrow(
            "SELECT payload FROM outbox WHERE published_at IS NULL ORDER BY created_at DESC LIMIT 1"
        )
        payload = dict(row["payload"])
        assert payload["event_type"] == "ApplicationSubmitted"


# =============================================================================
# Stream management
# =============================================================================

class TestStreamManagement:

    async def test_stream_version_returns_current(self, store: EventStore):
        app_id    = new_app_id()
        stream_id = f"loan-{app_id}"
        await store.append(stream_id, [make_submitted(app_id)], expected_version=-1)
        await store.append(stream_id, [make_requested(app_id)], expected_version=1)
        assert await store.stream_version(stream_id) == 2

    async def test_stream_version_raises_for_unknown(self, store: EventStore):
        with pytest.raises(StreamNotFoundError):
            await store.stream_version("loan-does-not-exist")

    async def test_archive_prevents_further_appends(self, store: EventStore):
        app_id    = new_app_id()
        stream_id = f"loan-{app_id}"
        await store.append(stream_id, [make_submitted(app_id)], expected_version=-1)
        await store.archive_stream(stream_id)
        with pytest.raises(PreconditionFailedError):
            await store.append(stream_id, [make_requested(app_id)], expected_version=1)

    async def test_archived_stream_still_readable(self, store: EventStore):
        app_id    = new_app_id()
        stream_id = f"loan-{app_id}"
        await store.append(stream_id, [make_submitted(app_id)], expected_version=-1)
        await store.archive_stream(stream_id)
        events = await store.load_stream(stream_id)
        assert len(events) == 1

    async def test_get_stream_metadata_fields(self, store: EventStore):
        app_id    = new_app_id()
        stream_id = f"loan-{app_id}"
        await store.append(stream_id, [make_submitted(app_id)], expected_version=-1)
        meta = await store.get_stream_metadata(stream_id)
        assert meta.stream_id       == stream_id
        assert meta.aggregate_type  == "LoanApplication"
        assert meta.current_version == 1
        assert meta.archived_at     is None