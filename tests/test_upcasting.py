"""
tests/test_upcasting.py

THE IMMUTABILITY TEST (graded — assessed independently during evaluation).

Spec requirement:
  (1) Directly query the events table in Postgres to get the raw stored
      payload of a v1 event
  (2) Load the same event through EventStore.load_stream() and verify
      it is upcasted to v2
  (3) Directly query the events table again and verify the raw stored
      payload is UNCHANGED

Any system where upcasting touches the stored events has broken the core
guarantee of event sourcing.

Requires: ledger-test-db running on localhost:5433
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest
import pytest_asyncio
from dotenv import load_dotenv
import os

from src.event_store import EventStore
from src.models.events import StoredEvent
from src.upcasting.registry import UpcasterRegistry
import src.upcasting.upcasters  # noqa: F401 — registers upcasters on default_registry
from src.upcasting.registry import default_registry

load_dotenv()

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test",
)


@pytest_asyncio.fixture(autouse=True)
async def clean_db(db_pool):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE TABLE outbox, events, event_streams, projection_checkpoints "
            "RESTART IDENTITY CASCADE"
        )
    yield


@pytest_asyncio.fixture
async def raw_store(db_pool) -> EventStore:
    """EventStore with NO upcasters — writes raw v1 events."""
    return EventStore(
        pool=db_pool,
        upcaster_registry=UpcasterRegistry(),   # empty registry
        outbox_destinations=["test"],
    )


@pytest_asyncio.fixture
async def upcasting_store(db_pool) -> EventStore:
    """EventStore with upcasters registered — loads events as v2."""
    return EventStore(
        pool=db_pool,
        upcaster_registry=default_registry,
        outbox_destinations=["test"],
    )


# =============================================================================
# THE GRADED IMMUTABILITY TEST
# =============================================================================

@pytest.mark.graded
async def test_upcasting_does_not_mutate_stored_payload(
    db_pool: asyncpg.Pool,
    raw_store: EventStore,
    upcasting_store: EventStore,
) -> None:
    """
    Graded immutability test: load as v2 via EventStore, confirm raw DB
    payload is byte-for-byte identical to what was written.

    Steps:
      1. Write a v1 CreditAnalysisCompleted directly (bypassing upcasters)
      2. Query raw DB — confirm v1 schema, no model_version field
      3. Load via upcasting EventStore — confirm v2 schema present
      4. Query raw DB again — confirm payload UNCHANGED
    """
    app_id    = f"APEX-{uuid.uuid4().hex[:6].upper()}"
    stream_id = f"credit-{app_id}"

    # ── Step 1: Write a v1 event via the raw store (no upcasters) ─────────────
    # We insert directly to simulate a historical event that predates v2 schema
    v1_payload = {
        "application_id":       app_id,
        "session_id":           "sess-cre-legacy",
        "risk_tier":            "MEDIUM",
        "recommended_limit_usd": "750000.0",
        "analysis_duration_ms": 15000,
        "input_data_hash":      "abc123def456",
        "completed_at":         datetime.now(timezone.utc).isoformat(),
    }
    event_id = uuid.uuid4()

    async with db_pool.acquire() as conn:
        # Create stream
        await conn.execute(
            "INSERT INTO event_streams (stream_id, aggregate_type, current_version) "
            "VALUES ($1, 'CreditRecord', 1)",
            stream_id,
        )
        # Insert raw v1 event directly — bypassing EventStore
        await conn.execute(
            """
            INSERT INTO events
                (event_id, stream_id, stream_position, event_type, event_version,
                 payload, metadata, recorded_at)
            VALUES ($1, $2, 1, 'CreditAnalysisCompleted', 1, $3::jsonb, '{}'::jsonb, NOW())
            """,
            event_id, stream_id, json.dumps(v1_payload),
        )

    # ── Step 2: Query raw DB — confirm v1 schema ──────────────────────────────
    async with db_pool.acquire() as conn:
        raw_before = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE event_id = $1",
            event_id,
        )

    raw_payload_before = (
        dict(raw_before["payload"])
        if isinstance(raw_before["payload"], dict)
        else json.loads(raw_before["payload"])
    )

    assert raw_before["event_version"] == 1, \
        "Raw DB must store event_version=1 (not mutated by any prior operation)"
    assert "model_version" not in raw_payload_before, \
        "Raw DB must NOT have model_version in v1 payload before upcast"
    assert "regulatory_basis" not in raw_payload_before, \
        "Raw DB must NOT have regulatory_basis in v1 payload before upcast"

    # ── Step 3: Load through upcasting EventStore — confirm v2 schema ─────────
    events = await upcasting_store.load_stream(stream_id)
    assert len(events) == 1
    upcasted_event = events[0]

    assert upcasted_event.event_version == 2, \
        f"EventStore.load_stream() must return v2, got v{upcasted_event.event_version}"
    assert "model_version" in upcasted_event.payload, \
        "Upcasted event must have model_version field"
    assert "regulatory_basis" in upcasted_event.payload, \
        "Upcasted event must have regulatory_basis field"
    # confidence must be None not fabricated
    decision = upcasted_event.payload.get("decision", {})
    confidence = decision.get("confidence") if isinstance(decision, dict) else None
    assert confidence is None, \
        f"confidence must be None for v1 events (never fabricated), got {confidence!r}"

    # ── Step 4: Query raw DB AGAIN — payload must be identical to before ───────
    async with db_pool.acquire() as conn:
        raw_after = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE event_id = $1",
            event_id,
        )

    raw_payload_after = (
        dict(raw_after["payload"])
        if isinstance(raw_after["payload"], dict)
        else json.loads(raw_after["payload"])
    )

    # THE CORE ASSERTION: stored payload is byte-for-byte identical
    assert raw_after["event_version"] == 1, \
        "IMMUTABILITY VIOLATION: event_version in DB was changed from 1"
    assert raw_payload_after == raw_payload_before, (
        "IMMUTABILITY VIOLATION: stored payload was mutated by upcast operation.\n"
        f"Before: {raw_payload_before}\n"
        f"After:  {raw_payload_after}"
    )
    assert "model_version" not in raw_payload_after, \
        "IMMUTABILITY VIOLATION: new field 'model_version' leaked into stored payload"

    print(f"\n✓ Immutability confirmed for {stream_id}")
    print(f"  DB payload unchanged: event_version=1, no new fields")
    print(f"  Upcasted view: event_version=2, model_version={upcasted_event.payload.get('model_version')!r}")


async def test_v2_events_pass_through_unchanged(
    upcasting_store: EventStore,
    db_pool: asyncpg.Pool,
) -> None:
    """v2 events in the DB must not be double-upcasted."""
    app_id    = f"APEX-{uuid.uuid4().hex[:6].upper()}"
    stream_id = f"credit-{app_id}"

    v2_payload = {
        "application_id":       app_id,
        "session_id":           "sess-cre-v2",
        "decision": {
            "risk_tier":            "LOW",
            "recommended_limit_usd": "900000.0",
            "confidence":           0.91,
        },
        "model_version":        "claude-sonnet-4-20250514",
        "model_deployment_id":  "dep-abc123",
        "input_data_hash":      "v2hashvalue",
        "analysis_duration_ms": 1200,
        "regulatory_basis":     ["REG-001"],
        "completed_at":         datetime.now(timezone.utc).isoformat(),
    }
    event_id = uuid.uuid4()

    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO event_streams (stream_id, aggregate_type, current_version) "
            "VALUES ($1, 'CreditRecord', 1)",
            stream_id,
        )
        await conn.execute(
            """
            INSERT INTO events
                (event_id, stream_id, stream_position, event_type, event_version,
                 payload, metadata, recorded_at)
            VALUES ($1, $2, 1, 'CreditAnalysisCompleted', 2, $3::jsonb, '{}'::jsonb, NOW())
            """,
            event_id, stream_id, json.dumps(v2_payload),
        )

    events = await upcasting_store.load_stream(stream_id)
    assert len(events) == 1
    ev = events[0]
    assert ev.event_version == 2, "v2 events must not be upcasted to v3"
    assert ev.payload["decision"]["confidence"] == 0.91, \
        "v2 confidence must be preserved exactly — not set to None"
