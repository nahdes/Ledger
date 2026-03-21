"""
tests/test_concurrency.py

The Double-Decision Concurrency Test — graded deliverable.

Required by the challenge spec:
  "Implement a test that spawns two concurrent asyncio tasks doing this.
   The test must assert:
   (a) total events appended to the stream = 4 (not 5)
   (b) the winning task's event has stream_position=4
   (c) the losing task's OptimisticConcurrencyError is raised, not silently swallowed."

Why this matters:
  In the Apex loan scenario, two fraud-detection agents simultaneously flag the
  same application. Without OCC, both flags are applied and the application's
  state is inconsistent — no one knows which fraud score is authoritative.
  With OCC, one agent wins; the other reloads and decides whether its analysis
  is still relevant. This is not an edge case — at 1,000 applications/hour with
  4 agents each, concurrency collisions happen constantly.

Requires: ledger-test-db running on localhost:5433
  docker compose up -d
"""
from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest
import pytest_asyncio
from dotenv import load_dotenv

from src.event_store import EventStore
from src.models.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    OptimisticConcurrencyError,
)
from src.upcasting.registry import UpcasterRegistry

load_dotenv()

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test",
)


@pytest_asyncio.fixture(autouse=True)
async def clean_db(db_pool):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE outbox, events, event_streams, projection_checkpoints
            RESTART IDENTITY CASCADE
            """
        )
    yield


@pytest_asyncio.fixture
async def store(db_pool) -> EventStore:
    return EventStore(
        pool=db_pool,
        upcaster_registry=UpcasterRegistry(),
        outbox_destinations=["test-destination"],
    )


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_app_id() -> str:
    return f"APEX-{uuid.uuid4().hex[:8].upper()}"


def make_submitted(app_id: str) -> ApplicationSubmitted:
    return ApplicationSubmitted(
        application_id       = app_id,
        applicant_id         = "COMP-CONCURRENT-001",
        requested_amount_usd = 750_000.0,
        loan_purpose         = "acquisition",
        submission_channel   = "api",
        submitted_at         = utcnow(),
    )


def make_requested(app_id: str) -> CreditAnalysisRequested:
    return CreditAnalysisRequested(
        application_id = app_id,
        requested_at   = utcnow(),
        priority       = "HIGH",
    )


def make_analysis(app_id: str, agent_label: str) -> CreditAnalysisCompleted:
    return CreditAnalysisCompleted(
        application_id       = app_id,
        session_id           = f"sess-cre-{agent_label}",
        decision             = {
            "risk_tier":             "MEDIUM",
            "recommended_limit_usd": "700000",
            "confidence":            0.81,
        },
        model_version        = "claude-sonnet-4-20250514",
        input_data_hash      = f"hash-{agent_label}",
        analysis_duration_ms = 1500,
        completed_at         = utcnow(),
    )


# =============================================================================
# THE DOUBLE-DECISION TEST (graded)
# =============================================================================

@pytest.mark.graded
async def test_concurrent_appends_exactly_one_wins(store):
    """
    Two AI agents simultaneously attempt to append a CreditAnalysisCompleted
    event to the same loan application stream at expected_version=3.

    Assertions (from challenge spec):
      (a) total events appended to the stream = 4, not 5
      (b) the winning task's event has stream_position=4
      (c) the losing task's OptimisticConcurrencyError is raised, not swallowed
    """
    app_id    = new_app_id()
    stream_id = f"loan-{app_id}"

    # ── Seed stream to version 3 ───────────────────────────────────────────────
    await store.append(stream_id, [make_submitted(app_id)],  expected_version=-1)
    await store.append(stream_id, [make_requested(app_id)],  expected_version=1)
    await store.append(stream_id, [make_requested(app_id)],  expected_version=2)
    # stream is now at version 3

    winners: dict[str, int] = {}
    losers:  dict[str, OptimisticConcurrencyError] = {}

    async def agent(label: str) -> None:
        """Simulates one AI agent attempting to append at expected_version=3."""
        try:
            new_version = await store.append(
                stream_id,
                [make_analysis(app_id, label)],
                expected_version=3,
            )
            winners[label] = new_version
        except OptimisticConcurrencyError as exc:
            losers[label] = exc

    # ── Fire both agents simultaneously ───────────────────────────────────────
    await asyncio.gather(agent("A"), agent("B"))

    # ── (c) The losing task's OptimisticConcurrencyError is raised, not swallowed
    assert len(losers) == 1, (
        f"Expected exactly 1 OCC error, got {len(losers)}.\n"
        f"winners={winners}, losers={losers}"
    )

    # ── (b) The winning task's event has stream_position=4
    assert len(winners) == 1, (
        f"Expected exactly 1 winner, got {len(winners)}.\n"
        f"winners={winners}, losers={losers}"
    )
    winning_version = list(winners.values())[0]
    assert winning_version == 4, (
        f"Winner must land at stream_position=4, got {winning_version}"
    )

    # ── (a) Total events appended to the stream = 4, not 5
    all_events = await store.load_stream(stream_id)
    assert len(all_events) == 4, (
        f"Stream must have exactly 4 events (not 5 — no split brain), "
        f"got {len(all_events)}"
    )

    # ── Verify the error carries the correct diagnostic fields
    occ_error = list(losers.values())[0]
    assert occ_error.expected_version == 3, (
        f"OCC error must report expected_version=3, got {occ_error.expected_version}"
    )
    assert occ_error.actual_version == 4, (
        f"OCC error must report actual_version=4, got {occ_error.actual_version}"
    )
    assert occ_error.stream_id == stream_id

    # ── Structured error dict (for MCP tool consumption)
    error_dict = occ_error.to_dict()
    assert error_dict["suggested_action"] == "reload_stream_and_retry"

    print(f"\n✓ Double-decision test passed")
    print(f"  Winner:  Agent {list(winners.keys())[0]} → stream_position={winning_version}")
    print(f"  Loser:   Agent {list(losers.keys())[0]}  → OptimisticConcurrencyError "
          f"(expected={occ_error.expected_version}, actual={occ_error.actual_version})")
    print(f"  Stream:  {len(all_events)} events total (correct — not 5)")


@pytest.mark.graded
async def test_losing_agent_can_reload_and_retry(store):
    """
    After losing the double-decision race, the losing agent reloads the stream,
    sees the winner's event already present, and can make a new decision.

    This is the retry pattern the spec describes:
    "The caller must reload and retry."
    """
    app_id    = new_app_id()
    stream_id = f"loan-{app_id}"

    await store.append(stream_id, [make_submitted(app_id)], expected_version=-1)
    await store.append(stream_id, [make_requested(app_id)], expected_version=1)
    await store.append(stream_id, [make_requested(app_id)], expected_version=2)

    # Agent A wins at version 3
    v_after_a = await store.append(
        stream_id, [make_analysis(app_id, "A")], expected_version=3
    )
    assert v_after_a == 4

    # Agent B loses — tries version 3, gets OCC
    try:
        await store.append(
            stream_id, [make_analysis(app_id, "B")], expected_version=3
        )
        pytest.fail("Agent B should have received OptimisticConcurrencyError")
    except OptimisticConcurrencyError as exc:
        assert exc.expected_version == 3
        assert exc.actual_version   == 4

        # ── Agent B reloads the stream ─────────────────────────────────────────
        reloaded = await store.load_stream(stream_id)
        assert len(reloaded) == 4
        assert reloaded[-1].event_type == "CreditAnalysisCompleted"

        # ── Agent B decides not to re-analyse (Agent A already did it)
        # In production: compare reloaded[-1].payload["session_id"] != own session
        agent_a_already_analysed = any(
            e.event_type == "CreditAnalysisCompleted" for e in reloaded
        )
        assert agent_a_already_analysed, (
            "After reload, Agent B must see Agent A's analysis and stand down"
        )

    print(f"\n✓ Reload-and-retry pattern verified")
    print(f"  Agent B correctly stands down after seeing Agent A's analysis")


async def test_many_concurrent_appends_all_serialized(store):
    """
    10 agents all race to append to the same stream at version 0.
    Exactly 1 should win. The other 9 should all receive OCC errors.
    The stream should have exactly 1 event after.
    """
    app_id    = new_app_id()
    stream_id = f"agent-flood-{app_id}"

    # Create the stream first
    await store.append(
        stream_id,
        [make_submitted(app_id)],
        expected_version=-1,
    )

    wins   = []
    errors = []

    async def racer(n: int) -> None:
        try:
            v = await store.append(
                stream_id,
                [make_requested(app_id)],
                expected_version=1,
            )
            wins.append(v)
        except OptimisticConcurrencyError:
            errors.append(n)

    await asyncio.gather(*[racer(i) for i in range(10)])

    assert len(wins)   == 1,  f"Expected 1 winner from 10 racers, got {len(wins)}"
    assert len(errors) == 9,  f"Expected 9 OCC errors from 10 racers, got {len(errors)}"

    events = await store.load_stream(stream_id)
    assert len(events) == 2   # original + exactly 1 winner

    print(f"\n✓ 10-way race: 1 winner, 9 OCC errors, stream has 2 events")