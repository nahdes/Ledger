"""
tests/test_gas_town.py

Full Gas Town crash recovery test using reconstruct_agent_context().

Spec requirement:
  "Simulated crash recovery: 5 events appended, reconstruct_agent_context()
   called without in-memory agent, verify reconstructed context is sufficient
   to continue correctly."

Also tests NEEDS_RECONCILIATION detection for partial decisions.

Requires: ledger-test-db running on localhost:5433
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from dotenv import load_dotenv

from src.event_store import EventStore
from src.integrity.gas_town import AgentContext, reconstruct_agent_context
from src.models.events import (
    AgentNodeExecuted,
    AgentSessionStarted,
    AgentToolCalled,
    CreditAnalysisRequested,
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
            "TRUNCATE TABLE outbox, events, event_streams, projection_checkpoints "
            "RESTART IDENTITY CASCADE"
        )
    yield


@pytest_asyncio.fixture
async def store(db_pool) -> EventStore:
    return EventStore(
        pool=db_pool,
        upcaster_registry=UpcasterRegistry(),
        outbox_destinations=["test"],
    )


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# THE GRADED GAS TOWN TEST
# =============================================================================

@pytest.mark.graded
async def test_reconstruct_agent_context_after_crash(store: EventStore) -> None:
    """
    Graded: append 5 events, destroy in-memory agent, reconstruct from DB.
    Verify reconstructed context is sufficient to continue correctly.
    """
    agent_type = "credit_analysis"
    session_id = f"sess-cre-{uuid.uuid4().hex[:8]}"
    app_id     = f"APEX-{uuid.uuid4().hex[:6].upper()}"
    stream_id  = f"agent-{agent_type}-{session_id}"

    # ── Append 5 events ────────────────────────────────────────────────────────
    await store.append(
        stream_id,
        [AgentSessionStarted(
            session_id=session_id, agent_type=agent_type,
            agent_id="credit-agent-1", application_id=app_id,
            model_version="claude-sonnet-4-20250514",
            context_source="fresh", context_token_count=1500,
            started_at=utcnow(),
        )],
        expected_version=-1,
    )
    await store.append(
        stream_id,
        [AgentNodeExecuted(
            session_id=session_id, agent_type=agent_type,
            node_name="validate_inputs", node_sequence=1,
            input_keys=["application_id"], output_keys=["validated"],
            llm_called=False, duration_ms=120, executed_at=utcnow(),
        )],
        expected_version=1,
    )
    await store.append(
        stream_id,
        [AgentToolCalled(
            session_id=session_id, agent_type=agent_type,
            tool_name="week3_extraction_pipeline",
            tool_input_summary="extract income statement",
            tool_output_summary="9 facts extracted",
            tool_duration_ms=3000, called_at=utcnow(),
        )],
        expected_version=2,
    )
    await store.append(
        stream_id,
        [AgentNodeExecuted(
            session_id=session_id, agent_type=agent_type,
            node_name="run_credit_model", node_sequence=2,
            input_keys=["extracted_facts"], output_keys=["credit_score"],
            llm_called=True, llm_tokens_input=3000, llm_tokens_output=200,
            llm_cost_usd=0.015, duration_ms=8000, executed_at=utcnow(),
        )],
        expected_version=3,
    )
    await store.append(
        stream_id,
        [AgentNodeExecuted(
            session_id=session_id, agent_type=agent_type,
            node_name="prepare_output", node_sequence=3,
            input_keys=["credit_score"], output_keys=["analysis_result"],
            llm_called=False, duration_ms=80, executed_at=utcnow(),
        )],
        expected_version=4,
    )

    # Confirm 5 events written
    pre_crash = await store.load_stream(stream_id)
    assert len(pre_crash) == 5

    # ── CRASH — destroy all in-memory state ────────────────────────────────────
    del agent_type, session_id, app_id

    # ── Recover — parse stream_id components ──────────────────────────────────
    parts              = stream_id.split("-", 1)
    recovered_type     = parts[1].rsplit("-sess-", 1)[0]
    recovered_session  = "sess-" + parts[1].rsplit("-sess-", 1)[1]

    # ── Reconstruct from event store alone ────────────────────────────────────
    ctx: AgentContext = await reconstruct_agent_context(
        store, recovered_type, recovered_session, token_budget=8000
    )

    # ── Assertions ────────────────────────────────────────────────────────────
    assert ctx.last_event_position == 5, \
        f"All 5 events must be replayed, got position {ctx.last_event_position}"

    assert ctx.model_version == "claude-sonnet-4-20250514", \
        f"Model version must be recoverable, got {ctx.model_version!r}"

    assert ctx.session_health_status == "OK", \
        f"No partial decisions in this session, expected OK, got {ctx.session_health_status!r}"

    assert len(ctx.pending_work) == 0, \
        f"No pending work expected, got: {ctx.pending_work}"

    assert ctx.total_events == 5
    assert ctx.context_text, "context_text must not be empty"

    # The agent can continue — check context has enough to resume
    assert "credit_analysis" in ctx.context_text.lower() or \
           "validate_inputs" in ctx.context_text or \
           "sess-cre" in ctx.context_text

    print(f"\n✓ Gas Town crash recovery verified")
    print(f"  Events replayed:      {ctx.total_events}")
    print(f"  Last position:        {ctx.last_event_position}")
    print(f"  Model version:        {ctx.model_version}")
    print(f"  Health status:        {ctx.session_health_status}")
    print(f"  Context length:       {len(ctx.context_text)} chars")


async def test_needs_reconciliation_detected(store: EventStore) -> None:
    """
    A session where a decision was requested but never completed must be
    flagged as NEEDS_RECONCILIATION.
    """
    agent_type = "credit_analysis"
    session_id = f"sess-cre-{uuid.uuid4().hex[:8]}"
    app_id     = f"APEX-{uuid.uuid4().hex[:6].upper()}"
    stream_id  = f"agent-{agent_type}-{session_id}"

    await store.append(
        stream_id,
        [AgentSessionStarted(
            session_id=session_id, agent_type=agent_type,
            agent_id="credit-agent-1", application_id=app_id,
            model_version="claude-sonnet-4-20250514",
            context_source="fresh", context_token_count=1000,
            started_at=utcnow(),
        )],
        expected_version=-1,
    )
    # Write a CreditAnalysisRequested but NO CreditAnalysisCompleted
    await store.append(
        stream_id,
        [CreditAnalysisRequested(application_id=app_id, requested_at=utcnow())],
        expected_version=1,
    )

    ctx = await reconstruct_agent_context(store, agent_type, session_id)

    assert ctx.session_health_status == "NEEDS_RECONCILIATION", \
        "Partial decision (requested but not completed) must be flagged"
    assert len(ctx.pending_work) > 0, \
        "pending_work must list the unresolved decision"
    print(f"\n✓ NEEDS_RECONCILIATION detected: {ctx.pending_work}")


async def test_empty_session_returns_crashed_status(store: EventStore) -> None:
    """A session with no events returns CRASHED health status."""
    ctx = await reconstruct_agent_context(store, "credit_analysis", "sess-cre-nonexistent")
    assert ctx.session_health_status == "CRASHED"
    assert ctx.total_events == 0
