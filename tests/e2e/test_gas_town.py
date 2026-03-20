"""
tests/e2e/test_gas_town.py

THE GAS TOWN CRASH RECOVERY TEST (graded).

Scenario:
  1. Start an agent session (AgentSessionStarted written to stream)
  2. Append 4 more events (analyses, tool calls)
  3. Discard the in-memory agent object — simulates crash / pod restart
  4. Reconstruct agent context purely from event store
  5. Assert reconstructed state is sufficient to continue safely

The Gas Town pattern means: an agent that has lost its in-memory state
can always recover from the event store. No side-channel state (Redis,
in-memory cache) is needed.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from src.event_store import EventStore
from src.models.events import (
    AgentNodeExecuted,
    AgentSessionCompleted,
    AgentSessionStarted,
    AgentToolCalled,
)
from src.upcasting.registry import UpcasterRegistry


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def make_agent_stream_id(agent_type: str, session_id: str) -> str:
    return f"agent-{agent_type}-{session_id}"


# =============================================================================
# A minimal AgentSessionReconstructor — read-only replica of agent state
# from the event store.  In Phase 5 this will be part of AgentSessionAggregate.
# =============================================================================

class ReconstructedAgentContext:
    """Plain data container populated by replaying agent stream events."""

    def __init__(self) -> None:
        self.session_id:          str | None = None
        self.agent_type:          str | None = None
        self.model_version:       str | None = None
        self.context_source:      str | None = None
        self.context_token_count: int = 0
        self.started_at:          datetime | None = None
        self.nodes_executed:      list[str] = []
        self.tools_called:        list[str] = []
        self.version:             int = 0
        self.is_completed:        bool = False
        self.next_agent:          str | None = None

    @classmethod
    async def load(
        cls,
        store: EventStore,
        agent_type: str,
        session_id: str,
    ) -> "ReconstructedAgentContext":
        stream_id = make_agent_stream_id(agent_type, session_id)
        events    = await store.load_stream(stream_id)
        ctx       = cls()

        for event in events:
            ctx.version = event.stream_position
            p = event.payload

            if event.event_type == "AgentSessionStarted":
                ctx.session_id          = p.get("session_id")
                ctx.agent_type          = p.get("agent_type")
                ctx.model_version       = p.get("model_version")
                ctx.context_source      = p.get("context_source")
                ctx.context_token_count = p.get("context_token_count", 0)
                ctx.started_at          = event.recorded_at

            elif event.event_type == "AgentNodeExecuted":
                ctx.nodes_executed.append(p.get("node_name", ""))

            elif event.event_type == "AgentToolCalled":
                ctx.tools_called.append(p.get("tool_name", ""))

            elif event.event_type == "AgentSessionCompleted":
                ctx.is_completed = True
                ctx.next_agent   = p.get("next_agent_triggered")

        return ctx

    def assert_context_loaded(self) -> None:
        """Gas Town assertion: agent must have a loaded context before proceeding."""
        if self.session_id is None:
            raise ValueError(f"GAS_TOWN: session context not loaded for {self.agent_type}")

    def assert_model_version_current(self, expected: str) -> None:
        """Model version locking: reject if deployed version differs from session."""
        if self.model_version and self.model_version != expected:
            raise ValueError(
                f"MODEL_VERSION_LOCK: session declared {self.model_version}, "
                f"deployed is {expected}"
            )


# =============================================================================
# THE GRADED TEST
# =============================================================================

@pytest.mark.graded
async def test_agent_reconstructs_after_crash(store: EventStore):
    """
    Graded: start 5 events, destroy in-memory object, reconstruct from DB,
    verify reconstructed context is correct.
    """
    agent_type = "document_processing"
    session_id = f"sess-doc-{uuid.uuid4().hex[:8]}"
    app_id     = f"APEX-{uuid.uuid4().hex[:6].upper()}"
    stream_id  = make_agent_stream_id(agent_type, session_id)

    # ── Step 1: Start the session (event 1) ───────────────────────────────────
    await store.append(
        stream_id,
        [
            AgentSessionStarted(
                session_id          = session_id,
                agent_type          = agent_type,
                agent_id            = "doc-agent-1",
                application_id      = app_id,
                model_version       = "claude-sonnet-4-20250514",
                context_source      = "fresh",
                context_token_count = 1459,
                started_at          = utcnow(),
            )
        ],
        expected_version=-1,
    )

    # ── Step 2: Append 4 more events (total = 5) ──────────────────────────────
    await store.append(
        stream_id,
        [
            AgentNodeExecuted(
                session_id    = session_id,
                agent_type    = agent_type,
                node_name     = "validate_inputs",
                node_sequence = 1,
                input_keys    = ["application_id", "documents"],
                output_keys   = ["validated_inputs"],
                llm_called    = False,
                duration_ms   = 320,
                executed_at   = utcnow(),
            )
        ],
        expected_version=1,
    )
    await store.append(
        stream_id,
        [
            AgentToolCalled(
                session_id           = session_id,
                agent_type           = agent_type,
                tool_name            = "week3_extraction_pipeline",
                tool_input_summary   = "PDF extraction: income_statement",
                tool_output_summary  = "Extracted 9 financial line items",
                tool_duration_ms     = 3177,
                called_at            = utcnow(),
            )
        ],
        expected_version=2,
    )
    await store.append(
        stream_id,
        [
            AgentNodeExecuted(
                session_id    = session_id,
                agent_type    = agent_type,
                node_name     = "run_extraction",
                node_sequence = 2,
                input_keys    = ["document_paths"],
                output_keys   = ["raw_facts"],
                llm_called    = False,
                duration_ms   = 145,
                executed_at   = utcnow(),
            )
        ],
        expected_version=3,
    )
    await store.append(
        stream_id,
        [
            AgentNodeExecuted(
                session_id        = session_id,
                agent_type        = agent_type,
                node_name         = "assess_quality",
                node_sequence     = 3,
                input_keys        = ["raw_facts"],
                output_keys       = ["quality_assessment"],
                llm_called        = True,
                llm_tokens_input  = 2880,
                llm_tokens_output = 305,
                llm_cost_usd      = 0.013215,
                duration_ms       = 9646,
                executed_at       = utcnow(),
            )
        ],
        expected_version=4,
    )

    # Confirm 5 events exist before crash
    pre_crash = await store.load_stream(stream_id)
    assert len(pre_crash) == 5, f"Expected 5 events before crash, got {len(pre_crash)}"

    # ── Step 3: CRASH — discard all in-memory state ───────────────────────────
    # In real deployment this happens when the pod is evicted.
    # Here we just don't reference any in-memory object going forward.
    del session_id, agent_type, app_id  # explicit destruction

    # ── Step 4: Reconstruct from stream_id alone ──────────────────────────────
    # Parse back from stream_id (what a scheduler/orchestrator would do)
    parts              = stream_id.split("-", 1)  # ["agent", "document_processing-sess-doc-XXXX"]
    recovered_type     = parts[1].rsplit("-sess-", 1)[0]   # "document_processing"
    recovered_session  = "sess-" + parts[1].rsplit("-sess-", 1)[1]   # "sess-doc-XXXX"

    ctx = await ReconstructedAgentContext.load(store, recovered_type, recovered_session)

    # ── Step 5: Assert reconstructed state ────────────────────────────────────
    assert ctx.version == 5, \
        f"Expected version=5 (all 5 events replayed), got {ctx.version}"

    assert ctx.model_version == "claude-sonnet-4-20250514", \
        "Model version must be recoverable from AgentSessionStarted"

    assert ctx.context_source == "fresh"

    assert len(ctx.nodes_executed) == 3, \
        f"Expected 3 nodes executed, got {ctx.nodes_executed}"
    assert "validate_inputs" in ctx.nodes_executed
    assert "assess_quality"  in ctx.nodes_executed

    assert len(ctx.tools_called) == 1
    assert "week3_extraction_pipeline" in ctx.tools_called

    # Gas Town assertion — context is loaded, agent can continue
    ctx.assert_context_loaded()
    ctx.assert_model_version_current("claude-sonnet-4-20250514")

    print(f"\n✓ Crash recovery verified for stream: {stream_id}")
    print(f"  Events replayed:  {ctx.version}")
    print(f"  Model version:    {ctx.model_version}")
    print(f"  Nodes executed:   {ctx.nodes_executed}")
    print(f"  Tools called:     {ctx.tools_called}")


async def test_clean_session_has_no_partial_state(store: EventStore):
    """A session with only AgentSessionStarted has no tool calls or node history."""
    agent_type = "credit_analysis"
    session_id = f"sess-cre-{uuid.uuid4().hex[:8]}"
    stream_id  = make_agent_stream_id(agent_type, session_id)

    await store.append(
        stream_id,
        [
            AgentSessionStarted(
                session_id          = session_id,
                agent_type          = agent_type,
                agent_id            = "credit-agent-1",
                application_id      = "APEX-CLEAN",
                model_version       = "claude-sonnet-4-20250514",
                context_source      = "fresh",
                context_token_count = 1500,
                started_at          = utcnow(),
            )
        ],
        expected_version=-1,
    )

    ctx = await ReconstructedAgentContext.load(store, agent_type, session_id)
    assert ctx.is_completed   is False
    assert ctx.nodes_executed == []
    assert ctx.tools_called   == []
    ctx.assert_context_loaded()
    print("\n✓ Clean session state verified — no partial decision")
