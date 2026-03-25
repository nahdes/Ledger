"""
DEMO STEP 5 — Gas Town Recovery
"Start an agent session, append events, simulate crash, reconstruct context"

Sequence:
  1. Start a credit analysis agent session (5 events)
  2. CRASH — delete all in-memory state (simulate process kill)
  3. Call reconstruct_agent_context() from the event store alone
  4. Show the agent can resume with full context

Usage:
    python demo_step5_gas_town.py
"""
import asyncio
import sys
import time
import uuid
from datetime import datetime, timezone

import asyncpg

sys.path.insert(0, ".")
from src.event_store import EventStore
from src.integrity.gas_town import reconstruct_agent_context
from src.models.events import (
    AgentNodeExecuted,
    AgentSessionStarted,
    AgentToolCalled,
    CreditAnalysisRequested,
)
from src.upcasting.registry import UpcasterRegistry

DATABASE_URL = "postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test"

SEP2 = "═" * 70


def ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:12]


async def main() -> None:
    pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=5)
    store = EventStore(
        pool=pool,
        upcaster_registry=UpcasterRegistry(),
        outbox_destinations=["demo"],
    )

    agent_type = "credit_analysis"
    session_id = f"sess-cre-{uuid.uuid4().hex[:8]}"
    app_id     = f"APEX-{uuid.uuid4().hex[:6].upper()}"
    stream_id  = f"agent-{agent_type}-{session_id}"

    print(f"\n{SEP2}")
    print(f"  STEP 5 — Gas Town Recovery")
    print(f"  Agent:   {agent_type}")
    print(f"  Session: {session_id}")
    print(f"  App:     {app_id}")
    print(f"{SEP2}\n")

    # ── PHASE 1: Normal operation — 5 events ─────────────────────────────────
    print(f"  {'─'*66}")
    print(f"  PHASE 1: Normal agent operation")
    print(f"  {'─'*66}\n")

    def utcnow():
        return datetime.now(timezone.utc)

    events_to_write = [
        ("AgentSessionStarted",  AgentSessionStarted(
            session_id=session_id, agent_type=agent_type,
            agent_id="credit-agent-prod-1", application_id=app_id,
            model_version="claude-sonnet-4-20250514",
            context_source="fresh", context_token_count=1500,
            started_at=utcnow(),
        )),
        ("AgentNodeExecuted[validate_inputs]", AgentNodeExecuted(
            session_id=session_id, agent_type=agent_type,
            node_name="validate_inputs", node_sequence=1,
            input_keys=["application_id"], output_keys=["validated"],
            llm_called=False, duration_ms=120, executed_at=utcnow(),
        )),
        ("AgentToolCalled[extraction_pipeline]", AgentToolCalled(
            session_id=session_id, agent_type=agent_type,
            tool_name="week3_extraction_pipeline",
            tool_input_summary="extract income statement",
            tool_output_summary="9 facts extracted: revenue, EBITDA, leverage…",
            tool_duration_ms=3200, called_at=utcnow(),
        )),
        ("AgentNodeExecuted[run_credit_model]", AgentNodeExecuted(
            session_id=session_id, agent_type=agent_type,
            node_name="run_credit_model", node_sequence=2,
            input_keys=["extracted_facts"], output_keys=["credit_score"],
            llm_called=True, llm_tokens_input=3000, llm_tokens_output=200,
            llm_cost_usd=0.015, duration_ms=8000, executed_at=utcnow(),
        )),
        ("AgentNodeExecuted[prepare_output]", AgentNodeExecuted(
            session_id=session_id, agent_type=agent_type,
            node_name="prepare_output", node_sequence=3,
            input_keys=["credit_score"], output_keys=["analysis_result"],
            llm_called=False, duration_ms=80, executed_at=utcnow(),
        )),
    ]

    for label, event in events_to_write:
        ver = events_to_write.index((label, event))
        v = await store.append(
            stream_id, [event], expected_version=ver if ver > 0 else -1
        )
        print(f"  [{ts()}]  ✓  Appended  {label}  →  stream version {v}")

    print(f"\n  [{ts()}]  Stream has {len(events_to_write)} events")

    # ── PHASE 2: CRASH ────────────────────────────────────────────────────────
    print(f"\n  {'─'*66}")
    print(f"  PHASE 2: *** PROCESS CRASH ***")
    print(f"  {'─'*66}\n")
    print(f"  [{ts()}]  Deleting all in-memory agent state…")

    # Simulate process kill — destroy every variable
    del agent_type, session_id, app_id, events_to_write
    # (stream_id survives — the only thing the new process would know)

    print(f"  [{ts()}]  In-memory state destroyed. Agent is gone.")
    print(f"  [{ts()}]  Only stream_id survives: {stream_id}")

    # ── PHASE 3: Recovery ────────────────────────────────────────────────────
    print(f"\n  {'─'*66}")
    print(f"  PHASE 3: Reconstruction from event store")
    print(f"  {'─'*66}\n")

    # Parse agent_type and session_id back from stream_id
    # stream_id format: agent-{agent_type}-{session_id}
    # session_id format: sess-{type}-{hex} so we split on "-sess-"
    parts         = stream_id.split("-", 1)
    rec_agent     = parts[1].rsplit("-sess-", 1)[0]
    rec_session   = "sess-" + parts[1].rsplit("-sess-", 1)[1]

    print(f"  [{ts()}]  Parsing stream_id: {stream_id}")
    print(f"  [{ts()}]  → agent_type:  {rec_agent}")
    print(f"  [{ts()}]  → session_id:  {rec_session}")
    print(f"  [{ts()}]  Calling reconstruct_agent_context()…\n")

    t0  = time.monotonic()
    ctx = await reconstruct_agent_context(
        store, rec_agent, rec_session, token_budget=8000
    )
    elapsed_ms = (time.monotonic() - t0) * 1000

    # ── PHASE 4: Show recovered state ────────────────────────────────────────
    print(f"\n  {'─'*66}")
    print(f"  PHASE 4: Recovered context")
    print(f"  {'─'*66}\n")
    print(f"  session_id:            {ctx.session_id}")
    print(f"  agent_type:            {ctx.agent_type}")
    print(f"  application_id:        {ctx.application_id}")
    print(f"  model_version:         {ctx.model_version}")
    print(f"  total_events:          {ctx.total_events}")
    print(f"  last_event_position:   {ctx.last_event_position}")
    print(f"  session_health_status: {ctx.session_health_status}")
    print(f"  pending_work:          {ctx.pending_work or '(none)'}")
    print(f"  summarised_events:     {ctx.summarised_events}")
    print(f"  verbatim_events:       {ctx.verbatim_events}")
    print(f"  reconstruct elapsed:   {elapsed_ms:.0f}ms")
    print(f"\n  Context text preview (first 400 chars):")
    print(f"  {'·'*66}")
    for line in ctx.context_text[:400].split('\n'):
        print(f"  {line}")
    print(f"  {'·'*66}")

    # ── Assertions ────────────────────────────────────────────────────────────
    print(f"\n  ── Verification ──────────────────────────────────────────────────")
    assert ctx.total_events == 5,           f"Expected 5 events, got {ctx.total_events}"
    assert ctx.last_event_position == 5,    f"Expected position 5, got {ctx.last_event_position}"
    assert ctx.model_version == "claude-sonnet-4-20250514", f"Model version lost"
    assert ctx.session_health_status == "OK", f"Expected OK, got {ctx.session_health_status}"
    assert len(ctx.pending_work) == 0,      f"Unexpected pending work: {ctx.pending_work}"

    print(f"  ✓  All 5 events recovered from event store")
    print(f"  ✓  model_version recovered: {ctx.model_version}")
    print(f"  ✓  session_health_status = OK  (no partial decisions)")
    print(f"  ✓  Agent can resume from position {ctx.last_event_position}")
    print(f"  ✓  No Redis, no in-memory cache, no external state required")

    print(f"\n{SEP2}\n")
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())