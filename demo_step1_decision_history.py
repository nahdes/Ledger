"""
DEMO STEP 1 — The Week Standard
"Show me the complete decision history of application ID X"

Runs end-to-end:
  - Full event stream from loan-{id}
  - All agent session events
  - All compliance check events
  - Causal links (correlation_id threading)
  - Cryptographic integrity verification

Target: complete in under 60 seconds.

Usage:
    python demo_step1_decision_history.py [APP_ID]
    python demo_step1_decision_history.py APEX-NARR05
"""
import asyncio
import sys
import time
from datetime import timezone

import asyncpg

sys.path.insert(0, ".")
from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check
from src.upcasting.registry import default_registry
import src.upcasting.upcasters  # noqa: F401 — registers upcasters

DATABASE_URL = "postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test"

SEP  = "─" * 70
SEP2 = "═" * 70

def ts(event):
    if event.recorded_at:
        dt = event.recorded_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%H:%M:%S.%f")[:12]
    return "??:??:??.???"

def fmt_payload(p: dict, keys: list[str]) -> str:
    parts = []
    for k in keys:
        v = p.get(k)
        if v is not None:
            if isinstance(v, dict):
                inner = {ik: v[ik] for ik in list(v)[:3]}
                parts.append(f"{k}={inner}")
            elif isinstance(v, list):
                parts.append(f"{k}={v[:2]}")
            elif isinstance(v, str) and len(v) > 40:
                parts.append(f"{k}={v[:40]}…")
            else:
                parts.append(f"{k}={v}")
    return "  ".join(parts)


async def main(app_id: str) -> None:
    t_start = time.monotonic()

    pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=5)
    store = EventStore(
        pool=pool,
        upcaster_registry=default_registry,
        outbox_destinations=["demo"],
    )

    print(f"\n{SEP2}")
    print(f"  STEP 1 — Complete Decision History")
    print(f"  Application: {app_id}")
    print(f"{SEP2}\n")

    # ── 1. Loan stream ─────────────────────────────────────────────────────────
    print(f"{'─'*70}")
    print(f"  LOAN STREAM  loan-{app_id}")
    print(f"{'─'*70}")

    loan_events = await store.load_stream(f"loan-{app_id}")
    if not loan_events:
        print(f"  ✗  No events found for loan-{app_id}")
        print(f"     Make sure the seed data or narrative tests have been run.")
        await pool.close()
        return

    correlation_ids: set[str] = set()
    for e in loan_events:
        corr = e.metadata.get("correlation_id", "")
        if corr:
            correlation_ids.add(corr)
        p = e.payload

        if e.event_type == "ApplicationSubmitted":
            detail = fmt_payload(p, ["applicant_id", "requested_amount_usd", "loan_purpose"])
        elif e.event_type == "DecisionGenerated":
            detail = fmt_payload(p, ["recommendation", "confidence", "contributing_sessions"])
        elif e.event_type == "ApplicationApproved":
            detail = fmt_payload(p, ["approved_amount_usd", "approved_by", "conditions"])
        elif e.event_type == "ApplicationDeclined":
            detail = fmt_payload(p, ["decline_reasons", "adverse_action_notice_required"])
        elif e.event_type == "HumanReviewCompleted":
            detail = fmt_payload(p, ["reviewer_id", "override", "final_decision", "override_reason"])
        else:
            detail = ""

        causal = f"  corr={corr[:8]}…" if corr else ""
        print(f"  [{e.stream_position:2d}] {ts(e)}  {e.event_type:<35s}{causal}")
        if detail:
            print(f"        {detail}")

    print(f"\n  Total loan events: {len(loan_events)}")

    # ── 2. Agent session streams ────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  AGENT SESSION STREAMS")
    print(f"{'─'*70}")

    async with pool.acquire() as conn:
        agent_streams = await conn.fetch(
            "SELECT stream_id FROM event_streams WHERE stream_id LIKE $1 ORDER BY stream_id",
            f"agent-%-{app_id[:10]}%" if len(app_id) > 10 else "agent-%",
        )
        # Also look for sessions that mention this app_id in events
        session_rows = await conn.fetch(
            """
            SELECT DISTINCT stream_id FROM events
            WHERE  stream_id LIKE 'agent-%'
            AND    payload::text LIKE $1
            ORDER  BY stream_id
            """,
            f"%{app_id}%",
        )

    agent_stream_ids = {r["stream_id"] for r in agent_streams} | {r["stream_id"] for r in session_rows}

    if not agent_stream_ids:
        print("  (no agent sessions found for this application)")
    else:
        for sid in sorted(agent_stream_ids):
            events = await store.load_stream(sid)
            agent_type = sid.split("-")[1] if "-" in sid else "?"
            print(f"\n  {sid}  ({len(events)} events)")
            for e in events:
                p = e.payload
                if e.event_type == "AgentSessionStarted":
                    detail = fmt_payload(p, ["model_version", "context_source"])
                elif e.event_type == "AgentNodeExecuted":
                    detail = fmt_payload(p, ["node_name", "llm_called", "duration_ms"])
                elif e.event_type == "AgentToolCalled":
                    detail = fmt_payload(p, ["tool_name"])
                elif e.event_type == "AgentSessionCompleted":
                    detail = fmt_payload(p, ["total_llm_calls", "total_cost_usd"])
                else:
                    detail = ""
                print(f"    [{e.stream_position:2d}] {ts(e)}  {e.event_type:<30s} {detail}")

    # ── 3. Compliance stream ────────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  COMPLIANCE STREAM  compliance-{app_id}")
    print(f"{'─'*70}")

    compliance_events = await store.load_stream(f"compliance-{app_id}")
    if not compliance_events:
        print("  (no compliance events found)")
    else:
        for e in compliance_events:
            p = e.payload
            verdict = "✓ PASSED" if e.event_type == "ComplianceRulePassed" else "✗ FAILED"
            rule_id = p.get("rule_id", "")
            rule_name = p.get("rule_name", "")
            reason = p.get("failure_reason", "")
            hard = " [HARD BLOCK]" if p.get("is_hard_block") else ""
            print(f"  [{e.stream_position:2d}] {ts(e)}  {verdict}  {rule_id}  {rule_name}{hard}")
            if reason:
                print(f"        reason: {reason}")

    # ── 4. Causal chain summary ─────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  CAUSAL CHAIN  (correlation_id links)")
    print(f"{'─'*70}")
    for cid in sorted(correlation_ids):
        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM events WHERE metadata->>'correlation_id' = $1",
                cid,
            )
        print(f"  correlation_id={cid}  →  {count} events across all streams")

    # ── 5. Cryptographic integrity verification ────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  CRYPTOGRAPHIC INTEGRITY VERIFICATION")
    print(f"{'─'*70}")

    result = await run_integrity_check(store, "loan", app_id)
    elapsed_ms = (time.monotonic() - t_start) * 1000

    print(f"  Entity:          loan-{app_id}")
    print(f"  Events verified: {result.events_verified}")
    print(f"  Chain valid:     {'✓ YES' if result.chain_valid else '✗ NO'}")
    print(f"  Tamper detected: {'✗ YES — ALERT' if result.tamper_detected else '✓ NO'}")
    print(f"  Previous hash:   {result.previous_hash[:16]}…")
    print(f"  Current hash:    {result.current_hash[:16]}…")
    print(f"  Checked at:      {result.checked_at.isoformat()}")

    # ── Final timing ────────────────────────────────────────────────────────────
    total_elapsed = (time.monotonic() - t_start) * 1000
    print(f"\n{SEP2}")
    print(f"  COMPLETE  —  {total_elapsed:.0f}ms elapsed")
    slo = "✓ WITHIN 60s SLO" if total_elapsed < 60_000 else "✗ EXCEEDED 60s SLO"
    print(f"  {slo}")
    print(f"{SEP2}\n")

    await pool.close()


if __name__ == "__main__":
    app_id = sys.argv[1] if len(sys.argv) > 1 else "APEX-NARR05"
    asyncio.run(main(app_id))
