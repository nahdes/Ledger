"""
DEMO STEP 2 — Concurrency Under Pressure
"Run the double-decision test live"

Shows two agents colliding on the same credit stream.
Agent A succeeds. Agent B gets OptimisticConcurrencyError, retries at the
correct version, and also succeeds.

The entire exchange is shown live with timestamps and error details.

Usage:
    python demo_step2_concurrency.py
"""
import asyncio
import sys
import time
import uuid
from datetime import datetime, timezone

import asyncpg

sys.path.insert(0, ".")
from src.event_store import EventStore
from src.models.events import CreditAnalysisCompleted, OptimisticConcurrencyError
from src.upcasting.registry import UpcasterRegistry

DATABASE_URL = "postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test"

SEP2 = "═" * 70

def ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:12]


async def main() -> None:
    pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=10)
    store = EventStore(
        pool=pool,
        upcaster_registry=UpcasterRegistry(),
        outbox_destinations=["demo"],
    )

    app_id    = f"DEMO-{uuid.uuid4().hex[:6].upper()}"
    stream_id = f"credit-{app_id}"

    print(f"\n{SEP2}")
    print(f"  STEP 2 — Concurrency Under Pressure")
    print(f"  Stream:  {stream_id}")
    print(f"{SEP2}\n")

    # ── Seed the stream at version 0 (empty, -1) ─────────────────────────────
    print(f"  [{ts()}]  Seeding stream at version -1 (new stream)\n")

    def make_credit_event(agent_label: str, risk_tier: str, confidence: float) -> CreditAnalysisCompleted:
        return CreditAnalysisCompleted(
            application_id=app_id,
            session_id=f"sess-cre-{agent_label}",
            decision={
                "risk_tier": risk_tier,
                "recommended_limit_usd": "500000",
                "confidence": confidence,
                "rationale": None,
                "key_concerns": [],
                "data_quality_caveats": [],
                "policy_overrides_applied": [],
            },
            model_version="claude-sonnet-4-20250514",
            input_data_hash=uuid.uuid4().hex[:16],
            analysis_duration_ms=1000,
            regulatory_basis=[],
            completed_at=datetime.now(timezone.utc),
        )

    # ── Define agent tasks ───────────────────────────────────────────────────
    agent_a_result = {}
    agent_b_result = {}
    log = []

    async def agent_a():
        """Agent A: reads stream at version -1, appends first."""
        log.append(f"  [{ts()}]  Agent A: reading stream (version=-1, empty)")
        await asyncio.sleep(0.01)   # tiny delay so output is ordered
        try:
            log.append(f"  [{ts()}]  Agent A: appending CreditAnalysisCompleted (LOW risk, confidence=0.87)")
            version = await store.append(
                stream_id,
                [make_credit_event("A", "LOW", 0.87)],
                expected_version=-1,
                correlation_id=str(uuid.uuid4()),
            )
            agent_a_result["status"]  = "SUCCESS"
            agent_a_result["version"] = version
            log.append(f"  [{ts()}]  Agent A: ✓ SUCCESS  stream now at version={version}")
        except OptimisticConcurrencyError as e:
            agent_a_result["status"] = "OCC_ERROR"
            agent_a_result["error"]  = e
            log.append(f"  [{ts()}]  Agent A: ✗ OCC ERROR  {e}")

    async def agent_b():
        """Agent B: reads stream at version -1 simultaneously, gets OCC, retries."""
        log.append(f"  [{ts()}]  Agent B: reading stream (version=-1, empty)")
        await asyncio.sleep(0.02)   # slightly behind A to make collision deterministic
        try:
            log.append(f"  [{ts()}]  Agent B: appending CreditAnalysisCompleted (MEDIUM risk, confidence=0.74)")
            await store.append(
                stream_id,
                [make_credit_event("B-first-attempt", "MEDIUM", 0.74)],
                expected_version=-1,   # B thinks stream is still empty — wrong!
                correlation_id=str(uuid.uuid4()),
            )
            agent_b_result["status"] = "SUCCESS_NO_COLLISION"
        except OptimisticConcurrencyError as e:
            agent_b_result["occ_error"] = e
            log.append(f"  [{ts()}]  Agent B: ✗ OptimisticConcurrencyError!")
            log.append(f"            error_type:       {e.to_dict()['error_type']}")
            log.append(f"            stream_id:        {e.to_dict()['stream_id']}")
            log.append(f"            expected_version: {e.to_dict()['expected_version']}")
            log.append(f"            actual_version:   {e.to_dict()['actual_version']}")
            log.append(f"            suggested_action: {e.to_dict()['suggested_action']}")
            log.append(f"")
            log.append(f"  [{ts()}]  Agent B: reloading stream to get current version…")

            # Reload and retry at the correct version
            events  = await store.load_stream(stream_id)
            current = events[-1].stream_position if events else 0
            log.append(f"  [{ts()}]  Agent B: stream has {len(events)} events, current version={current}")
            log.append(f"  [{ts()}]  Agent B: retrying append at expected_version={current}")

            version = await store.append(
                stream_id,
                [make_credit_event("B-retry", "MEDIUM", 0.74)],
                expected_version=current,
                correlation_id=str(uuid.uuid4()),
            )
            agent_b_result["status"]  = "SUCCESS_AFTER_RETRY"
            agent_b_result["version"] = version
            log.append(f"  [{ts()}]  Agent B: ✓ RETRY SUCCESS  stream now at version={version}")

    # ── Fire both agents concurrently ─────────────────────────────────────────
    print(f"  [{ts()}]  Firing Agent A and Agent B simultaneously via asyncio.gather()\n")
    await asyncio.gather(agent_a(), agent_b())

    # ── Print log in order ────────────────────────────────────────────────────
    for line in log:
        print(line)

    # ── Final state verification ──────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  FINAL STATE VERIFICATION")
    print(f"{'─'*70}")

    final_events = await store.load_stream(stream_id)
    credit_events = [e for e in final_events if e.event_type == "CreditAnalysisCompleted"]

    print(f"  Stream version:        {final_events[-1].stream_position if final_events else 0}")
    print(f"  Total events:          {len(final_events)}")
    print(f"  CreditAnalysis events: {len(credit_events)}  (one from A, one from B retry)")
    print(f"")

    for e in credit_events:
        d = e.payload.get("decision", {})
        rt = d.get("risk_tier", "?") if isinstance(d, dict) else "?"
        conf = d.get("confidence", "?") if isinstance(d, dict) else "?"
        sess = e.payload.get("session_id", "?")
        print(f"  pos={e.stream_position}  session={sess:<25s}  risk={rt}  confidence={conf}")

    print(f"\n  Agent A: {agent_a_result.get('status')}")
    print(f"  Agent B: {agent_b_result.get('status')}")
    print(f"")

    # Key assertion
    assert len(credit_events) == 2, f"Expected 2 events, got {len(credit_events)}"
    assert agent_a_result.get("status") == "SUCCESS"
    assert agent_b_result.get("status") == "SUCCESS_AFTER_RETRY"
    assert "occ_error" in agent_b_result

    print(f"  ✓  Stream integrity maintained: {len(credit_events)} events, no data lost")
    print(f"  ✓  OptimisticConcurrencyError raised and handled correctly")
    print(f"  ✓  Retry succeeded at correct version")
    print(f"\n{SEP2}\n")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
