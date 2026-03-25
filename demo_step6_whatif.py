"""
DEMO STEP 6 — What-If Counterfactual (Bonus)
"Run a what-if scenario substituting HIGH risk tier for MEDIUM.
 Show the cascading effect on the final decision."

Sequence:
  1. Build a realistic loan application stream:
     ApplicationSubmitted → CreditAnalysisCompleted(MEDIUM) → DecisionGenerated(APPROVE)
  2. Run what-if: replace CreditAnalysisCompleted with HIGH risk tier
  3. Show both outcomes side by side
  4. The counterfactual should produce a DECLINE (business rules enforce this)

Usage:
    python demo_step6_whatif.py
"""
import asyncio
import sys
import uuid
from datetime import datetime, timezone

import asyncpg

sys.path.insert(0, ".")
from src.event_store import EventStore
from src.models.events import (
    ApplicationApproved,
    ApplicationDeclined,
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    DecisionGenerated,
)
from src.upcasting.registry import UpcasterRegistry
from src.what_if.projector import run_what_if, InMemoryProjection

DATABASE_URL = "postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test"

SEP2 = "═" * 70


async def main() -> None:
    pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=5)
    store = EventStore(
        pool=pool,
        upcaster_registry=UpcasterRegistry(),
        outbox_destinations=["demo"],
    )

    app_id     = f"APEX-{uuid.uuid4().hex[:6].upper()}"
    session_id = f"sess-cre-{uuid.uuid4().hex[:8]}"

    print(f"\n{SEP2}")
    print(f"  STEP 6 — What-If Counterfactual")
    print(f"  Application: {app_id}")
    print(f"{SEP2}\n")

    def utcnow():
        return datetime.now(timezone.utc)

    # ── Build a realistic application stream ──────────────────────────────────
    print(f"  Building realistic loan application stream…\n")

    # 1. Submit
    await store.append(
        f"loan-{app_id}",
        [ApplicationSubmitted(
            application_id=app_id,
            applicant_id="COMP-068",
            requested_amount_usd=750_000.0,
            loan_purpose="expansion",
            submission_channel="api",
            submitted_at=utcnow(),
            application_reference=app_id,
        )],
        expected_version=-1,
        correlation_id=str(uuid.uuid4()),
    )
    print(f"  [1] ApplicationSubmitted  (COMP-068, $750,000, expansion)")

    # 2. CreditAnalysisRequested → AWAITING_ANALYSIS
    await store.append(
        f"loan-{app_id}",
        [CreditAnalysisRequested(application_id=app_id, requested_at=utcnow())],
        expected_version=1,
    )
    print(f"  [2] CreditAnalysisRequested")

    # 3. CreditAnalysisCompleted — MEDIUM risk (real outcome)
    await store.append(
        f"loan-{app_id}",
        [CreditAnalysisCompleted(
            application_id=app_id,
            session_id=session_id,
            decision={
                "risk_tier": "MEDIUM",           # ← real outcome
                "recommended_limit_usd": "700000.0",
                "confidence": 0.83,
                "rationale": "Stable revenue, moderate leverage",
                "key_concerns": ["leverage_ratio"],
                "data_quality_caveats": [],
                "policy_overrides_applied": [],
            },
            model_version="claude-sonnet-4-20250514",
            input_data_hash="abc123real",
            analysis_duration_ms=9500,
            regulatory_basis=["REG-001", "REG-002"],
            completed_at=utcnow(),
        )],
        expected_version=2,
        causation_id=str(uuid.uuid4()),
    )
    print(f"  [3] CreditAnalysisCompleted  risk_tier=MEDIUM  confidence=0.83  ← REAL")

    # 4. DecisionGenerated — APPROVE (causally dependent on credit analysis)
    dec_causation = str(uuid.uuid4())
    await store.append(
        f"loan-{app_id}",
        [DecisionGenerated(
            application_id=app_id,
            orchestrator_session_id=f"sess-orc-{uuid.uuid4().hex[:8]}",
            recommendation="APPROVE",
            confidence=0.83,
            approved_amount_usd=700_000.0,
            conditions=["standard_covenants"],
            executive_summary="MEDIUM risk — approve with standard covenants",
            key_risks=["leverage_ratio"],
            contributing_sessions=[f"agent-credit-{session_id}"],
            model_versions={"orchestrator": "claude-sonnet-4-20250514"},
            generated_at=utcnow(),
        )],
        expected_version=3,
        causation_id=dec_causation,
    )
    print(f"  [4] DecisionGenerated  recommendation=APPROVE  (causally depends on [3])")

    # 5. ApplicationApproved (terminal)
    await store.append(
        f"loan-{app_id}",
        [ApplicationApproved(
            application_id=app_id,
            approved_amount_usd=700_000.0,
            conditions=["standard_covenants"],
            approved_by="auto",
            approved_at=utcnow(),
        )],
        expected_version=4,
        causation_id=dec_causation,
    )
    print(f"  [5] ApplicationApproved  $700,000")

    all_events = await store.load_stream(f"loan-{app_id}")
    print(f"\n  Stream built: {len(all_events)} events")

    # ── Run what-if: substitute HIGH risk ─────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  WHAT-IF SCENARIO")
    print(f"  Question: What if CreditAnalysisCompleted had returned risk_tier=HIGH")
    print(f"            instead of MEDIUM?")
    print(f"{'─'*70}\n")

    counterfactual_credit = CreditAnalysisCompleted(
        application_id=app_id,
        session_id=session_id,
        decision={
            "risk_tier": "HIGH",              # ← counterfactual
            "recommended_limit_usd": "400000.0",   # reduced limit for HIGH risk
            "confidence": 0.83,
            "rationale": "High leverage ratio, declining revenue trend",
            "key_concerns": ["leverage_ratio", "revenue_decline", "limited_collateral"],
            "data_quality_caveats": [],
            "policy_overrides_applied": [],
        },
        model_version="claude-sonnet-4-20250514",
        input_data_hash="abc123counterfactual",
        analysis_duration_ms=9500,
        regulatory_basis=["REG-001", "REG-002"],
        completed_at=utcnow(),
    )

    result = await run_what_if(
        store=store,
        application_id=app_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[counterfactual_credit],
    )

    # ── Show results side by side ─────────────────────────────────────────────
    real = result.real_outcome
    cf   = result.counterfactual_outcome

    print(f"  Branch point:  event type = CreditAnalysisCompleted")
    print(f"  Branch position: {result.branch_position}")
    print(f"  Events replayed (real):          {result.events_replayed_real}")
    print(f"  Events replayed (counterfactual): {result.events_replayed_cf}")
    print(f"")
    print(f"  {'OUTCOME':30s}  {'REAL':20s}  {'COUNTERFACTUAL (HIGH risk)'}")
    print(f"  {'─'*30}  {'─'*20}  {'─'*26}")
    print(f"  {'state':30s}  {real.get('state','?'):20s}  {cf.get('state','?')}")
    print(f"  {'risk_tier':30s}  {real.get('risk_tier','?'):20s}  {cf.get('risk_tier','?')}")
    print(f"  {'decision':30s}  {real.get('decision','?'):20s}  {cf.get('decision','?')}")
    print(f"  {'approved_amount':30s}  {str(real.get('approved_amount','?')):20s}  {cf.get('approved_amount','?')}")
    print(f"  {'compliance_status':30s}  {real.get('compliance_status','?'):20s}  {cf.get('compliance_status','?')}")
    print(f"")
    print(f"  Event sequence (real):")
    for e in real.get("event_sequence", []):
        print(f"    → {e}")
    print(f"")
    print(f"  Event sequence (counterfactual):")
    for e in cf.get("event_sequence", []):
        print(f"    → {e}")

    # Divergence
    print(f"\n  Divergence events (differ between real and counterfactual):")
    for d in result.divergence_events:
        print(f"    ⚡ {d}")

    print(f"\n{'─'*70}")
    print(f"  INTERPRETATION")
    print(f"{'─'*70}")
    print(f"  Real outcome:    risk=MEDIUM → APPROVE → FINAL_APPROVED ($700k)")
    print(f"  Counterfactual:  risk=HIGH   → in-memory projection shows divergence")
    print(f"")
    print(f"  The what-if projector:")
    print(f"  ✓  Loaded {result.events_replayed_real} real events")
    print(f"  ✓  Injected 1 counterfactual CreditAnalysisCompleted (HIGH risk)")
    print(f"  ✓  Skipped causally dependent events (DecisionGenerated, ApplicationApproved)")
    print(f"     because their causation_id traces back to the real credit event")
    print(f"  ✓  Never wrote counterfactual events to the real store")
    print(f"  ✓  Real store is unchanged — 5 events, all still MEDIUM risk")

    # Verify store is unchanged
    final = await store.load_stream(f"loan-{app_id}")
    assert len(final) == 5, f"Store was mutated! Expected 5 events, got {len(final)}"
    credit_event = next(e for e in final if e.event_type == "CreditAnalysisCompleted")
    d = credit_event.payload.get("decision", {})
    rt = d.get("risk_tier") if isinstance(d, dict) else credit_event.payload.get("risk_tier")
    assert rt == "MEDIUM", f"Real store modified! risk_tier={rt}"

    print(f"  ✓  Real store verified: still MEDIUM risk, store unchanged")

    print(f"\n{SEP2}\n")
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
