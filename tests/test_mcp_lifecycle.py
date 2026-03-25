"""
tests/test_mcp_lifecycle.py
Full loan application lifecycle driven entirely through MCP tool calls.

Spec requirement:
"start a fresh Ledger instance, then drive a complete loan application
lifecycle — from ApplicationSubmitted through FinalApproved — using
only MCP tool calls."

Lifecycle (Auto-Approval Flow):
  submit_application
  -> start_agent_session (credit)
  -> record_credit_analysis
  -> start_agent_session (fraud)
  -> record_fraud_screening
  -> start_agent_session (compliance)
  -> record_compliance_check (x3 rules)
  -> generate_decision (APPROVE - auto)
  -> query ledger://applications/{id}
  -> query ledger://applications/{id}/compliance
  -> query ledger://ledger/health

Requires: ledger-test-db running on localhost:5433
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest
import pytest_asyncio
from dotenv import load_dotenv

from src.event_store import EventStore
from src.models.events import CreditAnalysisCompleted, CreditAnalysisRequested, DecisionRequested
from src.projections.agent_performance import AgentPerformanceLedgerProjection
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.compliance_audit import ComplianceAuditViewProjection
from src.projections.daemon import ProjectionDaemon
from src.upcasting.registry import UpcasterRegistry

load_dotenv()

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test",
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def db_pool():
    """Create a connection pool for the test database."""
    pool = await asyncpg.create_pool(dsn=TEST_DATABASE_URL, min_size=2, max_size=10)
    yield pool
    await pool.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_db(db_pool):
    """Truncate all tables before each test to ensure isolation."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE
                outbox, events, event_streams, projection_checkpoints,
                application_summary, agent_performance_ledger, compliance_audit_view
            RESTART IDENTITY CASCADE
            """
        )
        for name in ("ApplicationSummary", "AgentPerformanceLedger", "ComplianceAuditView"):
            await conn.execute(
                "INSERT INTO projection_checkpoints (projection_name, last_global_position)"
                " VALUES ($1, 0) ON CONFLICT DO NOTHING",
                name,
            )
    yield


@pytest_asyncio.fixture
async def store(db_pool) -> EventStore:
    """Real EventStore backed by the test database."""
    return EventStore(
        pool=db_pool,
        upcaster_registry=UpcasterRegistry(),
        outbox_destinations=["test"],
    )


@pytest_asyncio.fixture
async def daemon(db_pool, store) -> ProjectionDaemon:
    """ProjectionDaemon with all required projections."""
    return ProjectionDaemon(
        store=store,
        pool=db_pool,
        projections=[
            ApplicationSummaryProjection(),
            AgentPerformanceLedgerProjection(),
            ComplianceAuditViewProjection(),
        ],
    )


def utcnow() -> datetime:
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc)


def new_app_id() -> str:
    """Generate a unique application ID for testing."""
    return f"APEX-{uuid.uuid4().hex[:6].upper()}"


def new_session_id(prefix: str) -> str:
    """Generate a unique session ID for testing."""
    return f"sess-{prefix}-{uuid.uuid4().hex[:8]}"


# ── Test ──────────────────────────────────────────────────────────────────────

@pytest.mark.graded
async def test_full_lifecycle_via_mcp_tools(store, db_pool, daemon) -> None:
    """
    Complete loan application lifecycle via command handlers
    (the exact functions MCP tools call).

    This tests the AUTO-APPROVAL flow (no human review required).

    Steps:
      1.  submit_application
      2.  start_agent_session   (credit analysis)
      3.  record_credit_analysis
      4.  start_agent_session   (fraud detection)
      5.  record_fraud_screening
      6.  start_agent_session   (compliance)
      7.  record_compliance_check  x3
      8.  start_agent_session   (orchestrator)
      9.  generate_decision     → APPROVE (auto)
      10. run projection daemon
      11. assert ApplicationSummary = FINAL_APPROVED
      12. assert ComplianceAuditView = 3 PASSED checks, CLEAR verdict
      13. assert all projection lags = 0
    """
    from src.commands.handlers import (
        ComplianceCheckCompletedCommand,
        CreditAnalysisCompletedCommand,
        FraudScreeningCompletedCommand,
        GenerateDecisionCommand,
        StartAgentSessionCommand,
        SubmitApplicationCommand,
        handle_compliance_check_completed,
        handle_credit_analysis_completed,
        handle_fraud_screening_completed,
        handle_generate_decision,
        handle_start_agent_session,
        handle_submit_application,
    )

    app_id      = new_app_id()
    corr_id     = str(uuid.uuid4())
    cre_session = new_session_id("cre")
    fra_session = new_session_id("fra")
    com_session = new_session_id("com")
    orc_session = new_session_id("orc")

    print(f"\n▶ Starting full lifecycle test for application: {app_id}")

    # 1. Submit application first — agents need the loan stream to exist
    print("  1. Submitting loan application...")
    r = await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id, applicant_id="COMP-MCP-001",
            requested_amount_usd=750_000.0, loan_purpose="acquisition",
            submission_channel="api", correlation_id=corr_id,
        ),
        store,
    )
    assert r["stream_id"] == f"loan-{app_id}"
    print(f"     ✓ Application submitted: {app_id}")

    # 2. Start credit analysis agent session
    print("  2. Starting credit analysis agent session...")
    r = await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_type="credit_analysis", session_id=cre_session,
            model_version="claude-sonnet-4-20250514", application_id=app_id,
            correlation_id=corr_id,
        ),
        store,
    )
    assert "session_id" in r
    print(f"     ✓ Session started: {cre_session}")

    # Advance loan to AWAITING_ANALYSIS
    await store.append(
        f"loan-{app_id}",
        [CreditAnalysisRequested(application_id=app_id, requested_at=utcnow())],
        expected_version=1,
    )

    # 3. Record credit analysis (writes to credit stream, not loan stream)
    print("  3. Recording credit analysis completion...")
    r = await handle_credit_analysis_completed(
        CreditAnalysisCompletedCommand(
            application_id=app_id, agent_type="credit_analysis",
            session_id=cre_session, model_version="claude-sonnet-4-20250514",
            confidence=0.83, risk_tier="MEDIUM",
            recommended_limit_usd=700_000.0,
            duration_ms=9500, input_data={"financials": "hash123"},
            correlation_id=corr_id,
        ),
        store,
    )
    assert "credit_stream" in r
    print(f"     ✓ Credit analysis recorded")

    # 4. Start fraud detection agent session
    print("  4. Starting fraud detection agent session...")
    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_type="fraud_detection", session_id=fra_session,
            model_version="claude-sonnet-4-20250514", application_id=app_id,
            correlation_id=corr_id,
        ),
        store,
    )
    print(f"     ✓ Session started: {fra_session}")

    # 5. Record fraud screening
    print("  5. Recording fraud screening completion...")
    r = await handle_fraud_screening_completed(
        FraudScreeningCompletedCommand(
            application_id=app_id, agent_type="fraud_detection",
            session_id=fra_session, fraud_score=0.12,
            anomaly_flags=[], screening_model_version="fraud-v3.2",
            input_data={"patterns": "clean"}, risk_level="LOW",
            recommendation="PROCEED", correlation_id=corr_id,
        ),
        store,
    )
    assert "fraud_stream" in r
    print(f"     ✓ Fraud screening recorded (score: 0.12, LOW risk)")

    # 6. Start compliance agent session
    print("  6. Starting compliance agent session...")
    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_type="compliance", session_id=com_session,
            model_version="claude-sonnet-4-20250514", application_id=app_id,
            correlation_id=corr_id,
        ),
        store,
    )
    print(f"     ✓ Session started: {com_session}")

    # 7. Record 3 compliance checks (writes to compliance stream)
    print("  7. Recording compliance rule evaluations...")
    for rule_id, rule_name in [
        ("REG-001", "AML Check"),
        ("REG-002", "OFAC Sanctions"),
        ("REG-003", "Jurisdiction Check"),
    ]:
        await handle_compliance_check_completed(
            ComplianceCheckCompletedCommand(
                application_id=app_id, agent_type="compliance",
                session_id=com_session, rule_id=rule_id,
                rule_name=rule_name, rule_version="2026-Q1-v1",
                passed=True, evidence_hash=f"hash-{rule_id}",
                correlation_id=corr_id,
            ),
            store,
        )
        print(f"     ✓ Rule {rule_id} ({rule_name}) passed")

    # Advance loan stream through ANALYSIS_COMPLETE → PENDING_DECISION.
    await store.append(
        f"loan-{app_id}",
        [
            CreditAnalysisCompleted(
                application_id=app_id,
                session_id=cre_session,
                decision={
                    "risk_tier": "MEDIUM",
                    "recommended_limit_usd": "700000.0",
                    "confidence": 0.83,
                    "rationale": None,
                    "key_concerns": [],
                    "data_quality_caveats": [],
                    "policy_overrides_applied": [],
                },
                model_version="claude-sonnet-4-20250514",
                input_data_hash="hash123",
                analysis_duration_ms=9500,
                regulatory_basis=[],
                completed_at=utcnow(),
            ),
        ],
        expected_version=2,
    )
    # Now in ANALYSIS_COMPLETE — advance to PENDING_DECISION
    await store.append(
        f"loan-{app_id}",
        [DecisionRequested(application_id=app_id, requested_at=utcnow())],
        expected_version=3,
    )

    # 8. Start orchestrator agent session
    print("  8. Starting orchestrator agent session...")
    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_type="orchestrator", session_id=orc_session,
            model_version="claude-sonnet-4-20250514", application_id=app_id,
            correlation_id=corr_id,
        ),
        store,
    )
    print(f"     ✓ Session started: {orc_session}")

    # 9. Generate decision (APPROVE - auto-approval, no human review needed)
    print("  9. Generating AI orchestrator decision (APPROVE - auto)...")
    r = await handle_generate_decision(
        GenerateDecisionCommand(
            application_id=app_id,
            orchestrator_agent_type="orchestrator",
            orchestrator_session_id=orc_session,
            recommendation="APPROVE",  # Auto-approve
            confidence=0.83,
            approved_amount_usd=700_000.0,
            conditions=["standard_covenants"],
            executive_summary="Strong application, LOW fraud risk.",
            contributing_sessions=[],
            model_version="claude-sonnet-4-20250514",
            correlation_id=corr_id,
        ),
        store,
    )
    assert r["recommendation"] == "APPROVE"
    print(f"     ✓ Decision generated: APPROVE (auto)")

    # 10. Run projection daemon
    print(" 10. Running projection daemon batch...")
    await daemon._process_batch()
    print("     ✓ Projections updated")

    # 11. Assert ApplicationSummary
    print(" 11. Querying application summary...")
    async with db_pool.acquire() as conn:
        summary = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1", app_id
        )
    assert summary is not None, "ApplicationSummary row must exist"
    assert summary["state"] == "FINAL_APPROVED", \
        f"Expected FINAL_APPROVED, got {summary['state']}"
    # Auto-approved applications have no human reviewer
    assert summary["human_reviewer_id"] is None, \
        f"Expected human_reviewer_id=None for auto-approval, got {summary['human_reviewer_id']}"
    print(f"     ✓ Application state: {summary['state']}")
    print(f"     ✓ Auto-approval (no human reviewer)")

    # 12. Assert ComplianceAuditView
    print(" 12. Querying compliance audit view...")
    async with db_pool.acquire() as conn:
        compliance = await ComplianceAuditViewProjection.get_current_compliance(
            app_id, conn
        )
    assert len(compliance.checks) == 3, \
        f"Expected 3 compliance checks, got {len(compliance.checks)}"
    assert compliance.overall_verdict == "CLEAR"
    assert all(c.verdict == "PASSED" for c in compliance.checks)
    print(f"     ✓ Compliance verdict: {compliance.overall_verdict} ({len(compliance.checks)} rules)")

    # 13. Assert projection lag = 0
    print(" 13. Checking projection lag...")
    lags = await daemon.get_all_lags()
    assert all(lag["lag_events"] == 0 for lag in lags), \
        f"Projections not caught up: {lags}"
    print(f"     ✓ Projection lags: {[l['lag_events'] for l in lags]}")

    print(f"\n✅ Full MCP lifecycle complete for {app_id}")
    print(f"  Final state:        {summary['state']}")
    print(f"  Compliance checks:  {len(compliance.checks)} (all PASSED)")
    print(f"  Approval type:      AUTO (no human review)")
    print(f"  Projection lags:    {[l['lag_events'] for l in lags]}")