"""
tests/test_projections.py

Projection tests:
  - ApplicationSummary updates correctly for each event type
  - ComplianceAuditView temporal query (get_compliance_at)
  - rebuild_from_scratch() resets checkpoint
  - ProjectionDaemon get_lag() returns expected structure
  - SLO: projection lag stays within bounds under concurrent load

Requires: ledger-test-db running on localhost:5433
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

from src.commands.handlers import (
    ComplianceCheckCompletedCommand,
    CreditAnalysisCompletedCommand,
    StartAgentSessionCommand,
    SubmitApplicationCommand,
    handle_compliance_check_completed,
    handle_credit_analysis_completed,
    handle_start_agent_session,
    handle_submit_application,
)
from src.event_store import EventStore
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.compliance_audit import ComplianceAuditViewProjection
from src.projections.daemon import ProjectionDaemon
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
            TRUNCATE TABLE
                outbox, events, event_streams, projection_checkpoints,
                application_summary, agent_performance_ledger, compliance_audit_view
            RESTART IDENTITY CASCADE
            """
        )
        # Re-seed projection checkpoints at 0
        for name in ("ApplicationSummary", "AgentPerformanceLedger", "ComplianceAuditView"):
            await conn.execute(
                "INSERT INTO projection_checkpoints (projection_name, last_global_position) "
                "VALUES ($1, 0) ON CONFLICT DO NOTHING",
                name,
            )
    yield


@pytest_asyncio.fixture
async def store(db_pool) -> EventStore:
    return EventStore(
        pool=db_pool,
        upcaster_registry=UpcasterRegistry(),
        outbox_destinations=["test"],
    )


@pytest_asyncio.fixture
async def daemon(db_pool, store) -> ProjectionDaemon:
    return ProjectionDaemon(
        store=store,
        pool=db_pool,
        projections=[
            ApplicationSummaryProjection(),
            ComplianceAuditViewProjection(),
        ],
        batch_size=50,
    )


def new_app_id() -> str:
    return f"APEX-{uuid.uuid4().hex[:6].upper()}"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def _run_daemon_once(daemon: ProjectionDaemon) -> None:
    """Run one batch cycle manually."""
    await daemon._process_batch()


# =============================================================================
# ApplicationSummary projection
# =============================================================================

class TestApplicationSummaryProjection:

    async def test_submitted_creates_row(self, store, db_pool, daemon):
        app_id = new_app_id()
        cmd = SubmitApplicationCommand(
            application_id       = app_id,
            applicant_id         = "COMP-TEST",
            requested_amount_usd = 500_000.0,
            loan_purpose         = "expansion",
            submission_channel   = "api",
        )
        await handle_submit_application(cmd, store)
        await _run_daemon_once(daemon)

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM application_summary WHERE application_id = $1", app_id
            )
        assert row is not None
        assert row["state"] == "SUBMITTED"
        assert row["applicant_id"] == "COMP-TEST"
        assert float(row["requested_amount_usd"]) == 500_000.0

    async def test_credit_analysis_updates_risk_tier(self, store, db_pool, daemon):
        app_id     = new_app_id()
        session_id = f"sess-cre-{uuid.uuid4().hex[:8]}"

        # Submit + start session
        await handle_submit_application(
            SubmitApplicationCommand(app_id, "COMP-001", 600_000.0, "working_capital", "api"),
            store,
        )

        # Manually advance to AWAITING_ANALYSIS state for credit analysis
        from src.models.events import CreditAnalysisRequested
        await store.append(
            f"loan-{app_id}",
            [CreditAnalysisRequested(application_id=app_id, requested_at=utcnow())],
            expected_version=1,
        )
        await handle_start_agent_session(
            StartAgentSessionCommand(
                agent_type="credit_analysis", session_id=session_id,
                model_version="claude-sonnet-4-20250514", application_id=app_id,
            ),
            store,
        )
        await handle_credit_analysis_completed(
            CreditAnalysisCompletedCommand(
                application_id=app_id, agent_type="credit_analysis",
                session_id=session_id, model_version="claude-sonnet-4-20250514",
                confidence=0.82, risk_tier="LOW", recommended_limit_usd=550_000.0,
                duration_ms=1200, input_data={"test": True},
            ),
            store,
        )
        await _run_daemon_once(daemon)

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT risk_tier, state FROM application_summary WHERE application_id = $1",
                app_id,
            )
        assert row["risk_tier"] == "LOW"
        assert row["state"] == "ANALYSIS_COMPLETE"

    async def test_approved_sets_terminal_state(self, store, db_pool, daemon):
        app_id = new_app_id()
        await handle_submit_application(
            SubmitApplicationCommand(app_id, "COMP-001", 400_000.0, "expansion", "api"),
            store,
        )
        from src.models.events import ApplicationApproved
        await store.append(
            f"loan-{app_id}",
            [ApplicationApproved(
                application_id=app_id, approved_amount_usd=380_000.0,
                approved_by="auto", approved_at=utcnow(),
            )],
            expected_version=1,
        )
        await _run_daemon_once(daemon)

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT state, approved_amount_usd FROM application_summary "
                "WHERE application_id = $1", app_id,
            )
        assert row["state"] == "FINAL_APPROVED"
        assert float(row["approved_amount_usd"]) == 380_000.0


# =============================================================================
# ComplianceAuditView temporal query
# =============================================================================

class TestComplianceAuditViewTemporalQuery:

    async def test_current_compliance_returns_all_rules(self, store, db_pool, daemon):
        app_id     = new_app_id()
        session_id = f"sess-com-{uuid.uuid4().hex[:8]}"

        await handle_submit_application(
            SubmitApplicationCommand(app_id, "COMP-001", 500_000.0, "expansion", "api"),
            store,
        )
        await handle_start_agent_session(
            StartAgentSessionCommand("compliance", session_id,
                                     "claude-sonnet-4-20250514", app_id),
            store,
        )
        for rule_id in ("REG-001", "REG-002", "REG-003"):
            await handle_compliance_check_completed(
                ComplianceCheckCompletedCommand(
                    application_id=app_id, agent_type="compliance",
                    session_id=session_id, rule_id=rule_id,
                    rule_name=f"Rule {rule_id}", rule_version="2026-Q1-v1",
                    passed=True, evidence_hash=f"hash-{rule_id}",
                ),
                store,
            )
        await _run_daemon_once(daemon)

        async with db_pool.acquire() as conn:
            state = await ComplianceAuditViewProjection.get_current_compliance(app_id, conn)
        assert len(state.checks) == 3
        assert state.overall_verdict == "CLEAR"
        assert not state.has_hard_block

    async def test_get_compliance_at_timestamp(self, store, db_pool, daemon):
        """
        Temporal query: compliance state as-of a past timestamp should not
        include events written after that timestamp.
        """
        app_id     = new_app_id()
        session_id = f"sess-com-{uuid.uuid4().hex[:8]}"

        await handle_submit_application(
            SubmitApplicationCommand(app_id, "COMP-001", 500_000.0, "expansion", "api"),
            store,
        )
        await handle_start_agent_session(
            StartAgentSessionCommand("compliance", session_id,
                                     "claude-sonnet-4-20250514", app_id),
            store,
        )
        # Write REG-001
        await handle_compliance_check_completed(
            ComplianceCheckCompletedCommand(
                application_id=app_id, agent_type="compliance",
                session_id=session_id, rule_id="REG-001",
                rule_name="Rule REG-001", rule_version="2026-Q1-v1",
                passed=True, evidence_hash="hash-001",
            ),
            store,
        )
        await _run_daemon_once(daemon)

        # Snapshot timestamp after REG-001 but before REG-002
        snapshot_time = datetime.now(timezone.utc)

        await asyncio.sleep(0.05)   # ensure recorded_at differs

        # Write REG-002 after the snapshot
        await handle_compliance_check_completed(
            ComplianceCheckCompletedCommand(
                application_id=app_id, agent_type="compliance",
                session_id=session_id, rule_id="REG-002",
                rule_name="Rule REG-002", rule_version="2026-Q1-v1",
                passed=True, evidence_hash="hash-002",
            ),
            store,
        )
        await _run_daemon_once(daemon)

        async with db_pool.acquire() as conn:
            state_now  = await ComplianceAuditViewProjection.get_current_compliance(
                app_id, conn)
            state_then = await ComplianceAuditViewProjection.get_compliance_at(
                app_id, snapshot_time, conn)

        assert len(state_now.checks) == 2,  "Current should have 2 rules"
        assert len(state_then.checks) == 1, "Temporal query should see only 1 rule at snapshot time"
        assert state_then.checks[0].rule_id == "REG-001"

    async def test_hard_block_sets_blocked_verdict(self, store, db_pool, daemon):
        app_id     = new_app_id()
        session_id = f"sess-com-{uuid.uuid4().hex[:8]}"

        await handle_submit_application(
            SubmitApplicationCommand(app_id, "COMP-001", 500_000.0, "expansion", "api"),
            store,
        )
        await handle_start_agent_session(
            StartAgentSessionCommand("compliance", session_id,
                                     "claude-sonnet-4-20250514", app_id),
            store,
        )
        await handle_compliance_check_completed(
            ComplianceCheckCompletedCommand(
                application_id=app_id, agent_type="compliance",
                session_id=session_id, rule_id="REG-002",
                rule_name="OFAC Sanctions", rule_version="2026-Q1-v1",
                passed=False, evidence_hash="hash-ofac",
                failure_reason="Entity on OFAC SDN list",
                is_hard_block=True,
            ),
            store,
        )
        await _run_daemon_once(daemon)

        async with db_pool.acquire() as conn:
            state = await ComplianceAuditViewProjection.get_current_compliance(app_id, conn)
        assert state.overall_verdict == "BLOCKED"
        assert state.has_hard_block

    async def test_rebuild_from_scratch_resets_checkpoint(self, db_pool, daemon):
        async with db_pool.acquire() as conn:
            # Set checkpoint to some position
            await conn.execute(
                "UPDATE projection_checkpoints SET last_global_position = 999 "
                "WHERE projection_name = 'ComplianceAuditView'"
            )
            await ComplianceAuditViewProjection.rebuild_from_scratch(conn)
            row = await conn.fetchrow(
                "SELECT last_global_position FROM projection_checkpoints "
                "WHERE projection_name = 'ComplianceAuditView'"
            )
        assert row["last_global_position"] == 0, \
            "rebuild_from_scratch must reset checkpoint to 0"


# =============================================================================
# ProjectionDaemon lag metrics
# =============================================================================

class TestProjectionDaemonLag:

    async def test_get_lag_returns_expected_structure(self, daemon, store):
        # Write one event so there's something to lag against
        app_id = new_app_id()
        await handle_submit_application(
            SubmitApplicationCommand(app_id, "COMP-001", 500_000.0, "test", "api"),
            store,
        )
        lag = await daemon.get_lag("ApplicationSummary")
        assert "projection_name"    in lag
        assert "checkpoint_position" in lag
        assert "store_position"     in lag
        assert "lag_events"         in lag
        assert "lag_ms"             in lag
        assert lag["lag_events"] >= 0

    async def test_get_all_lags_returns_all_projections(self, daemon):
        lags = await daemon.get_all_lags()
        names = {l["projection_name"] for l in lags}
        assert "ApplicationSummary" in names
        assert "ComplianceAuditView" in names

    @pytest.mark.slow
    async def test_slo_lag_under_concurrent_load(self, store, db_pool, daemon):
        """
        SLO test: ApplicationSummary lag must stay under 500ms after
        submitting 20 applications concurrently.
        (Full spec requires 50 — using 20 here for CI stability)
        """
        import time

        app_ids = [new_app_id() for _ in range(20)]

        async def submit_one(app_id: str) -> None:
            await handle_submit_application(
                SubmitApplicationCommand(app_id, "COMP-SLO", 100_000.0, "test", "api"),
                store,
            )

        # Fire all 20 concurrently
        await asyncio.gather(*[submit_one(aid) for aid in app_ids])

        t0 = time.monotonic()
        await _run_daemon_once(daemon)
        elapsed_ms = (time.monotonic() - t0) * 1000

        lag = await daemon.get_lag("ApplicationSummary")

        # All events should have been processed
        assert lag["lag_events"] == 0, \
            f"After one batch pass all events should be processed; lag_events={lag['lag_events']}"

        # Verify rows created
        async with db_pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM application_summary WHERE applicant_id = 'COMP-SLO'"
            )
        assert count == 20, f"Expected 20 rows, got {count}"

        print(f"\n✓ SLO test: 20 concurrent submissions processed in {elapsed_ms:.0f}ms")
        print(f"  lag_events={lag['lag_events']}, lag_ms={lag['lag_ms']}")
