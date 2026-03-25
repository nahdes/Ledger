"""
DEMO STEP 3 — Temporal Compliance Query
"Query ledger://applications/{id}/compliance?as_of={timestamp}"

Shows compliance state as it existed at a specific past moment,
distinct from the current state.

Seeds an application with 3 compliance checks, snapshots the time
between check 1 and check 2, then shows:
  - State at snapshot_time: 1 rule (REG-001 only)
  - State now:              3 rules (REG-001, REG-002, REG-003)

Usage:
    python demo_step3_temporal.py
"""
import asyncio
import sys
import time
import uuid
from datetime import datetime, timezone

import asyncpg

sys.path.insert(0, ".")
from src.event_store import EventStore
from src.models.events import (
    ApplicationSubmitted,
    ComplianceRulePassed,
    ComplianceRuleFailed,
)
from src.projections.compliance_audit import ComplianceAuditViewProjection
from src.projections.daemon import ProjectionDaemon
from src.projections.compliance_audit import ComplianceAuditViewProjection
from src.upcasting.registry import UpcasterRegistry

DATABASE_URL = "postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test"

SEP2 = "═" * 70


async def seed_projection_checkpoints(conn):
    await conn.execute(
        "INSERT INTO projection_checkpoints (projection_name, last_global_position) "
        "VALUES ('ComplianceAuditView', 0) ON CONFLICT DO NOTHING"
    )


async def main() -> None:
    pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=10)
    store = EventStore(
        pool=pool,
        upcaster_registry=UpcasterRegistry(),
        outbox_destinations=["demo"],
    )

    app_id     = f"DEMO-{uuid.uuid4().hex[:6].upper()}"
    session_id = f"sess-com-{uuid.uuid4().hex[:8]}"
    comp_stream = f"compliance-{app_id}"

    print(f"\n{SEP2}")
    print(f"  STEP 3 — Temporal Compliance Query")
    print(f"  Application: {app_id}")
    print(f"{SEP2}\n")

    async with pool.acquire() as conn:
        await seed_projection_checkpoints(conn)

    # ── 1. Submit application ─────────────────────────────────────────────────
    await store.append(
        f"loan-{app_id}",
        [ApplicationSubmitted(
            application_id=app_id,
            applicant_id="COMP-DEMO",
            requested_amount_usd=500_000.0,
            loan_purpose="expansion",
            submission_channel="demo",
            submitted_at=datetime.now(timezone.utc),
            application_reference=app_id,
        )],
        expected_version=-1,
    )

    # ── 2. Write REG-001 ──────────────────────────────────────────────────────
    print(f"  Writing REG-001 (AML Check)  →  PASSED")
    await store.append(
        comp_stream,
        [ComplianceRulePassed(
            application_id=app_id,
            session_id=session_id,
            rule_id="REG-001",
            rule_name="AML Check",
            rule_version="2026-Q1-v1",
            evidence_hash="hash-001",
            evaluation_notes="No AML flags",
            evaluated_at=datetime.now(timezone.utc),
        )],
        expected_version=-1,
    )

    # Run daemon to project REG-001
    daemon = ProjectionDaemon(
        store=store, pool=pool,
        projections=[ComplianceAuditViewProjection()],
    )
    await daemon._process_batch()

    # ── Take snapshot AFTER REG-001, BEFORE REG-002 ───────────────────────────
    await asyncio.sleep(0.1)
    snapshot_time = datetime.now(timezone.utc)
    await asyncio.sleep(0.1)

    print(f"  ⏱  Snapshot taken at {snapshot_time.strftime('%H:%M:%S.%f')[:12]}")
    print(f"     (REG-001 written, REG-002 and REG-003 not yet written)")

    # ── 3. Write REG-002 and REG-003 ─────────────────────────────────────────
    print(f"\n  Writing REG-002 (OFAC Sanctions)  →  PASSED")
    await store.append(
        comp_stream,
        [ComplianceRulePassed(
            application_id=app_id,
            session_id=session_id,
            rule_id="REG-002",
            rule_name="OFAC Sanctions",
            rule_version="2026-Q1-v1",
            evidence_hash="hash-002",
            evaluation_notes="No OFAC flags",
            evaluated_at=datetime.now(timezone.utc),
        )],
        expected_version=1,
    )

    print(f"  Writing REG-003 (Jurisdiction)  →  PASSED")
    await store.append(
        comp_stream,
        [ComplianceRulePassed(
            application_id=app_id,
            session_id=session_id,
            rule_id="REG-003",
            rule_name="Jurisdiction Check",
            rule_version="2026-Q1-v1",
            evidence_hash="hash-003",
            evaluation_notes="Eligible jurisdiction",
            evaluated_at=datetime.now(timezone.utc),
        )],
        expected_version=2,
    )

    await daemon._process_batch()
    print()

    # ── 4. Query: current state ───────────────────────────────────────────────
    async with pool.acquire() as conn:
        current_state = await ComplianceAuditViewProjection.get_current_compliance(
            app_id, conn
        )

    print(f"{'─'*70}")
    print(f"  QUERY: ledger://applications/{app_id}/compliance  (CURRENT)")
    print(f"{'─'*70}")
    print(f"  overall_verdict:  {current_state.overall_verdict}")
    print(f"  rules_evaluated:  {len(current_state.checks)}")
    for c in current_state.checks:
        print(f"    {c.rule_id}  {c.rule_name:<30s}  →  {c.verdict}")

    # ── 5. Query: compliance as-of snapshot_time ──────────────────────────────
    async with pool.acquire() as conn:
        past_state = await ComplianceAuditViewProjection.get_compliance_at(
            app_id, snapshot_time, conn
        )

    print(f"\n{'─'*70}")
    print(f"  QUERY: ledger://applications/{app_id}/compliance")
    print(f"         ?as_of={snapshot_time.strftime('%Y-%m-%dT%H:%M:%S.%f')}Z  (TEMPORAL)")
    print(f"{'─'*70}")
    print(f"  overall_verdict:  {past_state.overall_verdict}")
    print(f"  rules_evaluated:  {len(past_state.checks)}")
    for c in past_state.checks:
        print(f"    {c.rule_id}  {c.rule_name:<30s}  →  {c.verdict}")

    if not past_state.checks:
        print(f"    (no rules existed at this timestamp)")

    # ── 6. Show the difference ────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  TEMPORAL DIFF")
    print(f"{'─'*70}")
    current_ids = {c.rule_id for c in current_state.checks}
    past_ids    = {c.rule_id for c in past_state.checks}
    added_since = current_ids - past_ids

    print(f"  Rules at snapshot time:   {sorted(past_ids) or '(none)'}")
    print(f"  Rules now:                {sorted(current_ids)}")
    print(f"  Added after snapshot:     {sorted(added_since)}")
    print(f"")
    print(f"  ✓  Temporal query returns {len(past_state.checks)} rules at past timestamp")
    print(f"  ✓  Current query returns  {len(current_state.checks)} rules now")
    print(f"  ✓  States are distinct — time-travel query working correctly")

    assert len(past_state.checks) < len(current_state.checks), \
        "Temporal query must return fewer rules than current state"

    print(f"\n{SEP2}\n")
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
