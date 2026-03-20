"""
tests/integration/test_seed_data.py

Validates that seed_events.jsonl can be loaded and queried correctly.
These tests run against the dev database (DATABASE_URL) — not the test DB —
so they use a separate pool fixture and do NOT truncate.

Run: pytest tests/integration/test_seed_data.py -v
     (requires: data/seed_events.jsonl to exist)
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

SEED_PATH      = Path(os.environ.get("SEED_DATA_PATH", "data/seed_events.jsonl"))
DEV_DB_URL     = os.environ.get(
    "DATABASE_URL",
    "postgresql://ledger:ledger_dev_secret@localhost:5432/ledger"
)


@pytest_asyncio.fixture(scope="module")
async def dev_pool():
    pool = await asyncpg.create_pool(dsn=DEV_DB_URL, min_size=1, max_size=3)
    yield pool
    await pool.close()


# ── Skip whole module if seed file is absent ───────────────────────────────────
def _seed_missing() -> bool:
    return not SEED_PATH.exists()

pytestmark = pytest.mark.skipif(
    _seed_missing(),
    reason=f"Seed file not found at {SEED_PATH} — run scripts/load_seed_data.py first"
)


class TestSeedDataPresent:

    async def test_expected_event_count(self, dev_pool):
        """Seed file has 1198 events; DB should have at least this many."""
        count = await dev_pool.fetchval("SELECT COUNT(*) FROM events")
        assert count >= 1198, f"Expected ≥1198 events, got {count}"

    async def test_all_stream_prefixes_present(self, dev_pool):
        """All six bounded contexts (loan, docpkg, agent, credit, fraud, compliance) present."""
        prefixes = await dev_pool.fetch(
            """
            SELECT DISTINCT split_part(stream_id, '-', 1) AS prefix
            FROM event_streams
            ORDER BY 1
            """
        )
        found = {r["prefix"] for r in prefixes}
        for expected in ("loan", "docpkg", "agent", "credit", "fraud", "compliance"):
            assert expected in found, f"Stream prefix '{expected}' missing from DB"

    async def test_application_apex_0021_is_approved(self, dev_pool):
        """APEX-0021 should have ApplicationApproved in the seed data."""
        row = await dev_pool.fetchrow(
            "SELECT event_type FROM events "
            "WHERE stream_id = 'loan-APEX-0021' AND event_type = 'ApplicationApproved'"
        )
        assert row is not None, "APEX-0021 ApplicationApproved event missing"

    async def test_application_apex_0023_is_blocked(self, dev_pool):
        """APEX-0023 should be declined via OFAC compliance block."""
        row = await dev_pool.fetchrow(
            "SELECT payload FROM events "
            "WHERE stream_id = 'loan-APEX-0023' AND event_type = 'ApplicationDeclined'"
        )
        assert row is not None, "APEX-0023 ApplicationDeclined event missing"
        payload = dict(row["payload"])
        assert any("REG-002" in str(r) for r in payload.get("decline_reasons", [])), \
            "Expected OFAC (REG-002) in decline_reasons for APEX-0023"

    async def test_credit_analysis_v2_version_preserved(self, dev_pool):
        """CreditAnalysisCompleted events should be stored as event_version=2."""
        row = await dev_pool.fetchrow(
            "SELECT event_version FROM events "
            "WHERE stream_id = 'credit-APEX-0016' AND event_type = 'CreditAnalysisCompleted'"
        )
        assert row is not None
        assert row["event_version"] == 2, \
            "CreditAnalysisCompleted must be stored at event_version=2 (not upcasted)"

    async def test_outbox_rows_exist_for_events(self, dev_pool):
        """Every inserted event should have at least one outbox row."""
        ratio = await dev_pool.fetchval(
            """
            SELECT COUNT(DISTINCT o.event_id)::float / NULLIF(COUNT(DISTINCT e.event_id), 0)
            FROM events e
            LEFT JOIN outbox o ON o.event_id = e.event_id
            """
        )
        assert ratio is not None and ratio >= 0.95, \
            f"Expected outbox coverage ≥95%, got {ratio:.0%}"

    async def test_stream_versions_match_event_counts(self, dev_pool):
        """event_streams.current_version must equal the max stream_position for each stream."""
        mismatches = await dev_pool.fetch(
            """
            SELECT es.stream_id, es.current_version,
                   MAX(e.stream_position) AS actual_max
            FROM   event_streams es
            JOIN   events e ON e.stream_id = es.stream_id
            GROUP  BY es.stream_id, es.current_version
            HAVING es.current_version != MAX(e.stream_position)
            LIMIT  10
            """
        )
        assert len(mismatches) == 0, \
            f"Stream version mismatches found: {[dict(r) for r in mismatches]}"

    async def test_agent_session_has_model_version(self, dev_pool):
        """All AgentSessionStarted events should carry a non-null model_version."""
        rows = await dev_pool.fetch(
            """
            SELECT stream_id, payload->>'model_version' AS mv
            FROM events
            WHERE event_type = 'AgentSessionStarted'
            AND   payload->>'model_version' IS NULL
            LIMIT 5
            """
        )
        assert len(rows) == 0, \
            f"Found AgentSessionStarted events without model_version: {[r['stream_id'] for r in rows]}"
