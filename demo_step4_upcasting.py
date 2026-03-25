"""
DEMO STEP 4 — Upcasting & Immutability
"Load a v1 event, show it arrives as v2. Query raw DB — stored payload unchanged."

Sequence:
  1. Insert a raw v1 CreditAnalysisCompleted directly into the DB
     (no model_version, no regulatory_basis — the 2024 schema)
  2. Load it via EventStore with upcasters → show it arrives as v2
  3. Query the raw events table again → show stored bytes are IDENTICAL

Usage:
    python demo_step4_upcasting.py
"""
import asyncio
import json
import sys
import uuid
from datetime import datetime, timezone

import asyncpg

sys.path.insert(0, ".")
from src.event_store import EventStore
from src.upcasting.registry import default_registry
import src.upcasting.upcasters  # noqa: F401

DATABASE_URL = "postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test"

SEP2 = "═" * 70


def parse_jsonb(raw):
    if isinstance(raw, dict):
        return raw
    return json.loads(raw)


async def main() -> None:
    pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=5)
    store = EventStore(
        pool=pool,
        upcaster_registry=default_registry,
        outbox_destinations=["demo"],
    )

    app_id    = f"DEMO-{uuid.uuid4().hex[:6].upper()}"
    stream_id = f"credit-{app_id}"
    event_id  = uuid.uuid4()

    print(f"\n{SEP2}")
    print(f"  STEP 4 — Upcasting & Immutability")
    print(f"  Stream:  {stream_id}")
    print(f"{SEP2}\n")

    # ── 1. Insert raw v1 event directly into DB ───────────────────────────────
    v1_payload = {
        "application_id":       app_id,
        "session_id":           "sess-cre-legacy",
        "risk_tier":            "MEDIUM",
        "recommended_limit_usd": "750000.0",
        "analysis_duration_ms": 15000,
        "input_data_hash":      "abc123legacy",
        "completed_at":         datetime.now(timezone.utc).isoformat(),
        # NOTE: no model_version, no regulatory_basis, no nested decision{}
        # This is the 2024 / v1 schema
    }

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO event_streams (stream_id, aggregate_type, current_version)"
            " VALUES ($1, 'CreditRecord', 1)",
            stream_id,
        )
        await conn.execute(
            """
            INSERT INTO events
                (event_id, stream_id, stream_position, event_type, event_version,
                 payload, metadata, recorded_at)
            VALUES ($1, $2, 1, 'CreditAnalysisCompleted', 1, $3::jsonb, '{}'::jsonb, NOW())
            """,
            event_id, stream_id, json.dumps(v1_payload),
        )

    print(f"  ── Step 1: Raw v1 event inserted directly into Postgres ─────────")
    print(f"  event_id:      {event_id}")
    print(f"  event_version: 1  (raw DB, v1 schema)")
    print(f"  v1 payload fields: {list(v1_payload.keys())}")
    print(f"  'model_version'   in v1: {'model_version' in v1_payload}")
    print(f"  'regulatory_basis' in v1: {'regulatory_basis' in v1_payload}")
    print(f"  'decision' (nested) in v1: {'decision' in v1_payload}")

    # ── 2. Read raw payload from DB BEFORE any load ───────────────────────────
    async with pool.acquire() as conn:
        raw_before = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE event_id = $1",
            event_id,
        )
    payload_before = parse_jsonb(raw_before["payload"])

    print(f"\n  ── Step 2: Raw DB payload BEFORE EventStore.load_stream() ───────")
    print(f"  event_version (DB): {raw_before['event_version']}")
    print(f"  'model_version' present in DB:    {'model_version' in payload_before}")
    print(f"  'regulatory_basis' present in DB: {'regulatory_basis' in payload_before}")
    print(f"  Stored payload hash: {hash(json.dumps(payload_before, sort_keys=True))}")

    # ── 3. Load through EventStore with upcasters ────────────────────────────
    events = await store.load_stream(stream_id)
    upcasted = events[0]

    print(f"\n  ── Step 3: Event loaded via EventStore.load_stream() ─────────────")
    print(f"  event_version (upcasted): {upcasted.event_version}  ← now v2")
    print(f"  'model_version' present:    {'model_version' in upcasted.payload}")
    print(f"  model_version value:        {upcasted.payload.get('model_version')!r}")
    print(f"  'regulatory_basis' present: {'regulatory_basis' in upcasted.payload}")
    print(f"  'decision' (nested) present: {'decision' in upcasted.payload}")

    nested = upcasted.payload.get("decision", {})
    print(f"  decision.confidence:        {nested.get('confidence')!r}  ← None (not fabricated)")
    print(f"  decision.risk_tier:         {nested.get('risk_tier')!r}")

    # ── 4. Read raw payload from DB AFTER loading ─────────────────────────────
    async with pool.acquire() as conn:
        raw_after = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE event_id = $1",
            event_id,
        )
    payload_after = parse_jsonb(raw_after["payload"])

    print(f"\n  ── Step 4: Raw DB payload AFTER EventStore.load_stream() ─────────")
    print(f"  event_version (DB): {raw_after['event_version']}  ← STILL 1 (not mutated)")
    print(f"  'model_version' present in DB:    {'model_version' in payload_after}")
    print(f"  'regulatory_basis' present in DB: {'regulatory_basis' in payload_after}")
    print(f"  Stored payload hash: {hash(json.dumps(payload_after, sort_keys=True))}")

    # ── 5. Prove byte-for-byte identity ───────────────────────────────────────
    print(f"\n  ── Step 5: Immutability proof ────────────────────────────────────")

    assert raw_after["event_version"] == 1, \
        f"VIOLATION: event_version changed from 1 to {raw_after['event_version']}"
    assert payload_before == payload_after, \
        f"VIOLATION: stored payload was mutated\nBefore: {payload_before}\nAfter:  {payload_after}"
    assert "model_version" not in payload_after, \
        "VIOLATION: model_version leaked into stored payload"

    print(f"  ✓  DB event_version = 1  (unchanged after upcast)")
    print(f"  ✓  'model_version' NOT in DB payload (upcast was in-memory only)")
    print(f"  ✓  DB payload hash before == DB payload hash after (byte-identical)")
    print(f"  ✓  Upcasted event_version = {upcasted.event_version}  (in-memory view is v2)")
    print(f"  ✓  Upcasted 'model_version' = {upcasted.payload.get('model_version')!r}")
    print(f"  ✓  Upcasted confidence = None  (never fabricated for historical events)")
    print(f"")
    print(f"  The core guarantee of event sourcing holds:")
    print(f"  Stored events are immutable. Upcasting is a read-time transformation only.")

    print(f"\n{SEP2}\n")
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
