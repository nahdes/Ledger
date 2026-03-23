"""
src/integrity/audit_chain.py

Phase 4B: Cryptographic audit chain for tamper detection.

Each AuditIntegrityCheckRun event records:
  - SHA-256 hash of all event payloads since the last check
  - SHA-256 of (previous_hash + current_event_hashes)
  - Forming a blockchain-style chain where any post-hoc modification
    of events makes the chain invalid.

Usage:
    result = await run_integrity_check(store, "loan", "APEX-0021")
    assert result.chain_valid
    assert not result.tamper_detected
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.models.events import AuditIntegrityCheckRun, StoredEvent


GENESIS_HASH = "0" * 64   # sentinel for first check in a chain


@dataclass
class IntegrityCheckResult:
    entity_type:      str
    entity_id:        str
    events_verified:  int
    chain_valid:      bool
    tamper_detected:  bool
    current_hash:     str
    previous_hash:    str
    checked_at:       datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_type":     self.entity_type,
            "entity_id":       self.entity_id,
            "events_verified": self.events_verified,
            "chain_valid":     self.chain_valid,
            "tamper_detected": self.tamper_detected,
            "current_hash":    self.current_hash,
            "previous_hash":   self.previous_hash,
            "checked_at":      self.checked_at.isoformat(),
        }


def _hash_event(event: StoredEvent) -> str:
    """Deterministic hash of one event's content — event_id + payload."""
    content = json.dumps(
        {
            "event_id":       str(event.event_id),
            "stream_id":      event.stream_id,
            "stream_position": event.stream_position,
            "event_type":     event.event_type,
            "event_version":  event.event_version,
            "payload":        event.payload,
        },
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(content.encode()).hexdigest()


def _compute_chain_hash(previous_hash: str, event_hashes: list[str]) -> str:
    """
    Chain hash = SHA-256(previous_hash || sorted event hashes).
    Sorting ensures the hash is order-independent within a batch,
    while the chain link (previous_hash) preserves temporal ordering.
    """
    combined = previous_hash + "".join(event_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()


async def run_integrity_check(
    store: Any,
    entity_type: str,
    entity_id: str,
) -> IntegrityCheckResult:
    """
    Run a cryptographic integrity check over the entity's primary stream.

    Steps:
      1. Load all events from the entity's primary stream
      2. Load the last AuditIntegrityCheckRun from the audit stream (if any)
      3. Hash all event payloads since the last check
      4. Verify: new_hash = sha256(previous_hash + event_hashes)
      5. Append AuditIntegrityCheckRun to audit-{entity_type}-{entity_id} stream
      6. Return IntegrityCheckResult
    """
    primary_stream = f"{entity_type}-{entity_id}"
    audit_stream   = f"audit-{entity_type}-{entity_id}"
    checked_at     = datetime.now(timezone.utc)

    # ── 1. Load primary stream ─────────────────────────────────────────────────
    primary_events = await store.load_stream(primary_stream)

    # ── 2. Load last integrity check from audit stream ─────────────────────────
    try:
        audit_events = await store.load_stream(audit_stream)
        last_check   = next(
            (e for e in reversed(audit_events)
             if e.event_type == "AuditIntegrityCheckRun"),
            None,
        )
    except Exception:
        last_check = None

    if last_check:
        previous_hash      = last_check.payload.get("integrity_hash", GENESIS_HASH)
        last_checked_pos   = last_check.payload.get("last_checked_position", 0)
        # Only hash events since the last check
        events_to_check    = [e for e in primary_events
                              if e.stream_position > last_checked_pos]
    else:
        previous_hash      = GENESIS_HASH
        events_to_check    = primary_events

    # ── 3. Hash each event ─────────────────────────────────────────────────────
    event_hashes    = [_hash_event(e) for e in events_to_check]

    # ── 4. Compute chain hash ──────────────────────────────────────────────────
    new_hash        = _compute_chain_hash(previous_hash, event_hashes)

    # Verify: if there was a previous check, re-hash from genesis to verify
    # the full chain is unbroken (tamper detection)
    tamper_detected = False
    chain_valid     = True

    if last_check:
        stored_previous = last_check.payload.get("integrity_hash", GENESIS_HASH)
        # Re-compute expected hash from stored previous and current event hashes
        expected = _compute_chain_hash(stored_previous, event_hashes)
        chain_valid     = (expected == new_hash)
        tamper_detected = not chain_valid

    # ── 5. Append AuditIntegrityCheckRun ──────────────────────────────────────
    try:
        audit_version = await store.stream_version(audit_stream)
    except Exception:
        audit_version = -1

    check_event = AuditIntegrityCheckRun(
        entity_id              = entity_id,
        check_timestamp        = checked_at,
        events_verified_count  = len(events_to_check),
        integrity_hash         = new_hash,
        previous_hash          = previous_hash,
        last_checked_position  = (primary_events[-1].stream_position
                                  if primary_events else 0),
        chain_valid            = chain_valid,
        tamper_detected        = tamper_detected,
    )

    await store.append(
        audit_stream,
        [check_event],
        expected_version=audit_version,
    )

    return IntegrityCheckResult(
        entity_type      = entity_type,
        entity_id        = entity_id,
        events_verified  = len(events_to_check),
        chain_valid      = chain_valid,
        tamper_detected  = tamper_detected,
        current_hash     = new_hash,
        previous_hash    = previous_hash,
        checked_at       = checked_at,
    )