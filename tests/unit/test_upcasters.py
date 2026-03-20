"""
tests/unit/test_upcasters.py

Unit tests for UpcasterRegistry and the concrete upcasters.

THE IMMUTABILITY TEST (graded) is in TestImmutabilityGuarantee.
It proves that upcast() never touches the stored payload.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from src.models.events import StoredEvent
from src.upcasting.registry import UpcasterRegistry


def make_stored(event_type: str, version: int, payload: dict) -> StoredEvent:
    return StoredEvent(
        event_id        = uuid.uuid4(),
        stream_id       = "test-stream",
        stream_position = 1,
        global_position = 1,
        event_type      = event_type,
        event_version   = version,
        payload         = dict(payload),   # explicit copy — mimics what the DB returns
        metadata        = {},
        recorded_at     = datetime.now(timezone.utc),
    )


# =============================================================================
# Registry mechanics
# =============================================================================

class TestRegistryMechanics:

    def test_register_decorator_stores_upcaster(self):
        r = UpcasterRegistry()
        @r.register("Foo", from_version=1)
        def fn(p): return {**p, "v2": True}
        assert r.has_upcaster("Foo", 1)
        assert not r.has_upcaster("Foo", 2)

    def test_upcast_applies_single_step(self):
        r = UpcasterRegistry()
        @r.register("Foo", from_version=1)
        def v1_to_v2(p): return {**p, "added": True}

        ev = make_stored("Foo", 1, {"orig": "x"})
        v2 = r.upcast(ev)
        assert v2.event_version == 2
        assert v2.payload["added"] is True
        assert v2.payload["orig"] == "x"

    def test_upcast_applies_chain_v1_to_v3(self):
        r = UpcasterRegistry()
        @r.register("Bar", from_version=1)
        def step1(p): return {**p, "b": True}
        @r.register("Bar", from_version=2)
        def step2(p): return {**p, "c": True}

        ev = make_stored("Bar", 1, {"a": True})
        v3 = r.upcast(ev)
        assert v3.event_version == 3
        assert v3.payload == {"a": True, "b": True, "c": True}

    def test_current_version_passes_through(self):
        r = UpcasterRegistry()
        @r.register("X", from_version=1)
        def fn(p): return {**p, "new": 1}

        already_v2 = make_stored("X", 2, {"existing": True})
        result = r.upcast(already_v2)
        assert result is already_v2   # same object returned

    def test_unknown_event_type_passes_through(self):
        r = UpcasterRegistry()
        ev = make_stored("UnknownEvent", 1, {"x": 1})
        assert r.upcast(ev) is ev


# =============================================================================
# THE IMMUTABILITY TEST  (graded)
# =============================================================================

class TestImmutabilityGuarantee:
    """
    Upcasting must NEVER mutate the original StoredEvent.
    These assertions are checked independently during evaluation.
    """

    def test_upcast_does_not_mutate_original_payload(self):
        r = UpcasterRegistry()

        @r.register("CreditAnalysisCompleted", from_version=1)
        def add_fields(payload):
            return {**payload, "model_version": "legacy-pre-2026", "confidence": None}

        original_payload = {
            "application_id": "APEX-0001",
            "decision": "APPROVE",
            "risk_tier": "LOW",
        }
        v1 = make_stored("CreditAnalysisCompleted", 1, original_payload)
        original_snapshot = dict(v1.payload)

        v2 = r.upcast(v1)

        # ── v2 is correct ──────────────────────────────────────────────────────
        assert v2.event_version == 2
        assert v2.payload["model_version"] == "legacy-pre-2026"
        assert v2.payload["confidence"] is None
        assert v2.payload["application_id"] == "APEX-0001"

        # ── v1 is completely unchanged ─────────────────────────────────────────
        assert v1.event_version == 1, \
            "IMMUTABILITY VIOLATION: original event_version was modified"
        assert v1.payload == original_snapshot, \
            f"IMMUTABILITY VIOLATION: original payload was mutated\n" \
            f"  before: {original_snapshot}\n" \
            f"  after:  {v1.payload}"
        assert "model_version" not in v1.payload, \
            "IMMUTABILITY VIOLATION: new field leaked into original payload"
        assert "confidence" not in v1.payload, \
            "IMMUTABILITY VIOLATION: new field leaked into original payload"

    def test_upcast_returns_new_object_not_same_reference(self):
        r = UpcasterRegistry()

        @r.register("TestEvent", from_version=1)
        def step(p): return {**p, "x": 1}

        v1 = make_stored("TestEvent", 1, {"orig": True})
        v2 = r.upcast(v1)
        assert v2 is not v1
        assert v2.payload is not v1.payload

    def test_chain_does_not_mutate_intermediate_payloads(self):
        """Each step must not modify the payload dict from the previous step."""
        r = UpcasterRegistry()
        seen = []

        @r.register("Chain", from_version=1)
        def step1(p):
            seen.append(dict(p))
            return {**p, "v2": True}

        @r.register("Chain", from_version=2)
        def step2(p):
            seen.append(dict(p))
            return {**p, "v3": True}

        v1 = make_stored("Chain", 1, {"orig": True})
        v3 = r.upcast(v1)

        assert v1.payload == {"orig": True}           # original unchanged
        assert seen[0] == {"orig": True}              # step1 saw only orig
        assert seen[1] == {"orig": True, "v2": True}  # step2 saw v2 additions
        assert v3.payload == {"orig": True, "v2": True, "v3": True}


# =============================================================================
# Concrete CreditAnalysisCompleted upcaster (Phase 4)
# =============================================================================

class TestCreditAnalysisConcreteUpcaster:
    """
    Tests the specific upcaster defined in src/upcasting/upcasters.py.
    """

    def _get_registry(self):
        try:
            import src.upcasting.upcasters  # noqa: F401 – triggers registration
            from src.upcasting.registry import default_registry
            return default_registry
        except ImportError:
            pytest.skip("src/upcasting/upcasters.py not yet implemented")

    def test_v1_gets_model_version_sentinel(self):
        r = self._get_registry()
        v1 = make_stored("CreditAnalysisCompleted", 1, {
            "application_id": "APEX-0016",
            "risk_tier": "MEDIUM",
            "recommended_limit_usd": "957000.0",
            "analysis_duration_ms": 20747,
            "input_data_hash": "fd44aaf0255f47c8",
        })
        v2 = r.upcast(v1)
        assert v2.event_version == 2
        assert "model_version" in v2.payload

    def test_v1_confidence_is_none_not_fabricated(self):
        """confidence must be None — never fabricate a value that wasn't measured."""
        r = self._get_registry()
        v1 = make_stored("CreditAnalysisCompleted", 1, {
            "application_id": "APEX-0016",
            "risk_tier": "LOW",
        })
        v2 = r.upcast(v1)
        decision = v2.payload.get("decision", {})
        confidence = decision.get("confidence") if isinstance(decision, dict) else v2.payload.get("confidence")
        assert confidence is None, \
            "confidence_score MUST be None for v1 events — never fabricate a value"

    def test_v2_event_passes_through_unchanged(self):
        """A v2 event from the seed data should not be double-upcasted."""
        r = self._get_registry()
        v2_payload = {
            "application_id": "APEX-0016",
            "session_id": "sess-cre-64885ba4",
            "decision": {
                "risk_tier": "MEDIUM",
                "recommended_limit_usd": "957000.0",
                "confidence": 0.81,
            },
            "model_version": "claude-sonnet-4-20250514",
            "model_deployment_id": "dep-18a397f1",
            "input_data_hash": "fd44aaf0255f47c8",
            "analysis_duration_ms": 20747,
            "regulatory_basis": [],
        }
        v2 = make_stored("CreditAnalysisCompleted", 2, v2_payload)
        result = r.upcast(v2)
        # No v2→v3 upcaster registered — returns same object
        assert result.event_version == 2
        assert result.payload["decision"]["confidence"] == 0.81
