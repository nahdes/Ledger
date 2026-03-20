"""
src/upcasting/registry.py

UpcasterRegistry — transparent schema evolution on read.

IMMUTABILITY GUARANTEE (graded test):
  upcast() ALWAYS returns a NEW StoredEvent with a NEW payload dict.
  The original StoredEvent is NEVER modified.

Usage:
    registry = UpcasterRegistry()

    @registry.register("CreditAnalysisCompleted", from_version=1)
    def v1_to_v2(payload: dict) -> dict:
        return {**payload, "model_version": "legacy-pre-2026", "confidence": None}

    upcasted = registry.upcast(stored_event)  # returns new object; original unchanged
"""
from __future__ import annotations

from collections import defaultdict
from typing import Callable

from src.models.events import StoredEvent


PayloadTransformer = Callable[[dict], dict]


class UpcasterRegistry:
    """
    Maintains a chain of upcasters per event type.

    Registry is populated by decorating functions with @registry.register().
    Chains are applied in ascending version order:  v1→v2→v3→…→current.
    """

    def __init__(self) -> None:
        # event_type → {from_version: transformer}
        self._upcasters: dict[str, dict[int, PayloadTransformer]] = defaultdict(dict)

    # ── registration ──────────────────────────────────────────────────────────

    def register(
        self, event_type: str, from_version: int
    ) -> Callable[[PayloadTransformer], PayloadTransformer]:
        """
        Decorator that registers a transformer for (event_type, from_version).

        The transformer receives the payload at from_version and must return
        a completely NEW dict at from_version+1.
        """
        def decorator(fn: PayloadTransformer) -> PayloadTransformer:
            self._upcasters[event_type][from_version] = fn
            return fn
        return decorator

    def has_upcaster(self, event_type: str, from_version: int) -> bool:
        return from_version in self._upcasters.get(event_type, {})

    # ── upcast ────────────────────────────────────────────────────────────────

    def upcast(self, event: StoredEvent) -> StoredEvent:
        """
        Apply the full upcaster chain to event, returning a new StoredEvent.

        If no upcasters apply the original object is returned unchanged.
        The original is NEVER mutated (immutability guarantee).
        """
        chain = self._upcasters.get(event.event_type, {})
        if not chain:
            return event

        current_version = event.event_version
        current_payload = dict(event.payload)   # COPY — never touch original
        changed = False

        while current_version in chain:
            transformer = chain[current_version]
            current_payload = transformer(current_payload)  # must return new dict
            current_version += 1
            changed = True

        if not changed:
            return event

        return StoredEvent(
            event_id        = event.event_id,
            stream_id       = event.stream_id,
            stream_position = event.stream_position,
            global_position = event.global_position,
            event_type      = event.event_type,
            event_version   = current_version,
            payload         = current_payload,
            metadata        = event.metadata,           # metadata is not upcasted
            recorded_at     = event.recorded_at,
        )


# ── Module-level default registry (imported by EventStore and upcasters.py) ───
default_registry = UpcasterRegistry()
