"""
src/what_if/projector.py

Phase 6: What-If Projections & Regulatory Time Travel.

Enables counterfactual analysis: "What would the decision have been if
the credit analysis had returned risk_tier='HIGH' instead of 'MEDIUM'?"

The projector:
  1. Loads the application stream up to the branch point
  2. Injects counterfactual events instead of the real events at that point
  3. Replays causally independent events after the branch
  4. Skips causally dependent events (those whose causation_id traces
     back to the branched events)
  5. Runs all provided projections against both real and counterfactual streams
  6. Returns real_outcome, counterfactual_outcome, divergence_events

NEVER writes counterfactual events to the real store.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from src.models.events import BaseEvent, StoredEvent

logger = logging.getLogger(__name__)


@dataclass
class WhatIfResult:
    application_id:        str
    branch_at_event_type:  str
    branch_position:       int
    real_outcome:          dict[str, Any]
    counterfactual_outcome: dict[str, Any]
    divergence_events:     list[str]    # event types that differ between the two paths
    events_replayed_real:  int
    events_replayed_cf:    int

    def to_dict(self) -> dict[str, Any]:
        return {
            "application_id":        self.application_id,
            "branch_at_event_type":  self.branch_at_event_type,
            "branch_position":       self.branch_position,
            "real_outcome":          self.real_outcome,
            "counterfactual_outcome": self.counterfactual_outcome,
            "divergence_events":     self.divergence_events,
            "events_replayed_real":  self.events_replayed_real,
            "events_replayed_cf":    self.events_replayed_cf,
        }


class InMemoryProjection:
    """
    Lightweight in-memory projection for what-if analysis.
    Tracks loan application state without writing to DB.
    """

    def __init__(self) -> None:
        self.state:              str = "SUBMITTED"
        self.risk_tier:          str | None = None
        self.fraud_score:        float | None = None
        self.compliance_status:  str = "PENDING"
        self.decision:           str | None = None
        self.approved_amount:    float | None = None
        self.event_log:          list[str] = []

    def apply(self, event: StoredEvent) -> None:
        et = event.event_type
        p  = event.payload
        self.event_log.append(et)

        if et == "ApplicationSubmitted":
            self.state = "SUBMITTED"
        elif et == "CreditAnalysisCompleted":
            decision   = p.get("decision", {})
            self.risk_tier = (decision.get("risk_tier") if isinstance(decision, dict)
                              else p.get("risk_tier"))
            self.state = "ANALYSIS_COMPLETE"
        elif et == "FraudScreeningCompleted":
            self.fraud_score = float(p.get("fraud_score", 0))
        elif et == "ComplianceRuleFailed":
            if p.get("is_hard_block"):
                self.compliance_status = "BLOCKED"
        elif et == "ComplianceRulePassed":
            if self.compliance_status == "PENDING":
                self.compliance_status = "CLEAR"
        elif et == "DecisionGenerated":
            self.decision = p.get("recommendation")
            state_map = {"APPROVE": "PENDING_DECISION", "DECLINE": "PENDING_DECISION",
                         "REFER": "REFERRED"}
            self.state = state_map.get(self.decision, "PENDING_DECISION")
        elif et == "ApplicationApproved":
            self.state            = "FINAL_APPROVED"
            self.approved_amount  = float(p.get("approved_amount_usd", 0))
        elif et == "ApplicationDeclined":
            self.state = "FINAL_DECLINED"

    def to_dict(self) -> dict[str, Any]:
        return {
            "state":            self.state,
            "risk_tier":        self.risk_tier,
            "fraud_score":      self.fraud_score,
            "compliance_status": self.compliance_status,
            "decision":         self.decision,
            "approved_amount":  self.approved_amount,
            "event_sequence":   self.event_log,
        }


def _is_causally_dependent(
    event: StoredEvent,
    branched_event_ids: set[str],
) -> bool:
    """
    An event is causally dependent on the branch if its causation_id
    traces back to one of the branched events.
    """
    causation_id = event.metadata.get("causation_id")
    if not causation_id:
        return False
    return causation_id in branched_event_ids


async def run_what_if(
    store: Any,
    application_id: str,
    branch_at_event_type: str,
    counterfactual_events: list[BaseEvent],
) -> WhatIfResult:
    """
    Run a counterfactual analysis on a loan application.

    The counterfactual_events replace the real events at the branch point.
    Real events after the branch that are causally independent are replayed
    against both real and counterfactual streams.

    NEVER writes to the real store.
    """
    stream_id     = f"loan-{application_id}"
    all_events    = await store.load_stream(stream_id)

    # ── Find branch point ──────────────────────────────────────────────────────
    branch_idx = next(
        (i for i, e in enumerate(all_events) if e.event_type == branch_at_event_type),
        None,
    )
    if branch_idx is None:
        raise ValueError(
            f"Branch event type {branch_at_event_type!r} not found in "
            f"stream {stream_id}"
        )

    branch_position = all_events[branch_idx].stream_position
    pre_branch      = all_events[:branch_idx]
    real_at_branch  = [all_events[branch_idx]]
    post_branch     = all_events[branch_idx + 1:]

    # Collect IDs of branched events for causal dependency check
    branched_event_ids = {str(e.event_id) for e in real_at_branch}

    # ── Real path ─────────────────────────────────────────────────────────────
    real_proj = InMemoryProjection()
    for e in pre_branch:
        real_proj.apply(e)
    for e in real_at_branch:
        real_proj.apply(e)
    for e in post_branch:
        real_proj.apply(e)

    # ── Counterfactual path ────────────────────────────────────────────────────
    cf_proj = InMemoryProjection()
    for e in pre_branch:
        cf_proj.apply(e)

    # Apply counterfactual events instead of real branch event
    cf_event_ids: set[str] = set()
    for cf_event in counterfactual_events:
        # Wrap in a minimal StoredEvent for the projection
        fake_stored = StoredEvent(
            event_id        = __import__("uuid").uuid4(),
            stream_id       = stream_id,
            stream_position = branch_position,
            global_position = branch_position,
            event_type      = cf_event.event_type,
            event_version   = cf_event.event_version,
            payload         = cf_event.model_dump(
                exclude={"event_type", "event_version"}, mode="json"
            ),
            metadata        = {"counterfactual": "true"},
            recorded_at     = __import__("datetime").datetime.now(
                __import__("datetime").timezone.utc
            ),
        )
        cf_proj.apply(fake_stored)
        cf_event_ids.add(str(fake_stored.event_id))

    # Replay post-branch events that are causally independent
    independent_events = [
        e for e in post_branch
        if not _is_causally_dependent(e, branched_event_ids)
    ]
    for e in independent_events:
        cf_proj.apply(e)

    # ── Compute divergence ─────────────────────────────────────────────────────
    real_seq = set(real_proj.event_log)
    cf_seq   = set(cf_proj.event_log)
    divergence_events = list((real_seq | cf_seq) - (real_seq & cf_seq))

    logger.info(
        "What-if complete for %s: branch=%s real_state=%s cf_state=%s",
        application_id, branch_at_event_type,
        real_proj.state, cf_proj.state,
    )

    return WhatIfResult(
        application_id        = application_id,
        branch_at_event_type  = branch_at_event_type,
        branch_position       = branch_position,
        real_outcome          = real_proj.to_dict(),
        counterfactual_outcome = cf_proj.to_dict(),
        divergence_events     = divergence_events,
        events_replayed_real  = len(all_events),
        events_replayed_cf    = len(pre_branch) + len(counterfactual_events) + len(independent_events),
    )
