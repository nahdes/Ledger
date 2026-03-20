"""
src/aggregates/loan_application.py

LoanApplicationAggregate — pure function, no I/O.

Given a sequence of StoredEvents, reconstructs the full loan application state.
All 6 business rules are named assertions that raise DomainError with a rule code
the MCP server can surface directly to agents.
"""
from __future__ import annotations

from typing import Any

from src.models.events import (
    VALID_TRANSITIONS,
    ApplicationState,
    DomainError,
    StoredEvent,
)


class LoanApplicationAggregate:

    def __init__(self, application_id: str) -> None:
        self.application_id       = application_id
        self.state                = ApplicationState.SUBMITTED
        self.version              = 0
        self.applicant_id: str | None = None
        self.requested_amount_usd: float | None = None
        self.loan_purpose: str | None = None

        # Credit analysis results
        self.agent_assessed_max_usd: float | None = None
        self.risk_tier: str | None = None
        self.last_confidence: float | None = None
        self.prior_credit_analysis: bool = False
        self.human_override: bool = False

        # Compliance tracking
        self.compliance_checks_required: list[str] = []
        self.compliance_checks_passed:   set[str]  = set()
        self.compliance_checks_failed:   set[str]  = set()

        # Fraud
        self.fraud_score: float | None = None

    # ── Aggregate load ────────────────────────────────────────────────────────

    @classmethod
    async def load(cls, store: Any, application_id: str) -> "LoanApplicationAggregate":
        stream_id = f"loan-{application_id}"
        events    = await store.load_stream(stream_id)
        agg       = cls(application_id)
        for e in events:
            agg._apply(e)
        return agg

    def is_new(self) -> bool:
        return self.version == 0

    # ── Event application ─────────────────────────────────────────────────────

    def _apply(self, event: StoredEvent) -> None:
        self.version = event.stream_position
        p = event.payload

        if event.event_type == "ApplicationSubmitted":
            self.state                = ApplicationState.SUBMITTED
            self.applicant_id         = p.get("applicant_id")
            self.requested_amount_usd = float(p.get("requested_amount_usd", 0))
            self.loan_purpose         = p.get("loan_purpose")

        elif event.event_type == "CreditAnalysisRequested":
            self._transition(ApplicationState.AWAITING_ANALYSIS)

        elif event.event_type == "CreditAnalysisCompleted":
            # payload shape differs between v1 and v2
            decision = p.get("decision", {})
            self.risk_tier           = decision.get("risk_tier") or p.get("risk_tier")
            self.agent_assessed_max_usd = float(
                decision.get("recommended_limit_usd") or p.get("recommended_limit_usd", 0) or 0
            )
            self.last_confidence     = decision.get("confidence") or p.get("confidence_score")
            self.prior_credit_analysis = True
            self._transition(ApplicationState.ANALYSIS_COMPLETE)

        elif event.event_type == "FraudScreeningCompleted":
            self.fraud_score = float(p.get("fraud_score", 0))

        elif event.event_type == "ComplianceCheckRequested":
            self.compliance_checks_required = list(p.get("rules_to_evaluate", []))

        elif event.event_type == "ComplianceRulePassed":
            self.compliance_checks_passed.add(p.get("rule_id", ""))

        elif event.event_type == "ComplianceRuleFailed":
            self.compliance_checks_failed.add(p.get("rule_id", ""))

        elif event.event_type == "DecisionRequested":
            self._transition(ApplicationState.PENDING_DECISION)

        elif event.event_type == "DecisionGenerated":
            rec = p.get("recommendation", "")
            if rec == "APPROVE":
                self._transition(ApplicationState.FINAL_APPROVED)
            elif rec == "DECLINE":
                self._transition(ApplicationState.FINAL_DECLINED)
            elif rec == "REFER":
                self._transition(ApplicationState.REFERRED)

        elif event.event_type == "ApplicationApproved":
            self.state = ApplicationState.FINAL_APPROVED

        elif event.event_type == "ApplicationDeclined":
            self.state = ApplicationState.FINAL_DECLINED

        elif event.event_type == "HumanReviewCompleted":
            self.human_override = p.get("override", False)

        # Unknown event types are silently ignored — forward compatibility

    def _transition(self, new_state: ApplicationState) -> None:
        allowed = VALID_TRANSITIONS.get(self.state, set())
        if new_state not in allowed:
            raise DomainError(
                rule    = "STATE_MACHINE",
                message = f"Invalid transition {self.state.value} → {new_state.value}",
                context = {"current": self.state.value, "attempted": new_state.value},
            )
        self.state = new_state

    # ── Business rules ────────────────────────────────────────────────────────

    def assert_awaiting_analysis(self) -> None:
        """Rule: analysis can only be recorded if the application is in AWAITING_ANALYSIS."""
        if self.state != ApplicationState.AWAITING_ANALYSIS:
            raise DomainError(
                rule    = "STATE_MACHINE",
                message = f"Expected AWAITING_ANALYSIS, current state is {self.state.value}",
                context = {"current_state": self.state.value},
            )

    def assert_analysis_complete(self) -> None:
        if self.state != ApplicationState.ANALYSIS_COMPLETE:
            raise DomainError(
                rule    = "STATE_MACHINE",
                message = f"Expected ANALYSIS_COMPLETE, current state is {self.state.value}",
                context = {"current_state": self.state.value},
            )

    def assert_credit_limit_within_assessed_max(self, proposed_amount: float) -> None:
        """Rule: approved amount must not exceed the agent's assessed max."""
        if self.agent_assessed_max_usd is None:
            return   # no assessed max yet — no constraint
        if proposed_amount > self.agent_assessed_max_usd:
            raise DomainError(
                rule    = "CREDIT_LIMIT",
                message = (
                    f"Proposed amount {proposed_amount:,.0f} exceeds agent-assessed "
                    f"max of {self.agent_assessed_max_usd:,.0f}"
                ),
                context = {
                    "proposed": proposed_amount,
                    "max_allowed": self.agent_assessed_max_usd,
                },
            )

    def assert_confidence_floor(self, confidence: float, decision: str) -> None:
        """Rule: APPROVE or DECLINE requires confidence ≥ 0.60. REFER is always valid."""
        if decision == "REFER":
            return
        if confidence < 0.60:
            raise DomainError(
                rule    = "CONFIDENCE_FLOOR",
                message = (
                    f"{decision} requires confidence ≥ 0.60, got {confidence:.2f}. "
                    f"Use REFER for low-confidence cases."
                ),
                context = {
                    "confidence": confidence,
                    "decision": decision,
                    "floor": 0.60,
                    "suggested_action": "REFER",
                },
            )

    def assert_all_compliance_checks_passed(self) -> None:
        """Rule: all required compliance checks must have passed before approval."""
        failed = self.compliance_checks_failed
        if failed:
            raise DomainError(
                rule    = "COMPLIANCE_DEPENDENCY",
                message = f"Compliance checks failed: {sorted(failed)}",
                context = {"failed_rules": sorted(failed)},
            )
        pending = set(self.compliance_checks_required) - self.compliance_checks_passed
        if pending:
            raise DomainError(
                rule    = "COMPLIANCE_DEPENDENCY",
                message = f"Required compliance checks not yet completed: {sorted(pending)}",
                context = {"pending_rules": sorted(pending)},
            )

    def assert_no_prior_credit_analysis(self) -> None:
        """Rule: model version lock — re-analysis only permitted after human override."""
        if self.prior_credit_analysis and not self.human_override:
            raise DomainError(
                rule    = "MODEL_VERSION_LOCK",
                message = "Credit analysis already performed. Human override required to re-analyse.",
                context = {"human_override_required": True},
            )

    def assert_valid_contributing_sessions(
        self,
        contributing: list[str],
        sessions_with_decisions: set[str],
    ) -> None:
        """Rule: every contributing session must have a decision event in the store."""
        invalid = [s for s in contributing if s not in sessions_with_decisions]
        if invalid:
            raise DomainError(
                rule    = "CAUSAL_CHAIN",
                message = f"Contributing sessions without recorded decisions: {invalid}",
                context = {"invalid_sessions": invalid},
            )
