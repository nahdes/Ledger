"""
tests/unit/test_loan_application.py

Unit tests for LoanApplicationAggregate.
No database. No async. Pure Python.

Pattern: build_aggregate(events) → assert state or assert DomainError.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from src.aggregates.loan_application import LoanApplicationAggregate
from src.models.events import ApplicationState, DomainError, StoredEvent
from tests.unit.conftest import stored, utcnow


# ── helpers ────────────────────────────────────────────────────────────────────

APP_ID    = "APEX-UNIT-001"
STREAM    = f"loan-{APP_ID}"


def build(events: list[StoredEvent]) -> LoanApplicationAggregate:
    agg = LoanApplicationAggregate(APP_ID)
    for e in events:
        agg._apply(e)
    return agg


def make_submitted(pos: int = 1) -> StoredEvent:
    from src.models.events import ApplicationSubmitted
    return stored(
        ApplicationSubmitted(
            application_id       = APP_ID,
            applicant_id         = "COMP-001",
            requested_amount_usd = 500_000.0,
            loan_purpose         = "expansion",
            submission_channel   = "web",
            submitted_at         = utcnow(),
        ),
        STREAM, pos,
    )


def make_analysis_requested(pos: int = 2) -> StoredEvent:
    from src.models.events import CreditAnalysisRequested
    return stored(
        CreditAnalysisRequested(application_id=APP_ID, requested_at=utcnow()),
        STREAM, pos,
    )


def make_analysis_completed(
    risk_tier: str = "MEDIUM",
    recommended_limit: float = 600_000.0,
    confidence: float = 0.87,
    pos: int = 3,
) -> StoredEvent:
    from src.models.events import CreditAnalysisCompleted
    return stored(
        CreditAnalysisCompleted(
            application_id       = APP_ID,
            session_id           = "sess-cre-test",
            decision             = {
                "risk_tier": risk_tier,
                "recommended_limit_usd": str(recommended_limit),
                "confidence": confidence,
            },
            model_version        = "claude-sonnet-4-20250514",
            input_data_hash      = "abc123",
            analysis_duration_ms = 1200,
            completed_at         = utcnow(),
        ),
        f"credit-{APP_ID}", pos,
    )


def make_compliance_check_requested(pos: int = 4) -> StoredEvent:
    from src.models.events import ComplianceCheckRequested
    return stored(
        ComplianceCheckRequested(
            application_id       = APP_ID,
            regulation_set_version = "2026-Q1",
            rules_to_evaluate    = ["REG-001", "REG-002"],
            requested_at         = utcnow(),
        ),
        STREAM, pos,
    )


def make_rule_passed(rule_id: str, pos: int) -> StoredEvent:
    from src.models.events import ComplianceRulePassed
    return stored(
        ComplianceRulePassed(
            application_id = APP_ID,
            session_id     = "sess-com-test",
            rule_id        = rule_id,
            rule_name      = rule_id,
            rule_version   = "2026-Q1-v1",
            evidence_hash  = "deadbeef",
            evaluated_at   = utcnow(),
        ),
        f"compliance-{APP_ID}", pos,
    )


def make_rule_failed(rule_id: str, pos: int) -> StoredEvent:
    from src.models.events import ComplianceRuleFailed
    return stored(
        ComplianceRuleFailed(
            application_id        = APP_ID,
            session_id            = "sess-com-test",
            rule_id               = rule_id,
            rule_name             = rule_id,
            rule_version          = "2026-Q1-v1",
            failure_reason        = f"{rule_id} check failed",
            is_hard_block         = True,
            remediation_available = False,
            evidence_hash         = "deadbeef",
            evaluated_at          = utcnow(),
        ),
        f"compliance-{APP_ID}", pos,
    )


# =============================================================================
# State machine
# =============================================================================

class TestStateMachine:

    def test_initial_state_is_submitted(self):
        agg = build([make_submitted()])
        assert agg.state == ApplicationState.SUBMITTED

    def test_transitions_to_awaiting_analysis(self):
        agg = build([make_submitted(), make_analysis_requested()])
        assert agg.state == ApplicationState.AWAITING_ANALYSIS

    def test_transitions_to_analysis_complete(self):
        agg = build([make_submitted(), make_analysis_requested(), make_analysis_completed()])
        assert agg.state == ApplicationState.ANALYSIS_COMPLETE

    def test_invalid_transition_raises_state_machine_error(self):
        agg = build([make_submitted()])
        with pytest.raises(DomainError) as exc:
            agg._transition(ApplicationState.FINAL_APPROVED)
        assert exc.value.rule == "STATE_MACHINE"
        assert "SUBMITTED" in exc.value.message

    def test_unknown_event_is_ignored_gracefully(self):
        unknown = StoredEvent(
            event_id        = uuid.uuid4(),
            stream_id       = STREAM,
            stream_position = 2,
            global_position = 2,
            event_type      = "FutureEventTypeV99",
            event_version   = 1,
            payload         = {"something": "new"},
            metadata        = {},
            recorded_at     = utcnow(),
        )
        agg = build([make_submitted(), unknown])
        assert agg.state == ApplicationState.SUBMITTED  # not crashed

    def test_version_tracks_last_position(self):
        agg = build([make_submitted(), make_analysis_requested()])
        assert agg.version == 2

    def test_applicant_id_extracted(self):
        agg = build([make_submitted()])
        assert agg.applicant_id == "COMP-001"

    def test_requested_amount_extracted(self):
        agg = build([make_submitted()])
        assert agg.requested_amount_usd == 500_000.0


# =============================================================================
# Rule 1: State assertions
# =============================================================================

class TestStateAssertions:

    def test_assert_awaiting_analysis_passes(self):
        agg = build([make_submitted(), make_analysis_requested()])
        agg.assert_awaiting_analysis()

    def test_assert_awaiting_analysis_fails_in_wrong_state(self):
        agg = build([make_submitted()])
        with pytest.raises(DomainError) as exc:
            agg.assert_awaiting_analysis()
        assert exc.value.rule == "STATE_MACHINE"


# =============================================================================
# Rule 2: Credit limit
# =============================================================================

class TestCreditLimit:

    def test_within_assessed_max_passes(self):
        agg = build([make_submitted(), make_analysis_requested(),
                     make_analysis_completed(recommended_limit=600_000.0)])
        agg.assert_credit_limit_within_assessed_max(500_000.0)

    def test_exceeds_assessed_max_raises(self):
        agg = build([make_submitted(), make_analysis_requested(),
                     make_analysis_completed(recommended_limit=400_000.0)])
        with pytest.raises(DomainError) as exc:
            agg.assert_credit_limit_within_assessed_max(500_000.0)
        assert exc.value.rule == "CREDIT_LIMIT"
        assert "400" in exc.value.message

    def test_no_prior_assessment_skips_check(self):
        agg = build([make_submitted()])
        agg.assert_credit_limit_within_assessed_max(999_999_999.0)  # no exception


# =============================================================================
# Rule 3: Confidence floor
# =============================================================================

class TestConfidenceFloor:

    def test_high_confidence_approve_passes(self):
        agg = build([make_submitted()])
        agg.assert_confidence_floor(0.87, "APPROVE")

    def test_high_confidence_decline_passes(self):
        agg = build([make_submitted()])
        agg.assert_confidence_floor(0.72, "DECLINE")

    def test_refer_always_passes(self):
        agg = build([make_submitted()])
        agg.assert_confidence_floor(0.10, "REFER")  # even absurdly low

    def test_low_confidence_approve_raises(self):
        agg = build([make_submitted()])
        with pytest.raises(DomainError) as exc:
            agg.assert_confidence_floor(0.55, "APPROVE")
        assert exc.value.rule == "CONFIDENCE_FLOOR"
        assert "REFER" in exc.value.message

    def test_low_confidence_decline_raises(self):
        agg = build([make_submitted()])
        with pytest.raises(DomainError) as exc:
            agg.assert_confidence_floor(0.59, "DECLINE")
        assert exc.value.rule == "CONFIDENCE_FLOOR"

    def test_exactly_060_passes(self):
        """Boundary: 0.60 is not below the floor."""
        agg = build([make_submitted()])
        agg.assert_confidence_floor(0.60, "APPROVE")

    def test_059_triggers_floor(self):
        agg = build([make_submitted()])
        with pytest.raises(DomainError):
            agg.assert_confidence_floor(0.599, "APPROVE")


# =============================================================================
# Rule 4: Compliance dependency
# =============================================================================

class TestComplianceDependency:

    def test_all_required_passed_allows_proceed(self):
        agg = build([make_submitted(), make_compliance_check_requested()])
        agg._apply(make_rule_passed("REG-001", 5))
        agg._apply(make_rule_passed("REG-002", 6))
        agg.assert_all_compliance_checks_passed()  # no exception

    def test_missing_required_check_raises(self):
        agg = build([make_submitted(), make_compliance_check_requested()])
        agg._apply(make_rule_passed("REG-001", 5))
        # REG-002 not yet passed
        with pytest.raises(DomainError) as exc:
            agg.assert_all_compliance_checks_passed()
        assert exc.value.rule == "COMPLIANCE_DEPENDENCY"
        assert "REG-002" in exc.value.message

    def test_failed_rule_raises_even_if_others_passed(self):
        agg = build([make_submitted(), make_compliance_check_requested()])
        agg._apply(make_rule_passed("REG-001", 5))
        agg._apply(make_rule_passed("REG-002", 6))
        agg._apply(make_rule_failed("REG-001", 7))
        with pytest.raises(DomainError) as exc:
            agg.assert_all_compliance_checks_passed()
        assert exc.value.rule == "COMPLIANCE_DEPENDENCY"
        assert "REG-001" in exc.value.message

    def test_no_required_checks_passes_vacuously(self):
        agg = build([make_submitted()])
        agg.assert_all_compliance_checks_passed()


# =============================================================================
# Rule 5: Model version lock (no re-analysis)
# =============================================================================

class TestModelVersionLock:

    def test_no_prior_analysis_passes(self):
        agg = build([make_submitted(), make_analysis_requested()])
        agg.assert_no_prior_credit_analysis()

    def test_prior_analysis_raises(self):
        agg = build([make_submitted(), make_analysis_requested(), make_analysis_completed()])
        with pytest.raises(DomainError) as exc:
            agg.assert_no_prior_credit_analysis()
        assert exc.value.rule == "MODEL_VERSION_LOCK"

    def test_human_override_unlocks(self):
        agg = build([make_submitted(), make_analysis_requested(), make_analysis_completed()])
        agg.human_override = True
        agg.assert_no_prior_credit_analysis()  # no exception


# =============================================================================
# Rule 6: Causal chain
# =============================================================================

class TestCausalChain:

    def test_valid_sessions_passes(self):
        agg = build([make_submitted()])
        valid = {"agent-credit_analysis-sess-cre-001", "agent-fraud-sess-fra-002"}
        agg.assert_valid_contributing_sessions(list(valid), valid)

    def test_ghost_session_raises(self):
        agg = build([make_submitted()])
        with pytest.raises(DomainError) as exc:
            agg.assert_valid_contributing_sessions(
                contributing         = ["agent-ghost-sess-999"],
                sessions_with_decisions = set(),
            )
        assert exc.value.rule == "CAUSAL_CHAIN"

    def test_partial_invalid_raises(self):
        agg = build([make_submitted()])
        real_session = "agent-credit-sess-real"
        with pytest.raises(DomainError):
            agg.assert_valid_contributing_sessions(
                contributing            = [real_session, "agent-ghost-sess-bad"],
                sessions_with_decisions = {real_session},
            )
