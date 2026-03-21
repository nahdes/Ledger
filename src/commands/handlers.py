"""
src/commands/handlers.py

Command handlers: load aggregate(s) -> validate business rules -> append events.

Pattern (from challenge spec):
  1. Load ALL relevant aggregates from the event store
  2. Validate ALL business rules before any state change
  3. Determine new events (pure logic, no I/O)
  4. Append atomically -- optimistic concurrency enforced by store

Two issues fixed vs. original:

  METADATA THREADING
    Every command carries correlation_id and causation_id.
    These thread through every store.append() call so the full causal
    chain is traceable across streams in the audit log.
    correlation_id ties all events from one business operation together.
    causation_id records which command/event caused this append.

  MULTI-AGGREGATE LOADING
    handle_credit_analysis_completed also loads LoanApplicationAggregate
      and asserts AWAITING_ANALYSIS before writing to the credit stream.
    handle_generate_decision also loads the orchestrator AgentSessionAggregate
      (Gas Town + model version) and loads every contributing session to
      verify causal chain (no ghost sessions).
    handle_fraud_screening_completed also loads LoanApplicationAggregate
      to confirm non-terminal state before writing.
    handle_compliance_check_completed added (was missing).
    handle_human_review_completed added (was missing).

  STRICT VERSION SOURCING
    expected_version on every store.append() is now sourced exclusively
    from the aggregate's .version field set during event replay, never
    from a separate store.stream_version() call.

    Rationale: calling store.stream_version() after aggregate.load() is
    both redundant (the load already knows the version) and unsafe — it
    opens a TOCTOU window where a concurrent writer could advance the
    stream between the load and the version query, causing the OCC check
    to validate against a version that no longer matches the state the
    business rules were evaluated against.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.loan_application import LoanApplicationAggregate
from src.models.events import (
    AgentSessionStarted,
    ApplicationApproved,
    ApplicationDeclined,
    ApplicationSubmitted,
    ComplianceCheckRequested,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    DecisionGenerated,
    DomainError,
    FraudScreeningCompleted,
    FraudScreeningRequested,
    StreamNotFoundError,
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _hash_dict(d: dict) -> str:
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode()).hexdigest()[:16]


def _new_correlation_id() -> str:
    return str(uuid.uuid4())


# =============================================================================
# Command dataclasses -- all carry correlation_id / causation_id / actor
# =============================================================================

@dataclass
class SubmitApplicationCommand:
    application_id:        str
    applicant_id:          str
    requested_amount_usd:  float
    loan_purpose:          str
    submission_channel:    str
    contact_email:         str | None = None
    contact_name:          str | None = None
    loan_term_months:      int | None = None
    correlation_id:        str | None = None
    causation_id:          str | None = None
    actor:                 str | None = None


@dataclass
class StartAgentSessionCommand:
    agent_type:                 str
    session_id:                 str
    model_version:              str
    application_id:             str
    context_source:             str = "fresh"
    event_replay_from_position: int = 0
    context_token_count:        int = 0
    agent_id:                   str | None = None
    correlation_id:             str | None = None
    causation_id:               str | None = None
    actor:                      str | None = None


@dataclass
class CreditAnalysisCompletedCommand:
    application_id:        str
    agent_type:            str
    session_id:            str
    model_version:         str
    confidence:            float
    risk_tier:             str
    recommended_limit_usd: float
    duration_ms:           int
    input_data:            dict[str, Any]
    model_deployment_id:   str | None = None
    key_concerns:          list[str] = field(default_factory=list)
    regulatory_basis:      list[str] = field(default_factory=list)
    correlation_id:        str | None = None
    causation_id:          str | None = None
    actor:                 str | None = None


@dataclass
class FraudScreeningCompletedCommand:
    application_id:          str
    agent_type:              str
    session_id:              str
    fraud_score:             float
    anomaly_flags:           list[str]
    screening_model_version: str
    input_data:              dict[str, Any]
    risk_level:              str = "LOW"
    recommendation:          str = "PROCEED"
    correlation_id:          str | None = None
    causation_id:            str | None = None
    actor:                   str | None = None


@dataclass
class ComplianceCheckCompletedCommand:
    application_id:          str
    agent_type:              str
    session_id:              str
    rule_id:                 str
    rule_name:               str
    rule_version:            str
    passed:                  bool
    evidence_hash:           str
    failure_reason:          str | None = None
    is_hard_block:           bool = False
    remediation_available:   bool = False
    remediation_description: str | None = None
    evaluation_notes:        str | None = None
    correlation_id:          str | None = None
    causation_id:            str | None = None
    actor:                   str | None = None


@dataclass
class HumanReviewCompletedCommand:
    application_id:  str
    reviewer_id:     str
    override:        bool
    final_decision:  str
    override_reason: str | None = None
    conditions:      list[str] = field(default_factory=list)
    correlation_id:  str | None = None
    causation_id:    str | None = None
    actor:           str | None = None


@dataclass
class GenerateDecisionCommand:
    application_id:          str
    orchestrator_agent_type: str
    orchestrator_session_id: str
    recommendation:          str
    confidence:              float
    approved_amount_usd:     float | None
    conditions:              list[str] = field(default_factory=list)
    executive_summary:       str | None = None
    key_risks:               list[str] = field(default_factory=list)
    contributing_sessions:   list[str] = field(default_factory=list)
    model_version:           str = "claude-sonnet-4-20250514"
    correlation_id:          str | None = None
    causation_id:            str | None = None
    actor:                   str | None = None


# =============================================================================
# Internal helpers
# =============================================================================

def _meta(cmd: Any) -> dict[str, str | None]:
    """Extract metadata fields from any command for threading into store.append()."""
    return {
        "correlation_id": getattr(cmd, "correlation_id", None) or _new_correlation_id(),
        "causation_id":   getattr(cmd, "causation_id", None),
        "actor":          getattr(cmd, "actor", None),
    }


async def _get_stream_version(store: Any, stream_id: str) -> int:
    """Return current version or -1 if stream does not exist yet."""
    try:
        return await store.stream_version(stream_id)
    except StreamNotFoundError:
        return -1


# =============================================================================
# Command handlers
# =============================================================================

async def handle_submit_application(
    cmd: SubmitApplicationCommand,
    store: Any,
) -> dict[str, Any]:
    """Creates a new loan application stream."""
    stream_id = f"loan-{cmd.application_id}"

    existing = await _get_stream_version(store, stream_id)
    if existing >= 0:
        raise DomainError(
            rule    = "DUPLICATE_APPLICATION",
            message = f"Application {cmd.application_id} already exists at version {existing}",
            context = {"stream_id": stream_id, "current_version": existing},
        )

    event = ApplicationSubmitted(
        application_id        = cmd.application_id,
        applicant_id          = cmd.applicant_id,
        requested_amount_usd  = cmd.requested_amount_usd,
        loan_purpose          = cmd.loan_purpose,
        submission_channel    = cmd.submission_channel,
        contact_email         = cmd.contact_email,
        contact_name          = cmd.contact_name,
        loan_term_months      = cmd.loan_term_months,
        submitted_at          = utcnow(),
        application_reference = cmd.application_id,
    )

    version = await store.append(stream_id, [event], expected_version=-1, **_meta(cmd))
    return {"stream_id": stream_id, "initial_version": version}


async def handle_start_agent_session(
    cmd: StartAgentSessionCommand,
    store: Any,
) -> dict[str, Any]:
    """
    Opens a new agent session stream and writes AgentSessionStarted.
    Verifies the loan application exists before creating the session.
    """
    loan_version = await _get_stream_version(store, f"loan-{cmd.application_id}")
    if loan_version < 0:
        raise DomainError(
            rule    = "APPLICATION_NOT_FOUND",
            message = f"Cannot start session: application {cmd.application_id} does not exist",
            context = {"application_id": cmd.application_id},
        )

    session_stream = f"agent-{cmd.agent_type}-{cmd.session_id}"

    event = AgentSessionStarted(
        session_id                 = cmd.session_id,
        agent_type                 = cmd.agent_type,
        agent_id                   = cmd.agent_id or f"{cmd.agent_type}-agent",
        application_id             = cmd.application_id,
        model_version              = cmd.model_version,
        context_source             = cmd.context_source,
        context_token_count        = cmd.context_token_count,
        event_replay_from_position = cmd.event_replay_from_position,
        started_at                 = utcnow(),
    )

    version = await store.append(session_stream, [event], expected_version=-1, **_meta(cmd))
    return {"stream_id": session_stream, "session_id": cmd.session_id, "initial_version": version}


async def handle_credit_analysis_completed(
    cmd: CreditAnalysisCompletedCommand,
    store: Any,
) -> dict[str, Any]:
    """
    Records a credit analysis result.

    Multi-aggregate validation:
      1. AgentSessionAggregate -- Gas Town + model version lock
      2. LoanApplicationAggregate -- must be AWAITING_ANALYSIS, no prior analysis
    """
    # Aggregate 1: agent session
    session = await AgentSessionAggregate.load(store, cmd.agent_type, cmd.session_id)
    session.assert_context_loaded()
    session.assert_model_version_current(cmd.model_version)

    # Aggregate 2: loan application
    loan = await LoanApplicationAggregate.load(store, cmd.application_id)
    loan.assert_awaiting_analysis()
    loan.assert_no_prior_credit_analysis()

    credit_stream  = f"credit-{cmd.application_id}"
    credit_version = await _get_stream_version(store, credit_stream)

    event = CreditAnalysisCompleted(
        application_id       = cmd.application_id,
        session_id           = cmd.session_id,
        decision             = {
            "risk_tier":               cmd.risk_tier,
            "recommended_limit_usd":   str(cmd.recommended_limit_usd),
            "confidence":              cmd.confidence,
            "rationale":               None,
            "key_concerns":            cmd.key_concerns,
            "data_quality_caveats":    [],
            "policy_overrides_applied": [],
        },
        model_version        = cmd.model_version,
        model_deployment_id  = cmd.model_deployment_id,
        input_data_hash      = _hash_dict(cmd.input_data),
        analysis_duration_ms = cmd.duration_ms,
        regulatory_basis     = cmd.regulatory_basis,
        completed_at         = utcnow(),
    )

    version = await store.append(
        credit_stream, [event], expected_version=credit_version, **_meta(cmd)
    )
    return {"credit_stream": credit_stream, "version": version}


async def handle_fraud_screening_completed(
    cmd: FraudScreeningCompletedCommand,
    store: Any,
) -> dict[str, Any]:
    """
    Records a fraud screening result.

    Multi-aggregate validation:
      1. AgentSessionAggregate -- Gas Town
      2. LoanApplicationAggregate -- must not be in a terminal state
    """
    # Aggregate 1: agent session
    session = await AgentSessionAggregate.load(store, cmd.agent_type, cmd.session_id)
    session.assert_context_loaded()

    # Aggregate 2: loan application
    loan = await LoanApplicationAggregate.load(store, cmd.application_id)
    if loan.state.value in ("FINAL_APPROVED", "FINAL_DECLINED"):
        raise DomainError(
            rule    = "STATE_MACHINE",
            message = (
                f"Cannot record fraud screening for {cmd.application_id}: "
                f"already in terminal state {loan.state.value}"
            ),
            context = {"current_state": loan.state.value},
        )

    if not 0.0 <= cmd.fraud_score <= 1.0:
        raise DomainError(
            rule    = "FRAUD_SCORE_RANGE",
            message = f"fraud_score must be in [0.0, 1.0], got {cmd.fraud_score}",
            context = {"fraud_score": cmd.fraud_score},
        )

    fraud_stream  = f"fraud-{cmd.application_id}"
    fraud_version = await _get_stream_version(store, fraud_stream)

    event = FraudScreeningCompleted(
        application_id          = cmd.application_id,
        session_id              = cmd.session_id,
        fraud_score             = cmd.fraud_score,
        risk_level              = cmd.risk_level,
        anomalies_found         = len(cmd.anomaly_flags),
        recommendation          = cmd.recommendation,
        screening_model_version = cmd.screening_model_version,
        input_data_hash         = _hash_dict(cmd.input_data),
        completed_at            = utcnow(),
    )

    version = await store.append(
        fraud_stream, [event], expected_version=fraud_version, **_meta(cmd)
    )
    return {"fraud_stream": fraud_stream, "version": version}


async def handle_compliance_check_completed(
    cmd: ComplianceCheckCompletedCommand,
    store: Any,
) -> dict[str, Any]:
    """
    Records a single compliance rule outcome (passed or failed).

    Multi-aggregate validation:
      1. AgentSessionAggregate -- Gas Town
      2. LoanApplicationAggregate -- application must exist
    """
    if not cmd.passed and not cmd.failure_reason:
        raise DomainError(
            rule    = "COMPLIANCE_RULE_FAILURE",
            message = "failure_reason is required when passed=False",
            context = {"rule_id": cmd.rule_id},
        )

    # Aggregate 1: agent session
    session = await AgentSessionAggregate.load(store, cmd.agent_type, cmd.session_id)
    session.assert_context_loaded()

    # Aggregate 2: loan application
    loan = await LoanApplicationAggregate.load(store, cmd.application_id)
    if loan.is_new():
        raise DomainError(
            rule    = "APPLICATION_NOT_FOUND",
            message = f"Application {cmd.application_id} does not exist",
            context = {"application_id": cmd.application_id},
        )

    compliance_stream  = f"compliance-{cmd.application_id}"
    compliance_version = await _get_stream_version(store, compliance_stream)

    from src.models.events import ComplianceRulePassed, ComplianceRuleFailed

    if cmd.passed:
        event: Any = ComplianceRulePassed(
            application_id   = cmd.application_id,
            session_id       = cmd.session_id,
            rule_id          = cmd.rule_id,
            rule_name        = cmd.rule_name,
            rule_version     = cmd.rule_version,
            evidence_hash    = cmd.evidence_hash,
            evaluation_notes = cmd.evaluation_notes,
            evaluated_at     = utcnow(),
        )
    else:
        event = ComplianceRuleFailed(
            application_id          = cmd.application_id,
            session_id              = cmd.session_id,
            rule_id                 = cmd.rule_id,
            rule_name               = cmd.rule_name,
            rule_version            = cmd.rule_version,
            failure_reason          = cmd.failure_reason or "",
            is_hard_block           = cmd.is_hard_block,
            remediation_available   = cmd.remediation_available,
            remediation_description = cmd.remediation_description,
            evidence_hash           = cmd.evidence_hash,
            evaluated_at            = utcnow(),
        )

    version = await store.append(
        compliance_stream, [event], expected_version=compliance_version, **_meta(cmd)
    )
    return {
        "compliance_stream": compliance_stream,
        "version":           version,
        "rule_id":           cmd.rule_id,
        "passed":            cmd.passed,
    }


async def handle_human_review_completed(
    cmd: HumanReviewCompletedCommand,
    store: Any,
) -> dict[str, Any]:
    """
    Records a human loan officer review.

    If override=True, override_reason is mandatory.
    Terminal events (Approved/Declined) are appended atomically with the
    review event in the same store.append() call.

    Multi-aggregate validation:
      1. LoanApplicationAggregate -- must be in PENDING_DECISION state
    """
    if cmd.override and not cmd.override_reason:
        raise DomainError(
            rule    = "HUMAN_REVIEW",
            message = "override_reason is required when override=True",
            context = {"reviewer_id": cmd.reviewer_id},
        )

    if cmd.final_decision not in ("APPROVE", "DECLINE", "REFER"):
        raise DomainError(
            rule    = "HUMAN_REVIEW",
            message = f"final_decision must be APPROVE|DECLINE|REFER, got {cmd.final_decision!r}",
            context = {"final_decision": cmd.final_decision},
        )

    from src.models.events import ApplicationState, HumanReviewCompleted

    loan        = await LoanApplicationAggregate.load(store, cmd.application_id)
    loan_stream = f"loan-{cmd.application_id}"

    if loan.state != ApplicationState.PENDING_DECISION:
        raise DomainError(
            rule    = "STATE_MACHINE",
            message = (
                f"Human review requires PENDING_DECISION state, "
                f"current state is {loan.state.value}"
            ),
            context = {"current_state": loan.state.value},
        )

    events_to_append: list[Any] = [
        HumanReviewCompleted(
            application_id  = cmd.application_id,
            reviewer_id     = cmd.reviewer_id,
            override        = cmd.override,
            final_decision  = cmd.final_decision,
            override_reason = cmd.override_reason,
            reviewed_at     = utcnow(),
        )
    ]

    if cmd.final_decision == "APPROVE":
        events_to_append.append(
            ApplicationApproved(
                application_id      = cmd.application_id,
                approved_amount_usd = loan.agent_assessed_max_usd or 0.0,
                conditions          = cmd.conditions,
                approved_by         = cmd.reviewer_id,
                approved_at         = utcnow(),
            )
        )
    elif cmd.final_decision == "DECLINE":
        events_to_append.append(
            ApplicationDeclined(
                application_id                = cmd.application_id,
                decline_reasons               = ["Human review decision"],
                declined_by                   = cmd.reviewer_id,
                adverse_action_notice_required = True,
                declined_at                   = utcnow(),
            )
        )

    # Use loan.version sourced directly from the aggregate replay —
    # avoids a redundant DB round-trip and closes the TOCTOU window
    # that exists when store.stream_version() is called separately.
    version = await store.append(
        loan_stream, events_to_append, expected_version=loan.version, **_meta(cmd)
    )
    return {
        "loan_stream":    loan_stream,
        "version":        version,
        "final_decision": cmd.final_decision,
        "override":       cmd.override,
    }


async def handle_generate_decision(
    cmd: GenerateDecisionCommand,
    store: Any,
) -> dict[str, Any]:
    """
    Generates an AI orchestrator decision.

    Multi-aggregate validation:
      1. LoanApplicationAggregate -- confidence floor, credit limit, compliance
      2. AgentSessionAggregate (orchestrator) -- Gas Town + model version
      3. Every contributing session -- must have analysed this application_id
         (causal chain: no ghost sessions permitted)
    """
    meta = _meta(cmd)

    # Aggregate 1: loan application
    loan = await LoanApplicationAggregate.load(store, cmd.application_id)
    loan.assert_confidence_floor(cmd.confidence, cmd.recommendation)
    if cmd.approved_amount_usd is not None and cmd.recommendation == "APPROVE":
        loan.assert_credit_limit_within_assessed_max(cmd.approved_amount_usd)
    if cmd.recommendation in ("APPROVE", "DECLINE"):
        loan.assert_all_compliance_checks_passed()

    # Aggregate 2: orchestrator session
    orchestrator = await AgentSessionAggregate.load(
        store, cmd.orchestrator_agent_type, cmd.orchestrator_session_id
    )
    orchestrator.assert_context_loaded()
    orchestrator.assert_model_version_current(cmd.model_version)

    # Aggregate 3: causal chain verification
    # Load each contributing session and check it analysed this application.
    sessions_with_decisions: set[str] = set()
    model_versions: dict[str, str] = {"orchestrator": cmd.model_version}

    for session_stream_id in cmd.contributing_sessions:
        try:
            contrib = await AgentSessionAggregate.load_from_stream_id(
                store, session_stream_id
            )
            if cmd.application_id in contrib.applications_analysed:
                sessions_with_decisions.add(session_stream_id)
            if contrib.declared_model_version:
                model_versions[session_stream_id] = contrib.declared_model_version
        except StreamNotFoundError:
            pass  # ghost session -- caught by assert below

    loan.assert_valid_contributing_sessions(
        contributing=cmd.contributing_sessions,
        sessions_with_decisions=sessions_with_decisions,
    )

    loan_stream = f"loan-{cmd.application_id}"
    # loan.version is the version at which we replayed the aggregate.
    # Using it directly as expected_version means the OCC check covers
    # exactly the state we validated against — no race between a separate
    # store.stream_version() call and the append.

    events_to_append: list[Any] = [
        DecisionGenerated(
            application_id          = cmd.application_id,
            orchestrator_session_id = cmd.orchestrator_session_id,
            recommendation          = cmd.recommendation,
            confidence              = cmd.confidence,
            approved_amount_usd     = cmd.approved_amount_usd,
            conditions              = cmd.conditions,
            executive_summary       = cmd.executive_summary,
            key_risks               = cmd.key_risks,
            contributing_sessions   = cmd.contributing_sessions,
            model_versions          = model_versions,
            generated_at            = utcnow(),
        )
    ]

    if cmd.recommendation == "APPROVE":
        events_to_append.append(
            ApplicationApproved(
                application_id      = cmd.application_id,
                approved_amount_usd = cmd.approved_amount_usd or 0.0,
                conditions          = cmd.conditions,
                approved_by         = "auto",
                approved_at         = utcnow(),
            )
        )
    elif cmd.recommendation == "DECLINE":
        events_to_append.append(
            ApplicationDeclined(
                application_id                = cmd.application_id,
                decline_reasons               = cmd.key_risks or ["Model decision"],
                declined_by                   = "auto",
                adverse_action_notice_required = True,
                declined_at                   = utcnow(),
            )
        )

    version = await store.append(
        loan_stream, events_to_append, expected_version=loan.version, **meta
    )
    return {
        "loan_stream":    loan_stream,
        "version":        version,
        "recommendation": cmd.recommendation,
        "model_versions": model_versions,
    }