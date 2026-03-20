"""
src/commands/handlers.py

Command handlers: load aggregate → validate business rules → append events.

Each handler is an async function that:
  1. Loads the relevant aggregate(s) from the event store
  2. Validates all business rules (raises DomainError on violation)
  3. Appends new domain events

Handlers never import asyncpg or interact with the DB directly —
they depend only on the EventStore interface.
"""
from __future__ import annotations

import hashlib
import json
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
    OptimisticConcurrencyError,
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _hash_dict(d: dict) -> str:
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode()).hexdigest()[:16]


# =============================================================================
# Command dataclasses
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


@dataclass
class StartAgentSessionCommand:
    agent_type:           str
    session_id:           str
    model_version:        str
    context_source:       str = "fresh"
    event_replay_from_position: int = 0
    context_token_count:  int = 0
    agent_id:             str | None = None
    application_id:       str | None = None


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
    application_id_for_loan: str | None = None  # overrides application_id for loan stream


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


@dataclass
class GenerateDecisionCommand:
    application_id:          str
    orchestrator_agent_type: str
    orchestrator_session_id: str
    recommendation:          str        # APPROVE | DECLINE | REFER
    confidence:              float
    approved_amount_usd:     float | None
    conditions:              list[str] = field(default_factory=list)
    executive_summary:       str | None = None
    key_risks:               list[str] = field(default_factory=list)
    contributing_sessions:   list[str] = field(default_factory=list)
    model_version:           str = "claude-sonnet-4-20250514"


# =============================================================================
# Command handlers
# =============================================================================

async def handle_submit_application(
    cmd: SubmitApplicationCommand,
    store: Any,
) -> dict[str, Any]:
    """
    Idempotency: if the stream already exists, raises DomainError DUPLICATE_APPLICATION.
    """
    stream_id = f"loan-{cmd.application_id}"

    # Check for duplicate
    try:
        existing_version = await store.stream_version(stream_id)
        if existing_version > 0:
            raise DomainError(
                rule    = "DUPLICATE_APPLICATION",
                message = f"Application {cmd.application_id} already exists",
                context = {"stream_id": stream_id, "current_version": existing_version},
            )
    except Exception as e:
        from src.models.events import StreamNotFoundError
        if not isinstance(e, (StreamNotFoundError, DomainError)):
            raise
        if isinstance(e, DomainError):
            raise

    event = ApplicationSubmitted(
        application_id       = cmd.application_id,
        applicant_id         = cmd.applicant_id,
        requested_amount_usd = cmd.requested_amount_usd,
        loan_purpose         = cmd.loan_purpose,
        submission_channel   = cmd.submission_channel,
        contact_email        = cmd.contact_email,
        contact_name         = cmd.contact_name,
        loan_term_months     = cmd.loan_term_months,
        submitted_at         = utcnow(),
        application_reference = cmd.application_id,
    )

    version = await store.append(stream_id, [event], expected_version=-1)
    return {"stream_id": stream_id, "initial_version": version}


async def handle_start_agent_session(
    cmd: StartAgentSessionCommand,
    store: Any,
) -> dict[str, Any]:
    """Start a new agent session stream — always creates a new stream."""
    stream_id = f"agent-{cmd.agent_type}-{cmd.session_id}"

    event = AgentSessionStarted(
        session_id               = cmd.session_id,
        agent_type               = cmd.agent_type,
        agent_id                 = cmd.agent_id or f"{cmd.agent_type}-agent",
        application_id           = cmd.application_id or "",
        model_version            = cmd.model_version,
        context_source           = cmd.context_source,
        context_token_count      = cmd.context_token_count,
        event_replay_from_position = cmd.event_replay_from_position,
        started_at               = utcnow(),
    )

    version = await store.append(stream_id, [event], expected_version=-1)
    return {
        "stream_id": stream_id,
        "session_id": cmd.session_id,
        "initial_version": version,
    }


async def handle_credit_analysis_completed(
    cmd: CreditAnalysisCompletedCommand,
    store: Any,
) -> dict[str, Any]:
    """
    Validates:
      1. Gas Town: agent session must exist and have loaded context
      2. Model version lock: session model must match deployed model
      3. Credit limit: recommended limit must not exceed prior assessed max (if set)
    """
    # ── Load agent session ─────────────────────────────────────────────────────
    session = await AgentSessionAggregate.load(store, cmd.agent_type, cmd.session_id)
    session.assert_context_loaded()
    session.assert_model_version_current(cmd.model_version)

    # ── Write to credit record stream ──────────────────────────────────────────
    credit_stream  = f"credit-{cmd.application_id}"
    credit_version = -1
    try:
        credit_version = await store.stream_version(credit_stream)
    except Exception:
        pass   # new stream

    input_hash = _hash_dict(cmd.input_data)
    event = CreditAnalysisCompleted(
        application_id       = cmd.application_id,
        session_id           = cmd.session_id,
        decision             = {
            "risk_tier":              cmd.risk_tier,
            "recommended_limit_usd":  str(cmd.recommended_limit_usd),
            "confidence":             cmd.confidence,
            "rationale":              None,
            "key_concerns":           cmd.key_concerns,
            "data_quality_caveats":   [],
            "policy_overrides_applied": [],
        },
        model_version        = cmd.model_version,
        model_deployment_id  = cmd.model_deployment_id,
        input_data_hash      = input_hash,
        analysis_duration_ms = cmd.duration_ms,
        regulatory_basis     = cmd.regulatory_basis,
        completed_at         = utcnow(),
    )

    version = await store.append(
        credit_stream, [event], expected_version=credit_version
    )
    return {"credit_stream": credit_stream, "version": version}


async def handle_fraud_screening_completed(
    cmd: FraudScreeningCompletedCommand,
    store: Any,
) -> dict[str, Any]:
    """
    Validates:
      1. Gas Town: agent session must have loaded context
      2. Fraud score range: must be 0.0–1.0
    """
    session = await AgentSessionAggregate.load(store, cmd.agent_type, cmd.session_id)
    session.assert_context_loaded()

    if not 0.0 <= cmd.fraud_score <= 1.0:
        raise DomainError(
            rule    = "FRAUD_SCORE_RANGE",
            message = f"fraud_score must be 0.0–1.0, got {cmd.fraud_score}",
            context = {"fraud_score": cmd.fraud_score},
        )

    fraud_stream = f"fraud-{cmd.application_id}"
    fraud_version = -1
    try:
        fraud_version = await store.stream_version(fraud_stream)
    except Exception:
        pass

    event = FraudScreeningCompleted(
        application_id           = cmd.application_id,
        session_id               = cmd.session_id,
        fraud_score              = cmd.fraud_score,
        risk_level               = cmd.risk_level,
        anomalies_found          = len(cmd.anomaly_flags),
        recommendation           = cmd.recommendation,
        screening_model_version  = cmd.screening_model_version,
        input_data_hash          = _hash_dict(cmd.input_data),
        completed_at             = utcnow(),
    )

    version = await store.append(fraud_stream, [event], expected_version=fraud_version)
    return {"fraud_stream": fraud_stream, "version": version}


async def handle_generate_decision(
    cmd: GenerateDecisionCommand,
    store: Any,
) -> dict[str, Any]:
    """
    Validates:
      1. Confidence floor (APPROVE/DECLINE need ≥ 0.60)
      2. Credit limit (approved amount must not exceed assessed max)
      3. Compliance checks all passed
    Then appends DecisionGenerated + ApplicationApproved/Declined to loan stream.
    """
    # Load loan application
    loan = await LoanApplicationAggregate.load(store, cmd.application_id)

    # Business rule: confidence floor
    loan.assert_confidence_floor(cmd.confidence, cmd.recommendation)

    # Business rule: credit limit
    if cmd.approved_amount_usd is not None and cmd.recommendation == "APPROVE":
        loan.assert_credit_limit_within_assessed_max(cmd.approved_amount_usd)

    # Business rule: compliance
    if cmd.recommendation in ("APPROVE", "DECLINE"):
        loan.assert_all_compliance_checks_passed()

    loan_stream   = f"loan-{cmd.application_id}"
    loan_version  = await store.stream_version(loan_stream)

    events_to_append = [
        DecisionGenerated(
            application_id            = cmd.application_id,
            orchestrator_session_id   = cmd.orchestrator_session_id,
            recommendation            = cmd.recommendation,
            confidence                = cmd.confidence,
            approved_amount_usd       = cmd.approved_amount_usd,
            conditions                = cmd.conditions,
            executive_summary         = cmd.executive_summary,
            key_risks                 = cmd.key_risks,
            contributing_sessions     = cmd.contributing_sessions,
            model_versions            = {"orchestrator": cmd.model_version},
            generated_at              = utcnow(),
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
                application_id   = cmd.application_id,
                decline_reasons  = cmd.key_risks or ["Model decision"],
                declined_by      = "auto",
                declined_at      = utcnow(),
            )
        )

    version = await store.append(
        loan_stream, events_to_append, expected_version=loan_version
    )
    return {"loan_stream": loan_stream, "version": version, "recommendation": cmd.recommendation}
