"""
src/models/events.py

Canonical domain model for ALL 34 event types observed in seed_events.jsonl
plus StoredEvent (the database representation) and exception hierarchy.

Event naming follows the seed data exactly:
  AgentSessionStarted (NOT AgentContextLoaded) — the seed uses 'Started'
  CreditAnalysisCompleted is event_version=2 in seed data
"""
from __future__ import annotations

import enum
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


# =============================================================================
# Exception hierarchy
# =============================================================================

class LedgerError(Exception):
    """Base for all domain errors."""


class DomainError(LedgerError):
    """Business rule violation — recoverable, should be surfaced to caller."""

    def __init__(self, rule: str, message: str, context: dict[str, Any] | None = None):
        self.rule    = rule
        self.message = message
        self.context = context or {}
        super().__init__(f"[{rule}] {message}")

    def to_dict(self) -> dict[str, Any]:
        return {"error_type": "DomainError", "rule": self.rule,
                "message": self.message, "context": self.context}


class OptimisticConcurrencyError(LedgerError):
    """Raised when expected_version != actual_version during append."""

    def __init__(self, stream_id: str, expected_version: int, actual_version: int):
        self.stream_id        = stream_id
        self.expected_version = expected_version
        self.actual_version   = actual_version
        super().__init__(
            f"OCC conflict on {stream_id}: "
            f"expected={expected_version} actual={actual_version}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_type":       "OptimisticConcurrencyError",
            "stream_id":        self.stream_id,
            "expected_version": self.expected_version,
            "actual_version":   self.actual_version,
            "suggested_action": "reload_stream_and_retry",
        }


class StreamNotFoundError(LedgerError):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        super().__init__(f"Stream not found: {stream_id}")


class PreconditionFailedError(LedgerError):
    """Used by archive_stream when stream is already archived."""


# =============================================================================
# State enumerations
# =============================================================================

class ApplicationState(str, enum.Enum):
    SUBMITTED          = "SUBMITTED"
    AWAITING_ANALYSIS  = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE  = "ANALYSIS_COMPLETE"
    PENDING_DECISION   = "PENDING_DECISION"
    FINAL_APPROVED     = "FINAL_APPROVED"
    FINAL_DECLINED     = "FINAL_DECLINED"
    REFERRED           = "REFERRED"

# Valid state machine transitions
VALID_TRANSITIONS: dict[ApplicationState, set[ApplicationState]] = {
    ApplicationState.SUBMITTED:         {ApplicationState.AWAITING_ANALYSIS},
    ApplicationState.AWAITING_ANALYSIS: {ApplicationState.ANALYSIS_COMPLETE},
    ApplicationState.ANALYSIS_COMPLETE: {ApplicationState.PENDING_DECISION},
    ApplicationState.PENDING_DECISION:  {
        ApplicationState.FINAL_APPROVED,
        ApplicationState.FINAL_DECLINED,
        ApplicationState.REFERRED,
    },
    ApplicationState.FINAL_APPROVED:    set(),
    ApplicationState.FINAL_DECLINED:    set(),
    ApplicationState.REFERRED:          set(),
}


# =============================================================================
# Stored representation (what lives in the DB)
# =============================================================================

class StoredEvent(BaseModel):
    """The database row — exactly maps to the events table."""
    event_id:         UUID
    stream_id:        str
    stream_position:  int
    global_position:  int
    event_type:       str
    event_version:    int
    payload:          dict[str, Any]
    metadata:         dict[str, Any]
    recorded_at:      datetime


class StreamMetadata(BaseModel):
    stream_id:       str
    aggregate_type:  str
    current_version: int
    created_at:      datetime
    archived_at:     datetime | None
    metadata:        dict[str, Any]


# =============================================================================
# Base domain event
# =============================================================================

class BaseEvent(BaseModel):
    """All domain events inherit from this."""
    event_type:    str = ""      # overridden by each subclass via Field(default=...)
    event_version: int = 1


# =============================================================================
# Loan Application stream  (stream_id: loan-APEX-XXXX)
# =============================================================================

class ApplicationSubmitted(BaseEvent):
    event_type:            str = Field("ApplicationSubmitted", frozen=True)
    application_id:        str
    applicant_id:          str
    requested_amount_usd:  float
    loan_purpose:          str
    loan_term_months:      int | None = None
    submission_channel:    str
    contact_email:         str | None = None
    contact_name:          str | None = None
    submitted_at:          datetime
    application_reference: str | None = None


class DocumentUploadRequested(BaseEvent):
    event_type:              str = Field("DocumentUploadRequested", frozen=True)
    application_id:          str
    required_document_types: list[str]
    deadline:                datetime | None = None
    requested_by:            str = "system"


class DocumentUploaded(BaseEvent):
    event_type:       str = Field("DocumentUploaded", frozen=True)
    application_id:   str
    document_id:      str
    document_type:    str
    document_format:  str
    filename:         str
    file_path:        str
    file_size_bytes:  int
    file_hash:        str
    fiscal_year:      int | None = None
    uploaded_at:      datetime
    uploaded_by:      str = "applicant"


class CreditAnalysisRequested(BaseEvent):
    event_type:      str = Field("CreditAnalysisRequested", frozen=True)
    application_id:  str
    requested_at:    datetime
    requested_by:    str | None = None
    priority:        str = "NORMAL"


class FraudScreeningRequested(BaseEvent):
    event_type:              str = Field("FraudScreeningRequested", frozen=True)
    application_id:          str
    requested_at:            datetime
    triggered_by_event_id:   str | None = None


class ComplianceCheckRequested(BaseEvent):
    event_type:              str = Field("ComplianceCheckRequested", frozen=True)
    application_id:          str
    requested_at:            datetime | None = None
    triggered_by_event_id:   str | None = None
    regulation_set_version:  str = "2026-Q1"
    rules_to_evaluate:       list[str] = Field(
        default=["REG-001", "REG-002", "REG-003", "REG-004", "REG-005", "REG-006"]
    )


class DecisionRequested(BaseEvent):
    event_type:              str = Field("DecisionRequested", frozen=True)
    application_id:          str
    requested_at:            datetime
    all_analyses_complete:   bool = True
    triggered_by_event_id:   str | None = None


class DecisionGenerated(BaseEvent):
    event_type:                 str = Field("DecisionGenerated", frozen=True)
    event_version:              int = 2
    application_id:             str
    orchestrator_session_id:    str
    recommendation:             str        # APPROVE | DECLINE | REFER
    confidence:                 float
    approved_amount_usd:        float | None = None
    conditions:                 list[str] = Field(default_factory=list)
    executive_summary:          str | None = None
    key_risks:                  list[str] = Field(default_factory=list)
    contributing_sessions:      list[str] = Field(default_factory=list)
    model_versions:             dict[str, str] = Field(default_factory=dict)
    generated_at:               datetime


class ApplicationApproved(BaseEvent):
    event_type:           str = Field("ApplicationApproved", frozen=True)
    application_id:       str
    approved_amount_usd:  float
    interest_rate_pct:    float | None = None
    term_months:          int | None = None
    conditions:           list[str] = Field(default_factory=list)
    approved_by:          str = "auto"
    effective_date:       str | None = None
    approved_at:          datetime


class ApplicationDeclined(BaseEvent):
    event_type:                   str = Field("ApplicationDeclined", frozen=True)
    application_id:               str
    decline_reasons:              list[str]
    declined_by:                  str
    adverse_action_notice_required: bool = True
    adverse_action_codes:         list[str] = Field(default_factory=list)
    declined_at:                  datetime


# =============================================================================
# Document Package stream  (stream_id: docpkg-APEX-XXXX)
# =============================================================================

class PackageCreated(BaseEvent):
    event_type:          str = Field("PackageCreated", frozen=True)
    package_id:          str
    application_id:      str
    required_documents:  list[str]
    created_at:          datetime


class DocumentAdded(BaseEvent):
    event_type:       str = Field("DocumentAdded", frozen=True)
    package_id:       str
    document_id:      str
    document_type:    str
    document_format:  str
    file_hash:        str
    added_at:         datetime


class DocumentFormatValidated(BaseEvent):
    event_type:      str = Field("DocumentFormatValidated", frozen=True)
    package_id:      str
    document_id:     str
    document_type:   str
    page_count:      int
    detected_format: str
    validated_at:    datetime


class ExtractionStarted(BaseEvent):
    event_type:        str = Field("ExtractionStarted", frozen=True)
    package_id:        str
    document_id:       str
    document_type:     str
    pipeline_version:  str
    extraction_model:  str
    started_at:        datetime


class ExtractionCompleted(BaseEvent):
    event_type:        str = Field("ExtractionCompleted", frozen=True)
    package_id:        str
    document_id:       str
    document_type:     str
    facts:             dict[str, Any]
    raw_text_length:   int
    tables_extracted:  int
    processing_ms:     int
    completed_at:      datetime


class QualityAssessmentCompleted(BaseEvent):
    event_type:                str = Field("QualityAssessmentCompleted", frozen=True)
    package_id:                str
    document_id:               str
    overall_confidence:        float
    is_coherent:               bool
    anomalies:                 list[str] = Field(default_factory=list)
    critical_missing_fields:   list[str] = Field(default_factory=list)
    reextraction_recommended:  bool = False
    auditor_notes:             str | None = None
    assessed_at:               datetime


class PackageReadyForAnalysis(BaseEvent):
    event_type:          str = Field("PackageReadyForAnalysis", frozen=True)
    package_id:          str
    application_id:      str
    documents_processed: int
    has_quality_flags:   bool = False
    quality_flag_count:  int = 0
    ready_at:            datetime


# =============================================================================
# Agent Session streams  (stream_id: agent-<type>-<session_id>)
# =============================================================================

class AgentSessionStarted(BaseEvent):
    """Replaces AgentContextLoaded in the seed data vocabulary."""
    event_type:               str = Field("AgentSessionStarted", frozen=True)
    session_id:               str
    agent_type:               str
    agent_id:                 str
    application_id:           str
    model_version:            str
    langgraph_graph_version:  str | None = None
    context_source:           str = "fresh"
    context_token_count:      int
    started_at:               datetime


class AgentInputValidated(BaseEvent):
    event_type:           str = Field("AgentInputValidated", frozen=True)
    session_id:           str
    agent_type:           str
    application_id:       str
    inputs_validated:     list[str]
    validation_duration_ms: int
    validated_at:         datetime


class AgentNodeExecuted(BaseEvent):
    event_type:         str = Field("AgentNodeExecuted", frozen=True)
    session_id:         str
    agent_type:         str
    node_name:          str
    node_sequence:      int
    input_keys:         list[str]
    output_keys:        list[str]
    llm_called:         bool
    llm_tokens_input:   int | None = None
    llm_tokens_output:  int | None = None
    llm_cost_usd:       float | None = None
    duration_ms:        int
    executed_at:        datetime


class AgentToolCalled(BaseEvent):
    event_type:          str = Field("AgentToolCalled", frozen=True)
    session_id:          str
    agent_type:          str
    tool_name:           str
    tool_input_summary:  str
    tool_output_summary: str
    tool_duration_ms:    int
    called_at:           datetime


class AgentOutputWritten(BaseEvent):
    event_type:      str = Field("AgentOutputWritten", frozen=True)
    session_id:      str
    agent_type:      str
    application_id:  str
    events_written:  list[dict[str, Any]]
    output_summary:  str
    written_at:      datetime


class AgentSessionCompleted(BaseEvent):
    event_type:             str = Field("AgentSessionCompleted", frozen=True)
    session_id:             str
    agent_type:             str
    application_id:         str
    total_nodes_executed:   int
    total_llm_calls:        int
    total_tokens_used:      int
    total_cost_usd:         float
    total_duration_ms:      int
    next_agent_triggered:   str | None = None
    completed_at:           datetime


# =============================================================================
# Credit Record stream  (stream_id: credit-APEX-XXXX)
# =============================================================================

class CreditRecordOpened(BaseEvent):
    event_type:      str = Field("CreditRecordOpened", frozen=True)
    application_id:  str
    applicant_id:    str
    opened_at:       datetime


class HistoricalProfileConsumed(BaseEvent):
    event_type:           str = Field("HistoricalProfileConsumed", frozen=True)
    application_id:       str
    session_id:           str
    fiscal_years_loaded:  list[int]
    has_prior_loans:      bool
    has_defaults:         bool
    revenue_trajectory:   str   # STABLE | GROWTH | DECLINING | VOLATILE | RECOVERING
    data_hash:            str
    consumed_at:          datetime


class ExtractedFactsConsumed(BaseEvent):
    event_type:              str = Field("ExtractedFactsConsumed", frozen=True)
    application_id:          str
    session_id:              str
    document_ids_consumed:   list[str]
    facts_summary:           str
    quality_flags_present:   bool
    consumed_at:             datetime


class CreditAnalysisCompleted(BaseEvent):
    """event_version=2 in seed data — has model_version, confidence, regulatory_basis."""
    event_type:          str = Field("CreditAnalysisCompleted", frozen=True)
    event_version:       int = 2
    application_id:      str
    session_id:          str
    decision:            dict[str, Any]   # risk_tier, recommended_limit_usd, confidence, …
    model_version:       str
    model_deployment_id: str | None = None
    input_data_hash:     str
    analysis_duration_ms: int
    regulatory_basis:    list[str] = Field(default_factory=list)
    completed_at:        datetime


# =============================================================================
# Fraud Detection stream  (stream_id: fraud-APEX-XXXX)
# =============================================================================

class FraudScreeningInitiated(BaseEvent):
    event_type:                str = Field("FraudScreeningInitiated", frozen=True)
    application_id:            str
    session_id:                str
    screening_model_version:   str
    initiated_at:              datetime


class FraudScreeningCompleted(BaseEvent):
    event_type:                str = Field("FraudScreeningCompleted", frozen=True)
    application_id:            str
    session_id:                str
    fraud_score:               float   # 0.0–1.0
    risk_level:                str     # LOW | MEDIUM | HIGH
    anomalies_found:           int
    recommendation:            str     # PROCEED | REVIEW | BLOCK
    screening_model_version:   str
    input_data_hash:           str
    completed_at:              datetime


# =============================================================================
# Compliance stream  (stream_id: compliance-APEX-XXXX)
# =============================================================================

class ComplianceCheckInitiated(BaseEvent):
    event_type:               str = Field("ComplianceCheckInitiated", frozen=True)
    application_id:           str
    session_id:               str
    regulation_set_version:   str
    rules_to_evaluate:        list[str]
    initiated_at:             datetime


class ComplianceRulePassed(BaseEvent):
    event_type:        str = Field("ComplianceRulePassed", frozen=True)
    application_id:    str
    session_id:        str
    rule_id:           str
    rule_name:         str
    rule_version:      str
    evidence_hash:     str
    evaluation_notes:  str | None = None
    evaluated_at:      datetime


class ComplianceRuleFailed(BaseEvent):
    event_type:               str = Field("ComplianceRuleFailed", frozen=True)
    application_id:           str
    session_id:               str
    rule_id:                  str
    rule_name:                str
    rule_version:             str
    failure_reason:           str
    is_hard_block:            bool
    remediation_available:    bool
    remediation_description:  str | None = None
    evidence_hash:            str
    evaluated_at:             datetime


class ComplianceRuleNoted(BaseEvent):
    event_type:      str = Field("ComplianceRuleNoted", frozen=True)
    application_id:  str
    session_id:      str
    rule_id:         str
    rule_name:       str
    note_type:       str
    note_text:       str
    evaluated_at:    datetime


class ComplianceCheckCompleted(BaseEvent):
    event_type:       str = Field("ComplianceCheckCompleted", frozen=True)
    application_id:   str
    session_id:       str
    rules_evaluated:  int
    rules_passed:     int
    rules_failed:     int
    rules_noted:      int
    has_hard_block:   bool
    overall_verdict:  str   # CLEAR | BLOCKED | CONDITIONAL
    completed_at:     datetime


# =============================================================================
# Registry: event_type → class (used by EventStore to deserialise payloads)
# =============================================================================

EVENT_REGISTRY: dict[str, type[BaseEvent]] = {
    cls.model_fields["event_type"].default: cls  # type: ignore[union-attr]
    for cls in [
        ApplicationSubmitted, DocumentUploadRequested, DocumentUploaded,
        CreditAnalysisRequested, FraudScreeningRequested, ComplianceCheckRequested,
        DecisionRequested, DecisionGenerated, ApplicationApproved, ApplicationDeclined,
        PackageCreated, DocumentAdded, DocumentFormatValidated, ExtractionStarted,
        ExtractionCompleted, QualityAssessmentCompleted, PackageReadyForAnalysis,
        AgentSessionStarted, AgentInputValidated, AgentNodeExecuted, AgentToolCalled,
        AgentOutputWritten, AgentSessionCompleted,
        CreditRecordOpened, HistoricalProfileConsumed, ExtractedFactsConsumed,
        CreditAnalysisCompleted,
        FraudScreeningInitiated, FraudScreeningCompleted,
        ComplianceCheckInitiated, ComplianceRulePassed, ComplianceRuleFailed,
        ComplianceRuleNoted, ComplianceCheckCompleted,
    ]
}
