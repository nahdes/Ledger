"""
src/mcp/tools.py

Phase 5: MCP Tools — the command (write) side.

8 tools following structural CQRS: tools write events, resources read projections.

LLM-consumption design principles applied:
  1. Preconditions in tool descriptions — the LLM's only contract
  2. Structured error dicts — enable autonomous recovery
  3. Every tool returns typed success/error responses
"""
from __future__ import annotations

import json
import logging
from typing import Any

from mcp.server import Server
from mcp.types import TextContent, Tool

from src.commands.handlers import (
    ComplianceCheckCompletedCommand,
    CreditAnalysisCompletedCommand,
    FraudScreeningCompletedCommand,
    GenerateDecisionCommand,
    HumanReviewCompletedCommand,
    StartAgentSessionCommand,
    SubmitApplicationCommand,
    handle_compliance_check_completed,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_generate_decision,
    handle_human_review_completed,
    handle_start_agent_session,
    handle_submit_application,
)
from src.integrity.audit_chain import run_integrity_check
from src.models.events import DomainError, OptimisticConcurrencyError

logger = logging.getLogger(__name__)


def _ok(data: dict[str, Any]) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps({"status": "ok", **data}))]


def _err(error_type: str, message: str, **kwargs: Any) -> list[TextContent]:
    return [TextContent(
        type="text",
        text=json.dumps({
            "status": "error",
            "error_type": error_type,
            "message": message,
            **kwargs,
        }),
    )]


def register_tools(app: Server, store: Any) -> None:
    """Register all 8 MCP tools on the server."""

    @app.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="submit_application",
                description=(
                    "Submit a new commercial loan application. "
                    "Creates a new LoanApplication event stream. "
                    "Returns stream_id and initial_version. "
                    "Error DUPLICATE_APPLICATION if application_id already exists."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["application_id", "applicant_id",
                                 "requested_amount_usd", "loan_purpose", "submission_channel"],
                    "properties": {
                        "application_id":       {"type": "string"},
                        "applicant_id":         {"type": "string"},
                        "requested_amount_usd": {"type": "number"},
                        "loan_purpose":         {"type": "string"},
                        "submission_channel":   {"type": "string"},
                        "contact_email":        {"type": "string"},
                        "correlation_id":       {"type": "string"},
                    },
                },
            ),
            Tool(
                name="start_agent_session",
                description=(
                    "Start a new agent session. MUST be called before any other agent "
                    "tool for this session — the Gas Town pattern requires a loaded "
                    "context before any decision can be recorded. "
                    "Returns session_id and stream_id. "
                    "Error GAS_TOWN if called out of order."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["agent_type", "session_id", "model_version", "application_id"],
                    "properties": {
                        "agent_type":           {"type": "string"},
                        "session_id":           {"type": "string"},
                        "model_version":        {"type": "string"},
                        "application_id":       {"type": "string"},
                        "context_source":       {"type": "string", "default": "fresh"},
                        "context_token_count":  {"type": "integer", "default": 0},
                        "agent_id":             {"type": "string"},
                        "correlation_id":       {"type": "string"},
                    },
                },
            ),
            Tool(
                name="record_credit_analysis",
                description=(
                    "Record a completed credit analysis for a loan application. "
                    "PRECONDITION: start_agent_session must have been called for "
                    "this session_id. Application must be in AWAITING_ANALYSIS state. "
                    "Error GAS_TOWN if session context not loaded. "
                    "Error MODEL_VERSION_LOCK if model_version mismatches session. "
                    "Error OptimisticConcurrencyError if concurrent write detected "
                    "(suggested_action: reload_stream_and_retry)."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["application_id", "agent_type", "session_id",
                                 "model_version", "confidence", "risk_tier",
                                 "recommended_limit_usd", "duration_ms", "input_data"],
                    "properties": {
                        "application_id":        {"type": "string"},
                        "agent_type":            {"type": "string"},
                        "session_id":            {"type": "string"},
                        "model_version":         {"type": "string"},
                        "confidence":            {"type": "number", "minimum": 0, "maximum": 1},
                        "risk_tier":             {"type": "string", "enum": ["LOW","MEDIUM","HIGH"]},
                        "recommended_limit_usd": {"type": "number"},
                        "duration_ms":           {"type": "integer"},
                        "input_data":            {"type": "object"},
                        "key_concerns":          {"type": "array", "items": {"type": "string"}},
                        "regulatory_basis":      {"type": "array", "items": {"type": "string"}},
                        "correlation_id":        {"type": "string"},
                    },
                },
            ),
            Tool(
                name="record_fraud_screening",
                description=(
                    "Record a completed fraud screening result. "
                    "PRECONDITION: start_agent_session must have been called. "
                    "fraud_score must be between 0.0 and 1.0. "
                    "Error FRAUD_SCORE_RANGE if out of bounds."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["application_id", "agent_type", "session_id",
                                 "fraud_score", "anomaly_flags",
                                 "screening_model_version", "input_data"],
                    "properties": {
                        "application_id":          {"type": "string"},
                        "agent_type":              {"type": "string"},
                        "session_id":              {"type": "string"},
                        "fraud_score":             {"type": "number", "minimum": 0, "maximum": 1},
                        "anomaly_flags":           {"type": "array", "items": {"type": "string"}},
                        "screening_model_version": {"type": "string"},
                        "input_data":              {"type": "object"},
                        "risk_level":              {"type": "string", "enum": ["LOW","MEDIUM","HIGH"]},
                        "recommendation":          {"type": "string", "enum": ["PROCEED","REVIEW","BLOCK"]},
                        "correlation_id":          {"type": "string"},
                    },
                },
            ),
            Tool(
                name="record_compliance_check",
                description=(
                    "Record the outcome of a single compliance rule evaluation. "
                    "PRECONDITION: start_agent_session must have been called. "
                    "failure_reason is required when passed=false. "
                    "Returns check_id and compliance_status."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["application_id", "agent_type", "session_id",
                                 "rule_id", "rule_name", "rule_version",
                                 "passed", "evidence_hash"],
                    "properties": {
                        "application_id":          {"type": "string"},
                        "agent_type":              {"type": "string"},
                        "session_id":              {"type": "string"},
                        "rule_id":                 {"type": "string"},
                        "rule_name":               {"type": "string"},
                        "rule_version":            {"type": "string"},
                        "passed":                  {"type": "boolean"},
                        "evidence_hash":           {"type": "string"},
                        "failure_reason":          {"type": "string"},
                        "is_hard_block":           {"type": "boolean"},
                        "correlation_id":          {"type": "string"},
                    },
                },
            ),
            Tool(
                name="generate_decision",
                description=(
                    "Generate a final AI decision for a loan application. "
                    "PRECONDITIONS: (1) start_agent_session must be called for orchestrator. "
                    "(2) confidence < 0.60 forces recommendation=REFER regardless of analysis. "
                    "(3) All compliance checks must have passed before APPROVE or DECLINE. "
                    "(4) contributing_sessions must reference sessions that processed this application. "
                    "Returns decision_id, recommendation, and model_versions dict."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["application_id", "orchestrator_agent_type",
                                 "orchestrator_session_id", "recommendation",
                                 "confidence", "approved_amount_usd"],
                    "properties": {
                        "application_id":          {"type": "string"},
                        "orchestrator_agent_type": {"type": "string"},
                        "orchestrator_session_id": {"type": "string"},
                        "recommendation":          {"type": "string", "enum": ["APPROVE","DECLINE","REFER"]},
                        "confidence":              {"type": "number", "minimum": 0, "maximum": 1},
                        "approved_amount_usd":     {"type": ["number","null"]},
                        "conditions":              {"type": "array", "items": {"type": "string"}},
                        "executive_summary":       {"type": "string"},
                        "key_risks":               {"type": "array", "items": {"type": "string"}},
                        "contributing_sessions":   {"type": "array", "items": {"type": "string"}},
                        "model_version":           {"type": "string"},
                        "correlation_id":          {"type": "string"},
                    },
                },
            ),
            Tool(
                name="record_human_review",
                description=(
                    "Record a human loan officer's review of an AI recommendation. "
                    "Application must be in PENDING_DECISION state. "
                    "override_reason is required when override=true. "
                    "If final_decision is APPROVE or DECLINE, the application is "
                    "closed atomically in the same event batch."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["application_id", "reviewer_id",
                                 "override", "final_decision"],
                    "properties": {
                        "application_id": {"type": "string"},
                        "reviewer_id":    {"type": "string"},
                        "override":       {"type": "boolean"},
                        "final_decision": {"type": "string", "enum": ["APPROVE","DECLINE","REFER"]},
                        "override_reason":{"type": "string"},
                        "conditions":     {"type": "array", "items": {"type": "string"}},
                        "correlation_id": {"type": "string"},
                    },
                },
            ),
            Tool(
                name="run_integrity_check",
                description=(
                    "Run a cryptographic audit chain integrity check on an entity's event stream. "
                    "Computes SHA-256 hash chain over all events and verifies it against "
                    "the previous check. Returns chain_valid and tamper_detected. "
                    "Rate-limited: max 1 call per minute per entity. "
                    "Requires compliance role."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["entity_type", "entity_id"],
                    "properties": {
                        "entity_type": {"type": "string", "enum": ["loan","agent","compliance","audit"]},
                        "entity_id":   {"type": "string"},
                    },
                },
            ),
        ]

    @app.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        try:
            if name == "submit_application":
                cmd    = SubmitApplicationCommand(**{
                    k: v for k, v in arguments.items()
                    if k in SubmitApplicationCommand.__dataclass_fields__
                })
                result = await handle_submit_application(cmd, store)
                return _ok(result)

            elif name == "start_agent_session":
                cmd    = StartAgentSessionCommand(**{
                    k: v for k, v in arguments.items()
                    if k in StartAgentSessionCommand.__dataclass_fields__
                })
                result = await handle_start_agent_session(cmd, store)
                return _ok(result)

            elif name == "record_credit_analysis":
                cmd    = CreditAnalysisCompletedCommand(**{
                    k: v for k, v in arguments.items()
                    if k in CreditAnalysisCompletedCommand.__dataclass_fields__
                })
                result = await handle_credit_analysis_completed(cmd, store)
                return _ok(result)

            elif name == "record_fraud_screening":
                cmd    = FraudScreeningCompletedCommand(**{
                    k: v for k, v in arguments.items()
                    if k in FraudScreeningCompletedCommand.__dataclass_fields__
                })
                result = await handle_fraud_screening_completed(cmd, store)
                return _ok(result)

            elif name == "record_compliance_check":
                cmd    = ComplianceCheckCompletedCommand(**{
                    k: v for k, v in arguments.items()
                    if k in ComplianceCheckCompletedCommand.__dataclass_fields__
                })
                result = await handle_compliance_check_completed(cmd, store)
                return _ok(result)

            elif name == "generate_decision":
                cmd    = GenerateDecisionCommand(**{
                    k: v for k, v in arguments.items()
                    if k in GenerateDecisionCommand.__dataclass_fields__
                })
                result = await handle_generate_decision(cmd, store)
                return _ok(result)

            elif name == "record_human_review":
                cmd    = HumanReviewCompletedCommand(**{
                    k: v for k, v in arguments.items()
                    if k in HumanReviewCompletedCommand.__dataclass_fields__
                })
                result = await handle_human_review_completed(cmd, store)
                return _ok(result)

            elif name == "run_integrity_check":
                result = await run_integrity_check(
                    store,
                    arguments["entity_type"],
                    arguments["entity_id"],
                )
                return _ok(result.to_dict())

            else:
                return _err("UNKNOWN_TOOL", f"Tool {name!r} not found")

        except DomainError as e:
            return _err("DomainError", e.message, rule=e.rule, context=e.context)
        except OptimisticConcurrencyError as e:
            return _err(
                "OptimisticConcurrencyError",
                str(e),
                stream_id=e.stream_id,
                expected_version=e.expected_version,
                actual_version=e.actual_version,
                suggested_action="reload_stream_and_retry",
            )
        except Exception as e:
            logger.exception("Unhandled error in tool %s", name)
            return _err("InternalError", str(e))