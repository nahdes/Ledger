"""
src/projections/application_summary.py

ApplicationSummary projection.
One row per loan application, maintained as events arrive.
SLO: p99 < 500ms lag in normal operation.
"""
from __future__ import annotations
from typing import Any
import asyncpg
from src.projections.base import Projection


class ApplicationSummaryProjection(Projection):

    @property
    def name(self) -> str:
        return "ApplicationSummary"

    @property
    def event_types(self) -> set[str]:
        return {
            "ApplicationSubmitted",
            "CreditAnalysisCompleted",
            "FraudScreeningCompleted",
            "ComplianceCheckRequested",
            "ComplianceRulePassed",
            "ComplianceRuleFailed",
            "DecisionGenerated",
            "ApplicationApproved",
            "ApplicationDeclined",
            "AgentSessionCompleted",
            "HumanReviewCompleted",
        }

    async def handle(self, event: Any, conn: asyncpg.Connection) -> None:
        p   = event.payload
        eid = p.get("application_id")
        if not eid:
            return

        et = event.event_type

        if et == "ApplicationSubmitted":
            await conn.execute(
                """
                INSERT INTO application_summary
                    (application_id, state, applicant_id, requested_amount_usd,
                     last_event_type, last_event_at)
                VALUES ($1,'SUBMITTED',$2,$3,$4,$5)
                ON CONFLICT (application_id) DO UPDATE SET
                    state              = 'SUBMITTED',
                    applicant_id       = EXCLUDED.applicant_id,
                    requested_amount_usd = EXCLUDED.requested_amount_usd,
                    last_event_type    = EXCLUDED.last_event_type,
                    last_event_at      = EXCLUDED.last_event_at,
                    updated_at         = NOW()
                """,
                eid,
                p.get("applicant_id"),
                float(p.get("requested_amount_usd", 0)),
                et, event.recorded_at,
            )

        elif et == "CreditAnalysisCompleted":
            decision = p.get("decision", {})
            risk_tier = decision.get("risk_tier") if isinstance(decision, dict) else p.get("risk_tier")
            await conn.execute(
                """
                UPDATE application_summary SET
                    risk_tier       = $2,
                    state           = 'ANALYSIS_COMPLETE',
                    last_event_type = $3,
                    last_event_at   = $4,
                    updated_at      = NOW()
                WHERE application_id = $1
                """,
                eid, risk_tier, et, event.recorded_at,
            )

        elif et == "FraudScreeningCompleted":
            await conn.execute(
                """
                UPDATE application_summary SET
                    fraud_score     = $2,
                    last_event_type = $3,
                    last_event_at   = $4,
                    updated_at      = NOW()
                WHERE application_id = $1
                """,
                eid, float(p.get("fraud_score", 0)), et, event.recorded_at,
            )

        elif et == "ComplianceCheckRequested":
            await conn.execute(
                """
                UPDATE application_summary SET
                    compliance_status = 'IN_PROGRESS',
                    last_event_type   = $2,
                    last_event_at     = $3,
                    updated_at        = NOW()
                WHERE application_id = $1
                """,
                eid, et, event.recorded_at,
            )

        elif et == "ComplianceRuleFailed":
            if p.get("is_hard_block"):
                await conn.execute(
                    """
                    UPDATE application_summary SET
                        compliance_status = 'BLOCKED',
                        last_event_type   = $2,
                        last_event_at     = $3,
                        updated_at        = NOW()
                    WHERE application_id = $1
                    """,
                    eid, et, event.recorded_at,
                )

        elif et == "DecisionGenerated":
            rec = p.get("recommendation", "")
            state_map = {"APPROVE": "PENDING_DECISION", "DECLINE": "PENDING_DECISION", "REFER": "REFERRED"}
            await conn.execute(
                """
                UPDATE application_summary SET
                    decision        = $2,
                    state           = $3,
                    last_event_type = $4,
                    last_event_at   = $5,
                    updated_at      = NOW()
                WHERE application_id = $1
                """,
                eid, rec, state_map.get(rec, "PENDING_DECISION"), et, event.recorded_at,
            )

        elif et == "ApplicationApproved":
            await conn.execute(
                """
                UPDATE application_summary SET
                    state               = 'FINAL_APPROVED',
                    approved_amount_usd = $2,
                    compliance_status   = COALESCE(compliance_status, 'CLEAR'),
                    final_decision_at   = $3,
                    last_event_type     = $4,
                    last_event_at       = $5,
                    updated_at          = NOW()
                WHERE application_id = $1
                """,
                eid,
                float(p.get("approved_amount_usd", 0)),
                event.recorded_at, et, event.recorded_at,
            )

        elif et == "ApplicationDeclined":
            await conn.execute(
                """
                UPDATE application_summary SET
                    state             = 'FINAL_DECLINED',
                    final_decision_at = $2,
                    last_event_type   = $3,
                    last_event_at     = $4,
                    updated_at        = NOW()
                WHERE application_id = $1
                """,
                eid, event.recorded_at, et, event.recorded_at,
            )

        elif et == "HumanReviewCompleted":
            await conn.execute(
                """
                UPDATE application_summary SET
                    human_reviewer_id = $2,
                    last_event_type   = $3,
                    last_event_at     = $4,
                    updated_at        = NOW()
                WHERE application_id = $1
                """,
                eid, p.get("reviewer_id"), et, event.recorded_at,
            )

        elif et == "AgentSessionCompleted":
            sess_id = p.get("session_id", "")
            await conn.execute(
                """
                UPDATE application_summary SET
                    agent_sessions_completed = array_append(
                        COALESCE(agent_sessions_completed, '{}'), $2
                    ),
                    last_event_type = $3,
                    last_event_at   = $4,
                    updated_at      = NOW()
                WHERE application_id = $1
                """,
                eid, sess_id, et, event.recorded_at,
            )