"""
src/projections/application_summary.py
ApplicationSummary projection — one row per loan application.
Tracks state transitions, risk tier, fraud score, compliance status,
and human reviewer information.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import asyncpg

from src.projections.base import Projection

logger = logging.getLogger(__name__)


@dataclass
class ApplicationSummaryState:
    application_id: str
    state: str = "SUBMITTED"
    applicant_id: str | None = None
    requested_amount_usd: float | None = None
    approved_amount_usd: float | None = None
    risk_tier: str | None = None
    fraud_score: float | None = None
    compliance_status: str = "PENDING"
    decision: str | None = None
    agent_sessions_completed: list[str] = field(default_factory=list)
    human_reviewer_id: str | None = None
    last_event_type: str | None = None
    last_event_at: datetime | None = None
    final_decision_at: datetime | None = None


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
            "ComplianceRulePassed",
            "ComplianceRuleFailed",
            "DecisionGenerated",
            "ApplicationApproved",
            "ApplicationDeclined",
            "HumanReviewCompleted",  # ← ADDED: Handle human review events
        }

    async def handle(self, event: Any, conn: asyncpg.Connection) -> None:
        p = event.payload
        et = event.event_type
        app_id = p.get("application_id")

        if not app_id:
            logger.debug(f"Skipping event {et}: no application_id in payload")
            return

        logger.debug(f"Processing event {et} for application {app_id}, payload keys: {list(p.keys()) if p else 'None'}")

        if et == "ApplicationSubmitted":
            await conn.execute(
                """
                INSERT INTO application_summary
                    (application_id, applicant_id, requested_amount_usd,
                     state, last_event_type, last_event_at, updated_at)
                VALUES ($1, $2, $3, 'SUBMITTED', $4, $5, NOW())
                ON CONFLICT (application_id) DO UPDATE
                    SET applicant_id          = EXCLUDED.applicant_id,
                        requested_amount_usd  = EXCLUDED.requested_amount_usd,
                        state                 = EXCLUDED.state,
                        last_event_type       = EXCLUDED.last_event_type,
                        last_event_at         = EXCLUDED.last_event_at,
                        updated_at            = NOW()
                """,
                app_id,
                p.get("applicant_id"),
                p.get("requested_amount_usd"),
                et,
                event.recorded_at,
            )
            logger.debug(f"ApplicationSubmitted processed for {app_id}")

        elif et == "CreditAnalysisCompleted":
            decision = p.get("decision", {})
            risk_tier = (decision.get("risk_tier") if isinstance(decision, dict)
                        else p.get("risk_tier"))
            await conn.execute(
                """
                UPDATE application_summary
                SET    risk_tier        = $2,
                       state            = 'ANALYSIS_COMPLETE',
                       last_event_type  = $3,
                       last_event_at    = $4,
                       updated_at       = NOW()
                WHERE  application_id   = $1
                """,
                app_id,
                risk_tier,
                et,
                event.recorded_at,
            )
            logger.debug(f"CreditAnalysisCompleted processed for {app_id}, risk_tier={risk_tier}")

        elif et == "FraudScreeningCompleted":
            await conn.execute(
                """
                UPDATE application_summary
                SET    fraud_score      = $2,
                       last_event_type  = $3,
                       last_event_at    = $4,
                       updated_at       = NOW()
                WHERE  application_id   = $1
                """,
                app_id,
                p.get("fraud_score"),
                et,
                event.recorded_at,
            )
            logger.debug(f"FraudScreeningCompleted processed for {app_id}")

        elif et == "ComplianceRuleFailed":
            is_hard_block = p.get("is_hard_block", False)
            if is_hard_block:
                await conn.execute(
                    """
                    UPDATE application_summary
                    SET    compliance_status = 'BLOCKED',
                           last_event_type   = $2,
                           last_event_at     = $3,
                           updated_at        = NOW()
                    WHERE  application_id    = $1
                    """,
                    app_id,
                    et,
                    event.recorded_at,
                )
                logger.debug(f"ComplianceRuleFailed (hard block) processed for {app_id}")

        elif et == "ComplianceRulePassed":
            await conn.execute(
                """
                UPDATE application_summary
                SET    compliance_status = 'CLEAR',
                       last_event_type   = $2,
                       last_event_at     = $3,
                       updated_at        = NOW()
                WHERE  application_id    = $1
                """,
                app_id,
                et,
                event.recorded_at,
            )
            logger.debug(f"ComplianceRulePassed processed for {app_id}")

        elif et == "DecisionGenerated":
            await conn.execute(
                """
                UPDATE application_summary
                SET    decision         = $2,
                       state            = 'PENDING_DECISION',
                       last_event_type  = $3,
                       last_event_at    = $4,
                       updated_at       = NOW()
                WHERE  application_id   = $1
                """,
                app_id,
                p.get("recommendation"),
                et,
                event.recorded_at,
            )
            logger.debug(f"DecisionGenerated processed for {app_id}")

        elif et == "ApplicationApproved":
            await conn.execute(
                """
                UPDATE application_summary
                SET    state             = 'FINAL_APPROVED',
                       approved_amount_usd = $2,
                       final_decision_at = $3,
                       last_event_type   = $4,
                       last_event_at     = $5,
                       updated_at        = NOW()
                WHERE  application_id    = $1
                """,
                app_id,
                p.get("approved_amount_usd"),
                event.recorded_at,
                et,
                event.recorded_at,
            )
            logger.debug(f"ApplicationApproved processed for {app_id}")

        elif et == "ApplicationDeclined":
            await conn.execute(
                """
                UPDATE application_summary
                SET    state             = 'FINAL_DECLINED',
                       final_decision_at = $2,
                       last_event_type   = $3,
                       last_event_at     = $4,
                       updated_at        = NOW()
                WHERE  application_id    = $1
                """,
                app_id,
                event.recorded_at,
                et,
                event.recorded_at,
            )
            logger.debug(f"ApplicationDeclined processed for {app_id}")

        # ← NEW: Handle HumanReviewCompleted events to update human_reviewer_id
        elif et == "HumanReviewCompleted":
            logger.debug(f"Processing HumanReviewCompleted for app {app_id}, payload: {p}")
            
            reviewer_id = p.get("reviewer_id")
            final_decision = p.get("final_decision")
            
            logger.debug(f"reviewer_id={reviewer_id}, final_decision={final_decision}")
            
            if reviewer_id is None:
                logger.error(f"reviewer_id is None in payload: {p}")
                logger.error(f"Available payload keys: {list(p.keys()) if p else 'None'}")
            
            # Map decision to state
            state_map = {
                "APPROVE": "FINAL_APPROVED",
                "DECLINE": "FINAL_DECLINED",
                "REFER": "REFERRED",
            }
            new_state = state_map.get(final_decision, "PENDING_DECISION")
            
            await conn.execute(
                """
                UPDATE application_summary
                SET    human_reviewer_id = $2,
                       decision          = $3,
                       state             = $4,
                       final_decision_at = $5,
                       last_event_type   = $6,
                       last_event_at     = $7,
                       updated_at        = NOW()
                WHERE  application_id    = $1
                """,
                app_id,
                reviewer_id,
                final_decision,
                new_state,
                event.recorded_at,
                et,
                event.recorded_at,
            )
            logger.debug(f"HumanReviewCompleted processed for {app_id}, reviewer_id={reviewer_id}, state={new_state}")

        else:
            logger.debug(f"Unhandled event type {et} for application {app_id}")