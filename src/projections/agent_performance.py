"""
src/projections/agent_performance.py

AgentPerformanceLedger projection.
Aggregated metrics per (agent_id, model_version). SLO: p99 < 50ms.
Answers: "Has model v2.3 been making systematically different decisions than v2.2?"
"""
from __future__ import annotations
from typing import Any
import asyncpg
from src.projections.base import Projection


class AgentPerformanceLedgerProjection(Projection):

    @property
    def name(self) -> str:
        return "AgentPerformanceLedger"

    @property
    def event_types(self) -> set[str]:
        return {
            "AgentSessionStarted",
            "AgentSessionCompleted",
            "CreditAnalysisCompleted",
            "DecisionGenerated",
            "HumanReviewCompleted",
        }

    async def handle(self, event: Any, conn: asyncpg.Connection) -> None:
        p  = event.payload
        et = event.event_type

        if et == "AgentSessionStarted":
            agent_id      = p.get("agent_id", "")
            model_version = p.get("model_version", "unknown")
            await conn.execute(
                """
                INSERT INTO agent_performance_ledger
                    (agent_id, model_version, first_seen_at, last_seen_at)
                VALUES ($1, $2, $3, $3)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                    last_seen_at = EXCLUDED.last_seen_at
                """,
                agent_id, model_version, event.recorded_at,
            )

        elif et == "CreditAnalysisCompleted":
            agent_id      = p.get("session_id", "")   # use session_id as proxy
            model_version = p.get("model_version", "unknown")
            decision      = p.get("decision", {})
            confidence    = (decision.get("confidence") if isinstance(decision, dict)
                             else p.get("confidence_score")) or 0.0
            duration_ms   = p.get("analysis_duration_ms", 0)
            await conn.execute(
                """
                INSERT INTO agent_performance_ledger
                    (agent_id, model_version, analyses_completed,
                     total_confidence_sum, total_duration_ms_sum,
                     first_seen_at, last_seen_at)
                VALUES ($1, $2, 1, $3, $4, $5, $5)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                    analyses_completed    = agent_performance_ledger.analyses_completed + 1,
                    total_confidence_sum  = agent_performance_ledger.total_confidence_sum + EXCLUDED.total_confidence_sum,
                    total_duration_ms_sum = agent_performance_ledger.total_duration_ms_sum + EXCLUDED.total_duration_ms_sum,
                    last_seen_at          = EXCLUDED.last_seen_at
                """,
                agent_id, model_version,
                float(confidence) if confidence is not None else 0.0,
                int(duration_ms), event.recorded_at,
            )

        elif et == "DecisionGenerated":
            orch_session  = p.get("orchestrator_session_id", "")
            model_versions = p.get("model_versions", {})
            model_version  = (model_versions.get("orchestrator") if isinstance(model_versions, dict)
                              else "unknown") or "unknown"
            rec = p.get("recommendation", "")
            approve = 1 if rec == "APPROVE" else 0
            decline = 1 if rec == "DECLINE" else 0
            refer   = 1 if rec == "REFER"   else 0
            await conn.execute(
                """
                INSERT INTO agent_performance_ledger
                    (agent_id, model_version, decisions_generated,
                     approve_count, decline_count, refer_count,
                     first_seen_at, last_seen_at)
                VALUES ($1, $2, 1, $3, $4, $5, $6, $6)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                    decisions_generated = agent_performance_ledger.decisions_generated + 1,
                    approve_count       = agent_performance_ledger.approve_count + EXCLUDED.approve_count,
                    decline_count       = agent_performance_ledger.decline_count + EXCLUDED.decline_count,
                    refer_count         = agent_performance_ledger.refer_count + EXCLUDED.refer_count,
                    last_seen_at        = EXCLUDED.last_seen_at
                """,
                orch_session, model_version,
                approve, decline, refer, event.recorded_at,
            )

        elif et == "HumanReviewCompleted":
            if p.get("override"):
                # Track override rate — need to find the model version from context
                # Use a placeholder agent_id from reviewer context
                reviewer_id = p.get("reviewer_id", "human")
                await conn.execute(
                    """
                    INSERT INTO agent_performance_ledger
                        (agent_id, model_version, human_override_count, first_seen_at, last_seen_at)
                    VALUES ($1, 'human-override', 1, $2, $2)
                    ON CONFLICT (agent_id, model_version) DO UPDATE SET
                        human_override_count = agent_performance_ledger.human_override_count + 1,
                        last_seen_at         = EXCLUDED.last_seen_at
                    """,
                    reviewer_id, event.recorded_at,
                )