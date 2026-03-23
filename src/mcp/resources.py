"""
src/mcp/resources.py

Phase 5: MCP Resources — the query (read) side.
All reads come from projections, never from replaying aggregate streams
(with two justified exceptions: audit-trail and session streams).

6 resources:
  ledger://applications/{id}              — ApplicationSummary      p99 < 50ms
  ledger://applications/{id}/compliance   — ComplianceAuditView     p99 < 200ms
  ledger://applications/{id}/audit-trail  — AuditLedger direct load p99 < 500ms
  ledger://agents/{id}/performance        — AgentPerformanceLedger  p99 < 50ms
  ledger://agents/{id}/sessions/{sid}     — AgentSession direct load p99 < 300ms
  ledger://ledger/health                  — ProjectionDaemon lags   p99 < 10ms
"""
from __future__ import annotations

import json
import logging
from typing import Any
from urllib.parse import parse_qs, urlparse

import asyncpg
from mcp.server import Server
from mcp.types import Resource, TextContent

from src.projections.compliance_audit import ComplianceAuditViewProjection

logger = logging.getLogger(__name__)


def _json(data: Any) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps(data, default=str))]


def register_resources(
    app: Server,
    store: Any,
    pool: asyncpg.Pool,
    daemon: Any,          # ProjectionDaemon
) -> None:

    @app.list_resources()
    async def list_resources() -> list[Resource]:
        return [
            Resource(
                uri="ledger://applications/{id}",
                name="Loan Application Summary",
                description="Current state of a loan application from ApplicationSummary projection. p99 < 50ms.",
                mimeType="application/json",
            ),
            Resource(
                uri="ledger://applications/{id}/compliance",
                name="Compliance Audit View",
                description="Full compliance record. Supports temporal query: add ?as_of=ISO_TIMESTAMP for time-travel. p99 < 200ms.",
                mimeType="application/json",
            ),
            Resource(
                uri="ledger://applications/{id}/audit-trail",
                name="Audit Trail",
                description="Complete audit event stream for an application. Supports ?from=pos&to=pos range. p99 < 500ms.",
                mimeType="application/json",
            ),
            Resource(
                uri="ledger://agents/{id}/performance",
                name="Agent Performance Ledger",
                description="Aggregated performance metrics per agent and model version. p99 < 50ms.",
                mimeType="application/json",
            ),
            Resource(
                uri="ledger://agents/{id}/sessions/{session_id}",
                name="Agent Session",
                description="Full agent session event stream with direct replay capability. p99 < 300ms.",
                mimeType="application/json",
            ),
            Resource(
                uri="ledger://ledger/health",
                name="Ledger Health",
                description="ProjectionDaemon lag metrics for all projections. Watchdog endpoint. p99 < 10ms.",
                mimeType="application/json",
            ),
        ]

    @app.read_resource()
    async def read_resource(uri: str) -> list[TextContent]:
        parsed   = urlparse(uri)
        host     = parsed.netloc      # e.g. "applications"
        path     = parsed.path        # e.g. "/APEX-0021/compliance"
        parts    = [p for p in path.split("/") if p]
        qs       = parse_qs(parsed.query)

        try:
            # ── ledger://ledger/health ─────────────────────────────────────────
            if host == "ledger" and parts == ["health"]:
                lags = await daemon.get_all_lags()
                return _json({"lags": lags})

            # ── ledger://agents/{id}/performance ──────────────────────────────
            if host == "agents" and len(parts) == 2 and parts[1] == "performance":
                agent_id = parts[0]
                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT agent_id, model_version, analyses_completed,
                               decisions_generated, total_confidence_sum,
                               total_duration_ms_sum, approve_count,
                               decline_count, refer_count, human_override_count,
                               first_seen_at, last_seen_at
                        FROM   agent_performance_ledger
                        WHERE  agent_id = $1
                        """,
                        agent_id,
                    )
                result = []
                for r in rows:
                    n = r["analyses_completed"] or 1
                    result.append({
                        "agent_id":          r["agent_id"],
                        "model_version":     r["model_version"],
                        "analyses_completed": r["analyses_completed"],
                        "decisions_generated": r["decisions_generated"],
                        "avg_confidence":    round(float(r["total_confidence_sum"] or 0) / n, 3),
                        "avg_duration_ms":   round(float(r["total_duration_ms_sum"] or 0) / n, 1),
                        "approve_rate":      round(r["approve_count"] / max(r["decisions_generated"],1), 3),
                        "decline_rate":      round(r["decline_count"] / max(r["decisions_generated"],1), 3),
                        "refer_rate":        round(r["refer_count"]   / max(r["decisions_generated"],1), 3),
                        "human_override_count": r["human_override_count"],
                        "first_seen_at":     r["first_seen_at"],
                        "last_seen_at":      r["last_seen_at"],
                    })
                return _json(result)

            # ── ledger://agents/{id}/sessions/{session_id} ────────────────────
            if host == "agents" and len(parts) == 3 and parts[1] == "sessions":
                agent_id   = parts[0]
                session_id = parts[2]
                # Justified direct stream load — session replay is the explicit use case
                stream_id  = f"agent-{agent_id}-{session_id}"
                events     = await store.load_stream(stream_id)
                return _json({
                    "stream_id": stream_id,
                    "event_count": len(events),
                    "events": [
                        {
                            "stream_position": e.stream_position,
                            "event_type":      e.event_type,
                            "event_version":   e.event_version,
                            "payload":         e.payload,
                            "recorded_at":     e.recorded_at,
                        }
                        for e in events
                    ],
                })

            if host != "applications" or not parts:
                return _json({"error": f"Unknown resource URI: {uri}"})

            app_id = parts[0]

            # ── ledger://applications/{id} ─────────────────────────────────────
            if len(parts) == 1:
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT * FROM application_summary WHERE application_id = $1",
                        app_id,
                    )
                if not row:
                    return _json({"error": f"Application {app_id} not found in projection"})
                return _json(dict(row))

            # ── ledger://applications/{id}/compliance ──────────────────────────
            if parts[1] == "compliance":
                as_of = qs.get("as_of", [None])[0]
                async with pool.acquire() as conn:
                    if as_of:
                        from datetime import datetime, timezone
                        ts    = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
                        state = await ComplianceAuditViewProjection.get_compliance_at(
                            app_id, ts, conn
                        )
                    else:
                        state = await ComplianceAuditViewProjection.get_current_compliance(
                            app_id, conn
                        )
                return _json(state.summary())

            # ── ledger://applications/{id}/audit-trail ─────────────────────────
            if parts[1] == "audit-trail":
                # Justified exception: audit trail requires direct stream load
                # for complete regulatory traceability
                from_pos = int(qs.get("from", [0])[0])
                to_pos   = qs.get("to", [None])[0]
                events   = await store.load_stream(
                    f"loan-{app_id}",
                    from_position=from_pos,
                    to_position=int(to_pos) if to_pos else None,
                )
                return _json({
                    "application_id": app_id,
                    "event_count":    len(events),
                    "events": [
                        {
                            "stream_position":  e.stream_position,
                            "global_position":  e.global_position,
                            "event_type":       e.event_type,
                            "event_version":    e.event_version,
                            "payload":          e.payload,
                            "metadata":         e.metadata,
                            "recorded_at":      e.recorded_at,
                        }
                        for e in events
                    ],
                })

            return _json({"error": f"Unknown sub-resource: {parts[1]}"})

        except Exception as exc:
            logger.exception("Resource read error for %s", uri)
            return _json({"error": str(exc)})