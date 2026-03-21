"""
src/aggregates/agent_session.py

AgentSessionAggregate — models the lifecycle of one agent session.

Gas Town pattern: the aggregate is reconstructed purely from the event stream.
No external state (Redis, in-memory cache) is required for recovery.
"""
from __future__ import annotations

from typing import Any

from src.models.events import DomainError, StoredEvent


class AgentSessionAggregate:

    def __init__(self, agent_type: str, session_id: str) -> None:
        self.agent_type    = agent_type
        self.session_id    = session_id
        self.version       = 0

        # Set from AgentSessionStarted
        self.context_loaded:          bool       = False
        self.declared_model_version:  str | None = None
        self.context_source:          str | None = None
        self.event_replay_from_position: int     = 0
        self.context_token_count:     int        = 0
        self.agent_id:                str | None = None
        self.application_id:          str | None = None

        # Set from execution events
        self.applications_analysed:   set[str]   = set()
        self.nodes_executed:          list[str]  = []
        self.tools_called:            list[str]  = []
        self.last_event_type:         str | None = None
        self.is_completed_flag:       bool       = False
        self.has_partial_decision:    bool       = False
        self.total_llm_calls:         int        = 0
        self.total_cost_usd:          float      = 0.0

    @property
    def stream_id(self) -> str:
        return f"agent-{self.agent_type}-{self.session_id}"

    def is_new(self) -> bool:
        return self.version == 0

    @classmethod
    async def load(
        cls,
        store: Any,
        agent_type: str,
        session_id: str,
    ) -> "AgentSessionAggregate":
        stream_id = f"agent-{agent_type}-{session_id}"
        events    = await store.load_stream(stream_id)
        agg       = cls(agent_type, session_id)
        for e in events:
            agg._apply(e)
        return agg

    @classmethod
    async def load_from_stream_id(
        cls,
        store: Any,
        stream_id: str,
    ) -> "AgentSessionAggregate":
        """
        Load from a full stream_id string (agent-{agent_type}-{session_id}).
        Used by handle_generate_decision to load contributing sessions for
        causal chain verification without knowing agent_type/session_id separately.
        """
        # stream_id format: agent-{agent_type}-{session_id}
        # agent_type itself may contain hyphens (e.g. credit_analysis, fraud_detection)
        # session_id format: sess-{type}-{hex} so we split on last occurrence of "-sess-"
        if "-sess-" in stream_id:
            prefix, rest = stream_id.split("-sess-", 1)
            agent_type   = prefix.replace("agent-", "", 1)
            session_id   = "sess-" + rest
        else:
            # Fallback: everything after "agent-" prefix split at last hyphen group
            without_prefix = stream_id[len("agent-"):]
            parts          = without_prefix.rsplit("-", 2)
            agent_type     = parts[0] if len(parts) > 1 else without_prefix
            session_id     = "-".join(parts[1:]) if len(parts) > 1 else ""
        events = await store.load_stream(stream_id)
        agg    = cls(agent_type, session_id)
        for e in events:
            agg._apply(e)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        self.version        = event.stream_position
        self.last_event_type = event.event_type
        p = event.payload

        if event.event_type == "AgentSessionStarted":
            self.context_loaded          = True
            self.declared_model_version  = p.get("model_version")
            self.context_source          = p.get("context_source", "fresh")
            self.context_token_count     = p.get("context_token_count", 0)
            self.agent_id                = p.get("agent_id")
            self.application_id          = p.get("application_id")
            # event_replay_from_position only present in Gas Town (event_replay source)
            self.event_replay_from_position = p.get("event_replay_from_position", 0)

        elif event.event_type == "AgentNodeExecuted":
            node_name = p.get("node_name", "")
            self.nodes_executed.append(node_name)
            if p.get("llm_called"):
                self.total_llm_calls += 1
            if p.get("llm_cost_usd"):
                self.total_cost_usd += float(p["llm_cost_usd"])

        elif event.event_type == "AgentToolCalled":
            self.tools_called.append(p.get("tool_name", ""))

        elif event.event_type in (
            "CreditAnalysisCompleted",
            "FraudScreeningCompleted",
            "ComplianceCheckCompleted",
        ):
            app_id = p.get("application_id")
            if app_id:
                self.applications_analysed.add(app_id)

        elif event.event_type == "AgentSessionCompleted":
            self.is_completed_flag = True
            self.total_cost_usd    = float(p.get("total_cost_usd", self.total_cost_usd))

        # Unknown event types silently ignored — forward compatibility

    # ── Business rules ────────────────────────────────────────────────────────

    def assert_context_loaded(self) -> None:
        """
        Gas Town rule: agent must have a loaded context before performing work.
        Raised if the stream has no AgentSessionStarted event.
        """
        if not self.context_loaded:
            raise DomainError(
                rule    = "GAS_TOWN",
                message = (
                    f"Agent {self.agent_type}/{self.session_id} has no loaded context. "
                    "Session must be started before any analysis can be recorded."
                ),
                context = {
                    "agent_type": self.agent_type,
                    "session_id": self.session_id,
                    "suggested_action": "call start_agent_session first",
                },
            )

    def assert_model_version_current(self, deployed_version: str) -> None:
        """
        Model version locking: refuse if the session was started with a different
        model version than the one currently deployed.
        """
        if self.declared_model_version is None:
            return   # no version declared yet — no constraint
        if self.declared_model_version != deployed_version:
            raise DomainError(
                rule    = "MODEL_VERSION_LOCK",
                message = (
                    f"Session declared model_version={self.declared_model_version!r} "
                    f"but deployed version is {deployed_version!r}. "
                    "Re-start the session with the current model version."
                ),
                context = {
                    "session_version":  self.declared_model_version,
                    "deployed_version": deployed_version,
                },
            )