"""
tests/unit/test_agent_session.py

Unit tests for AgentSessionAggregate.
No database. No async. Pure Python.
"""
from __future__ import annotations

import pytest

from src.aggregates.agent_session import AgentSessionAggregate
from src.models.events import DomainError
from tests.unit.conftest import stored, utcnow


AGENT_TYPE = "credit_analysis"
SESSION_ID = "sess-cre-unit-001"
STREAM     = f"agent-{AGENT_TYPE}-{SESSION_ID}"
APP_ID     = "APEX-UNIT-001"


def build(events) -> AgentSessionAggregate:
    agg = AgentSessionAggregate(AGENT_TYPE, SESSION_ID)
    for e in events:
        agg._apply(e)
    return agg


def make_started(model_version: str = "claude-sonnet-4-20250514", pos: int = 1):
    from src.models.events import AgentSessionStarted
    return stored(
        AgentSessionStarted(
            session_id          = SESSION_ID,
            agent_type          = AGENT_TYPE,
            agent_id            = "credit-agent-1",
            application_id      = APP_ID,
            model_version       = model_version,
            context_source      = "fresh",
            context_token_count = 1500,
            started_at          = utcnow(),
        ),
        STREAM, pos,
    )


def make_node(node_name: str = "validate_inputs", pos: int = 2):
    from src.models.events import AgentNodeExecuted
    return stored(
        AgentNodeExecuted(
            session_id    = SESSION_ID,
            agent_type    = AGENT_TYPE,
            node_name     = node_name,
            node_sequence = pos - 1,
            input_keys    = ["application_id"],
            output_keys   = ["validated"],
            llm_called    = False,
            duration_ms   = 100,
            executed_at   = utcnow(),
        ),
        STREAM, pos,
    )


def make_tool(tool_name: str = "week3_extraction_pipeline", pos: int = 2):
    from src.models.events import AgentToolCalled
    return stored(
        AgentToolCalled(
            session_id           = SESSION_ID,
            agent_type           = AGENT_TYPE,
            tool_name            = tool_name,
            tool_input_summary   = "test input",
            tool_output_summary  = "test output",
            tool_duration_ms     = 500,
            called_at            = utcnow(),
        ),
        STREAM, pos,
    )


def make_credit_completed(app_id: str = APP_ID, pos: int = 3):
    from src.models.events import CreditAnalysisCompleted
    return stored(
        CreditAnalysisCompleted(
            application_id       = app_id,
            session_id           = SESSION_ID,
            decision             = {"risk_tier": "MEDIUM", "recommended_limit_usd": "450000", "confidence": 0.8},
            model_version        = "claude-sonnet-4-20250514",
            input_data_hash      = "abc123",
            analysis_duration_ms = 1200,
            completed_at         = utcnow(),
        ),
        STREAM, pos,
    )


# =============================================================================
# Gas Town pattern
# =============================================================================

class TestGasTownPattern:

    def test_no_session_started_context_not_loaded(self):
        agg = build([])
        assert agg.context_loaded is False

    def test_session_started_loads_context(self):
        agg = build([make_started()])
        assert agg.context_loaded is True

    def test_assert_context_loaded_raises_without_session(self):
        agg = build([])
        with pytest.raises(DomainError) as exc:
            agg.assert_context_loaded()
        assert exc.value.rule == "GAS_TOWN"

    def test_assert_context_loaded_passes_after_start(self):
        agg = build([make_started()])
        agg.assert_context_loaded()  # no exception

    def test_model_version_stored_from_session(self):
        agg = build([make_started(model_version="claude-sonnet-4-20250514")])
        assert agg.declared_model_version == "claude-sonnet-4-20250514"

    def test_context_source_stored(self):
        agg = build([make_started()])
        assert agg.context_source == "fresh"


# =============================================================================
# Model version locking
# =============================================================================

class TestModelVersionLocking:

    def test_matching_version_passes(self):
        agg = build([make_started(model_version="claude-sonnet-4-20250514")])
        agg.assert_model_version_current("claude-sonnet-4-20250514")

    def test_mismatched_version_raises(self):
        agg = build([make_started(model_version="claude-sonnet-4-20250514")])
        with pytest.raises(DomainError) as exc:
            agg.assert_model_version_current("claude-opus-4-20250514")
        assert exc.value.rule == "MODEL_VERSION_LOCK"
        assert "claude-opus-4-20250514" in exc.value.message
        assert "claude-sonnet-4-20250514" in exc.value.message

    def test_no_declared_version_skips_check(self):
        agg = build([])
        agg.assert_model_version_current("any-version")  # no exception

    def test_seed_model_version_accepted(self):
        """The model_version in seed data is 'claude-sonnet-4-20250514'."""
        agg = build([make_started(model_version="claude-sonnet-4-20250514")])
        agg.assert_model_version_current("claude-sonnet-4-20250514")


# =============================================================================
# Application tracking
# =============================================================================

class TestApplicationTracking:

    def test_credit_completed_tracked(self):
        agg = build([make_started(), make_credit_completed(app_id="APEX-0016")])
        assert "APEX-0016" in agg.applications_analysed

    def test_multiple_applications_tracked(self):
        agg = build([
            make_started(),
            make_credit_completed(app_id="APEX-0016", pos=2),
            make_credit_completed(app_id="APEX-0017", pos=3),
        ])
        assert "APEX-0016" in agg.applications_analysed
        assert "APEX-0017" in agg.applications_analysed

    def test_nodes_tracked(self):
        agg = build([make_started(), make_node("validate_inputs", 2), make_node("run_extraction", 3)])
        assert "validate_inputs" in agg.nodes_executed
        assert "run_extraction"  in agg.nodes_executed

    def test_tools_tracked(self):
        agg = build([make_started(), make_tool("week3_extraction_pipeline", 2)])
        assert "week3_extraction_pipeline" in agg.tools_called


# =============================================================================
# State reconstruction
# =============================================================================

class TestStateReconstruction:

    def test_version_tracks_last_position(self):
        agg = build([make_started(pos=1), make_node(pos=2)])
        assert agg.version == 2

    def test_stream_id_property(self):
        agg = AgentSessionAggregate(AGENT_TYPE, SESSION_ID)
        assert agg.stream_id == f"agent-{AGENT_TYPE}-{SESSION_ID}"

    def test_is_new_before_events(self):
        assert AgentSessionAggregate(AGENT_TYPE, SESSION_ID).is_new() is True

    def test_not_new_after_session_started(self):
        agg = build([make_started()])
        assert agg.is_new() is False

    def test_last_event_type_tracked(self):
        agg = build([make_started(), make_node("validate_inputs")])
        assert agg.last_event_type == "AgentNodeExecuted"
