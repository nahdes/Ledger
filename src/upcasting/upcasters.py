"""
src/upcasting/upcasters.py

Concrete upcasters for known schema migrations.

RULE: Never fabricate values that were not measured.
  confidence_score for historical v1 CreditAnalysisCompleted → None (not 0.0)
  model_version for historical v1 → "legacy-pre-2026" (explicit sentinel)

Import this module to register upcasters into default_registry.
"""
from src.upcasting.registry import default_registry


@default_registry.register("CreditAnalysisCompleted", from_version=1)
def credit_analysis_v1_to_v2(payload: dict) -> dict:
    """
    CreditAnalysisCompleted v1 → v2

    v1 schema (pre-2026):
        risk_tier, recommended_limit_usd, analysis_duration_ms, input_data_hash
        (no model_version, no confidence, no regulatory_basis)

    v2 schema (seed data):
        + model_version       — sentinel for historical events
        + confidence          — None (was not captured in v1)
        + regulatory_basis    — empty list
        + decision            — wraps the flat fields
    """
    return {
        **payload,
        "model_version":    payload.get("model_version", "legacy-pre-2026"),
        "regulatory_basis": payload.get("regulatory_basis", []),
        # confidence is None for historical events — never fabricate a value
        "decision": {
            "risk_tier":             payload.get("risk_tier"),
            "recommended_limit_usd": payload.get("recommended_limit_usd"),
            "confidence":            None,
            "rationale":             payload.get("rationale"),
            "key_concerns":          payload.get("key_concerns", []),
            "data_quality_caveats":  [],
            "policy_overrides_applied": [],
        },
    }


@default_registry.register("DecisionGenerated", from_version=1)
def decision_generated_v1_to_v2(payload: dict) -> dict:
    """
    DecisionGenerated v1 → v2

    v1: flat recommendation / approved_amount fields
    v2: adds contributing_sessions, model_versions, executive_summary
    """
    return {
        **payload,
        "contributing_sessions": payload.get("contributing_sessions", []),
        "model_versions":        payload.get("model_versions", {}),
        "executive_summary":     payload.get("executive_summary"),
    }
