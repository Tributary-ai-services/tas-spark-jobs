"""Schema validation for the AIQG response envelope.

Mirrors the JSON shape emitted by tas-llm-router's MultiEmitter onto
the tas.aiqg.events.v1 Kafka topic. A real production payload (with
secret values masked) is parsed via the schema's field tree to catch
drift between the producer and the aggregator's expectations.
"""

import importlib.util
import json
import os

# Load the AIQG schema module by path. Loading via `sys.path.insert` +
# `import schema` would collide with tests/test_schema.py, which loads
# events_aggregator/schema.py under the same module name and wins
# whichever import happens first.
_aiqg_schema_path = os.path.join(
    os.path.dirname(__file__), '..', 'jobs', 'aiqg_aggregator', 'schema.py'
)
_spec = importlib.util.spec_from_file_location('aiqg_schema', _aiqg_schema_path)
_aiqg_schema = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_aiqg_schema)
AIQG_RESPONSE_ENVELOPE = _aiqg_schema.AIQG_RESPONSE_ENVELOPE
RESPONSE_DATA = _aiqg_schema.RESPONSE_DATA


def _field(struct, name):
    for f in struct.fields:
        if f.name == name:
            return f
    raise AssertionError(f"{name} not in {[f.name for f in struct.fields]}")


def test_envelope_required_ce_fields():
    for name in ('specversion', 'type', 'data'):
        _field(AIQG_RESPONSE_ENVELOPE, name)


def test_response_data_has_pk_fields():
    """time + tenant_id + response_event_id form the hypertable PK."""
    for name in ('tenant_id', 'response_event_id'):
        _field(RESPONSE_DATA, name)


def test_response_data_has_clear_scores():
    scores = _field(RESPONSE_DATA, 'clear_scores').dataType
    for name in ('composite', 'cost', 'latency', 'efficacy', 'assurance', 'reliability'):
        _field(scores, name)


def test_response_data_has_token_accounting():
    ta = _field(RESPONSE_DATA, 'token_accounting').dataType
    for name in ('prompt_tokens', 'completion_tokens', 'total_tokens', 'total_cost_usd'):
        _field(ta, name)


def test_token_accounting_has_cost_decomposition():
    """CLEAR v0.2 decomposition fields (Contract v1, AIQG-3).

    These land in dedicated aiqg.event_metrics columns via migration 002.
    A producer rename here silently NULLs those columns -- Spark's
    from_json() returns NULL for an absent field rather than failing --
    so the field names are pinned.
    """
    ta = _field(RESPONSE_DATA, 'token_accounting').dataType
    for name in ('reduction_mode',
                 'projected_direct_payload_waste_usd',
                 'projected_reduction_relevance_usd',
                 'projected_reduction_slm_usd',
                 'projected_reduction_combined_usd',
                 'induced_output_waste_estimated_usd',
                 'genuine_post_model_waste_usd',
                 'context_efficiency_ratio'):
        _field(ta, name)


def test_production_decomposed_token_accounting_is_typed():
    """A real decomposed token_accounting block from aiqg.event_metrics.

    Captured 2026-09-28 from the live hypertable (the most recent row
    carrying a non-zero genuine_post_model_waste_usd). Every key here that
    migration 002 gives a column must be typed in TOKEN_ACCOUNTING, or the
    aggregator writes NULL into it forever -- Spark's from_json() returns
    NULL for an absent field instead of failing, so this is exactly the
    class of drift that fails silently.

    Note what is ABSENT and why: projected_direct_payload_waste_usd and
    induced_output_waste_estimated_usd are `omitempty` on a float64, so a
    zero is omitted rather than sent. That is the reason the columns are
    nullable rather than NOT NULL DEFAULT 0.
    """
    block = {
        "total_tokens": 14008,
        "prompt_tokens": 14008,
        "input_cost_usd": 0.007004,
        "reduction_mode": "projected",
        "total_cost_usd": 0.007004,
        "actual_cost_usd": 0.007004,
        "output_cost_usd": 0,
        "completion_tokens": 0,
        "actual_cost_source": "vendor_usage",
        "model_pricing_version": "pricing-v2026-06-05",
        "context_efficiency_ratio": 0,
        "projected_reduction_slm_usd": 0.001751,
        "genuine_post_model_waste_usd": 0.007004,
        "projected_reduction_combined_usd": 0.007004,
        "projected_reduction_relevance_usd": 0.007004,
        "projected_reduction_slm_confidence": "low",
        "projected_reduction_relevance_confidence": "medium",
    }
    ta = _field(RESPONSE_DATA, 'token_accounting').dataType
    typed = {f.name for f in ta.fields}
    stored = {
        "reduction_mode", "context_efficiency_ratio",
        "projected_reduction_slm_usd", "genuine_post_model_waste_usd",
        "projected_reduction_combined_usd", "projected_reduction_relevance_usd",
    }
    for name in stored:
        assert name in block, f"fixture lost {name}; recapture it from the hypertable"
        assert name in typed, f"production emits token_accounting.{name} but the schema drops it"

    # The bound invariant migration 002 enforces, on this exact row:
    #   direct + induced + genuine <= total_cost_usd.
    # It sits exactly ON the bound, which is why the CHECK carries an epsilon.
    total = (block.get("projected_direct_payload_waste_usd", 0.0)
             + block.get("induced_output_waste_estimated_usd", 0.0)
             + block.get("genuine_post_model_waste_usd", 0.0))
    assert total <= block["total_cost_usd"] + 1e-9


def test_response_data_has_agent_context():
    """Identity attribution feeds the per-agent / per-flow rollups."""
    ac = _field(RESPONSE_DATA, 'agent_context').dataType
    for name in ('agent_id', 'agent_name', 'user_id', 'conversation_id',
                 'flow_id', 'principal_id', 'client_ip', 'identity_source'):
        _field(ac, name)


def test_response_data_has_classification_drift():
    """Axis-1 workflow drift fields (Plan #8) on the response data."""
    for name in ('workflow_declared', 'workflow_declared_op', 'workflow_inferred',
                 'workflow_drift', 'otel_map_version'):
        _field(RESPONSE_DATA, name)


def test_agent_context_has_attribution_drift():
    """Axis-1 attribution drift fields (Plan #8) on agent_context."""
    ac = _field(RESPONSE_DATA, 'agent_context').dataType
    for name in ('agent_declared', 'agent_inferred', 'agent_drift', 'drift_source'):
        _field(ac, name)


def test_production_payload_matches_schema_shape():
    """A captured envelope from tas.aiqg.events.v1 should parse cleanly.

    If this fails, the emitter shape has diverged from the schema and
    the aggregator will start dropping events. Update the schema in
    the SAME change that updates the emitter.
    """
    sample = {
        "specversion": "1.0",
        "type": "com.tas.aiqg.response.v1",
        "source": "urn:tas:service:tas-llm-router",
        "id": "b4050cea-ff07-4631-9e30-37609924a3f6",
        "time": "2026-06-08T18:22:23.058269197Z",
        "datacontenttype": "application/json",
        "data": {
            "response_event_id": "b4050cea-ff07-4631-9e30-37609924a3f6",
            "request_event_id": "0abe0687-af19-494d-99e8-1826d59c7ee9",
            "tenant_id": "a689c0b2-02ca-46d1-9916-f9a30c00222a",
            "aiqg_account_id": "bb27246b-7898-4936-a789-ff82cfa7308c",
            "status": "success",
            "http_status": 200,
            "finish_reason": "length",
            "event_timestamps": {
                "request_received_at": "2026-06-08T18:22:21.022606019Z",
                "response_complete_at": "2026-06-08T18:22:23.058269197Z",
                "end_to_end_ms": 2035,
            },
            "token_accounting": {
                "prompt_tokens": 9,
                "completion_tokens": 5,
                "total_tokens": 14,
                "input_cost_usd": 0.00000135,
                "output_cost_usd": 0.000003,
                "total_cost_usd": 0.0000043499,
            },
            "assurance": {
                "inbound_count": 0,
                "outbound_count": 0,
            },
            "clear_scores": {
                "composite": 92,
                "cost": 100,
                "latency": 100,
                "efficacy": 60,
                "assurance": 100,
                "reliability": 100,
            },
            "agent_context": {
                "agent_id": "agent-7",
                "agent_name": "Coder",
                "user_id": "u_42",
                "flow_id": "f_12",
                "identity_source": "asserted",
            },
        },
    }
    envelope_fields = {f.name for f in AIQG_RESPONSE_ENVELOPE.fields}
    for k in sample:
        assert k in envelope_fields, f"production field '{k}' missing from envelope schema"
    data_fields = {f.name for f in RESPONSE_DATA.fields}
    for k in sample["data"]:
        assert k in data_fields, f"production data.{k} missing from RESPONSE_DATA schema"
    # round-trip JSON serialization sanity
    assert json.loads(json.dumps(sample))["data"]["clear_scores"]["composite"] == 92
