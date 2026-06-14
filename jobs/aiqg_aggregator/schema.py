"""AIQG response envelope schema for Spark Structured Streaming.

Matches the CloudEvents 1.0 envelope emitted by tas-llm-router's
events.MultiEmitter for type=com.tas.aiqg.response.v1 messages on
the tas.aiqg.events.v1 topic.

Only the fields the aggregator extracts into aiqg.event_metrics are
typed here. Unknown fields stay in the raw JSON for replay.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT_TIMESTAMPS = StructType([
    StructField("request_received_at", TimestampType(), True),
    StructField("response_complete_at", TimestampType(), True),
    StructField("end_to_end_ms", LongType(), True),
])

TOKEN_ACCOUNTING = StructType([
    StructField("prompt_tokens", IntegerType(), True),
    StructField("completion_tokens", IntegerType(), True),
    StructField("total_tokens", IntegerType(), True),
    StructField("input_cost_usd", DoubleType(), True),
    StructField("output_cost_usd", DoubleType(), True),
    StructField("total_cost_usd", DoubleType(), True),
])

ASSURANCE = StructType([
    StructField("inbound_count", IntegerType(), True),
    StructField("outbound_count", IntegerType(), True),
    # Per-NIST AI RMF characteristic finding counts. Populated when
    # the response carries assurance findings (PR #53 in tas-llm-router).
    StructField("nist_secure_resilient", IntegerType(), True),
    StructField("nist_privacy_enhanced", IntegerType(), True),
    StructField("nist_valid_reliable", IntegerType(), True),
    StructField("nist_safe", IntegerType(), True),
])

CLEAR_SCORES = StructType([
    StructField("composite", IntegerType(), True),
    StructField("cost", IntegerType(), True),
    StructField("latency", IntegerType(), True),
    StructField("efficacy", IntegerType(), True),
    StructField("assurance", IntegerType(), True),
    StructField("reliability", IntegerType(), True),
])

# Self-asserted identity attribution (AgentContext in tas-llm-router's
# event.go). Present on the response envelope when the request carried
# baggage / TAS-Agent-* / traceparent headers or an AIQG token; the whole
# struct is NULL when unattributed. Feeds the per-agent / per-flow rollups
# behind /metrics/agents and /flows.
AGENT_CONTEXT = StructType([
    StructField("agent_id", StringType(), True),
    StructField("agent_name", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("conversation_id", StringType(), True),
    StructField("flow_id", StringType(), True),
    StructField("principal_id", StringType(), True),
    StructField("client_ip", StringType(), True),
    StructField("identity_source", StringType(), True),
    # linked-tier flow/step topology (tool_call_id echo reconstruction).
    StructField("step_id", StringType(), True),
    StructField("parent_step_id", StringType(), True),
    StructField("flow_step_seq", IntegerType(), True),
])

# The data payload of a com.tas.aiqg.response.v1 CloudEvent.
RESPONSE_DATA = StructType([
    StructField("response_event_id", StringType(), True),
    StructField("request_event_id", StringType(), True),
    StructField("tenant_id", StringType(), True),
    StructField("aiqg_account_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("http_status", IntegerType(), True),
    StructField("finish_reason", StringType(), True),
    # vendor/model/workflow are NOT on the response envelope today —
    # they live on the paired request envelope. Future enrichment in
    # the emitter will copy them across; until then these stay NULL.
    StructField("vendor", StringType(), True),
    StructField("model", StringType(), True),
    StructField("workflow", StringType(), True),
    StructField("source_app", StringType(), True),
    # Experiments runner (Phase D) — the experiment + variant that claimed
    # this request; powers the per-variant /results rollup.
    StructField("experiment_id", StringType(), True),
    StructField("experiment_variant", StringType(), True),
    StructField("event_timestamps", EVENT_TIMESTAMPS, True),
    StructField("token_accounting", TOKEN_ACCOUNTING, True),
    StructField("assurance", ASSURANCE, True),
    StructField("clear_scores", CLEAR_SCORES, True),
    StructField("agent_context", AGENT_CONTEXT, True),
])

# Outer CloudEvents 1.0 envelope. We only use the type + time + the
# nested data block, but the schema as a whole has to match the JSON
# Spark sees for from_json() to succeed.
AIQG_RESPONSE_ENVELOPE = StructType([
    StructField("specversion", StringType(), False),
    StructField("type", StringType(), False),
    StructField("source", StringType(), True),
    StructField("id", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("datacontenttype", StringType(), True),
    StructField("data", RESPONSE_DATA, True),
])
