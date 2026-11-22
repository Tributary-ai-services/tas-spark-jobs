"""Tool-call events: com.tas.aiqg.tool_call.v1 -> aiqg.tool_call_events.

Plan #22 T.7 (design org-structure-and-attribution.md §9.5). Two sources emit
the same event type on tas.aiqg.events.v1: the MCP proxy (tas-mcp,
observed_by="proxy") and the LLM gateway (tas-llm-router,
observed_by="gateway"). A call that went through both is stored as ONE row.

Join strategy: merged at WRITE time, in the same transaction as the insert.
-------------------------------------------------------------------------
Why not at read time: aiqg.tool_call_1h is a continuous aggregate, and a
continuous aggregate cannot de-duplicate two raw rows into one. Two rows per
call would double every count the Tools page shows.

Why not ON CONFLICT: aiqg.tool_call_events is a hypertable, and every unique
index on a hypertable must include its partition column (occurred_at). The
proxy and the gateway stamp different times for the same call, so a UNIQUE
(tenant_id, tool_call_id) is not expressible.

So each batch, for incoming rows that carry a tool_call_id:
  1. find stored rows with the same (tenant_id, tool_call_id) within
     MERGE_WINDOW of the incoming time (index tool_call_merge_idx);
  2. merge every sighting of the call into one row: for each column the
     proxy's value wins and the gateway's fills a gap; observed_by becomes
     'both' when both sources are present;
  3. delete the stored partner rows and insert the merged row.
Rows without a tool_call_id are inserted as they are and never merged.

Rules worth knowing:
  * outcome: proxy wins, EXCEPT that a gateway no_result overrides a proxy ok.
    The proxy delivered the result; only the gateway can see that the result
    never reached the model (§9.5 "the gateway is the only source that can see
    it"). A proxy failure (tool_error, timeout, ...) is kept, since it is the
    reason the result never came back.
  * Ambiguity guard: a group with more than one distinct proxy event, or more
    than one distinct gateway event, is NOT merged. Some clients reuse
    tool_call ids (local models emit "call_0" every turn), and merging those
    would lose calls. They are stored unmerged instead.
  * Idempotent under at-least-once delivery: a re-delivered sighting merges
    into the stored row and produces the same row (the stored 'both' row keeps
    the gateway's event id in gateway_event_id, so it still counts as one
    gateway sighting).
  * A merge that lands more than 4 hours after the call is in raw but not in
    the hourly rollup until that window is refreshed by hand (the policy
    reaches back 4 hours; see migration 009).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from schema import AIQG_TOOL_CALL_ENVELOPE

TOOL_CALL_TYPE = "com.tas.aiqg.tool_call.v1"

# Tenant of a proxy call made without a valid token (operator-only view).
UNATTRIBUTED = "(unattributed)"
OBSERVED_BY = ("proxy", "gateway")
OUTCOMES = ("ok", "tool_error", "timeout", "unreachable", "blocked", "no_result")

# Canonical UUID text, case-insensitive (used with ~*). tool_call_events.token_id
# is UUID; anything else is stored NULL instead of failing the batch (same rule
# as event_metrics, tas-spark-jobs#11).
UUID_RE = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

# How far apart the proxy's and the gateway's timestamps for one call may be.
# The gateway sees the result when the next request echoes the tool_call_id,
# which for a human-in-the-loop agent can be many minutes later.
MERGE_WINDOW = "1 day"

# Every column of aiqg.tool_call_events the writer sets, in one place, so the
# staging projection, the merge and the inserts cannot drift apart.
COLUMNS = (
    "occurred_at", "tenant_id", "event_id", "gateway_event_id",
    "token_id", "owner_sub", "user_id", "agent_id", "source_app",
    "flow_id", "step_id", "tool_call_id", "tool_name", "server_id",
    "observed_by", "outcome",
    "duration_ms", "round_trip_ms", "result_bytes", "result_tokens_est",
    "reduced_bytes", "findings", "scan_skipped",
)

# Columns merged by "first non-NULL value, proxy first". The others are
# computed specially in merge_statements().
_PLAIN = tuple(c for c in COLUMNS if c not in (
    "occurred_at", "tenant_id", "event_id", "gateway_event_id",
    "tool_call_id", "observed_by", "outcome",
))


def parse_tool_call_envelopes(kafka_df: DataFrame) -> DataFrame:
    """Project com.tas.aiqg.tool_call.v1 envelopes to tool_call_events columns.

    Dropped, like a response event without its PK: no tool_name, no CloudEvent
    id, or an observed_by / outcome outside the contract (the table's CHECK
    constraints would otherwise fail the whole batch), and a gateway event with
    no tenant_id (the gateway always authenticates first).

    A proxy event with no tenant_id is a call made without a valid TAS-Auth
    token. The proxy serves those (observe mode, design §9.5), and they are
    kept under the tenant UNATTRIBUTED for the operator-only view, not
    dropped: an untokened caller is exactly what that view exists to show.
    """
    return (
        kafka_df
        .select(
            F.col("timestamp").alias("kafka_ts"),
            F.from_json(F.col("value").cast("string"), AIQG_TOOL_CALL_ENVELOPE).alias("ce"),
            F.col("value").cast("string").alias("raw_json"),
        )
        .filter(F.col("ce.specversion") == "1.0")
        .filter(F.col("ce.type") == TOOL_CALL_TYPE)
        .filter(F.col("ce.id").isNotNull() & (F.col("ce.id") != ""))
        .withColumn("_tenant", F.when((F.col("ce.data.tenant_id").isNull()) | (F.col("ce.data.tenant_id") == ""),
                                      F.when(F.col("ce.data.observed_by") == "proxy", F.lit(UNATTRIBUTED)))
                                .otherwise(F.col("ce.data.tenant_id")))
        .filter(F.col("_tenant").isNotNull())
        .filter(F.col("ce.data.tool_name").isNotNull() & (F.col("ce.data.tool_name") != ""))
        .filter(F.col("ce.data.observed_by").isin(*OBSERVED_BY))
        .filter(F.col("ce.data.outcome").isin(*OUTCOMES))
        .select(
            F.coalesce(F.col("ce.data.occurred_at"), F.col("ce.time"), F.col("kafka_ts"))
             .alias("occurred_at"),
            F.col("_tenant").alias("tenant_id"),
            F.col("ce.id").alias("event_id"),
            F.col("ce.data.token_id").alias("token_id"),
            F.col("ce.data.owner_sub").alias("owner_sub"),
            F.col("ce.data.user_id").alias("user_id"),
            F.col("ce.data.agent_id").alias("agent_id"),
            F.col("ce.data.source_app").alias("source_app"),
            F.col("ce.data.flow_id").alias("flow_id"),
            F.col("ce.data.step_id").alias("step_id"),
            F.col("ce.data.tool_call_id").alias("tool_call_id"),
            F.col("ce.data.tool_name").alias("tool_name"),
            F.col("ce.data.server_id").alias("server_id"),
            F.col("ce.data.observed_by").alias("observed_by"),
            F.col("ce.data.outcome").alias("outcome"),
            F.col("ce.data.duration_ms").cast("int").alias("duration_ms"),
            F.col("ce.data.round_trip_ms").cast("int").alias("round_trip_ms"),
            F.col("ce.data.result_bytes").alias("result_bytes"),
            F.col("ce.data.result_tokens_est").cast("int").alias("result_tokens_est"),
            F.col("ce.data.reduced_bytes").alias("reduced_bytes"),
            # {"<category>": count} has arbitrary keys; carried as JSON text
            # and cast to JSONB in SQL.
            F.get_json_object(F.col("raw_json"), "$.data.findings").alias("findings"),
            F.col("ce.data.scan_skipped").alias("scan_skipped"),
        )
        .withWatermark("occurred_at", "2 minutes")
    )


def _cols(prefix: str = "") -> str:
    return ", ".join(prefix + c for c in COLUMNS)


def merge_statements(staging_table: str) -> list:
    """SQL, in order, that moves one staged batch into aiqg.tool_call_events.

    Run in one transaction. The staging table has the parse_tool_call_envelopes
    columns, all as JDBC wrote them (token_id and findings as text).
    """
    first = ",\n            ".join(
        f"(array_agg({c} ORDER BY src_rank, occurred_at, event_id) "
        f"FILTER (WHERE {c} IS NOT NULL))[1] AS {c}"
        for c in _PLAIN
    )
    return [
        # 1. Normalise the batch: typed, UUID-guarded, blanks to NULL. A
        #    gateway sighting carries its own id in gateway_event_id, so after
        #    a merge the stored row still remembers which gateway event it holds.
        f"""
        CREATE TEMP TABLE tc_incoming ON COMMIT DROP AS
        SELECT occurred_at, tenant_id, event_id,
               CASE WHEN observed_by = 'gateway' THEN event_id END AS gateway_event_id,
               CASE WHEN token_id ~* '{UUID_RE}' THEN token_id::uuid END AS token_id,
               NULLIF(owner_sub, '') AS owner_sub,
               NULLIF(user_id, '') AS user_id,
               NULLIF(agent_id, '') AS agent_id,
               NULLIF(source_app, '') AS source_app,
               NULLIF(flow_id, '') AS flow_id,
               NULLIF(step_id, '') AS step_id,
               NULLIF(tool_call_id, '') AS tool_call_id,
               tool_name,
               NULLIF(server_id, '') AS server_id,
               observed_by, outcome,
               duration_ms, round_trip_ms, result_bytes, result_tokens_est, reduced_bytes,
               CASE WHEN findings IS NOT NULL AND findings <> ''
                         AND jsonb_typeof(findings::jsonb) = 'object'
                    THEN findings::jsonb END AS findings,
               scan_skipped
        FROM {staging_table}
        """,
        # 2. Stored sightings of the same calls. The outer bounds let
        #    TimescaleDB skip chunks; the EXISTS applies the per-row window.
        f"""
        CREATE TEMP TABLE tc_partners ON COMMIT DROP AS
        SELECT {_cols('e.')}
        FROM aiqg.tool_call_events e
        WHERE e.tool_call_id IS NOT NULL
          AND e.occurred_at >= (SELECT min(occurred_at) FROM tc_incoming) - INTERVAL '{MERGE_WINDOW}'
          AND e.occurred_at <= (SELECT max(occurred_at) FROM tc_incoming) + INTERVAL '{MERGE_WINDOW}'
          AND EXISTS (
              SELECT 1 FROM tc_incoming i
              WHERE i.tenant_id = e.tenant_id AND i.tool_call_id = e.tool_call_id
                AND e.occurred_at BETWEEN i.occurred_at - INTERVAL '{MERGE_WINDOW}'
                                      AND i.occurred_at + INTERVAL '{MERGE_WINDOW}')
        """,
        # 3. Every sighting of each keyed call, ranked proxy-first.
        f"""
        CREATE TEMP TABLE tc_sightings ON COMMIT DROP AS
        SELECT u.*,
               CASE WHEN observed_by IN ('proxy', 'both') THEN 0 ELSE 1 END AS src_rank,
               CASE WHEN observed_by IN ('proxy', 'both') THEN event_id END AS proxy_id,
               CASE WHEN observed_by = 'gateway' THEN event_id
                    WHEN observed_by = 'both' THEN gateway_event_id END AS gw_id
        FROM (
            SELECT {_cols()}, true AS stored FROM tc_partners
            UNION ALL
            SELECT {_cols()}, false AS stored FROM tc_incoming WHERE tool_call_id IS NOT NULL
        ) u
        """,
        # 4. Groups that are one call seen by at most one proxy event and at
        #    most one gateway event. Anything else is a reused id: not merged.
        """
        CREATE TEMP TABLE tc_groups ON COMMIT DROP AS
        SELECT tenant_id, tool_call_id
        FROM tc_sightings
        GROUP BY tenant_id, tool_call_id
        HAVING count(DISTINCT proxy_id) <= 1 AND count(DISTINCT gw_id) <= 1
        """,
        # 5. One merged row per group.
        f"""
        CREATE TEMP TABLE tc_merged ON COMMIT DROP AS
        SELECT
            (array_agg(s.occurred_at ORDER BY src_rank, occurred_at, event_id))[1] AS occurred_at,
            s.tenant_id,
            (array_agg(s.event_id ORDER BY src_rank, occurred_at, event_id))[1] AS event_id,
            min(s.gw_id) AS gateway_event_id,
            {first},
            s.tool_call_id,
            CASE WHEN bool_or(observed_by IN ('proxy', 'both'))
                      AND bool_or(observed_by IN ('gateway', 'both')) THEN 'both'
                 WHEN bool_or(observed_by IN ('proxy', 'both')) THEN 'proxy'
                 ELSE 'gateway' END AS observed_by,
            CASE WHEN bool_or(s.outcome = 'no_result')
                      AND coalesce((array_agg(s.outcome ORDER BY src_rank, occurred_at, event_id)
                                    FILTER (WHERE s.outcome <> 'no_result'))[1], 'ok') = 'ok'
                 THEN 'no_result'
                 ELSE (array_agg(s.outcome ORDER BY src_rank, occurred_at, event_id)
                       FILTER (WHERE s.outcome <> 'no_result'))[1] END AS outcome
        FROM tc_sightings s
        JOIN tc_groups g USING (tenant_id, tool_call_id)
        GROUP BY s.tenant_id, s.tool_call_id
        """,
        # 6. Replace the stored partners of merged groups with the merged row.
        """
        DELETE FROM aiqg.tool_call_events e
        USING tc_partners p, tc_groups g
        WHERE p.tenant_id = g.tenant_id AND p.tool_call_id = g.tool_call_id
          AND e.occurred_at = p.occurred_at AND e.tenant_id = p.tenant_id
          AND e.event_id = p.event_id
        """,
        f"""
        INSERT INTO aiqg.tool_call_events ({_cols()})
        SELECT {_cols()} FROM tc_merged
        ON CONFLICT (occurred_at, tenant_id, event_id) DO NOTHING
        """,
        # 7. Everything else as it came: no tool_call_id, or an ambiguous
        #    group. ON CONFLICT DO NOTHING makes a re-delivery a no-op.
        f"""
        INSERT INTO aiqg.tool_call_events ({_cols()})
        SELECT {_cols('i.')} FROM tc_incoming i
        WHERE i.tool_call_id IS NULL
           OR NOT EXISTS (SELECT 1 FROM tc_groups g
                          WHERE g.tenant_id = i.tenant_id AND g.tool_call_id = i.tool_call_id)
        ON CONFLICT (occurred_at, tenant_id, event_id) DO NOTHING
        """,
    ]


def apply_batch(conn, staging_table: str) -> None:
    """Merge a staged batch into aiqg.tool_call_events in one transaction."""
    with conn.cursor() as cur:
        for stmt in merge_statements(staging_table):
            cur.execute(stmt)
        cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
    conn.commit()
