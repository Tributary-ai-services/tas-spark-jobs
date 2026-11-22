"""Tool catalogue snapshots -> aiqg.tool_catalogue (Plan #22 T.6/T.7).

The "never called" report (design org-structure-and-attribution.md §9.5) needs
to know what was OFFERED, not only what was called. Two sources on
tas.aiqg.events.v1 say so; both become rows of
(snapshot_at, server_id, tool_name, schema_bytes, observed_by, tenant_id):

  * the MCP proxy (tas-mcp) emits com.tas.aiqg.tool_catalogue.v1, one event per
    server per hour carrying its tools/list:
        {"snapshot_at": <RFC 3339 hour>, "server_id": "<id>",
         "observed_by": "proxy", "tenant_id": null,
         "tools": [{"name": ..., "schema_bytes": <int>}]}
    -> one row per tool. tenant_id is NULL: the federation is shared.
  * the LLM gateway (tas-llm-router#284) adds tools_offered
    [{"name", "schema_bytes"}] to its com.tas.aiqg.response.v1 event, once per
    toolset per tenant per hour per replica
    -> one row per tool, snapshot_at = the event's hour, server_id NULL (the
       model's tool list does not say which server serves a tool),
       observed_by 'gateway', tenant_id from the event.

Dedupe: INSERT ... ON CONFLICT DO NOTHING on the snapshot key
(snapshot_at, server_id, tool_name, observed_by, tenant_id), which migration
009 declares NULLS NOT DISTINCT. Kafka re-delivery, a retried micro-batch and
the same toolset reported by several gateway replicas in one hour all collapse
to one row. The first write of a key wins, so a later schema_bytes for the same
tool in the same hour is not recorded; the next hour's snapshot has it.

snapshot_at is truncated to the UTC hour in SQL for both sources. That is a
no-op on a conforming proxy event and is what makes the gateway's rows
snapshots at all (its time is the response's). It is done in Postgres with an
explicit 'UTC', not in Spark, so it does not depend on the JVM's time zone.

Malformed input is dropped or NULLed, never fatal (a failed batch stops the
whole job, see main.py):
  * dropped: no tool name; observed_by outside ('proxy', 'gateway'); a proxy
    event with no server_id (a proxy row is per server by definition); a
    gateway event with no tenant_id (event_metrics drops those too); a
    non-array tools / tools_offered (from_json yields NULL, nothing explodes);
    a NULL array element.
  * NULLed: schema_bytes that is negative, over INTEGER range, or not a number;
    empty-string tenant_id.
  * snapshot_at that is missing or unparsable falls back to the CloudEvent
    time, then the Kafka timestamp (the same rule as tool_call occurred_at).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from schema import AIQG_TOOL_CATALOGUE_ENVELOPE

TOOL_CATALOGUE_TYPE = "com.tas.aiqg.tool_catalogue.v1"
RESPONSE_TYPE = "com.tas.aiqg.response.v1"
OBSERVED_BY = ("proxy", "gateway")

# Every column of aiqg.tool_catalogue the writer sets (all of them).
COLUMNS = ("snapshot_at", "server_id", "tool_name", "schema_bytes", "observed_by", "tenant_id")
SNAPSHOT_KEY = ("snapshot_at", "server_id", "tool_name", "observed_by", "tenant_id")
INT_MAX = 2147483647


def _blank_to_null(c):
    return F.when(F.trim(c) == "", F.lit(None)).otherwise(c)


def parse_tool_catalogue_envelopes(kafka_df: DataFrame) -> DataFrame:
    """Project catalogue events and gateway tools_offered to one row per tool.

    Output columns: snapshot_at (timestamp, not yet truncated), server_id,
    tool_name, schema_bytes (long, not yet range-checked), observed_by,
    tenant_id. The writer's SQL finishes the normalisation.
    """
    ce = (
        kafka_df
        .select(
            F.col("timestamp").alias("kafka_ts"),
            F.from_json(F.col("value").cast("string"), AIQG_TOOL_CATALOGUE_ENVELOPE).alias("ce"),
        )
        .filter(F.col("ce.specversion") == "1.0")
        .filter(F.col("ce.type").isin(TOOL_CATALOGUE_TYPE, RESPONSE_TYPE))
    )
    is_proxy = F.col("ce.type") == TOOL_CATALOGUE_TYPE
    event_time = F.coalesce(F.col("ce.time"), F.col("kafka_ts"))
    rows = (
        ce
        .select(
            F.when(is_proxy, F.coalesce(F.try_to_timestamp(F.col("ce.data.snapshot_at")), event_time))
             .otherwise(event_time).alias("snapshot_at"),
            F.when(is_proxy, _blank_to_null(F.col("ce.data.server_id"))).alias("server_id"),
            F.when(is_proxy, F.col("ce.data.observed_by")).otherwise(F.lit("gateway")).alias("observed_by"),
            _blank_to_null(F.col("ce.data.tenant_id")).alias("tenant_id"),
            F.explode(F.when(is_proxy, F.col("ce.data.tools"))
                       .otherwise(F.col("ce.data.tools_offered"))).alias("tool"),
        )
        .filter(F.col("tool").isNotNull())
        .filter(F.col("tool.name").isNotNull() & (F.trim(F.col("tool.name")) != ""))
        .filter(F.col("observed_by").isin(*OBSERVED_BY))
        .filter(F.col("snapshot_at").isNotNull())
        # A proxy row is per server; a gateway row is per tenant.
        .filter((F.col("observed_by") != "proxy") | F.col("server_id").isNotNull())
        .filter((F.col("observed_by") != "gateway") | F.col("tenant_id").isNotNull())
        .select(
            "snapshot_at", "server_id",
            F.col("tool.name").alias("tool_name"),
            F.col("tool.schema_bytes").alias("schema_bytes"),
            "observed_by", "tenant_id",
        )
    )
    return rows


def insert_statement(staging_table: str) -> str:
    """The one statement that moves a staged batch into aiqg.tool_catalogue.

    The staging table has the parse_tool_catalogue_envelopes columns as JDBC
    wrote them. The WHERE repeats the Spark filters so a staged row that would
    break a constraint is skipped rather than failing the batch.
    """
    key = ", ".join(SNAPSHOT_KEY)
    return f"""
        INSERT INTO aiqg.tool_catalogue ({", ".join(COLUMNS)})
        SELECT date_trunc('hour', snapshot_at, 'UTC'),
               CASE WHEN observed_by = 'proxy' THEN NULLIF(server_id, '') END,
               tool_name,
               CASE WHEN schema_bytes BETWEEN 0 AND {INT_MAX} THEN schema_bytes::int END,
               observed_by,
               NULLIF(tenant_id, '')
        FROM {staging_table}
        WHERE snapshot_at IS NOT NULL
          AND tool_name IS NOT NULL AND tool_name <> ''
          AND observed_by IN ('proxy', 'gateway')
        ON CONFLICT ({key}) DO NOTHING
    """


def apply_batch(conn, staging_table: str) -> int:
    """Insert a staged batch into aiqg.tool_catalogue; returns rows inserted."""
    with conn.cursor() as cur:
        cur.execute(insert_statement(staging_table))
        inserted = cur.rowcount
        cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
    conn.commit()
    return inserted
