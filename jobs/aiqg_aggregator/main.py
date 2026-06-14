"""
AIQG Aggregator — Spark Structured Streaming job.

Reads CloudEvents from the tas.aiqg.events.v1 Kafka topic, filters to
com.tas.aiqg.response.v1, and upserts scalar metrics into the
aiqg.event_metrics hypertable in TimescaleDB. The 1m / 1h continuous
aggregates are computed in-database by TimescaleDB policies.

Why a separate job from events_aggregator:
- AIQG envelopes use a rich nested data{} payload (CLEAR scores,
  token_accounting, event_timestamps) that doesn't fit the generic
  CE extension shape that public.events expects.
- Independent failure domain — an AIQG schema bug must not stall the
  generic activity/compliance pipeline.
- Independent consumer group + checkpoint so back-pressure tuning
  can diverge without coupling.

Consumer group: spark-aiqg-aggregator.
"""

import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from schema import AIQG_RESPONSE_ENVELOPE


def get_env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("tas-aiqg-aggregator")
        .config("spark.sql.streaming.checkpointLocation",
                get_env("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/aiqg-aggregator"))
        .config("spark.hadoop.fs.s3a.endpoint", get_env("MINIO_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", get_env("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", get_env("MINIO_SECRET_KEY", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession) -> DataFrame:
    """Subscribe to the AIQG events topic.

    AIQG_TOPIC defaults to the v1 topic. The schema is versioned in the
    topic name (tas.aiqg.events.v1) so a breaking change spawns v2
    rather than overloading this subscriber.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", get_env("KAFKA_BOOTSTRAP", "localhost:9092"))
        .option("subscribe", get_env("AIQG_TOPIC", "tas.aiqg.events.v1"))
        .option("startingOffsets", get_env("STARTING_OFFSETS", "latest"))
        .option("kafka.group.id", "spark-aiqg-aggregator")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_response_envelopes(kafka_df: DataFrame) -> DataFrame:
    """Parse the CE envelope and project the columns aiqg.event_metrics expects.

    Filter to com.tas.aiqg.response.v1 — the paired request envelope on
    the same topic is dropped. The emitter denormalizes the slicing
    dimensions (vendor / model / workflow / source_app) + the agent_context
    identity block onto the response envelope, so we read them directly
    here without a stateful join.
    """
    return (
        kafka_df
        .select(
            F.col("timestamp").alias("kafka_ts"),
            F.from_json(F.col("value").cast("string"), AIQG_RESPONSE_ENVELOPE).alias("ce"),
            F.col("value").cast("string").alias("raw_json"),
        )
        .filter(F.col("ce.specversion") == "1.0")
        .filter(F.col("ce.type") == "com.tas.aiqg.response.v1")
        # Drop messages that didn't carry a tenant_id or response_event_id —
        # those are the primary-key columns and a NULL would fail the
        # INSERT. Logs the count via metrics on the driver.
        .filter(F.col("ce.data.tenant_id").isNotNull())
        .filter(F.col("ce.data.response_event_id").isNotNull())
        .select(
            F.coalesce(F.col("ce.time"), F.col("kafka_ts")).alias("time"),
            F.col("ce.data.tenant_id").alias("tenant_id"),
            F.col("ce.data.aiqg_account_id").alias("aiqg_account_id"),
            F.col("ce.data.vendor").alias("vendor"),
            F.col("ce.data.model").alias("model"),
            F.col("ce.data.workflow").alias("workflow"),
            F.col("ce.data.source_app").alias("source_app"),
            F.col("ce.data.status").alias("status"),
            F.col("ce.data.http_status").alias("http_status"),
            F.col("ce.data.finish_reason").alias("finish_reason"),
            F.col("ce.data.clear_scores.composite").alias("clear_composite"),
            F.col("ce.data.clear_scores.cost").alias("clear_cost"),
            F.col("ce.data.clear_scores.latency").alias("clear_latency"),
            F.col("ce.data.clear_scores.efficacy").alias("clear_efficacy"),
            F.col("ce.data.clear_scores.assurance").alias("clear_assurance"),
            F.col("ce.data.clear_scores.reliability").alias("clear_reliability"),
            F.col("ce.data.event_timestamps.end_to_end_ms").cast("int").alias("end_to_end_ms"),
            F.col("ce.data.token_accounting.prompt_tokens").alias("prompt_tokens"),
            F.col("ce.data.token_accounting.completion_tokens").alias("completion_tokens"),
            F.col("ce.data.token_accounting.total_tokens").alias("total_tokens"),
            F.col("ce.data.token_accounting.total_cost_usd").alias("total_cost_usd"),
            F.col("ce.data.assurance.inbound_count").alias("assurance_inbound_count"),
            F.col("ce.data.assurance.outbound_count").alias("assurance_outbound_count"),
            F.col("ce.data.assurance.nist_secure_resilient").alias("nist_secure_resilient"),
            F.col("ce.data.assurance.nist_privacy_enhanced").alias("nist_privacy_enhanced"),
            F.col("ce.data.assurance.nist_valid_reliable").alias("nist_valid_reliable"),
            F.col("ce.data.assurance.nist_safe").alias("nist_safe"),
            F.col("ce.data.response_event_id").alias("response_event_id"),
            F.col("ce.data.request_event_id").alias("request_event_id"),
            # Self-asserted identity attribution (NULL when unattributed) —
            # powers the per-agent / per-flow rollups behind /metrics/agents
            # and /flows.
            F.col("ce.data.agent_context.agent_id").alias("agent_id"),
            F.col("ce.data.agent_context.agent_name").alias("agent_name"),
            F.col("ce.data.agent_context.user_id").alias("user_id"),
            F.col("ce.data.agent_context.conversation_id").alias("conversation_id"),
            F.col("ce.data.agent_context.flow_id").alias("flow_id"),
            F.col("ce.data.agent_context.principal_id").alias("principal_id"),
            F.col("ce.data.agent_context.client_ip").alias("client_ip"),
            F.col("ce.data.agent_context.identity_source").alias("identity_source"),
            F.col("ce.data.agent_context.step_id").alias("step_id"),
            F.col("ce.data.agent_context.parent_step_id").alias("parent_step_id"),
            F.col("ce.data.agent_context.flow_step_seq").alias("flow_step_seq"),
            # Tag findings as raw JSON string — the typed schema can't
            # express a map with arbitrary string keys cleanly, so we
            # extract the sub-object as JSON via path query and cast
            # to JSONB in the upsert SQL. NULL when no matchers fired.
            F.get_json_object(F.col("raw_json"), "$.data.assurance.tag_findings").alias("tags"),
            F.col("raw_json").alias("raw"),
        )
        .withWatermark("time", "2 minutes")
    )


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Stage + INSERT ... ON CONFLICT DO NOTHING into aiqg.event_metrics.

    Same pattern as events_aggregator.write_raw_batch: stream the batch
    to a temp table via JDBC, then a single upsert SQL pulls it into
    the hypertable. ON CONFLICT DO NOTHING because foreachBatch is
    at-least-once — a retry must be idempotent. The PK
    (time, tenant_id, response_event_id) makes the dedupe deterministic.
    """
    if batch_df.isEmpty():
        return

    jdbc_url = get_env("JDBC_URL", "jdbc:postgresql://localhost:5432/tas_events")
    jdbc_props = {
        "user": get_env("JDBC_USER", "tas_events_rw"),
        "password": get_env("JDBC_PASSWORD", "taseventspass"),
        "driver": "org.postgresql.Driver",
    }

    staging_table = f"aiqg_event_metrics_stage_{batch_id}"

    (
        batch_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", jdbc_props["user"])
        .option("password", jdbc_props["password"])
        .option("driver", jdbc_props["driver"])
        .option("batchsize", "500")
        .mode("overwrite")
        .save()
    )

    import psycopg2

    jdbc_parts = jdbc_url.replace("jdbc:postgresql://", "").split("/")
    host_port = jdbc_parts[0].split(":")
    host = host_port[0]
    port = host_port[1] if len(host_port) > 1 else "5432"
    dbname = jdbc_parts[1] if len(jdbc_parts) > 1 else "tas_events"

    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname,
        user=jdbc_props["user"], password=jdbc_props["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO aiqg.event_metrics (
                    time, tenant_id, aiqg_account_id, vendor, model, workflow, source_app,
                    status, http_status, finish_reason,
                    clear_composite, clear_cost, clear_latency,
                    clear_efficacy, clear_assurance, clear_reliability,
                    end_to_end_ms,
                    prompt_tokens, completion_tokens, total_tokens, total_cost_usd,
                    assurance_inbound_count, assurance_outbound_count,
                    nist_secure_resilient, nist_privacy_enhanced,
                    nist_valid_reliable, nist_safe,
                    response_event_id, request_event_id,
                    agent_id, agent_name, user_id, conversation_id,
                    flow_id, principal_id, client_ip, identity_source,
                    step_id, parent_step_id, flow_step_seq,
                    tags, raw
                )
                SELECT
                    time, tenant_id, aiqg_account_id, vendor, model, workflow, source_app,
                    status, http_status, finish_reason,
                    clear_composite, clear_cost, clear_latency,
                    clear_efficacy, clear_assurance, clear_reliability,
                    end_to_end_ms,
                    prompt_tokens, completion_tokens, total_tokens, total_cost_usd,
                    assurance_inbound_count, assurance_outbound_count,
                    nist_secure_resilient, nist_privacy_enhanced,
                    nist_valid_reliable, nist_safe,
                    response_event_id, request_event_id,
                    agent_id, agent_name, user_id, conversation_id,
                    flow_id, principal_id, client_ip, identity_source,
                    step_id, parent_step_id, flow_step_seq,
                    NULLIF(tags, '')::jsonb, raw::jsonb
                FROM {staging_table}
                ON CONFLICT (time, tenant_id, response_event_id) DO NOTHING;
            """)
            cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
        conn.commit()
    finally:
        conn.close()


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka(spark)
    parsed = parse_response_envelopes(kafka_df)

    query = (
        parsed.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation",
                get_env("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/aiqg-aggregator") + "/raw/")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
