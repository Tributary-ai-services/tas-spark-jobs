"""
TAS Events Aggregator — Spark Structured Streaming job.

Reads CloudEvents from tas.activity.* and tas.compliance.* Kafka topics,
writes raw events to TimescaleDB via a staging-table upsert pattern
(at-least-once safe). Continuous aggregates are computed in-database
by TimescaleDB policies, not by this job.

Consumer group: spark-aggregator (distinct from aether-be-live-feed).
"""

import os
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from schema import CE_SCHEMA


def get_env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("tas-events-aggregator")
        .config("spark.sql.streaming.checkpointLocation",
                get_env("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/events-aggregator"))
        .config("spark.hadoop.fs.s3a.endpoint", get_env("MINIO_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", get_env("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", get_env("MINIO_SECRET_KEY", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession) -> DataFrame:
    """Read from both activity and compliance topics via regex subscription."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", get_env("KAFKA_BOOTSTRAP", "localhost:9092"))
        .option("subscribePattern", r"tas\.(activity|compliance)\..*")
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "spark-aggregator")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_cloudevents(kafka_df: DataFrame) -> DataFrame:
    """Parse CloudEvents JSON from Kafka message values."""
    return (
        kafka_df
        .select(
            F.col("topic"),
            F.col("timestamp").alias("kafka_ts"),
            F.from_json(F.col("value").cast("string"), CE_SCHEMA).alias("ce"),
            F.col("value").cast("string").alias("raw_json"),
        )
        .filter(F.col("ce.specversion") == "1.0")
        .select(
            F.coalesce(F.col("ce.time"), F.col("kafka_ts")).alias("time"),
            F.col("ce.id").alias("event_id"),
            F.coalesce(F.col("ce.tenantid"), F.lit("")).alias("tenant_id"),
            F.col("ce.source").alias("source"),
            F.col("ce.type").alias("type"),
            F.col("ce.subject").alias("subject"),
            F.col("ce.spaceid").alias("space_id"),
            F.col("ce.userid").alias("user_id"),
            F.col("ce.requestid").alias("request_id"),
            F.col("ce.severity").alias("severity"),
            # Convert array to Postgres TEXT[] format: {val1,val2}
            F.when(
                F.col("ce.frameworks").isNotNull(),
                F.concat(F.lit("{"), F.array_join(F.col("ce.frameworks"), ","), F.lit("}"))
            ).alias("frameworks"),
            F.col("ce.data").alias("data"),
            F.col("raw_json").alias("raw"),
        )
        .withWatermark("time", "2 minutes")
    )


def write_raw_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write a micro-batch to TimescaleDB using staging-table upsert.

    Spark's foreachBatch provides at-least-once semantics — a batch can be
    replayed after checkpoint recovery. Plain JDBC append would fail on
    the (time, tenant_id, event_id) PK. Instead we:
    1. Write to a temp staging table
    2. INSERT ... ON CONFLICT DO NOTHING from staging into events
    3. Drop the staging table
    """
    if batch_df.isEmpty():
        return

    jdbc_url = get_env("JDBC_URL", "jdbc:postgresql://localhost:5432/tas_events")
    jdbc_props = {
        "user": get_env("JDBC_USER", "tas_events_rw"),
        "password": get_env("JDBC_PASSWORD", "taseventspass"),
        "driver": "org.postgresql.Driver",
    }

    staging_table = f"events_stage_{batch_id}"

    # Step 1: write batch to staging table
    (
        batch_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", jdbc_props["user"])
        .option("password", jdbc_props["password"])
        .option("driver", jdbc_props["driver"])
        .option("batchsize", "1000")
        .mode("overwrite")
        .save()
    )

    # Step 2: upsert from staging into events, then drop staging
    import psycopg2

    # Parse JDBC URL to psycopg2 format
    # jdbc:postgresql://host:port/db -> host, port, db
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
                INSERT INTO events (time, event_id, tenant_id, source, type, subject,
                                    space_id, user_id, request_id, severity, frameworks,
                                    data, raw)
                SELECT time, event_id, tenant_id, source, type, subject,
                       space_id, user_id, request_id, severity,
                       frameworks::text[],
                       data::jsonb, raw::jsonb
                FROM {staging_table}
                ON CONFLICT (time, tenant_id, event_id) DO NOTHING;
            """)
            cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
        conn.commit()
    finally:
        conn.close()


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka(spark)
    parsed = parse_cloudevents(kafka_df)

    query = (
        parsed.writeStream
        .foreachBatch(write_raw_batch)
        .option("checkpointLocation",
                get_env("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/events-aggregator") + "/raw/")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
