"""CloudEvents 1.0 schema for Spark Structured Streaming."""

from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Matches the JSON structure of a CloudEvents 1.0 message with TAS extensions.
# Extension attributes use lowercase-only names per CE 1.0 §4.2.
CE_SCHEMA = StructType([
    StructField("specversion", StringType(), False),
    StructField("id", StringType(), False),
    StructField("source", StringType(), False),
    StructField("type", StringType(), False),
    StructField("time", TimestampType(), True),
    StructField("datacontenttype", StringType(), True),
    StructField("subject", StringType(), True),
    # TAS extension attributes
    StructField("tenantid", StringType(), True),
    StructField("spaceid", StringType(), True),
    StructField("userid", StringType(), True),
    StructField("requestid", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("frameworks", ArrayType(StringType()), True),
    # data is kept as raw string — inserted as JSONB into TimescaleDB
    StructField("data", StringType(), True),
])
