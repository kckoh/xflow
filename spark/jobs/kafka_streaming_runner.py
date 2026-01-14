"""
Kafka Streaming Runner
Reads JSON from Kafka, applies Spark SQL, writes to S3 (Delta).

Usage: spark-submit kafka_streaming_runner.py '<config_json>'
"""
import sys
import json
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _spark_type(type_name: str):
    if not type_name:
        return StringType()
    t = type_name.lower()
    if t in ["int", "integer", "long"]:
        return LongType()
    if t in ["double", "float", "number"]:
        return DoubleType()
    if t in ["bool", "boolean"]:
        return BooleanType()
    return StringType()


def _build_schema(columns):
    if not columns:
        return None
    fields = []
    for c in columns:
        name = c.get("name")
        if not name:
            continue
        fields.append(StructField(name, _spark_type(c.get("type")), True))
    return StructType(fields) if fields else None


def _apply_s3_config(builder: SparkSession.Builder):
    endpoint = os.getenv("AWS_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL") or os.getenv("S3_ENDPOINT_URL")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION", "ap-northeast-2")

    if endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
        builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")

    if access_key and secret_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", access_key)
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", secret_key)
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    else:
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )

    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", region)
    return builder


def _build_output_path(destination: dict, job_name: str) -> str:
    path = destination.get("path")
    if not path:
        raise ValueError("Destination path is required")

    if path.startswith("s3://"):
        path = path.replace("s3://", "s3a://", 1)

    if not path.endswith("/"):
        path += "/"

    glue_table_name = destination.get("glue_table_name")
    return path + (glue_table_name or job_name)


def _escape_at_identifiers(query: str) -> str:
    if not query or "@" not in query:
        return query

    parts = query.split("'")
    for i in range(0, len(parts), 2):
        parts[i] = __escape_at_identifiers_segment(parts[i])
    return "'".join(parts)


def __escape_at_identifiers_segment(segment: str) -> str:
    out = []
    i = 0
    while i < len(segment):
        if segment[i] == "`":
            end = segment.find("`", i + 1)
            if end == -1:
                out.append(segment[i:])
                break
            out.append(segment[i:end + 1])
            i = end + 1
            continue

        if segment[i] == "@":
            j = i + 1
            while j < len(segment) and (segment[j].isalnum() or segment[j] == "_"):
                j += 1
            if j > i + 1:
                out.append("`")
                out.append(segment[i:j])
                out.append("`")
                i = j
                continue
        out.append(segment[i])
        i += 1
    return "".join(out)


def run_streaming_job(config: dict):
    dataset_id = config.get("id", "unknown")
    dataset_name = config.get("name", "kafka-stream")
    query = config.get("query") or "SELECT * FROM input"
    destination = config.get("destination", {})

    kafka_cfg = config.get("kafka", {})
    bootstrap_servers = kafka_cfg.get("bootstrap_servers")
    topic = kafka_cfg.get("topic")
    if not bootstrap_servers or not topic:
        raise ValueError("Kafka bootstrap_servers and topic are required")

    source_schema = _build_schema(config.get("source_schema", []))

    builder = SparkSession.builder.appName(f"Kafka-Streaming-{dataset_name}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    builder = _apply_s3_config(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting Kafka streaming job")
    logger.info(f"Dataset: {dataset_name}")
    logger.info(f"Topic: {topic}")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

    if source_schema:
        parsed = json_df.select(from_json(col("json_str"), source_schema).alias("data"))
        input_df = parsed.select("data.*")
    else:
        input_df = json_df

    input_df.createOrReplaceTempView("input")

    query = _escape_at_identifiers(query)
    result_df = spark.sql(query)

    output_path = _build_output_path(destination, dataset_name)
    checkpoint_path = destination.get("checkpoint_path") or f"s3a://xflows-output/checkpoints/{dataset_id}/"

    query_handle = result_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start(output_path)

    logger.info("Streaming query started")
    query_handle.awaitTermination()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit kafka_streaming_runner.py '<config_json>'")
        sys.exit(1)

    try:
        config = json.loads(sys.argv[1])
        run_streaming_job(config)
    except Exception as e:
        logger.error(f"Kafka streaming job failed: {e}")
        raise
