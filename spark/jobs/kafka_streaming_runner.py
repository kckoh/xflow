"""
Kafka Streaming Runner
Reads JSON from Kafka, applies Spark SQL, writes to S3 (Delta).

Usage: spark-submit kafka_streaming_runner.py '<config_json>'
"""
import sys
import json
import logging
import os
import re
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
    suffix = glue_table_name or job_name
    
    # Prevent path duplication (e.g., .../table_name/table_name)
    if path.rstrip("/").endswith(suffix):
        return path.rstrip("/")
    
    return path + suffix


def _write_batch(batch_df, batch_id: int, output_path: str) -> None:
    if batch_df.rdd.isEmpty():
        logger.info("Batch %s empty; no records to write", batch_id)
        return

    if "_corrupt_record" in batch_df.columns:
        corrupt_count = batch_df.filter(col("_corrupt_record").isNotNull()).count()
        if corrupt_count:
            logger.warning("Batch %s has %s corrupt records", batch_id, corrupt_count)
        batch_df = batch_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        if batch_df.rdd.isEmpty():
            logger.info("Batch %s had only corrupt records", batch_id)
            return

    batch_df = _normalize_for_delta(batch_df, output_path)
    batch_df.write.format("delta").mode("append").option("mergeSchema", "true").save(output_path)


def _write_batch_with_query(batch_df, batch_id: int, output_path: str, query: str) -> None:
    """Write batch with SQL query applied"""
    if batch_df.rdd.isEmpty():
        logger.info("Batch %s empty; no records to write", batch_id)
        return

    spark = batch_df.sparkSession
    
    # Create temp view for SQL query
    batch_df.createOrReplaceTempView("input")
    
    # Apply SQL query
    result_df = spark.sql(query)
    
    if result_df.rdd.isEmpty():
        logger.info("Batch %s empty after query; no records to write", batch_id)
        return

    if "_corrupt_record" in result_df.columns:
        corrupt_count = result_df.filter(col("_corrupt_record").isNotNull()).count()
        if corrupt_count:
            logger.warning("Batch %s has %s corrupt records", batch_id, corrupt_count)
        result_df = result_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        if result_df.rdd.isEmpty():
            logger.info("Batch %s had only corrupt records", batch_id)
            return

    result_df = _normalize_for_delta(result_df, output_path)
    result_df.write.format("delta").mode("append").option("mergeSchema", "true").save(output_path)
    logger.info("Batch %s written with query applied: %s records", batch_id, result_df.count())


def _load_existing_schema(spark, output_path: str):
    try:
        return spark.read.format("delta").load(output_path).schema
    except Exception:
        return None


def _normalize_for_delta(batch_df, output_path: str):
    spark = batch_df.sparkSession
    existing_schema = _load_existing_schema(spark, output_path)
    if not existing_schema:
        return batch_df

    existing_fields = {f.name: f.dataType for f in existing_schema.fields}
    changed = []
    df = batch_df
    for name, expected_type in existing_fields.items():
        if name not in df.columns:
            continue
        current_type = df.schema[name].dataType
        if current_type != expected_type:
            raw_name = f"{name}__raw"
            if raw_name not in df.columns:
                df = df.withColumn(raw_name, col(name).cast("string"))
            df = df.withColumn(name, col(name).cast(expected_type))
            changed.append(name)

    if changed:
        logger.warning("Type change detected; raw columns added: %s", ", ".join(changed))
    return df


def _process_batch(batch_df, batch_id: int, output_path: str, base_query: str) -> None:
    if batch_df.rdd.isEmpty():
        logger.info("Batch %s empty; no records to process", batch_id)
        return

    spark = batch_df.sparkSession
    json_rdd = batch_df.select("json_str").rdd.map(lambda r: r.json_str)
    parsed_df = spark.read.json(json_rdd)

    if parsed_df.rdd.isEmpty():
        logger.info("Batch %s had no parseable JSON", batch_id)
        return

    if "_corrupt_record" in parsed_df.columns:
        corrupt_count = parsed_df.filter(col("_corrupt_record").isNotNull()).count()
        if corrupt_count:
            logger.warning("Batch %s has %s corrupt records", batch_id, corrupt_count)
        parsed_df = parsed_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        if parsed_df.rdd.isEmpty():
            logger.info("Batch %s had only corrupt records", batch_id)
            return

    parsed_df.createOrReplaceTempView("input")
    schema_columns = parsed_df.columns
    query = _escape_at_identifiers(base_query)
    query = _adjust_query_for_schema(query, schema_columns)
    result_df = spark.sql(query)
    _write_batch(result_df, batch_id, output_path)


def _normalize_identifiers(query: str) -> str:
    """Replace double quotes with backticks for identifiers outside strings."""
    if not query or '"' not in query:
        return query

    # Split by single quotes to avoid touching string literals
    parts = query.split("'")
    for i in range(0, len(parts), 2):
        # In non-string segments, replace " with `
        parts[i] = parts[i].replace('"', '`')
    return "'".join(parts)


def _escape_at_identifiers(query: str) -> str:
    if not query or ("@" not in query and '"' not in query):
        return query

    # Normalize double quotes to backticks first
    query = _normalize_identifiers(query)

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


def _split_select_list(select_list: str) -> list:
    parts = []
    buf = []
    depth = 0
    quote = None
    for ch in select_list:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue

        if ch in ("'", "`", '"'):
            buf.append(ch)
            quote = ch
            continue

        if ch == "(":
            depth += 1
            buf.append(ch)
            continue
        if ch == ")":
            depth = max(depth - 1, 0)
            buf.append(ch)
            continue

        if ch == "," and depth == 0:
            part = "".join(buf).strip()
            if part:
                parts.append(part)
            buf = []
            continue

        buf.append(ch)

    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def _output_name(expr: str) -> str:
    expr = expr.strip()
    if not expr:
        return ""
    lowered = expr.lower()
    if " as " in lowered:
        return expr.rsplit(" ", 1)[-1].strip("`\"")
    parts = expr.split()
    if len(parts) >= 2:
        return parts[-1].strip("`\"")
    return expr.strip("`\"")


def _adjust_query_for_schema(query: str, schema_columns: list) -> str:
    match = re.match(r"(?is)^\s*select\s+(.*?)\s+from\s+input\b(.*)$", query or "")
    if not match:
        return query

    select_list = match.group(1).strip()
    tail = match.group(2) or ""
    if select_list == "*":
        new_list = [f"`{col}`" for col in schema_columns]
        return f"SELECT {', '.join(new_list)} FROM input{tail}"

    items = _split_select_list(select_list)
    seen = {}
    for item in items:
        name = _output_name(item)
        if name:
            seen[name] = item

    new_items = []
    for col_name in schema_columns:
        expr = seen.pop(col_name, None)
        if expr:
            new_items.append(expr)
        else:
            new_items.append(f"NULL AS `{col_name}`")

    # Keep any leftover expressions that are not direct column refs.
    for expr in items:
        name = _output_name(expr)
        if not name or name in seen:
            new_items.append(expr)
            if name in seen:
                seen.pop(name, None)

    return f"SELECT {', '.join(new_items)} FROM input{tail}"


def run_streaming_job(config: dict):
    dataset_id = config.get("id", "unknown")
    dataset_name = config.get("name", "kafka-stream")
    query = config.get("query") or "SELECT * FROM input"
    destination = config.get("destination", {})
    source_format = (config.get("format") or "json").lower()
    auto_schema = bool(config.get("auto_schema", False))

    kafka_cfg = config.get("kafka", {})
    bootstrap_servers = kafka_cfg.get("bootstrap_servers")
    topic = kafka_cfg.get("topic")
    group_id = kafka_cfg.get("group_id")
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

    reader = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest")
    if group_id:
        reader = reader.option("kafka.group.id", group_id)
    kafka_df = reader.load()

    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

    if source_format == "raw":
        custom_regex = config.get("custom_regex")
        custom_regex = config.get("custom_regex")
        if custom_regex:
            # 1. Parse regex pattern to find group names
            try:
                pattern = re.compile(custom_regex)
                group_map = pattern.groupindex  # {'ip': 1, 'date': 2}
                pattern = re.compile(custom_regex)
                group_map = pattern.groupindex  # {'ip': 1, 'date': 2}
            except Exception as e:
                logger.warning(f"Invalid regex pattern: {e}")
                group_map = {}

            if group_map:
                # 2. Build select expressions using regexp_extract
                from pyspark.sql.functions import regexp_extract
                
                # Convert Python regex (?P<name>...) to plain groups (...) for Spark
                # Spark's regexp_extract only supports numbered groups, not named groups
                spark_regex = re.sub(r'\(\?P<[^>]+>', '(', custom_regex)
                
                # Add json_str to see actual Kafka messages
                exprs = [col("json_str")]
                for name, idx in group_map.items():
                    exprs.append(regexp_extract(col("json_str"), spark_regex, idx).alias(name))
                
                input_df = json_df.select(*exprs)
                
                # Check if auto_schema should be enabled for regex
                if not auto_schema: 
                     # Using regex implies we want to inspect the fields like auto_schema
                     pass 
            else:
                input_df = json_df.select(col("json_str").alias("raw_value"))
        else:
            input_df = json_df.select(col("json_str").alias("raw_value"))
    elif auto_schema:
        input_df = json_df
    elif source_schema:
        parsed = json_df.select(
            from_json(
                col("json_str"),
                source_schema,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
            ).alias("data")
        )
        input_df = parsed.select("data.*")
    else:
        input_df = json_df

    input_df.createOrReplaceTempView("input")

    schema_columns = [c.get("name") for c in (config.get("source_schema") or []) if c.get("name")]
    query = _escape_at_identifiers(query)
    if schema_columns and not auto_schema:
        # Disable automatic schema adjustment as it strips custom transformations (COALESCE, etc.)
        pass

    output_path = _build_output_path(destination, dataset_name)
    checkpoint_path = destination.get("checkpoint_path") or f"s3a://xflows-output/checkpoints/{dataset_id}/"

    if auto_schema and source_format == "json":
        query_handle = input_df.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .foreachBatch(lambda df, batch_id: _process_batch(df, batch_id, output_path, query)) \
            .start()
    else:
        # For raw format, also apply query in foreachBatch
        query_handle = input_df.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .foreachBatch(lambda df, batch_id: _write_batch_with_query(df, batch_id, output_path, query)) \
            .start()

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
