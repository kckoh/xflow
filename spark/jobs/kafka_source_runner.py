import os
import sys
import json
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============ Transform Registry (Ported from etl_runner.py) ============

def transform_select_fields(df: DataFrame, config: dict) -> DataFrame:
    columns = config.get("selectedColumns", [])
    if columns:
        selections = [col(c).alias(c.replace(".", "_")) for c in columns]
        return df.select(*selections)
    return df

def transform_drop_columns(df: DataFrame, config: dict) -> DataFrame:
    columns = config.get("columns", [])
    for c in columns:
        df = df.drop(c)
    return df

def transform_filter(df: DataFrame, config: dict) -> DataFrame:
    expression = config.get("expression", "")
    if expression:
        return df.filter(expression)
    return df

def transform_sql(df: DataFrame, config: dict) -> DataFrame:
    sql_query = config.get("sql")
    if not sql_query:
        return df
    spark = df.sparkSession
    df.createOrReplaceTempView("input")
    return spark.sql(sql_query)

TRANSFORMS = {
    "select-fields": transform_select_fields,
    "drop-columns": transform_drop_columns,
    "filter": transform_filter,
    "sql": transform_sql,
}

def apply_transforms(df: DataFrame, transforms: list) -> DataFrame:
    for transform in transforms:
        t_type = transform.get("type")
        t_config = transform.get("config", {})
        if t_type in TRANSFORMS:
            logger.info(f"📝 Applying transform: {t_type}")
            df = TRANSFORMS[t_type](df, t_config)
    return df

# ============ Processing Logic ============

def process_batch(batch_df, batch_id, target_path: str, transforms: list, schema: StructType = None):
    logger.info(f"=== Batch {batch_id} Processing Start ===")
    spark = SparkSession.builder.getOrCreate()
    try:
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id} is empty, skipping.")
            return

        processed_df = batch_df
        
        # 1. Handle JSON Parsing
        if "value" in batch_df.columns:
            # If schema is missing, try to infer it from this batch
            if not schema:
                logger.info("Auto-inference: No schema provided. Attempting to detect JSON structure.")
                try:
                    # In Spark 3.x, we can use spark.read.json to infer from a DataFrame of strings
                    # We take a sample to speed up inference if the batch is huge
                    sample_rdd = (
                        batch_df.select("value")
                        .limit(10)
                        .rdd.map(
                            lambda r: r.value.decode("utf-8")
                            if isinstance(r.value, (bytes, bytearray))
                            else r.value
                        )
                    )
                    inferred_df = spark.read.json(sample_rdd)
                    if len(inferred_df.columns) > 0 and "_corrupt_record" not in inferred_df.columns:
                        schema = inferred_df.schema
                        logger.info(f"✅ Inferred Schema: {schema.simpleString()}")
                    else:
                        logger.info("Auto-inference: Data does not appear to be valid JSON structure.")
                except Exception as e:
                    logger.warning(f"Auto-inference failed: {e}")

            if schema:
                logger.info(f"Parsing JSON with schema")
                # When using from_json, we need to handle its structure
                parsed_df = batch_df.select(
                    from_json(col("value").cast("string"), schema).alias("data")
                )
                # Expand data.* and keep only the top level columns
                processed_df = parsed_df.select("data.*")
            else:
                logger.info(f"Treating as raw string (Fallback)")
                processed_df = batch_df.select(col("value").cast("string"))

        # 2. Apply user-defined transforms
        if transforms:
            # Important: if transforms exist but schema was just inferred, 
            # the 컬럼명이 transforms의 조건과 맞아야 합니다.
            processed_df = apply_transforms(processed_df, transforms)

        # 3. Write to S3
        count = processed_df.count()
        logger.info(f"Batch {batch_id} | Final Count: {count}")
        
        if count > 0:
            logger.info(f"Writing to S3: {target_path}")
            processed_df.write \
                .mode("append") \
                .parquet(target_path)
            
            # Show snippet
            processed_df.show(5, truncate=False)
        else:
            logger.info(f"Batch {batch_id} is empty after transforms. Skipping save.")
            
    except Exception as e:
        logger.error(f"Error in batch {batch_id}: {e}")

def run_pipeline(config: dict):
    # Debug: Print full config (Base64 decoded)
    logger.info(f"JOB CONFIG RECEIVED: {json.dumps(config, indent=2)}")
    
    # Support both 'source' (legacy) and 'sources' (multi-source) patterns
    sources = config.get("sources", [])
    if not sources and config.get("source"):
        sources = [config.get("source")]
    
    # Extract primary topic and schema from the first Kafka source found
    kafka_source = next((s for s in sources if s.get("type") == "kafka" or "topic" in s), sources[0] if sources else {})
    
    topic = kafka_source.get("topic") or config.get("topic")
    job_id = config.get("job_id", "manual_run")
    
    # Check for filters in diverse locations
    transforms = config.get("transforms", [])
    if not transforms and "config" in kafka_source and "filters" in kafka_source["config"]:
        logger.info("Found filters inside source config, wrapping as transform")
        transforms = [{"type": "filter", "config": {"expression": kafka_source["config"]["filters"]}}]

    logger.info(f"FINAL TRANSFORMS TO APPLY: {json.dumps(transforms)}")

    if not topic:
        logger.error("No Kafka topic found in configuration!")
        sys.exit(1)

    # Schema handling
    schema_json = kafka_source.get("schema")
    spark_schema = None
    if schema_json:
        try:
            spark_schema = StructType.fromJson(schema_json)
            logger.info("Successfully loaded schema from config")
        except Exception as e:
            logger.warning(f"Failed to parse schema JSON: {e}")

    KAFKA_BOOTSTRAP_SERVERS = (
        config.get("bootstrap_servers")
        or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    security_protocol = (config.get("security_protocol") or "PLAINTEXT").upper()
    sasl_mechanism = config.get("sasl_mechanism")
    sasl_username = config.get("sasl_username")
    sasl_password = config.get("sasl_password")
    TARGET_S3_PATH = f"s3a://xflows-output/datasets/{job_id}/"
    CHECKPOINT_PATH = f"s3a://xflows-output/checkpoints/{job_id}/"
    
    logger.info(
        f"=== [Streaming ETL] Bootstrap: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {topic} | Target: {TARGET_S3_PATH} ==="
    )
    
    spark = SparkSession.builder \
        .appName(f"Kafka-ETL-{topic}") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Read Stream
        reader = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
        )

        if security_protocol and security_protocol != "PLAINTEXT":
            reader = reader.option("kafka.security.protocol", security_protocol)
            if sasl_mechanism:
                reader = reader.option("kafka.sasl.mechanism", sasl_mechanism)
            if sasl_username and sasl_password:
                jaas = (
                    'org.apache.kafka.common.security.plain.PlainLoginModule required '
                    f'username="{sasl_username}" password="{sasl_password}";'
                )
                reader = reader.option("kafka.sasl.jaas.config", jaas)

        raw_stream = reader.load()

        # Batch processing with transforms
        query = raw_stream.writeStream \
            .foreachBatch(lambda df, id: process_batch(df, id, TARGET_S3_PATH, transforms, spark_schema)) \
            .option("checkpointLocation", CHECKPOINT_PATH) \
            .start()
            
        logger.info(f"Pipeline Started. Checkpoint: {CHECKPOINT_PATH}")
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Pipeline Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    config_b64 = os.getenv("JOB_CONFIG")
    if config_b64:
        import base64
        config = json.loads(base64.b64decode(config_b64).decode("utf-8"))
        run_pipeline(config)
    else:
        sys.exit(1)
