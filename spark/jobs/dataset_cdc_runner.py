"""
Dataset CDC Runner - Spark Streaming Job
역할: Kafka에서 읽어서 Transform 적용 후 S3에 저장

사용법: spark-submit dataset_cdc_runner.py '{"id":"...", "sources":[...], ...}'
"""
import sys
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, lit, to_date
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def apply_transforms(df, transforms: list):
    """Transform 체인 적용"""
    for t in transforms:
        t_type = t.get("type")
        config = t.get("config", {})
        
        if t_type == "filter":
            condition = config.get("condition")
            if condition:
                df = df.filter(expr(condition))
        
        elif t_type == "select-fields":
            fields = config.get("fields", [])
            if fields:
                df = df.select([col(f) for f in fields if f in df.columns])
        
        elif t_type == "drop-columns":
            for c in config.get("columns", []):
                if c in df.columns:
                    df = df.drop(c)
    
    return df


def run_pipeline(config: dict):
    """메인 파이프라인 실행"""
    dataset_id = config.get("id", "unknown")
    dataset_name = config.get("name", "unnamed")
    sources = config.get("sources", [])
    transforms = config.get("transforms", [])
    targets = config.get("targets", [])
    
    # Source 정보
    source = sources[0] if sources else {}
    source_config = source.get("config", {})
    server_name = source_config.get("server_name", "dbserver1")
    schema_name = source_config.get("schema", "public")
    table_name = source_config.get("table", "unknown")
    
    # Kafka 토픽: {server_name}.{schema}.{table}
    kafka_topic = f"{server_name}.{schema_name}.{table_name}"
    
    # Target 정보
    target = targets[0] if targets else {}
    output_path = target.get("config", {}).get("path", f"s3a://warehouse/datasets/{dataset_name}/")
    checkpoint_path = f"s3a://warehouse/checkpoints/{dataset_id}/"
    
    logger.info(f"=== CDC Pipeline 시작 ===")
    logger.info(f"Dataset: {dataset_name}")
    logger.info(f"Kafka Topic: {kafka_topic}")
    logger.info(f"Output: {output_path}")
    
    # Spark 세션
    spark = SparkSession.builder \
        .appName(f"CDC-{dataset_name}") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack-main:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Kafka에서 읽기
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # JSON 파싱 (get_json_object 사용)
    # from_json으로 StructType(StringType)을 쓰면 JSON Object가 null이 됨
    # 따라서 get_json_object로 문자열 그대로 추출함
    kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    
    # INSERT/UPDATE만 (op = c, u, r)
    # 먼저 op를 추출해서 필터링
    filtered_df = kafka_df \
        .withColumn("op", expr("get_json_object(json_str, '$.op')")) \
        .filter(col("op").isin(["c", "u", "r"]))
        
    # 필요한 컬럼 추출
    result_df = filtered_df.select(
        expr("get_json_object(json_str, '$.after')").alias("payload"),
        col("op").alias("operation"),
        (expr("get_json_object(json_str, '$.ts_ms')").cast(LongType()) / 1000).cast(TimestampType()).alias("event_time"),
        to_date((expr("get_json_object(json_str, '$.ts_ms')").cast(LongType()) / 1000).cast(TimestampType())).alias("event_date")
    )
    
    # Transform 적용
    if transforms:
        result_df = apply_transforms(result_df, transforms)
    
    # S3에 저장
    query = result_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("event_date") \
        .trigger(processingTime="5 seconds") \
        .outputMode("append") \
        .start()
    
    logger.info("Pipeline 실행 중... (Ctrl+C로 종료)")
    query.awaitTermination()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit dataset_cdc_runner.py '<config_json>'")
        sys.exit(1)
    
    try:
        config = json.loads(sys.argv[1])
        run_pipeline(config)
    except Exception as e:
        logger.error(f"Pipeline 실패: {e}")
        raise
