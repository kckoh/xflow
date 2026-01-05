"""
Unified CDC Runner - Spark Streaming Job for Multiple Tables
역할: 하나의 Connection(Connector)에서 오는 여러 테이블의 CDC 데이터를 통합 처리
      - topic 컬럼을 기반으로 데이터를 분기(Routing)
      - 각 테이블별 설정(Transform, Target Path) 적용 (UDF 사용)
      - S3에 개별 저장

사용법: spark-submit unified_cdc_runner.py '{"connection_name": "...", "tables": [...]}'
"""
import os
import sys
import json
import logging
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, to_date, udf
from pyspark.sql.types import LongType, TimestampType, StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# K8s 환경 변수 (Docker Compose에서는 기본값 사용)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

# === UDF 정의 ===
def transform_json_payload(json_str: str, method: str, params_json: str) -> str:
    """
    JSON Paylod를 변환하는 UDF
    - json_str: 원본 데이터 JSON 문자열
    - method: 'select-fields' | 'drop-columns' | 'filter'
    - params_json: 설정값 JSON 문자열 (예: '["id", "name"]')
    """
    if not json_str:
        return None
    
    try:
        data = json.loads(json_str)
        params = json.loads(params_json)
        
        if method == "select-fields":
            # 선택된 필드만 남김
            # params: List[str] (selected columns)
            new_data = {k: v for k, v in data.items() if k in params}
            return json.dumps(new_data)
            
        elif method == "drop-columns":
            # 특정 컬럼 제거
            # params: List[str] (dropped columns)
            for col_name in params:
                if col_name in data:
                    del data[col_name]
            return json.dumps(data)
            
        # Filter는 여기서 처리 안 함 (Row 자체를 날려야 하므로 UDF 보다는 DataFrame Filter가 적합)
        # 하지만 payload 내부 값에 따른 필터링이라면 여기서 None을 리턴하고 나중에 지우는 방식도 가능.
        # 현재 구조상 Filter는 SQL Expression 변환이 어려우므로 (Schema 모름),
        # 여기서는 Field 조작만 다룸. Filter는 추후 과제.
        
        return json_str # 매칭되는 거 없으면 그대로 리턴
        
    except Exception as e:
        # 파싱 에러 시 원본 리턴하거나 Null 리턴 (여기선 원본 유지)
        return json_str

# UDF 등록
transform_udf = udf(transform_json_payload, StringType())


def apply_transforms(df, transforms: list):
    """Transform 체인 적용"""
    for t in transforms:
        t_type = t.get("type")
        config = t.get("config", {})
        
        if t_type == "select-fields":
            fields = config.get("selectedColumns", []) # UI에서 selectedColumns로 줌
            if not fields:
                fields = config.get("fields", []) # 백엔드/구형 호환
            
            if fields:
                # UDF 호출: payload 컬럼을 변환
                params_str = json.dumps(fields)
                df = df.withColumn("payload", transform_udf(col("payload"), lit("select-fields"), lit(params_str)))
                
        elif t_type == "drop-columns":
            columns = config.get("columns", [])
            if columns:
                params_str = json.dumps(columns)
                df = df.withColumn("payload", transform_udf(col("payload"), lit("drop-columns"), lit(params_str)))
        
        # Filter (JSON 값 기반 필터링)
        # 스키마를 모르므로 get_json_object로 추출해서 비교해야 함
        elif t_type == "filter":
            # 예: amount > 1000
            # condition 문자열을 Spark SQL로 변환하려면 'amount'를 'get_json_object(payload, "$.amount")' 로 치환해야 함.
            # 복잡하므로 MVP에서는 'Selected Fields'만 완벽 지원하는 것으로 타협.
            pass
            
    return df

def process_batch(batch_df, batch_id, tables_config: list, connection_name: str):
    """
    Micro-Batch 처리 함수 (foreachBatch)
    """
    logger.info(f"=== Batch {batch_id} Processing Start ===")
    
    if batch_df.isEmpty():
        logger.info("Batch is empty. Skipping.")
        return

    # 캐싱
    batch_df.persist()
    
    try:
        for table_cfg in tables_config:
            schema_name = table_cfg.get("schema", "public")
            table_name = table_cfg.get("table", "unknown")
            output_path = table_cfg.get("target_path")
            transforms = table_cfg.get("transforms", [])
            
            target_topic = f"{connection_name}.{schema_name}.{table_name}"
            # logger.info(f"Processing Table: {table_name} (Topic: {target_topic})")
            
            # 1. 해당 토픽 데이터만 필터링
            table_df = batch_df.filter(col("topic") == target_topic)
            
            if table_df.isEmpty():
                continue
            
            logger.info(f"Found data for {table_name}. Applying transforms...")

            # 2. JSON 파싱 및 구조화 (DELETE도 포함)
            processed_df = table_df.selectExpr("CAST(value AS STRING) as json_str") \
                .withColumn("op", expr("get_json_object(json_str, '$.op')")) \
                .filter(col("op").isin(["c", "u", "r", "d"])) \
                .select(
                    # DELETE는 before 필드 사용, 나머지는 after 필드 사용
                    expr("""
                        CASE 
                            WHEN get_json_object(json_str, '$.op') = 'd' 
                            THEN get_json_object(json_str, '$.before')
                            ELSE get_json_object(json_str, '$.after')
                        END
                    """).alias("payload"),
                    col("op").alias("operation"),
                    (expr("get_json_object(json_str, '$.ts_ms')").cast(LongType()) / 1000).cast(TimestampType()).alias("event_time"),
                    to_date((expr("get_json_object(json_str, '$.ts_ms')").cast(LongType()) / 1000).cast(TimestampType())).alias("event_date")
                )
            
            # 3. Transform 적용 (UDF)
            if transforms:
                processed_df = apply_transforms(processed_df, transforms)
            
            # 4. S3 저장
            processed_df.write \
                .mode("append") \
                .partitionBy("event_date") \
                .parquet(output_path)
                
            logger.info(f"Saved {table_name} to {output_path}")
            
    except Exception as e:
        logger.error(f"Batch Processing Failed: {e}")
        raise e
    finally:
        batch_df.unpersist()

def run_pipeline(config: dict):
    connection_name = config.get("connection_name")
    tables = config.get("tables", [])

    if not connection_name or not tables:
        logger.error("Invalid Config: connection_name or tables missing")
        sys.exit(1)

    logger.info(f"=== Unified CDC Pipeline Start: {connection_name} ===")
    logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Environment: {ENVIRONMENT}")

    connection_id = config.get("connection_id", "unknown")

    # SparkSession 설정 (K8s에서는 IAM Role 사용)
    builder = SparkSession.builder.appName(f"CDC-Unified-{connection_id}")

    if ENVIRONMENT == "development":
        # Docker Compose: LocalStack 사용
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localstack-main:4566") \
            .config("spark.hadoop.fs.s3a.access.key", "test") \
            .config("spark.hadoop.fs.s3a.secret.key", "test") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true")

    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    kafka_pattern = f"{connection_name}.*"
    logger.info(f"Subscribing to pattern: {kafka_pattern}")

    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribePattern", kafka_pattern) \
        .option("startingOffsets", "earliest") \
        .load()
        
    query = kafka_stream.writeStream \
        .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, tables, connection_name)) \
        .option("checkpointLocation", f"s3a://warehouse/checkpoints/{connection_id}/") \
        .trigger(processingTime="5 seconds") \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit unified_cdc_runner.py '<config_json>'")
        sys.exit(1)
    
    try:
        config_str = sys.argv[1]
        logger.info(f"Config: {config_str}")
        config = json.loads(config_str)
        run_pipeline(config)
    except Exception as e:
        logger.error(f"Pipeline Failed: {e}")
        raise
