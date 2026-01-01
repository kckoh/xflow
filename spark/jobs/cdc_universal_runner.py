"""
Universal CDC Streaming Runner
- Subscribes to all CDC topics using pattern matching
- Stores raw Debezium JSON with metadata
- Works with any table schema
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, regexp_extract, current_timestamp, coalesce, when
from pyspark.sql.types import StringType

# 1. Spark Session Init
spark = SparkSession.builder \
    .appName("Universal_CDC_Runner") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack-main:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Read from Kafka - Subscribe to ALL CDC topics via pattern
# Pattern: dbserver1.public.* (PostgreSQL) or mongoserver1.*.* (MongoDB)
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribePattern", "dbserver1\\.public\\..*|mongoserver1\\..*\\..*") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# 3. Transform - Extract metadata and keep raw JSON
# Schema: op, source_type, database, table_name, key_id, data_json, kafka_ts
processed_df = kafka_df.select(
    # Kafka metadata
    col("topic"),
    col("timestamp").alias("kafka_ts"),
    col("value").cast("string").alias("raw_json")
).withColumn(
    # Extract operation type (c, u, d, r)
    "op", get_json_object(col("raw_json"), "$.payload.op")
).withColumn(
    # Extract table name from topic (e.g., "dbserver1.public.users" -> "users")
    "table_name", regexp_extract(col("topic"), r"\.([^.]+)$", 1)
).withColumn(
    # Extract database/schema from topic
    "schema_name", regexp_extract(col("topic"), r"\.([^.]+)\.[^.]+$", 1)
).withColumn(
    # Determine source type from topic prefix
    "source_type", 
    regexp_extract(col("topic"), r"^([^.]+)", 1)
).withColumn(
    # Extract the data payload - use 'after' for c/u, 'before' for d
    "data_json", 
    coalesce(
        get_json_object(col("raw_json"), "$.payload.after"),
        get_json_object(col("raw_json"), "$.payload.before")
    )
).withColumn(
    # Add processing timestamp
    "processed_at", current_timestamp()
)

# 4. Select final columns
output_df = processed_df.select(
    "op",
    "source_type",
    "schema_name",
    "table_name",
    "data_json",
    "kafka_ts",
    "processed_at"
)

# 5. Write to S3 - Partition by source_type and table_name
query = output_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3a://warehouse/cdc_raw/") \
    .option("checkpointLocation", "s3a://warehouse/checkpoints/cdc_universal/") \
    .partitionBy("source_type", "table_name") \
    .trigger(processingTime="10 seconds") \
    .start()

print(">>> Universal CDC Streaming Started!")
print(">>> Listening to: dbserver1.public.* and mongoserver1.*.*")
print(">>> Output: s3a://warehouse/cdc_raw/{source_type}/{table_name}/")
query.awaitTermination()
