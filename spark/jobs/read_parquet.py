from pyspark.sql import SparkSession
import sys
import os

# Spark Session 생성 (S3 설정 포함)
spark = SparkSession.builder \
    .appName("Read S3 Parquet") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack-main:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 읽을 경로
path = "s3a://etl-output/products/"

print(f"📖 Reading Parquet from: {path}")

try:
    df = spark.read.parquet(path)
    
    print("\n[Schema]")
    df.printSchema()
    
    print(f"\n[Data Preview] - Total Rows: {df.count()}")
    df.show(truncate=False)
except Exception as e:
    print(f"❌ Error reading parquet: {e}")

spark.stop()
