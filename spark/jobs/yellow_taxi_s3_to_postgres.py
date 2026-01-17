"""
NYC Yellow Taxi: S3 to PostgreSQL Loader
Reads parquet data from S3 and bulk loads to PostgreSQL using JDBC.

This is Step 2 after yellow_taxi_loader.py writes data to S3.
Separating read and write allows for better resource utilization.

Usage:
    spark-submit yellow_taxi_s3_to_postgres.py --s3-input s3a://xflow-benchmark/yellow_taxi/
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType


def create_spark_session(config: dict) -> SparkSession:
    """Create SparkSession optimized for JDBC writes"""

    num_executors = config.get("num_executors", 2)
    executor_cores = config.get("executor_cores", 6)
    total_cores = num_executors * executor_cores
    shuffle_partitions = max(100, total_cores * 3)

    print(f"=== Spark Configuration ===")
    print(f"   spark.sql.shuffle.partitions: {shuffle_partitions}")

    builder = SparkSession.builder \
        .appName("Yellow Taxi S3 to PostgreSQL") \
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions)) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # S3 configuration
    s3_config = config.get("s3_config", {})

    if s3_config.get("use_iam_role"):
        region = s3_config.get("region", "ap-northeast-2")
        builder = builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                   "com.amazonaws.auth.WebIdentityTokenCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")

    return builder.getOrCreate()


def run_s3_to_postgres(config: dict):
    """Load data from S3 to PostgreSQL"""
    start_time = datetime.now()

    s3_input_path = config.get("s3_input_path")
    pg_config = config.get("postgres", {})

    print("=" * 60)
    print("S3 to PostgreSQL Loader")
    print("=" * 60)
    print(f"Source: {s3_input_path}")
    print(f"Target: {pg_config.get('host')}:{pg_config.get('port')}/{pg_config.get('database')}")
    print("=" * 60)

    spark = create_spark_session(config)

    try:
        # Read from S3 (processed/ folder has consistent schema from yellow_taxi_loader)
        print(f"\nReading from S3: {s3_input_path}")
        df = spark.read.parquet(s3_input_path)

        print(f"Schema: {df.columns}")
        total_rows = df.count()
        print(f"Total rows to load: {total_rows:,}")

        # Remove metadata columns (load_year, load_month) if present
        columns_to_write = [c for c in df.columns if c not in ['load_year', 'load_month']]
        df = df.select(columns_to_write)

        # JDBC connection properties
        jdbc_url = f"jdbc:postgresql://{pg_config.get('host')}:{pg_config.get('port')}/{pg_config.get('database')}"

        jdbc_properties = {
            "user": pg_config.get("user", "postgres"),
            "password": pg_config.get("password", "mysecretpassword"),
            "driver": "org.postgresql.Driver",
            # Batch size for optimal write performance
            "batchsize": str(config.get("batch_size", 10000)),
            # Disable auto-commit for better performance
            "rewriteBatchedStatements": "true",
        }

        # Repartition for parallel writes
        # More partitions = more parallel JDBC connections
        num_partitions = config.get("write_partitions", 16)
        df = df.repartition(num_partitions)

        print(f"\nWriting to PostgreSQL with {num_partitions} parallel connections...")
        print(f"Batch size: {jdbc_properties['batchsize']}")

        # Write mode: overwrite or append
        write_mode = config.get("write_mode", "overwrite")

        df.write \
            .mode(write_mode) \
            .jdbc(
                url=jdbc_url,
                table="yellow_taxi_trips",
                properties=jdbc_properties
            )

        end_time = datetime.now()
        elapsed = end_time - start_time

        print("\n" + "=" * 60)
        print("Load Summary")
        print("=" * 60)
        print(f"Total rows: {total_rows:,}")
        print(f"Elapsed time: {elapsed}")
        if elapsed.total_seconds() > 0:
            print(f"Throughput: {total_rows / elapsed.total_seconds():,.0f} rows/sec")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S3 to PostgreSQL Loader")
    parser.add_argument("--s3-input", type=str, required=True,
                       help="S3 input path (parquet)")
    parser.add_argument("--pg-host", type=str,
                       default="xflow-benchmark.cxmkkiw0c40b.ap-northeast-2.rds.amazonaws.com",
                       help="PostgreSQL host")
    parser.add_argument("--pg-port", type=int, default=5432,
                       help="PostgreSQL port")
    parser.add_argument("--pg-database", type=str, default="postgres",
                       help="PostgreSQL database")
    parser.add_argument("--pg-user", type=str, default="postgres",
                       help="PostgreSQL user")
    parser.add_argument("--pg-password", type=str, default="mysecretpassword",
                       help="PostgreSQL password")
    parser.add_argument("--batch-size", type=int, default=10000,
                       help="JDBC batch size")
    parser.add_argument("--write-partitions", type=int, default=16,
                       help="Number of parallel JDBC connections")
    parser.add_argument("--write-mode", type=str, choices=["overwrite", "append"],
                       default="overwrite", help="Write mode")
    parser.add_argument("--use-iam-role", action="store_true",
                       help="Use IAM role (IRSA) for S3 access")

    args = parser.parse_args()

    config = {
        "s3_input_path": args.s3_input,
        "postgres": {
            "host": args.pg_host,
            "port": args.pg_port,
            "database": args.pg_database,
            "user": args.pg_user,
            "password": args.pg_password,
        },
        "batch_size": args.batch_size,
        "write_partitions": args.write_partitions,
        "write_mode": args.write_mode,
        "s3_config": {
            "use_iam_role": args.use_iam_role,
            "region": "ap-northeast-2"
        },
        "num_executors": 2,
        "executor_cores": 6
    }

    run_s3_to_postgres(config)
