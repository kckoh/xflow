"""
NYC Yellow Taxi: S3 to PostgreSQL Loader
Reads parquet data from S3 and bulk loads to PostgreSQL using COPY (fast).

This is Step 2 after yellow_taxi_loader.py writes data to S3.
Uses PostgreSQL COPY command for 10x faster loading than JDBC.

Usage:
    spark-submit yellow_taxi_s3_to_postgres.py --s3-input s3a://xflow-benchmark/yellow_taxi/
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType


def create_spark_session(config: dict) -> SparkSession:
    """Create SparkSession optimized for COPY writes"""

    num_executors = config.get("num_executors", 2)
    executor_cores = config.get("executor_cores", 6)
    total_cores = num_executors * executor_cores
    shuffle_partitions = max(100, total_cores * 3)

    print(f"=== Spark Configuration ===")
    print(f"   spark.sql.shuffle.partitions: {shuffle_partitions}")

    builder = SparkSession.builder \
        .appName("Yellow Taxi S3 to PostgreSQL (COPY)") \
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


def make_copy_writer(pg_host, pg_port, pg_database, pg_user, pg_password, columns):
    """Create a partition writer function with connection parameters"""

    def write_partition_with_copy(iterator):
        """Write partition data using PostgreSQL COPY - much faster than JDBC"""
        import os
        import psycopg2
        from io import StringIO
        from datetime import datetime as dt

        rows = list(iterator)
        if not rows:
            return

        # Read from env vars in executor (K8s), fallback to passed params
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST", pg_host),
            port=int(os.getenv("PG_PORT", pg_port)),
            database=os.getenv("PG_DATABASE", pg_database),
            user=os.getenv("PG_USER", pg_user),
            password=os.getenv("PG_PASSWORD", pg_password)
        )

        try:
            cur = conn.cursor()

            # Build TSV data in memory
            buffer = StringIO()
            for row in rows:
                values = []
                for val in row:
                    if val is None:
                        values.append('\\N')
                    elif isinstance(val, dt):
                        values.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                    else:
                        values.append(str(val).replace('\t', ' ').replace('\n', ' '))
                buffer.write('\t'.join(values) + '\n')

            buffer.seek(0)

            # Use COPY for fast bulk insert
            cur.copy_from(
                buffer,
                'yellow_taxi_trips',
                columns=columns,
                null='\\N'
            )

            conn.commit()
        finally:
            conn.close()

    return write_partition_with_copy


def run_s3_to_postgres(config: dict):
    """Load data from S3 to PostgreSQL using COPY"""
    start_time = datetime.now()

    s3_input_path = config.get("s3_input_path")
    pg_config = config.get("postgres", {})

    print("=" * 60)
    print("S3 to PostgreSQL Loader (COPY - Fast Mode)")
    print("=" * 60)
    print(f"Source: {s3_input_path}")
    print(f"Target: {pg_config.get('host')}:{pg_config.get('port')}/{pg_config.get('database')}")
    print("=" * 60)

    spark = create_spark_session(config)

    try:
        # Read from S3
        print(f"\nReading from S3: {s3_input_path}")
        df = spark.read.parquet(s3_input_path)

        print(f"Schema: {df.columns}")
        total_rows = df.count()
        print(f"Total rows to load: {total_rows:,}")

        # Remove metadata columns
        columns_to_write = [c for c in df.columns if c not in ['load_year', 'load_month']]
        df = df.select(columns_to_write)

        # Handle overwrite mode - truncate table first
        write_mode = config.get("write_mode", "overwrite")
        if write_mode == "overwrite":
            import psycopg2
            conn = psycopg2.connect(
                host=pg_config.get("host"),
                port=pg_config.get("port"),
                database=pg_config.get("database"),
                user=pg_config.get("user"),
                password=pg_config.get("password")
            )
            cur = conn.cursor()
            print("\nTruncating table yellow_taxi_trips...")
            cur.execute("TRUNCATE TABLE yellow_taxi_trips")
            conn.commit()
            conn.close()

        # Repartition to more partitions to avoid OOM (smaller data per partition)
        num_partitions = 1000
        df = df.repartition(num_partitions)
        print(f"\nWriting to PostgreSQL with {num_partitions} parallel COPY connections...")

        # Create the copy writer function with connection params
        copy_writer = make_copy_writer(
            pg_host=pg_config.get("host"),
            pg_port=pg_config.get("port"),
            pg_database=pg_config.get("database"),
            pg_user=pg_config.get("user"),
            pg_password=pg_config.get("password"),
            columns=columns_to_write
        )

        # Use foreachPartition with COPY
        df.rdd.foreachPartition(copy_writer)

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
    import os

    parser = argparse.ArgumentParser(description="S3 to PostgreSQL Loader")
    parser.add_argument("--s3-input", type=str, required=True,
                       help="S3 input path (parquet)")
    parser.add_argument("--pg-host", type=str,
                       default=os.getenv("PG_HOST", "localhost"),
                       help="PostgreSQL host (or set PG_HOST env var)")
    parser.add_argument("--pg-port", type=int,
                       default=int(os.getenv("PG_PORT", "5432")),
                       help="PostgreSQL port (or set PG_PORT env var)")
    parser.add_argument("--pg-database", type=str,
                       default=os.getenv("PG_DATABASE", "postgres"),
                       help="PostgreSQL database (or set PG_DATABASE env var)")
    parser.add_argument("--pg-user", type=str,
                       default=os.getenv("PG_USER", "postgres"),
                       help="PostgreSQL user (or set PG_USER env var)")
    parser.add_argument("--pg-password", type=str,
                       default=os.getenv("PG_PASSWORD", ""),
                       help="PostgreSQL password (or set PG_PASSWORD env var)")
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
