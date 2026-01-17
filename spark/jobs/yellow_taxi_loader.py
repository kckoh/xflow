"""
NYC Yellow Taxi Data Loader
Downloads NYC Yellow Taxi parquet files (2011-2024) and saves to S3,
then bulk loads to PostgreSQL using COPY command.

Architecture:
1. Spark reads parquet files directly from CloudFront CDN (parallel download)
2. Normalizes column names across different years
3. Saves to S3 as CSV (for PostgreSQL COPY compatibility)
4. Uses psycopg2 COPY to bulk load from S3

Usage:
    spark-submit yellow_taxi_loader.py --years 2023,2024
    spark-submit yellow_taxi_loader.py --years 2011-2024
"""

import argparse
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType
)


# Target schema for yellow_taxi_trips table
TARGET_SCHEMA = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("rate_code", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("surcharge", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
])

TARGET_COLUMNS = [f.name for f in TARGET_SCHEMA.fields]

# Column mapping for different NYC Taxi schema versions
COLUMN_MAPPING = {
    # VendorID variations
    'VendorID': 'vendor_id',
    'vendorid': 'vendor_id',
    'vendor_id': 'vendor_id',

    # Pickup datetime variations
    'tpep_pickup_datetime': 'pickup_datetime',
    'Trip_Pickup_DateTime': 'pickup_datetime',
    'Pickup_DateTime': 'pickup_datetime',
    'pickup_datetime': 'pickup_datetime',

    # Dropoff datetime variations
    'tpep_dropoff_datetime': 'dropoff_datetime',
    'Trip_Dropoff_DateTime': 'dropoff_datetime',
    'Dropoff_DateTime': 'dropoff_datetime',
    'dropoff_datetime': 'dropoff_datetime',

    # Passenger count
    'passenger_count': 'passenger_count',
    'Passenger_Count': 'passenger_count',

    # Trip distance
    'trip_distance': 'trip_distance',
    'Trip_Distance': 'trip_distance',

    # Location (old schema with lat/lon)
    'pickup_longitude': 'pickup_longitude',
    'Start_Lon': 'pickup_longitude',
    'pickup_latitude': 'pickup_latitude',
    'Start_Lat': 'pickup_latitude',
    'dropoff_longitude': 'dropoff_longitude',
    'End_Lon': 'dropoff_longitude',
    'dropoff_latitude': 'dropoff_latitude',
    'End_Lat': 'dropoff_latitude',

    # Rate code
    'RatecodeID': 'rate_code',
    'RateCodeID': 'rate_code',
    'Rate_Code': 'rate_code',
    'rate_code': 'rate_code',

    # Store and forward flag
    'store_and_fwd_flag': 'store_and_fwd_flag',
    'Store_and_fwd_flag': 'store_and_fwd_flag',

    # Payment type
    'payment_type': 'payment_type',
    'Payment_Type': 'payment_type',

    # Fare components
    'fare_amount': 'fare_amount',
    'Fare_Amt': 'fare_amount',
    'Fare_amount': 'fare_amount',

    'surcharge': 'surcharge',
    'Surcharge': 'surcharge',
    'mta_tax': 'surcharge',
    'MTA_Tax': 'surcharge',

    'tip_amount': 'tip_amount',
    'Tip_Amt': 'tip_amount',
    'Tip_amount': 'tip_amount',

    'tolls_amount': 'tolls_amount',
    'Tolls_Amt': 'tolls_amount',
    'Tolls_amount': 'tolls_amount',

    'total_amount': 'total_amount',
    'Total_Amt': 'total_amount',
    'Total_amount': 'total_amount',
}


def create_spark_session(config: dict) -> SparkSession:
    """Create SparkSession with optimized settings for large data processing"""

    # Calculate optimal shuffle partitions based on executor config
    # Rule of thumb: 2-3x the total number of cores
    num_executors = config.get("num_executors", 2)
    executor_cores = config.get("executor_cores", 6)
    total_cores = num_executors * executor_cores

    # Optimal shuffle partitions: 2-3x cores, minimum 100
    shuffle_partitions = max(100, total_cores * 3)

    print(f"=== Spark Configuration ===")
    print(f"   Total cores: {total_cores}")
    print(f"   spark.sql.shuffle.partitions: {shuffle_partitions}")

    builder = SparkSession.builder \
        .appName("NYC Yellow Taxi Loader") \
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions)) \
        .config("spark.sql.files.maxPartitionBytes", "128m") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # S3 configuration
    s3_config = config.get("s3_config", {})

    if s3_config.get("use_iam_role"):
        # Production: Use IRSA
        region = s3_config.get("region", "ap-northeast-2")
        builder = builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                   "com.amazonaws.auth.WebIdentityTokenCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") \
            .config("spark.hadoop.fs.s3a.multipart.size", "64m") \
            .config("spark.hadoop.fs.s3a.multipart.threshold", "128m")
    else:
        # LocalStack or explicit credentials
        access_key = s3_config.get("access_key", "test")
        secret_key = s3_config.get("secret_key", "test")
        endpoint = s3_config.get("endpoint", "http://localstack-main:4566")

        builder = builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true")

    return builder.getOrCreate()


def normalize_columns(df):
    """Normalize column names to match target schema"""
    # Build rename mapping for existing columns
    rename_exprs = []
    mapped_columns = set()

    for col_name in df.columns:
        if col_name in COLUMN_MAPPING:
            target_name = COLUMN_MAPPING[col_name]
            if target_name not in mapped_columns:
                rename_exprs.append(F.col(col_name).alias(target_name))
                mapped_columns.add(target_name)
        elif col_name.lower() in [c.lower() for c in TARGET_COLUMNS]:
            # Case-insensitive match
            for target in TARGET_COLUMNS:
                if col_name.lower() == target.lower() and target not in mapped_columns:
                    rename_exprs.append(F.col(col_name).alias(target))
                    mapped_columns.add(target)
                    break

    # Select only mapped columns
    if rename_exprs:
        df = df.select(rename_exprs)

    # Add missing columns as NULL
    for col_name in TARGET_COLUMNS:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None))

    # Cast to target types and select in order
    result_exprs = []
    for field in TARGET_SCHEMA.fields:
        if field.name in df.columns:
            result_exprs.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            result_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))

    return df.select(result_exprs)


def parse_year_range(year_str: str) -> list:
    """Parse year range string like '2011-2024' or '2023,2024'"""
    years = []

    for part in year_str.split(','):
        part = part.strip()
        if '-' in part:
            start, end = part.split('-')
            years.extend(range(int(start), int(end) + 1))
        else:
            years.append(int(part))

    return sorted(set(years))


def generate_urls(years: list) -> list:
    """Generate URLs for all months in the given years"""
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    urls = []

    for year in years:
        for month in range(1, 13):
            urls.append({
                "url": base_url.format(year=year, month=month),
                "year": year,
                "month": month
            })

    return urls


def download_to_s3(url: str, year: int, month: int, s3_bucket: str, s3_prefix: str) -> str:
    """Download parquet file from URL and upload to S3"""
    import requests
    import boto3
    import tempfile
    import os

    s3_key = f"{s3_prefix}/raw/yellow_taxi_{year}_{month:02d}.parquet"
    s3_path = f"s3a://{s3_bucket}/{s3_key}"

    # Check if already exists in S3
    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        print(f"   Already in S3: {s3_path}")
        return s3_path
    except:
        pass  # File doesn't exist, proceed with download

    print(f"   Downloading {year}-{month:02d} from {url}...")

    try:
        # Download to temp file
        response = requests.get(url, timeout=600, stream=True)
        print(f"   Response status: {response.status_code}")
        response.raise_for_status()

        # Stream directly to S3 using multipart upload
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
            for chunk in response.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp_path = tmp.name

        size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
        print(f"   Downloaded: {size_mb:.1f} MB, uploading to S3...")

        # Upload to S3
        s3_client.upload_file(tmp_path, s3_bucket, s3_key)
        os.unlink(tmp_path)

        print(f"   Uploaded to: {s3_path}")
        return s3_path

    except requests.exceptions.HTTPError as e:
        print(f"   HTTP Error: {e}")
        raise
    except Exception as e:
        print(f"   Failed: {type(e).__name__}: {e}")
        raise


def load_and_process_file(spark: SparkSession, url: str, year: int, month: int,
                          s3_bucket: str, s3_prefix: str):
    """Download parquet file to S3 and load into Spark"""
    import traceback

    try:
        # Download to S3 first (driver-side)
        s3_path = download_to_s3(url, year, month, s3_bucket, s3_prefix)

        # Read from S3 (distributed across executors)
        df = spark.read.parquet(s3_path)
        df = normalize_columns(df)

        # Add metadata columns for partitioning
        df = df.withColumn("load_year", F.lit(year))
        df = df.withColumn("load_month", F.lit(month))

        row_count = df.count()
        print(f"   ✓ {year}-{month:02d}: {row_count:,} rows")
        return df
    except Exception as e:
        print(f"   ✗ {year}-{month:02d}: {type(e).__name__}: {e}")
        traceback.print_exc()
        return None


def run_etl(config: dict):
    """Main ETL execution - memory efficient streaming approach"""
    start_time = datetime.now()

    years = config.get("years", [2024])
    s3_output_path = config.get("s3_output_path", "s3a://xflow-benchmark/yellow_taxi/")
    output_format = config.get("output_format", "parquet")  # parquet or csv

    # S3 bucket for intermediate storage (raw downloaded files)
    s3_bucket = config.get("s3_bucket", "xflow-benchmark")
    s3_prefix = config.get("s3_prefix", "yellow_taxi")

    print("=" * 60)
    print("NYC Yellow Taxi Data Loader (Streaming Mode)")
    print("=" * 60)
    print(f"Years: {years}")
    print(f"S3 Bucket: {s3_bucket}")
    print(f"S3 Prefix: {s3_prefix}")
    print(f"Output: {s3_output_path}")
    print(f"Format: {output_format}")
    print("=" * 60)

    # Create Spark session
    spark = create_spark_session(config)

    try:
        # Generate all URLs
        urls = generate_urls(years)
        print(f"\nProcessing {len(urls)} files...")

        # Process files one by one and write immediately (memory efficient)
        success_count = 0
        failed_files = []
        total_rows = 0
        first_write = True

        for i, url_info in enumerate(urls):
            year = url_info["year"]
            month = url_info["month"]
            url = url_info["url"]

            print(f"\n[{i+1}/{len(urls)}] Processing {year}-{month:02d}...")

            try:
                # Download to S3 first
                s3_path = download_to_s3(url, year, month, s3_bucket, s3_prefix)

                # Read from S3
                df = spark.read.parquet(s3_path)
                df = normalize_columns(df)

                # Add metadata columns
                df = df.withColumn("load_year", F.lit(year))
                df = df.withColumn("load_month", F.lit(month))

                row_count = df.count()
                total_rows += row_count

                # Write immediately (first: overwrite, rest: append)
                write_mode = "overwrite" if first_write else "append"

                if output_format == "csv":
                    df.write \
                        .mode(write_mode) \
                        .option("header", "true") \
                        .option("nullValue", "\\N") \
                        .option("compression", "gzip") \
                        .partitionBy("load_year") \
                        .csv(s3_output_path)
                else:
                    df.write \
                        .mode(write_mode) \
                        .option("compression", "snappy") \
                        .partitionBy("load_year") \
                        .parquet(s3_output_path)

                first_write = False
                success_count += 1
                print(f"   ✓ {year}-{month:02d}: {row_count:,} rows written")

                # Clear DataFrame from memory
                df.unpersist()

            except Exception as e:
                import traceback
                print(f"   ✗ {year}-{month:02d}: {type(e).__name__}: {e}")
                traceback.print_exc()
                failed_files.append(f"{year}-{month:02d}")

        if success_count == 0:
            raise ValueError("No data loaded successfully")

        # Summary
        end_time = datetime.now()
        elapsed = end_time - start_time

        print("\n" + "=" * 60)
        print("ETL Summary")
        print("=" * 60)
        print(f"Total rows: {total_rows:,}")
        print(f"Success files: {success_count}")
        print(f"Failed files: {len(failed_files)}")
        if failed_files:
            print(f"Failed: {', '.join(failed_files)}")
        print(f"Output path: {s3_output_path}")
        print(f"Elapsed time: {elapsed}")
        if elapsed.total_seconds() > 0:
            print(f"Throughput: {total_rows / elapsed.total_seconds():,.0f} rows/sec")
        print("=" * 60)

        return {
            "total_rows": total_rows,
            "success_files": success_count,
            "failed_files": failed_files,
            "output_path": s3_output_path,
            "elapsed_seconds": elapsed.total_seconds()
        }

    finally:
        spark.stop()


def bulk_load_to_postgres(config: dict):
    """
    Bulk load data from S3 to PostgreSQL using aws_s3 extension or COPY.
    This function is called separately after Spark ETL completes.
    """
    import psycopg2

    pg_config = config.get("postgres", {})
    s3_path = config.get("s3_output_path")

    conn = psycopg2.connect(
        host=pg_config.get("host", "xflow-benchmark.cxmkkiw0c40b.ap-northeast-2.rds.amazonaws.com"),
        port=pg_config.get("port", 5432),
        database=pg_config.get("database", "postgres"),
        user=pg_config.get("user", "postgres"),
        password=pg_config.get("password", "mysecretpassword"),
    )

    try:
        conn.autocommit = False
        cur = conn.cursor()

        # Create table if not exists
        print("Creating table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
                vendor_id TEXT,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                passenger_count FLOAT,
                trip_distance FLOAT,
                pickup_longitude FLOAT,
                pickup_latitude FLOAT,
                rate_code FLOAT,
                store_and_fwd_flag TEXT,
                dropoff_longitude FLOAT,
                dropoff_latitude FLOAT,
                payment_type TEXT,
                fare_amount FLOAT,
                surcharge FLOAT,
                tip_amount FLOAT,
                tolls_amount FLOAT,
                total_amount FLOAT,
                load_year INT,
                load_month INT
            )
        """)

        # Check if aws_s3 extension is available
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM pg_extension WHERE extname = 'aws_s3'
            )
        """)
        has_aws_s3 = cur.fetchone()[0]

        if has_aws_s3:
            # Use aws_s3 extension for direct S3 load
            print("Using aws_s3 extension for bulk load...")
            # This requires the aws_s3 extension to be installed on RDS
            # and proper IAM role configuration
            cur.execute("""
                SELECT aws_s3.table_import_from_s3(
                    'yellow_taxi_trips',
                    '',
                    '(FORMAT CSV, HEADER true, NULL ''\\N'')',
                    aws_commons.create_s3_uri(
                        %s,  -- bucket
                        %s,  -- path
                        %s   -- region
                    )
                )
            """, (
                config.get("s3_bucket", "xflow-benchmark"),
                config.get("s3_key", "yellow_taxi/"),
                config.get("region", "ap-northeast-2")
            ))
        else:
            print("aws_s3 extension not available.")
            print("Please use Spark JDBC write or manual COPY command.")

        conn.commit()
        print("Bulk load completed!")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    import json

    parser = argparse.ArgumentParser(description="NYC Yellow Taxi Data Loader")
    parser.add_argument("--years", type=str, default="2024",
                       help="Years to load (e.g., '2024' or '2011-2024' or '2023,2024')")
    parser.add_argument("--s3-bucket", type=str, default="xflow-benchmark",
                       help="S3 bucket for intermediate storage")
    parser.add_argument("--s3-prefix", type=str, default="yellow_taxi",
                       help="S3 prefix for intermediate storage")
    parser.add_argument("--s3-output", type=str,
                       default="s3a://xflow-benchmark/yellow_taxi/processed/",
                       help="S3 output path for final processed data")
    parser.add_argument("--format", type=str, choices=["parquet", "csv"],
                       default="parquet", help="Output format")
    parser.add_argument("--partitions", type=int, default=100,
                       help="Number of output partitions")
    parser.add_argument("--config", type=str,
                       help="JSON config string or file path")
    parser.add_argument("--use-iam-role", action="store_true",
                       help="Use IAM role (IRSA) for S3 access")
    parser.add_argument("--load-to-postgres", action="store_true",
                       help="Also load data to PostgreSQL after S3 write")

    args = parser.parse_args()

    # Build config
    if args.config:
        if args.config.startswith("{"):
            config = json.loads(args.config)
        else:
            with open(args.config, "r") as f:
                config = json.load(f)
    else:
        config = {
            "years": parse_year_range(args.years),
            "s3_bucket": args.s3_bucket,
            "s3_prefix": args.s3_prefix,
            "s3_output_path": args.s3_output,
            "output_format": args.format,
            "output_partitions": args.partitions,
            "s3_config": {
                "use_iam_role": args.use_iam_role,
                "region": "ap-northeast-2"
            },
            "num_executors": 2,
            "executor_cores": 6
        }

    # Run Spark ETL
    result = run_etl(config)

    # Optional: Load to PostgreSQL
    if args.load_to_postgres:
        config["s3_output_path"] = result["output_path"]
        bulk_load_to_postgres(config)
