#!/usr/bin/env python3
"""
Generate benchmark data using Spark and write to S3 as CSV.
Then use aws_s3.table_import_from_s3() to load into PostgreSQL.

Usage:
    spark-submit generate_benchmark.py --size 10gb
    spark-submit generate_benchmark.py --size 100gb
"""

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    expr, rand, floor, md5, concat, lit, col,
    current_timestamp, monotonically_increasing_id
)

# Data size configurations
DATA_SIZES = {
    "1gb": 5_000_000,
    "10gb": 50_000_000,
    "30gb": 150_000_000,
    "50gb": 250_000_000,
    "100gb": 500_000_000,
}

def generate_benchmark_data(spark, num_rows, num_partitions):
    """Generate benchmark DataFrame"""
    print(f"Generating {num_rows:,} rows with {num_partitions} partitions...")

    # Create base DataFrame with IDs
    df = spark.range(1, num_rows + 1, numPartitions=num_partitions)

    # Add columns matching benchmark table schema
    df = df \
        .withColumn("user_id", expr("uuid()")) \
        .withColumn("created_at",
            expr("current_timestamp() - make_interval(0, 0, 0, cast(floor(rand() * 365) as int), cast(floor(rand() * 24) as int), cast(floor(rand() * 60) as int), cast(floor(rand() * 60) as int))")) \
        .withColumn("event_type",
            expr("array('purchase','view','click','signup','logout','search','add_to_cart','checkout')[cast(floor(rand() * 8) as int)]")) \
        .withColumn("amount",
            expr("cast(round(rand() * 10000, 2) as decimal(10,2))")) \
        .withColumn("metadata",
            expr("concat(md5(cast(rand() as string)), md5(cast(rand() as string)), md5(cast(rand() as string)))"))

    return df


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark data with Spark")
    parser.add_argument(
        "--size",
        choices=list(DATA_SIZES.keys()),
        required=True,
        help="Data size to generate (1gb, 10gb, 30gb, 50gb, 100gb)",
    )
    parser.add_argument(
        "--output",
        default="s3a://xflow-output/benchmark",
        help="S3 output path prefix",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=None,
        help="Number of partitions (default: auto based on size)",
    )
    args = parser.parse_args()

    num_rows = DATA_SIZES[args.size]

    # Auto-calculate partitions if not specified
    if args.partitions:
        num_partitions = args.partitions
    else:
        # Roughly 1 partition per 1M rows
        num_partitions = max(10, num_rows // 1_000_000)

    output_path = f"{args.output}_{args.size}_csv"

    print(f"=" * 60)
    print(f"Generating {args.size.upper()} benchmark data")
    print(f"Rows: {num_rows:,}")
    print(f"Partitions: {num_partitions}")
    print(f"Output: {output_path}")
    print(f"=" * 60)

    # Create Spark session
    spark = SparkSession.builder \
        .appName(f"Generate Benchmark {args.size}") \
        .getOrCreate()

    # Generate data
    df = generate_benchmark_data(spark, num_rows, num_partitions)

    # Show sample
    print("\nSample data:")
    df.show(5, truncate=False)

    # Write to S3 as CSV (for PostgreSQL COPY)
    print(f"\nWriting to {output_path}...")
    df.write \
        .option("header", "false") \
        .option("delimiter", ",") \
        .mode("overwrite") \
        .csv(output_path)

    print(f"\n✅ Data written to {output_path}")
    print(f"\nNext steps:")
    print(f"1. Create table in PostgreSQL:")
    print(f"""
   CREATE TABLE IF NOT EXISTS benchmark_{args.size} (
       id BIGINT PRIMARY KEY,
       user_id UUID NOT NULL,
       created_at TIMESTAMP NOT NULL,
       event_type VARCHAR(50) NOT NULL,
       amount DECIMAL(10,2),
       metadata TEXT
   );
""")
    print(f"2. Import from S3:")
    print(f"""
   SELECT aws_s3.table_import_from_s3(
       'benchmark_{args.size}',
       'id,user_id,created_at,event_type,amount,metadata',
       '(FORMAT csv)',
       aws_commons.create_s3_uri(
           'xflow-output',
           'benchmark_{args.size}_csv/',
           'ap-northeast-2'
       )
   );
""")

    spark.stop()
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
