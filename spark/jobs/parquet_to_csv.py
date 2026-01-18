"""
Parquet to CSV Converter - Memory Efficient with PyArrow
Read parquet with pyarrow, write CSV directly without pandas conversion.
"""

import argparse
import os
import boto3


def list_parquet_files(bucket: str, prefix: str):
    """List all parquet files in S3"""
    s3 = boto3.client('s3')
    files = []
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.parquet'):
                files.append(key)

    return sorted(files)


def get_completed_files(bucket: str, prefix: str) -> set:
    """Check which files have already been converted"""
    s3 = boto3.client('s3')
    completed = set()
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):
                filename = key.split('/')[-1].replace('.csv', '.parquet')
                completed.add(filename)

    return completed


def process_single_file(args):
    """
    Process one parquet file to CSV using PyArrow (memory efficient).
    Uses pyarrow CSV writer directly - no pandas conversion.
    """
    import pyarrow.parquet as pq
    import pyarrow.csv as pa_csv
    from io import BytesIO
    import boto3

    input_bucket, input_key, output_bucket, output_prefix, columns_to_exclude = args

    filename = input_key.split('/')[-1].replace('.parquet', '.csv')
    output_key = f"{output_prefix}{filename}"

    try:
        # Read parquet from S3 using PyArrow
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=input_bucket, Key=input_key)
        parquet_data = BytesIO(response['Body'].read())

        # Read with pyarrow
        table = pq.read_table(parquet_data)

        # Remove partition columns
        columns = [c for c in table.column_names if c not in columns_to_exclude]
        table = table.select(columns)

        num_rows = table.num_rows
        if num_rows == 0:
            return (filename, 0, "empty")

        # Write CSV to memory buffer using PyArrow (no pandas!)
        csv_buffer = BytesIO()
        write_options = pa_csv.WriteOptions(include_header=False)
        pa_csv.write_csv(table, csv_buffer, write_options=write_options)

        # Upload to S3
        csv_buffer.seek(0)
        s3.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )

        # Free memory
        del table
        del parquet_data
        del csv_buffer

        return (filename, num_rows, "success")

    except Exception as e:
        return (filename, 0, f"error: {str(e)[:100]}")


def parse_s3_path(s3_path: str):
    """Parse s3://bucket/prefix into (bucket, prefix)"""
    path = s3_path.replace('s3a://', '').replace('s3://', '')
    parts = path.split('/', 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    return bucket, prefix


def main():
    parser = argparse.ArgumentParser(description="Parquet to CSV - Memory Efficient")
    parser.add_argument("--input", type=str, required=True, help="S3 parquet input path")
    parser.add_argument("--output", type=str, required=True, help="S3 CSV output path")
    parser.add_argument("--use-iam-role", action="store_true", help="Use IAM role for S3")
    parser.add_argument("--resume", action="store_true", help="Skip already converted files")
    args = parser.parse_args()

    print("=" * 60)
    print("Parquet to CSV - Memory Efficient PyArrow")
    print("=" * 60)
    print(f"Input:  {args.input}")
    print(f"Output: {args.output}")
    print(f"Resume: {args.resume}")
    print("=" * 60)

    # Parse paths
    input_bucket, input_prefix = parse_s3_path(args.input)
    output_bucket, output_prefix = parse_s3_path(args.output)

    # List parquet files
    print(f"\nListing parquet files in s3://{input_bucket}/{input_prefix}...")
    parquet_files = list_parquet_files(input_bucket, input_prefix)
    print(f"Found {len(parquet_files)} parquet files")

    # Check completed files if resume mode
    completed = set()
    if args.resume:
        completed = get_completed_files(output_bucket, output_prefix)
        print(f"Already completed: {len(completed)} files")

    # Filter files to process
    files_to_process = []
    for key in parquet_files:
        filename = key.split('/')[-1]
        if filename not in completed:
            files_to_process.append(key)

    print(f"Files to process: {len(files_to_process)}")

    if not files_to_process:
        print("\nAll files already converted!")
        return

    # Create Spark session just for parallelization
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("Parquet to CSV (Memory Efficient)") \
        .getOrCreate()
    sc = spark.sparkContext

    columns_to_exclude = ['load_year', 'load_month']

    # Create arguments for each file
    file_args = [
        (input_bucket, key, output_bucket, output_prefix, columns_to_exclude)
        for key in files_to_process
    ]

    try:
        # Distribute file list - one task per file
        # Use fewer partitions to reduce concurrent memory usage
        num_partitions = min(len(file_args), 50)
        print(f"\nProcessing {len(files_to_process)} files with {num_partitions} parallel tasks...")

        results_rdd = sc.parallelize(file_args, numSlices=num_partitions)
        results = results_rdd.map(process_single_file).collect()

        # Summary
        success = sum(1 for r in results if r[2] == "success")
        total_rows = sum(r[1] for r in results)
        errors = [r for r in results if r[2].startswith("error")]

        print("\n" + "=" * 60)
        print(f"DONE! Converted {success}/{len(files_to_process)} files")
        print(f"Total rows: {total_rows:,}")

        if errors:
            print(f"\nErrors ({len(errors)}):")
            for filename, _, err in errors[:10]:
                print(f"  - {filename}: {err}")

        print("=" * 60)

    except Exception as e:
        print(f"\nError: {e}")
        print("Run with --resume to continue from last completed file")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
