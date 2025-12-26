#!/usr/bin/env python3
"""
Test Data Generator for MinIO Data Lake

This script:
1. Generates sample Parquet files with test data
2. Uploads them to MinIO with Hive-style partitioning
3. Creates metadata files for partition information
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from datetime import datetime, timedelta
import json
import random
import sys
from pathlib import Path

# Configuration
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "datalake"


def create_minio_client():
    """Create and return MinIO client"""
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def generate_sales_data(num_rows=1000):
    """Generate sample sales data"""
    print(f"📊 Generating {num_rows} rows of sales data...")

    # Generate dates for the last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    data = {
        'transaction_id': [f'TXN{i:06d}' for i in range(num_rows)],
        'product_id': [f'PROD{random.randint(1, 100):03d}' for _ in range(num_rows)],
        'customer_id': [f'CUST{random.randint(1, 500):04d}' for _ in range(num_rows)],
        'quantity': [random.randint(1, 10) for _ in range(num_rows)],
        'price': [round(random.uniform(10.0, 500.0), 2) for _ in range(num_rows)],
        'timestamp': [start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        ) for _ in range(num_rows)]
    }

    df = pd.DataFrame(data)

    # Calculate total amount
    df['total_amount'] = df['quantity'] * df['price']

    # Add partition columns
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day

    return df


def generate_users_data(num_rows=500):
    """Generate sample users data"""
    print(f"👥 Generating {num_rows} rows of users data...")

    first_names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia']

    data = {
        'user_id': [f'USER{i:04d}' for i in range(num_rows)],
        'first_name': [random.choice(first_names) for _ in range(num_rows)],
        'last_name': [random.choice(last_names) for _ in range(num_rows)],
        'email': [f'user{i}@example.com' for i in range(num_rows)],
        'age': [random.randint(18, 70) for _ in range(num_rows)],
        'city': [random.choice(cities) for _ in range(num_rows)],
        'signup_date': [datetime.now() - timedelta(days=random.randint(1, 365)) for _ in range(num_rows)]
    }

    df = pd.DataFrame(data)

    # Add partition columns
    df['signup_year'] = df['signup_date'].dt.year
    df['signup_month'] = df['signup_date'].dt.month

    return df


def upload_parquet_to_minio(client, table_name, df, partition_cols, temp_dir="./temp"):
    """Upload DataFrame as Parquet to MinIO with partitioning"""

    # Create temp directory
    Path(temp_dir).mkdir(exist_ok=True)

    # Get unique partition values
    if partition_cols:
        partition_groups = df.groupby(partition_cols)
    else:
        partition_groups = [(None, df)]

    uploaded_files = []

    for partition_values, group_df in partition_groups:
        # Build partition path
        if partition_cols and partition_values is not None:
            if isinstance(partition_values, tuple):
                partition_path = "/".join([
                    f"{col}={val}" for col, val in zip(partition_cols, partition_values)
                ])
            else:
                partition_path = f"{partition_cols[0]}={partition_values}"
        else:
            partition_path = ""

        # Remove partition columns from data (Hive-style partitioning)
        data_df = group_df.drop(columns=partition_cols) if partition_cols else group_df

        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data_{timestamp}_{random.randint(1000, 9999)}.parquet"

        # Full object path
        if partition_path:
            object_path = f"{table_name}/{partition_path}/{filename}"
        else:
            object_path = f"{table_name}/{filename}"

        # Save to temp file
        temp_file = f"{temp_dir}/{filename}"
        table = pa.Table.from_pandas(data_df)
        pq.write_table(table, temp_file, compression='snappy')

        # Upload to MinIO
        print(f"   📤 Uploading: {object_path}")
        client.fput_object(
            bucket_name=BUCKET_NAME,
            object_name=object_path,
            file_path=temp_file
        )

        uploaded_files.append(object_path)

        # Clean up temp file
        Path(temp_file).unlink()

    # Clean up temp directory
    Path(temp_dir).rmdir()

    return uploaded_files


def create_metadata_file(client, table_name, partition_cols, description=""):
    """Create and upload metadata file for the table"""
    metadata = {
        "table_name": table_name,
        "partition_columns": partition_cols,
        "description": description,
        "created_at": datetime.now().isoformat(),
        "format": "parquet",
        "compression": "snappy"
    }

    metadata_json = json.dumps(metadata, indent=2)
    metadata_path = f"{table_name}/_metadata.json"

    print(f"   📝 Creating metadata file: {metadata_path}")

    # Upload metadata
    from io import BytesIO
    client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=metadata_path,
        data=BytesIO(metadata_json.encode()),
        length=len(metadata_json)
    )


def main():
    """Main function"""
    print("🚀 MinIO Test Data Generator")
    print("=" * 50)
    print()

    # Create MinIO client
    try:
        client = create_minio_client()
        print(f"Connected to MinIO at {MINIO_ENDPOINT}")
    except Exception as e:
        print(f"Failed to connect to MinIO: {e}")
        print("   Make sure MinIO is running (docker-compose up -d)")
        sys.exit(1)

    # Ensure bucket exists
    if not client.bucket_exists(BUCKET_NAME):
        print(f"Bucket '{BUCKET_NAME}' does not exist")
        sys.exit(1)

    print(f"Bucket '{BUCKET_NAME}' exists")
    print()

    # Test 1: Sales table with year/month/day partitions
    print("Test 1: Sales Table (partitioned by year, month, day)")
    print("-" * 50)
    sales_df = generate_sales_data(num_rows=1000)
    sales_files = upload_parquet_to_minio(
        client,
        "sales",
        sales_df,
        partition_cols=['year', 'month', 'day']
    )
    create_metadata_file(
        client,
        "sales",
        partition_cols=['year', 'month', 'day'],
        description="Sales transaction data"
    )
    print(f"Uploaded {len(sales_files)} partition(s) to sales table")
    print()

    # Test 2: Users table with signup_year/signup_month partitions
    print("Test 2: Users Table (partitioned by signup_year, signup_month)")
    print("-" * 50)
    users_df = generate_users_data(num_rows=500)
    users_files = upload_parquet_to_minio(
        client,
        "users_data",
        users_df,
        partition_cols=['signup_year', 'signup_month']
    )
    create_metadata_file(
        client,
        "users_data",
        partition_cols=['signup_year', 'signup_month'],
        description="User registration data"
    )
    print(f"Uploaded {len(users_files)} partition(s) to users_data table")
    print()

    # Test 3: Simple table without partitions
    print("Test 3: Products Table (no partitions)")
    print("-" * 50)
    products_df = pd.DataFrame({
        'product_id': [f'PROD{i:03d}' for i in range(100)],
        'product_name': [f'Product {i}' for i in range(100)],
        'category': [random.choice(['Electronics', 'Clothing', 'Food', 'Books']) for _ in range(100)],
        'price': [round(random.uniform(5.0, 1000.0), 2) for _ in range(100)]
    })
    products_files = upload_parquet_to_minio(
        client,
        "products",
        products_df,
        partition_cols=None
    )
    create_metadata_file(
        client,
        "products",
        partition_cols=[],
        description="Product catalog"
    )
    print(f"Uploaded {len(products_files)} file(s) to products table")
    print()

    print("=" * 50)
    print("Test data generation complete!")
    print()
    print("Summary:")
    print(f"   - Sales table: {len(sales_files)} partitions")
    print(f"   - Users table: {len(users_files)} partitions")
    print(f"   - Products table: {len(products_files)} files")
    print()
    print("Next steps:")
    print("   1. Check MinIO: http://localhost:9001")
    print("   2. Start FastAPI: uvicorn main:app --reload")
    print("   3. Check logs to see automatic table creation!")
    print("   4. View history: http://localhost:8000/api/data-lake/history")


if __name__ == "__main__":
    main()
