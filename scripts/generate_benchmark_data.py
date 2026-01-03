#!/usr/bin/env python3
"""
Generate benchmark data in PostgreSQL for Spark ETL testing.

Usage:
    # Generate only 1GB data
    python generate_benchmark_data.py --size 1gb

    # Generate 10GB data (separate table)
    python generate_benchmark_data.py --size 10gb

    # Generate all sizes (1gb, 10gb, 30gb, 50gb, 100gb)
    python generate_benchmark_data.py --size all

Each size creates a separate table: benchmark_1gb, benchmark_10gb, etc.
Run incrementally - start with 1gb, test, then generate larger sizes.
"""

import argparse
import os
from datetime import datetime

import psycopg2

# Database connection settings
DB_CONFIG = {
    "host": os.getenv("DB_HOST", ""),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "xflow"),
    "user": os.getenv("DB_USER", ""),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Data size configurations (rows per size)
# Approximately 200 bytes per row
DATA_SIZES = {
    "1gb": 5_000_000,
    "10gb": 50_000_000,
    "30gb": 150_000_000,
    "50gb": 250_000_000,
    "100gb": 500_000_000,
}

# Batch size for large inserts (to avoid memory issues)
BATCH_SIZE = 1_000_000


def create_table(conn, table_name):
    """Create benchmark table if not exists"""
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGSERIAL PRIMARY KEY,
            user_id UUID NOT NULL,
            created_at TIMESTAMP NOT NULL,
            event_type VARCHAR(50) NOT NULL,
            amount DECIMAL(10,2),
            metadata TEXT
        )
    """)
    conn.commit()
    cur.close()
    print(f"Table {table_name} created/verified")


def get_row_count(conn, table_name):
    """Get current row count"""
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cur.fetchone()[0]
    cur.close()
    return count


def insert_data(conn, table_name, total_rows):
    """Insert benchmark data using PostgreSQL generate_series (much faster)"""
    cur = conn.cursor()

    # Check existing rows
    existing = get_row_count(conn, table_name)
    if existing >= total_rows:
        print(
            f"Table already has {existing:,} rows (target: {total_rows:,}). Skipping."
        )
        return

    remaining = total_rows - existing
    print(f"Existing: {existing:,}, Need to insert: {remaining:,} more rows")
    print("Using PostgreSQL generate_series (server-side generation)...")

    start_time = datetime.now()
    inserted = 0

    # Insert in batches to avoid memory issues on large datasets
    while inserted < remaining:
        batch_size = min(BATCH_SIZE, remaining - inserted)

        cur.execute(f"""
            INSERT INTO {table_name} (user_id, created_at, event_type, amount, metadata)
            SELECT
                gen_random_uuid(),
                NOW() - (random() * INTERVAL '365 days'),
                (ARRAY['purchase','view','click','signup','logout','search','add_to_cart','checkout'])[floor(random()*8+1)::int],
                round((random() * 10000)::numeric, 2),
                md5(random()::text) || md5(random()::text) || md5(random()::text)
            FROM generate_series(1, {batch_size})
        """)
        conn.commit()

        inserted += batch_size
        elapsed = (datetime.now() - start_time).total_seconds()
        rows_per_sec = inserted / elapsed if elapsed > 0 else 0
        progress = (inserted / remaining) * 100

        print(
            f"Progress: {progress:.1f}% ({inserted:,}/{remaining:,}) - {rows_per_sec:.0f} rows/sec"
        )

    cur.close()

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nCompleted: {inserted:,} rows in {elapsed:.1f} seconds")


def get_table_size(conn, table_name):
    """Get table size in bytes"""
    cur = conn.cursor()
    cur.execute(f"SELECT pg_total_relation_size('{table_name}')")
    size = cur.fetchone()[0]
    cur.close()
    return size


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark data")
    parser.add_argument(
        "--size",
        choices=list(DATA_SIZES.keys()) + ["all"],
        required=True,
        help="Data size to generate (1gb, 10gb, 30gb, 50gb, 100gb, or all)",
    )
    args = parser.parse_args()

    if not DB_CONFIG["password"]:
        print("Error: DB_PASSWORD environment variable required")
        print("\nUsage:")
        print("  DB_PASSWORD=xxx python generate_benchmark_data.py --size 1gb")
        print("\nSizes available: 1gb, 10gb, 30gb, 50gb, 100gb, all")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    print(f"Connected to {DB_CONFIG['host']}/{DB_CONFIG['database']}")

    # Determine which sizes to generate
    if args.size == "all":
        sizes_to_generate = list(DATA_SIZES.keys())
    else:
        sizes_to_generate = [args.size]

    for size in sizes_to_generate:
        table_name = f"benchmark_{size}"
        total_rows = DATA_SIZES[size]

        print(f"\n{'=' * 60}")
        print(f"Generating {size.upper()} data: {total_rows:,} rows -> {table_name}")
        print(f"{'=' * 60}")

        create_table(conn, table_name)
        insert_data(conn, table_name, total_rows)

        # Verify size
        actual_size = get_table_size(conn, table_name)
        print(f"Actual table size: {actual_size / (1024**3):.2f} GB")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
