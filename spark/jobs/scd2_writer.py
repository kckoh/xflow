"""
SCD Type 2 (Slowly Changing Dimension) Writer for Delta Lake

Tracks full history of changes with:
- valid_from: When this version became active
- valid_to: When this version expired (NULL for current)
- is_current: TRUE for current version, FALSE for historical

Usage:
    from scd2_writer import write_scd2_merge, detect_primary_keys

    primary_keys = detect_primary_keys(df)
    write_scd2_merge(spark, df, path, primary_keys)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime


def detect_primary_keys(df: DataFrame) -> list:
    """
    Auto-detect primary key column(s) from DataFrame schema

    Priority:
    1. Columns named 'id', 'pk', 'primary_key'
    2. Columns ending with '_id' (e.g., user_id, order_id)
    3. First column if no matches found

    Args:
        df: DataFrame to analyze

    Returns:
        List of primary key column names
    """
    columns = df.columns

    # Priority 1: Exact matches
    exact_matches = ['id', 'pk', 'primary_key', 'uid', 'uuid']
    for col in columns:
        if col.lower() in exact_matches:
            print(f"   [SCD2] Detected primary key: {col}")
            return [col]

    # Priority 2: Columns ending with _id
    id_columns = [col for col in columns if col.lower().endswith('_id')]
    if id_columns:
        print(f"   [SCD2] Detected primary key: {id_columns[0]}")
        return [id_columns[0]]

    # Priority 3: First column as fallback
    if columns:
        print(f"   [SCD2] No obvious primary key found, using first column: {columns[0]}")
        return [columns[0]]

    raise ValueError("DataFrame has no columns, cannot detect primary key")


def write_scd2_merge(spark: SparkSession, df: DataFrame, path: str, primary_keys: list = None, timestamp_col: str = None, source_types: list = None):
    """
    Write DataFrame using SCD Type 2 (Slowly Changing Dimension) pattern

    SCD Type 2 tracks full history of changes:
    - Existing records that change: Old version expires, new version inserted
    - New records: Inserted as current
    - Unchanged records: No action

    **Special handling for immutable sources (S3 logs, API):**
    - S3 log files are immutable (never updated once written)
    - API responses are snapshots at a point in time
    - Use simple append mode instead of SCD Type 2
    - Avoids unnecessary complexity and Primary Key detection for unchanging data

    Args:
        spark: SparkSession
        df: DataFrame with new/updated records
        path: Delta Lake table path
        primary_keys: List of primary key column names (auto-detected if None)
        timestamp_col: Optional timestamp column from source (for audit)
        source_types: List of source types (e.g., ['rdb', 's3', 'mongodb'])

    Example:
        >>> write_scd2_merge(spark, df, "s3a://bucket/table", primary_keys=["id"])

    Query Examples:
        # Get current data only
        SELECT * FROM table WHERE is_current = TRUE

        # Get data as of specific date
        SELECT * FROM table
        WHERE valid_from <= '2026-01-10' AND (valid_to > '2026-01-10' OR valid_to IS NULL)

        # Get full history for a specific ID
        SELECT * FROM table WHERE id = 123 ORDER BY valid_from
    """
    # Check if source is immutable (S3 logs, API responses)
    # Immutable sources: data doesn't change after collection, use simple append
    # - S3 logs: Log files never change once written
    # - API: Responses are snapshots at a point in time (e.g., GitHub issues)
    is_immutable_source = source_types and any(st in ['s3', 's3_logs', 'api'] for st in source_types)

    if is_immutable_source:
        # Immutable data sources: use simple append instead of SCD Type 2
        source_name = "API" if 'api' in source_types else "S3 Logs"
        print(f"   [{source_name}] Using append mode (immutable data, no SCD Type 2 needed)")

        df.write.format("delta").mode("append").save(path)

        record_count = df.count()
        print(f"   [{source_name}] ✅ Appended {record_count} records to {path}")
        return

    # === RDB/MongoDB: Use full SCD Type 2 ===

    current_time = datetime.utcnow()
    current_time_str = current_time.isoformat()

    # Auto-detect primary keys if not provided
    if not primary_keys:
        primary_keys = detect_primary_keys(df)

    print(f"   [SCD2] Applying SCD Type 2 MERGE")
    print(f"      Primary keys: {primary_keys}")
    print(f"      Current timestamp: {current_time_str}")

    # Validate primary keys exist in DataFrame
    missing_keys = [pk for pk in primary_keys if pk not in df.columns]
    if missing_keys:
        raise ValueError(f"Primary key columns not found in DataFrame: {missing_keys}")

    # Add SCD Type 2 metadata columns to new data
    new_data = df.withColumn("valid_from", F.lit(current_time_str)) \
                 .withColumn("valid_to", F.lit(None).cast("string")) \
                 .withColumn("is_current", F.lit(True))

    # Check if Delta table exists
    try:
        delta_table = DeltaTable.forPath(spark, path)
        table_exists = True
        print(f"   [SCD2] Existing Delta table found, performing MERGE")
    except Exception as e:
        table_exists = False
        print(f"   [SCD2] No existing table, creating new Delta table with SCD2 schema")

    if not table_exists:
        # First run: Just write the data with SCD2 columns
        new_data.write.format("delta").mode("overwrite").save(path)
        record_count = new_data.count()
        print(f"   [SCD2] ✅ Initial table created with {record_count} records")
        return

    # ========== MERGE Logic ==========

    # Build merge condition (match on primary keys and is_current=TRUE)
    merge_condition_parts = [f"target.{pk} = source.{pk}" for pk in primary_keys]
    merge_condition_parts.append("target.is_current = TRUE")
    merge_condition = " AND ".join(merge_condition_parts)

    # Build change detection condition (any non-key column changed)
    # Exclude metadata columns from change detection
    metadata_cols = ['valid_from', 'valid_to', 'is_current']
    data_columns = [col for col in df.columns if col not in primary_keys + metadata_cols]

    change_conditions = []
    for col in data_columns:
        # Handle NULL comparisons properly
        change_conditions.append(
            f"(target.{col} <> source.{col} OR "
            f"(target.{col} IS NULL AND source.{col} IS NOT NULL) OR "
            f"(target.{col} IS NOT NULL AND source.{col} IS NULL))"
        )

    change_condition = " OR ".join(change_conditions) if change_conditions else "FALSE"

    print(f"   [SCD2] Merge condition: {merge_condition}")
    print(f"   [SCD2] Tracking changes in {len(data_columns)} data columns")

    # Step 1: MERGE to expire changed records and insert truly new records
    merge_builder = delta_table.alias("target").merge(
        new_data.alias("source"),
        merge_condition
    )

    # When matched AND data changed: Expire the old record
    merge_builder = merge_builder.whenMatchedUpdate(
        condition=change_condition,
        set={
            "valid_to": F.lit(current_time_str),
            "is_current": F.lit(False)
        }
    )

    # When not matched by target: Insert new record
    merge_builder = merge_builder.whenNotMatchedInsertAll()

    # Execute merge
    merge_builder.execute()
    print(f"   [SCD2] Step 1: Expired changed records and inserted new records")

    # Step 2: Re-insert changed records as new current versions
    # Reload Delta table to get expired records
    delta_table = DeltaTable.forPath(spark, path)

    # Find records that were just expired (valid_to = current_time)
    expired_df = delta_table.toDF().filter(
        (F.col("valid_to") == current_time_str) & (F.col("is_current") == False)
    )

    expired_count = expired_df.count()

    if expired_count > 0:
        print(f"   [SCD2] Found {expired_count} changed records, inserting new versions")

        # Get primary keys of expired records
        expired_keys = expired_df.select(primary_keys)

        # Join with new data to get updated values
        join_condition = [expired_keys[pk] == new_data[pk] for pk in primary_keys]
        updated_records = new_data.join(expired_keys, primary_keys, "inner")

        # Append new versions of changed records
        updated_records.write.format("delta").mode("append").save(path)
        print(f"   [SCD2] Step 2: Inserted {updated_records.count()} new versions")

    # Summary
    delta_table = DeltaTable.forPath(spark, path)
    total_records = delta_table.toDF().count()
    current_records = delta_table.toDF().filter(F.col("is_current") == True).count()
    historical_records = total_records - current_records

    print(f"   [SCD2] ✅ MERGE completed")
    print(f"      Current records: {current_records}")
    print(f"      Historical versions: {historical_records}")
    print(f"      Total records: {total_records}")


def query_scd2_current(spark: SparkSession, path: str) -> DataFrame:
    """
    Query only current (active) records from SCD Type 2 table

    Args:
        spark: SparkSession
        path: Delta Lake table path

    Returns:
        DataFrame with only current records
    """
    return spark.read.format("delta").load(path).filter(F.col("is_current") == True)


def query_scd2_as_of(spark: SparkSession, path: str, as_of_date: str) -> DataFrame:
    """
    Query SCD Type 2 table as of a specific date (time travel)

    Args:
        spark: SparkSession
        path: Delta Lake table path
        as_of_date: ISO format date string (e.g., '2026-01-10T12:00:00')

    Returns:
        DataFrame with records valid at the specified time
    """
    df = spark.read.format("delta").load(path)

    return df.filter(
        (F.col("valid_from") <= as_of_date) &
        ((F.col("valid_to") > as_of_date) | F.col("valid_to").isNull())
    )


def query_scd2_history(spark: SparkSession, path: str, primary_key_value: any, primary_key_col: str = "id") -> DataFrame:
    """
    Query full history for a specific record

    Args:
        spark: SparkSession
        path: Delta Lake table path
        primary_key_value: Value to search for
        primary_key_col: Primary key column name

    Returns:
        DataFrame with all versions of the record, ordered by valid_from
    """
    df = spark.read.format("delta").load(path)

    return df.filter(F.col(primary_key_col) == primary_key_value).orderBy("valid_from")
