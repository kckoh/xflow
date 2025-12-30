"""
Generic ETL Runner for Spark
Accepts job configuration as JSON and executes ETL pipeline.

Usage:
    spark-submit etl_runner.py '<json_config>'
    spark-submit etl_runner.py --config-file /path/to/config.json
"""

import sys
import json
from pyspark.sql import SparkSession, DataFrame


# ============ Transform Registry ============

def transform_select_fields(df: DataFrame, config: dict) -> DataFrame:
    """Select specific columns from DataFrame"""
    columns = config.get("selectedColumns", [])
    if columns:
        return df.select(*columns)
    return df


def transform_drop_columns(df: DataFrame, config: dict) -> DataFrame:
    """Drop specified columns from DataFrame"""
    columns = config.get("columns", [])
    for col in columns:
        df = df.drop(col)
    return df


def transform_filter(df: DataFrame, config: dict) -> DataFrame:
    """Filter rows based on SQL expression"""
    expression = config.get("expression", "")
    if expression:
        return df.filter(expression)
    return df


# Single-input transforms
TRANSFORMS = {
    "select-fields": transform_select_fields,
    "drop-columns": transform_drop_columns,
    "filter": transform_filter,
}


# ============ Multi-Input Transforms ============

def transform_union(dfs: list, config: dict) -> DataFrame:
    """
    Union multiple DataFrames.
    Uses unionByName with allowMissingColumns=True to handle schema differences.
    Missing columns will be filled with NULL.
    """
    if len(dfs) < 2:
        raise ValueError("Union requires at least 2 DataFrames")

    print(f"🔗 Unioning {len(dfs)} DataFrames")

    # Start with first DataFrame
    result = dfs[0]

    # Union with remaining DataFrames using unionByName
    # allowMissingColumns=True fills missing columns with NULL
    for i, df in enumerate(dfs[1:], start=2):
        print(f"   Merging DataFrame {i}...")
        result = result.unionByName(df, allowMissingColumns=True)

    print(f"   Union complete. Total columns: {len(result.columns)}")
    return result


# Multi-input transforms (require special handling)
MULTI_INPUT_TRANSFORMS = {
    "union": transform_union,
}


# ============ Source Readers ============

def read_rdb_source(spark: SparkSession, source_config: dict) -> DataFrame:
    """Read data from relational database using JDBC"""
    connection = source_config.get("connection", {})

    # Build JDBC URL
    db_type = connection.get("type", "postgres")
    host = connection.get("host", "postgres")
    port = connection.get("port", 5432)
    database = connection.get("database_name", "mydb")

    if db_type == "postgres":
        jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        driver = "org.postgresql.Driver"
    elif db_type in ["mysql", "mariadb"]:
        jdbc_url = f"jdbc:mysql://{host}:{port}/{database}"
        driver = "com.mysql.cj.jdbc.Driver"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    # Read from table or query
    table = source_config.get("table")
    query = source_config.get("query")

    reader = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("user", connection.get("user_name", "postgres")) \
        .option("password", connection.get("password", "postgres")) \
        .option("driver", driver) \
        .option("fetchsize", "10000")

    if query:
        reader = reader.option("dbtable", f"({query}) as subquery")
    elif table:
        reader = reader.option("dbtable", table)
        # Add partitioning for large tables (parallel read)
        partition_column = source_config.get("partition_column", "id")
        num_partitions = source_config.get("num_partitions", 16)  # More partitions for large tables
        reader = reader \
            .option("numPartitions", num_partitions) \
            .option("partitionColumn", partition_column) \
            .option("lowerBound", "1") \
            .option("upperBound", "10000000")
    else:
        raise ValueError("Either 'table' or 'query' must be specified in source config")

    return reader.load()


# ============ Destination Writers ============

def write_s3_destination(df: DataFrame, dest_config: dict, job_name: str = "output"):
    """Write DataFrame to S3 as Parquet"""
    path = dest_config.get("path")
    if not path:
        raise ValueError("Destination path is required")

    # Convert s3:// to s3a:// (Spark requires s3a scheme)
    if path.startswith("s3://"):
        path = path.replace("s3://", "s3a://", 1)

    # If path ends with / (bucket root), append job name as folder
    if path.endswith("/"):
        path = path + job_name
    elif not path.split("/")[-1]:  # Empty last segment
        path = path.rstrip("/") + "/" + job_name

    options = dest_config.get("options", {})
    compression = options.get("compression", "snappy")
    mode = options.get("mode", "overwrite")
    partition_by = options.get("partitionBy", [])
    coalesce_num = options.get("coalesce")  # None means use default partitions

    # For large datasets, keep original partitions to avoid shuffle OOM
    if coalesce_num:
        writer = df.coalesce(coalesce_num).write
    else:
        writer = df.write  # Keep original 16 partitions from JDBC read

    writer = writer \
        .mode(mode) \
        .option("compression", compression)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.parquet(path)
    print(f"✅ Data written to {path}")


# ============ Main ETL Runner ============

def create_spark_session(config: dict) -> SparkSession:
    """Create SparkSession with S3 configuration"""
    dest = config.get("destination", {})

    builder = SparkSession.builder.appName(f"ETL: {config.get('name', 'Unknown')}")

    # S3 configuration for LocalStack/MinIO
    if dest.get("type") == "s3":
        s3_config = dest.get("s3_config", {})
        endpoint = s3_config.get("endpoint", "http://localstack-main:4566")
        access_key = s3_config.get("access_key", "test")
        secret_key = s3_config.get("secret_key", "test")

        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") \
            .config("spark.hadoop.fs.s3a.multipart.size", "5M")

    return builder.getOrCreate()


def apply_transforms(df: DataFrame, transforms: list) -> DataFrame:
    """Apply a chain of transformations to DataFrame"""
    for transform in transforms:
        transform_type = transform.get("type")
        transform_config = transform.get("config", {})

        if transform_type not in TRANSFORMS:
            print(f"⚠️ Unknown transform type: {transform_type}, skipping...")
            continue

        print(f"📝 Applying transform: {transform_type}")
        df = TRANSFORMS[transform_type](df, transform_config)

    return df


def run_etl(config: dict):
    """Main ETL execution function with multi-source support"""
    print(f"🚀 Starting ETL job: {config.get('name', 'Unknown')}")

    # Create Spark session
    spark = create_spark_session(config)

    try:
        # Dictionary to store DataFrames by nodeId
        dataframes = {}

        # Handle multiple sources (new) or single source (legacy)
        sources = config.get("sources", [])
        if not sources and config.get("source"):
            # Legacy single source - wrap in list
            sources = [config.get("source")]

        # Read all sources
        print(f"📖 Reading {len(sources)} source(s)...")
        for idx, source_config in enumerate(sources):
            node_id = source_config.get("nodeId", f"source_{idx}")
            source_type = source_config.get("type", "rdb")

            print(f"   [{node_id}] Reading from {source_type}: {source_config.get('table', 'query')}")
            if source_type == "rdb":
                df = read_rdb_source(spark, source_config)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")

            dataframes[node_id] = df
            print(f"   [{node_id}] Schema:")
            df.printSchema()

        # Apply transforms
        transforms = config.get("transforms", [])
        last_node_id = list(dataframes.keys())[-1] if dataframes else None

        for transform in transforms:
            node_id = transform.get("nodeId", f"transform_{len(dataframes)}")
            transform_type = transform.get("type")
            transform_config = transform.get("config", {})
            input_node_ids = transform.get("inputNodeIds", [])

            print(f"📝 [{node_id}] Applying transform: {transform_type}")

            if transform_type in MULTI_INPUT_TRANSFORMS:
                # Multi-input transform (like union)
                if not input_node_ids:
                    raise ValueError(f"Transform {transform_type} requires inputNodeIds")

                input_dfs = []
                for input_id in input_node_ids:
                    if input_id not in dataframes:
                        raise ValueError(f"Input node {input_id} not found for transform {node_id}")
                    input_dfs.append(dataframes[input_id])

                if len(input_dfs) < 2:
                    raise ValueError(f"Transform {transform_type} requires at least 2 inputs, got {len(input_dfs)}")

                result_df = MULTI_INPUT_TRANSFORMS[transform_type](input_dfs, transform_config)

            elif transform_type in TRANSFORMS:
                # Single-input transform
                if input_node_ids:
                    input_df = dataframes[input_node_ids[0]]
                elif last_node_id:
                    input_df = dataframes[last_node_id]
                else:
                    raise ValueError(f"No input available for transform {node_id}")

                result_df = TRANSFORMS[transform_type](input_df, transform_config)

            else:
                print(f"⚠️ Unknown transform type: {transform_type}, skipping...")
                continue

            dataframes[node_id] = result_df
            last_node_id = node_id

        # Get final DataFrame for destination
        if not last_node_id:
            raise ValueError("No data to write to destination")

        final_df = dataframes[last_node_id]

        # Write to destination
        dest_config = config.get("destination", {})
        dest_type = dest_config.get("type", "s3")

        print(f"💾 Writing to destination: {dest_type}")
        if dest_type == "s3":
            write_s3_destination(final_df, dest_config, config.get("name", "output"))
        else:
            raise ValueError(f"Unsupported destination type: {dest_type}")

        print("✅ ETL job completed successfully!")

    except Exception as e:
        print(f"❌ ETL job failed: {str(e)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit etl_runner.py '<json_config>'")
        print("       spark-submit etl_runner.py --config-file /path/to/config.json")
        sys.exit(1)

    # Parse config from command line
    if sys.argv[1] == "--config-file":
        config_path = sys.argv[2]
        with open(config_path, "r") as f:
            config = json.load(f)
    else:
        config = json.loads(sys.argv[1])

    run_etl(config)
