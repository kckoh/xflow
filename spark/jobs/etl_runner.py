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
    columns = config.get("columns", [])
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


TRANSFORMS = {
    "select-fields": transform_select_fields,
    "drop-columns": transform_drop_columns,
    "filter": transform_filter,
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
        .option("driver", driver)

    if query:
        reader = reader.option("dbtable", f"({query}) as subquery")
    elif table:
        reader = reader.option("dbtable", table)
    else:
        raise ValueError("Either 'table' or 'query' must be specified in source config")

    return reader.load()


# ============ Destination Writers ============

def write_s3_destination(df: DataFrame, dest_config: dict):
    """Write DataFrame to S3 as Parquet"""
    path = dest_config.get("path")
    if not path:
        raise ValueError("Destination path is required")

    options = dest_config.get("options", {})
    compression = options.get("compression", "snappy")
    mode = options.get("mode", "overwrite")
    partition_by = options.get("partitionBy", [])
    coalesce_num = options.get("coalesce", 1)

    writer = df.coalesce(coalesce_num).write \
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
    """Main ETL execution function"""
    print(f"🚀 Starting ETL job: {config.get('name', 'Unknown')}")

    # Create Spark session
    spark = create_spark_session(config)

    try:
        # Read source data
        source_config = config.get("source", {})
        source_type = source_config.get("type", "rdb")

        print(f"📖 Reading from source: {source_type}")
        if source_type == "rdb":
            df = read_rdb_source(spark, source_config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        print(f"📊 Source data count: {df.count()}")
        df.printSchema()

        # Apply transforms
        transforms = config.get("transforms", [])
        if transforms:
            df = apply_transforms(df, transforms)
            print(f"📊 Transformed data count: {df.count()}")

        # Write to destination
        dest_config = config.get("destination", {})
        dest_type = dest_config.get("type", "s3")

        print(f"💾 Writing to destination: {dest_type}")
        if dest_type == "s3":
            write_s3_destination(df, dest_config)
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
