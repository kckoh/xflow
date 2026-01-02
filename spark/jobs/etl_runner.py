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
    """Select specific columns from DataFrame.
    Dot notation columns are aliased with underscores (e.g., company.name → company_name)
    """
    from pyspark.sql.functions import col

    columns = config.get("selectedColumns", [])
    if columns:
        # Convert dot notation to underscore alias to avoid column name conflicts
        selections = [col(c).alias(c.replace(".", "_")) for c in columns]
        return df.select(*selections)
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


def transform_s3_select_fields(df: DataFrame, config: dict) -> DataFrame:
    """Select specific fields from S3 logs"""
    selected_fields = config.get("selected_fields", [])
    if selected_fields and len(selected_fields) > 0:
        # Validate fields exist
        available_fields = df.columns
        invalid_fields = [f for f in selected_fields if f not in available_fields]
        if invalid_fields:
            raise ValueError(f"Invalid fields: {invalid_fields}. Available: {available_fields}")
        return df.select(*selected_fields)
    return df


def transform_s3_filter(df: DataFrame, config: dict) -> DataFrame:
    """
    Filter S3 log rows based on conditions with user-defined field names.

    Config example:
    {
        "filters": {
            "statusCodeField": "status",        # User's field name for status code
            "statusCodeMin": 400,
            "statusCodeMax": 599,
            "ipField": "ip_client",             # User's field name for IP
            "ipPatterns": "192.168.*",
            "pathField": "request_path",        # User's field name for path
            "pathPattern": "/api/.*"
        }
    }
    """
    from pyspark.sql import functions as F

    filters_config = config.get("filters", {})
    if not filters_config:
        return df

    filtered_df = df

    # Status code range filter (with configurable field name)
    status_field = filters_config.get("statusCodeField")
    if status_field and status_field in df.columns:
        if "statusCodeMin" in filters_config and filters_config["statusCodeMin"]:
            min_code = int(filters_config["statusCodeMin"])
            filtered_df = filtered_df.filter(F.col(status_field) >= min_code)
            print(f"   Filter: {status_field} >= {min_code}")

        if "statusCodeMax" in filters_config and filters_config["statusCodeMax"]:
            max_code = int(filters_config["statusCodeMax"])
            filtered_df = filtered_df.filter(F.col(status_field) <= max_code)
            print(f"   Filter: {status_field} <= {max_code}")

    # IP patterns filter (with configurable field name)
    ip_field = filters_config.get("ipField")
    if ip_field and ip_field in df.columns:
        if "ipPatterns" in filters_config and filters_config["ipPatterns"]:
            ip_patterns = filters_config["ipPatterns"]
            # Support both string and list
            if isinstance(ip_patterns, str):
                patterns = [p.strip() for p in ip_patterns.split(",") if p.strip()]
            else:
                patterns = ip_patterns

            if patterns:
                # Convert wildcard patterns to regex
                regex_patterns = [p.replace("*", ".*").replace(".", r"\.") for p in patterns]
                combined_pattern = "|".join(regex_patterns)
                filtered_df = filtered_df.filter(F.col(ip_field).rlike(combined_pattern))
                print(f"   Filter: {ip_field} matches pattern: {combined_pattern}")

    # Path pattern filter (with configurable field name)
    path_field = filters_config.get("pathField")
    if path_field and path_field in df.columns:
        if "pathPattern" in filters_config and filters_config["pathPattern"]:
            path_pattern = filters_config["pathPattern"]
            filtered_df = filtered_df.filter(F.col(path_field).rlike(path_pattern))
            print(f"   Filter: {path_field} matches pattern: {path_pattern}")

    return filtered_df

# Single-input transforms
TRANSFORMS = {
    "select-fields": transform_select_fields,
    "drop-columns": transform_drop_columns,
    "filter": transform_filter,
    "s3-select-fields": transform_s3_select_fields,
    "s3-filter": transform_s3_filter,
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


def read_nosql_source(spark: SparkSession, source_config: dict) -> DataFrame:
    """
    Read data from NoSQL databases using appropriate Spark connectors.
    Currently supports: MongoDB
    
    Preserves nested document structure (structs, arrays).
    """
    connection = source_config.get("connection", {})
    collection = source_config.get("collection")
    
    if not collection:
        raise ValueError("Collection name is required for NoSQL source")
    
    # NoSQL type (default: mongodb, future: cassandra, dynamodb, etc.)
    db_type = connection.get("type", "mongodb")
    
    # MongoDB connection details (from DAG enrichment)
    uri = connection.get("uri")
    database = connection.get("database")
    
    if not uri or not database:
        raise ValueError(f"{db_type} uri and database are required in connection config")
    
    print(f"   Reading from {db_type}: {database}.{collection}")
    
    if db_type == "mongodb":
        # Use base URI with authSource, then set database/collection via options
        # This ensures authSource is properly handled
        base_uri = uri  # e.g., mongodb://mongo:mongo@mongodb:27017/?authSource=admin
        
        # Read using MongoDB Spark Connector with separate database/collection options
        df = spark.read \
            .format("mongodb") \
            .option("spark.mongodb.read.connection.uri", base_uri) \
            .option("spark.mongodb.read.database", database) \
            .option("spark.mongodb.read.collection", collection) \
            .load()
    
    # Future NoSQL support
    # elif db_type == "dynamodb":
    #     df = spark.read \
    #         .format("dynamodb") \
    #         .option("tableName", collection) \
    #         .load()
    
    else:
        raise ValueError(f"Unsupported NoSQL database type: {db_type}")

    return df


def read_s3_logs_source(spark: SparkSession, source_config: dict) -> DataFrame:
    """
    Read log files from S3 and parse using custom regex pattern with named groups

    Args:
        spark: SparkSession
        source_config: Source configuration with connection details
            - connection.bucket: S3 bucket name
            - connection.path: S3 path/prefix
            - connection.endpoint: S3 endpoint (for LocalStack)
            - connection.access_key: S3 access key
            - connection.secret_key: S3 secret key
            - customRegex: Custom regex pattern with Python named groups
              e.g., r'^(?P<client_ip>\S+) .* \[(?P<timestamp>.*?)\] (?P<status_code>\d+)'

    Returns:
        DataFrame with fields matching regex named groups
    """
    import re
    from pyspark.sql import functions as F

    connection = source_config.get("connection", {})
    bucket = connection.get("bucket")
    path = connection.get("path")
    custom_regex = source_config.get("customRegex")

    if not bucket or not path:
        raise ValueError("S3 bucket and path are required for S3 log source")

    if not custom_regex:
        raise ValueError("Custom regex pattern is required for parsing logs. Please define a pattern with named groups.")

    # Configure S3 access
    endpoint = connection.get("endpoint", "http://localstack-main:4566")
    access_key = connection.get("access_key", "test")
    secret_key = connection.get("secret_key", "test")

    spark.conf.set("spark.hadoop.fs.s3a.endpoint", endpoint)
    spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)

    # Read log files from S3
    s3_path = f"s3a://{bucket}/{path}"
    print(f"   Reading logs from: {s3_path}")
    print(f"   Using custom regex pattern with named groups")

    # Read as text
    raw_df = spark.read.text(s3_path)

    # Extract named groups from Python regex pattern
    # Python regex: (?P<name>pattern) -> extract group names
    try:
        compiled_pattern = re.compile(custom_regex)
        named_groups = list(compiled_pattern.groupindex.keys())
        if not named_groups:
            raise ValueError("Regex pattern must contain at least one named group (?P<field_name>pattern)")
        print(f"   Detected fields from regex: {named_groups}")
    except re.error as e:
        raise ValueError(f"Invalid regex pattern: {str(e)}")

    # Convert Python named group regex to Spark regex (remove ?P<name>)
    # Python: (?P<field>\S+) -> Spark: (\S+)
    spark_regex = re.sub(r'\?P<[^>]+>', '', custom_regex)

    # Build select expressions for each named group
    select_exprs = []
    for idx, field_name in enumerate(named_groups, start=1):
        select_exprs.append(
            F.regexp_extract('value', spark_regex, idx).alias(field_name)
        )

    # Extract fields using regex
    parsed_df = raw_df.select(*select_exprs)

    # Filter out failed parsing (if first field is empty, parsing likely failed)
    if named_groups:
        first_field = named_groups[0]
        parsed_df = parsed_df.filter(F.col(first_field) != '')

    record_count = parsed_df.count()
    print(f"   ✅ Parsed {record_count} log records")
    print(f"   Schema:")
    parsed_df.printSchema()

    return parsed_df


# ============ Destination Writers ============

def write_s3_destination(df: DataFrame, dest_config: dict, job_name: str = "output", has_nosql_source: bool = False):
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
        writer_df = df.coalesce(coalesce_num)
    else:
        writer_df = df  # Keep original 16 partitions from JDBC read

    # Handle VOID types (all-null columns) - only for NoSQL sources
    # Parquet doesn't support VOID type, convert to STRING
    if has_nosql_source:
        from pyspark.sql.types import StringType
        
        for field in writer_df.schema.fields:
            dtype_str = str(field.dataType)
            if "VoidType" in dtype_str or "void" in dtype_str.lower():
                print(f"   Converting VOID column to STRING: {field.name}")
                writer_df = writer_df.withColumn(field.name, writer_df[field.name].cast(StringType()))

    writer = writer_df.write \
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

        # Track if any source is NoSQL (for VOID type handling)
        has_nosql_source = False

        # Read all sources
        print(f"📖 Reading {len(sources)} source(s)...")
        for idx, source_config in enumerate(sources):
            node_id = source_config.get("nodeId", f"source_{idx}")
            source_type = source_config.get("type", "rdb")

            print(f"   [{node_id}] Reading from {source_type}: {source_config.get('table') or source_config.get('collection') or source_config.get('connection', {}).get('bucket', 'unknown')}")
            if source_type == "rdb":
                df = read_rdb_source(spark, source_config)
            elif source_type in ["mongodb", "nosql"]:
                df = read_nosql_source(spark, source_config)
                has_nosql_source = True  # Mark that we have NoSQL source
            elif source_type == "s3":
                df = read_s3_logs_source(spark, source_config)
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
            write_s3_destination(final_df, dest_config, config.get("name", "output"), has_nosql_source)
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
