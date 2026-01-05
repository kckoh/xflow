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
            "pathField": "request_path",        # User's field name for path
            "pathPattern": "/api/.*",
            "timestampField": "timestamp",      # User's field name for timestamp
            "timestampFrom": "2026-01-02T10:00",
            "timestampTo": "2026-01-02T12:00"
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

    # Timestamp range filter (with configurable field name)
    timestamp_field = filters_config.get("timestampField")
    if timestamp_field and timestamp_field in df.columns:
        timestamp_from = filters_config.get("timestampFrom")
        timestamp_to = filters_config.get("timestampTo")
        if timestamp_from or timestamp_to:
            parsed_ts = F.to_timestamp(F.col(timestamp_field), "dd/MMM/yyyy:HH:mm:ss Z")
            if timestamp_from:
                filtered_df = filtered_df.filter(
                    parsed_ts >= F.to_timestamp(F.lit(timestamp_from), "yyyy-MM-dd'T'HH:mm")
                )
                print(f"   Filter: {timestamp_field} >= {timestamp_from}")
            if timestamp_to:
                filtered_df = filtered_df.filter(
                    parsed_ts <= F.to_timestamp(F.lit(timestamp_to), "yyyy-MM-dd'T'HH:mm")
                )
                print(f"   Filter: {timestamp_field} <= {timestamp_to}")

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

    df = reader.load()
    
    # Apply Incremental Load Filtering (Watermark)
    incremental_config = source_config.get("incremental_config", {})
    if incremental_config.get("enabled"):
        timestamp_col = incremental_config.get("timestamp_column")
        last_sync = incremental_config.get("last_sync_timestamp")
        
        if timestamp_col and last_sync:
            print(f"   [Incremental] Filtering data: {timestamp_col} > '{last_sync}'")
            df = df.filter(f"{timestamp_col} > '{last_sync}'")
            
    return df


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
    # Support both old field names (access_key, secret_key, endpoint) and new (access_key_id, secret_access_key, region)
    access_key = connection.get("access_key_id") or connection.get("access_key")
    secret_key = connection.get("secret_access_key") or connection.get("secret_key")
    endpoint = connection.get("endpoint")

    # Only configure S3 credentials if explicitly provided in connection
    # Otherwise, use Spark's global credentials (from IRSA in Kubernetes)
    if access_key and secret_key:
        # Explicit credentials provided (LocalStack or user-specific AWS)
        spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)

        # Handle endpoint
        if endpoint:
            spark.conf.set("spark.hadoop.fs.s3a.endpoint", endpoint)
        else:
            region = connection.get("region", "ap-northeast-2")
            endpoint = f"s3.{region}.amazonaws.com"
            spark.conf.set("spark.hadoop.fs.s3a.endpoint", endpoint)

        # Path-style access for LocalStack
        if endpoint.startswith("http://"):
            spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    else:
        # No credentials in connection - use Spark's global credentials (IRSA)
        print("   Using Spark's global S3 credentials (IRSA)")

    # Build S3 path
    s3_path = f"s3a://{bucket}/{path}"
    print(f"   Reading logs from: {s3_path}")
    print(f"   Using custom regex pattern with named groups")

    # Check for incremental load (hybrid file + record filtering)
    incremental_config = source_config.get("incremental_config", {})

    if incremental_config.get("enabled") and incremental_config.get("last_sync_timestamp"):
        import boto3
        from datetime import datetime, timedelta

        last_sync_str = incremental_config.get("last_sync_timestamp")
        last_sync = datetime.fromisoformat(last_sync_str.replace('Z', '+00:00'))

        # Grace period: files that might still be updated
        grace_period = timedelta(hours=1)
        grace_cutoff = last_sync - grace_period

        print(f"   [Incremental] Hybrid filtering: file-level + record-level")
        print(f"      last_sync: {last_sync}")
        print(f"      grace_period: {grace_period}")

        # Initialize S3 client
        boto3_kwargs = {}

        # Only add credentials if explicitly provided
        if access_key and secret_key:
            boto3_kwargs['aws_access_key_id'] = access_key
            boto3_kwargs['aws_secret_access_key'] = secret_key

            # Only add endpoint_url for LocalStack (not for real AWS)
            if endpoint and endpoint.startswith("http://"):
                boto3_kwargs['endpoint_url'] = endpoint

        # If no credentials, boto3 will use default credential chain (IRSA)
        s3_client = boto3.client('s3', **boto3_kwargs)

        # List all files in the path
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path)

        new_files = []      # Files created after last_sync (read all records)
        recent_files = []   # Files within grace period (read with timestamp filter)

        if 'Contents' in response:
            for obj in response['Contents']:
                # Skip directories
                if obj['Key'].endswith('/'):
                    continue

                file_modified = obj['LastModified'].replace(tzinfo=None)
                file_path = f"s3a://{bucket}/{obj['Key']}"

                if file_modified > last_sync:
                    # Case 1: New file - read all records
                    new_files.append(file_path)
                    print(f"      → [NEW] {obj['Key']} (modified: {file_modified})")
                elif file_modified > grace_cutoff:
                    # Case 2: Recent file (might still be updated) - read with timestamp filter
                    recent_files.append(file_path)
                    print(f"      → [RECENT] {obj['Key']} (modified: {file_modified}) - will filter by timestamp")
                # Case 3: Old file - skip
                # else:
                #     print(f"      → [SKIP] {obj['Key']} (modified: {file_modified})")

        # Helper function to parse logs with regex
        def parse_logs(raw_df):
            """Parse raw text DataFrame using custom regex"""
            # Extract named groups from Python regex pattern
            try:
                compiled_pattern = re.compile(custom_regex)
                named_groups = list(compiled_pattern.groupindex.keys())
                if not named_groups:
                    raise ValueError("Regex pattern must contain at least one named group (?P<field_name>pattern)")
            except re.error as e:
                raise ValueError(f"Invalid regex pattern: {str(e)}")

            # Convert Python named group regex to Spark regex (remove ?P<name>)
            spark_regex = re.sub(r'\?P<[^>]+>', '', custom_regex)

            # Build select expressions for each named group
            select_exprs = []
            for idx, field_name in enumerate(named_groups, start=1):
                select_exprs.append(
                    F.regexp_extract('value', spark_regex, idx).alias(field_name)
                )

            # Extract fields using regex
            parsed = raw_df.select(*select_exprs)

            # Filter out failed parsing (if first field is empty, parsing likely failed)
            if named_groups:
                first_field = named_groups[0]
                parsed = parsed.filter(F.col(first_field) != '')

            return parsed, named_groups

        # Process files with incremental filtering
        parsed_dfs_to_union = []

        if new_files:
            print(f"   [Incremental] Reading {len(new_files)} new files (all records)")
            df_new = spark.read.text(new_files)
            parsed_new, named_groups = parse_logs(df_new)
            parsed_dfs_to_union.append(parsed_new)
            print(f"      → Parsed {parsed_new.count()} records from new files")

        if recent_files:
            print(f"   [Incremental] Reading {len(recent_files)} recent files (with timestamp filter)")
            df_recent = spark.read.text(recent_files)
            parsed_recent, named_groups = parse_logs(df_recent)

            # Apply record-level timestamp filter to recent files
            # Find timestamp field in the parsed schema
            timestamp_field = incremental_config.get("timestamp_column", "timestamp")
            if timestamp_field in parsed_recent.columns:
                # Convert timestamp string to comparable format and filter
                # Assuming timestamp is in format like "02/Jan/2026:10:56:00 +0900"
                parsed_ts = F.to_timestamp(F.col(timestamp_field), "dd/MMM/yyyy:HH:mm:ss Z")
                last_sync_ts = F.to_timestamp(F.lit(last_sync_str), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")

                before_filter = parsed_recent.count()
                parsed_recent = parsed_recent.filter(parsed_ts > last_sync_ts)
                after_filter = parsed_recent.count()

                print(f"      → Parsed {before_filter} records, filtered to {after_filter} records (timestamp > {last_sync})")
            else:
                print(f"      ⚠️ Warning: timestamp field '{timestamp_field}' not found in parsed schema, skipping record-level filter")
                print(f"      → Available fields: {parsed_recent.columns}")

            parsed_dfs_to_union.append(parsed_recent)

        if not parsed_dfs_to_union:
            print(f"   [Incremental] No new or recent files - returning empty DataFrame")
            # Need to get named_groups for empty schema
            try:
                compiled_pattern = re.compile(custom_regex)
                named_groups = list(compiled_pattern.groupindex.keys())
            except:
                named_groups = ["value"]

            from pyspark.sql.types import StructType, StructField, StringType
            schema = StructType([StructField(name, StringType(), True) for name in named_groups])
            return spark.createDataFrame([], schema)

        # Union all parsed DataFrames
        print(f"   Detected fields from regex: {named_groups}")
        parsed_df = parsed_dfs_to_union[0]
        for df in parsed_dfs_to_union[1:]:
            parsed_df = parsed_df.union(df)

    else:
        # No incremental - read all files and parse
        raw_df = spark.read.text(s3_path)

        # Extract named groups from Python regex pattern
        try:
            compiled_pattern = re.compile(custom_regex)
            named_groups = list(compiled_pattern.groupindex.keys())
            if not named_groups:
                raise ValueError("Regex pattern must contain at least one named group (?P<field_name>pattern)")
            print(f"   Detected fields from regex: {named_groups}")
        except re.error as e:
            raise ValueError(f"Invalid regex pattern: {str(e)}")

        # Convert Python named group regex to Spark regex (remove ?P<name>)
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

    # Ensure path ends with / before appending job name
    if not path.endswith("/"):
        path = path + "/"
    
    path = path + job_name

    options = dest_config.get("options", {})
    compression = options.get("compression", "snappy")
    
    # Determine write mode: Enforce 'append' if incremental load is enabled
    incremental_config = dest_config.get("incremental_config", {})
    if incremental_config.get("enabled"):
        mode = "append"
    else:
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

    # S3 configuration
    if dest.get("type") == "s3":
        s3_config = dest.get("s3_config", {})

        # Check if using IAM role (production) or explicit credentials (local)
        if s3_config.get("use_iam_role"):
            # Production: Use IAM role from ServiceAccount (IRSA)
            region = s3_config.get("region", "ap-northeast-2")
            builder = builder \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                       "com.amazonaws.auth.WebIdentityTokenCredentialsProvider") \
                .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com") \
                .config("spark.hadoop.fs.s3a.fast.upload", "true")
        else:
            # Non-IAM: Use explicit credentials (LocalStack or user-provided)
            # Support both old field names (access_key, secret_key, endpoint) and new (access_key_id, secret_access_key, region)
            access_key = s3_config.get("access_key_id") or s3_config.get("access_key")
            secret_key = s3_config.get("secret_access_key") or s3_config.get("secret_key")
            endpoint = s3_config.get("endpoint")

            builder = builder \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.fast.upload", "true") \
                .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") \
                .config("spark.hadoop.fs.s3a.multipart.size", "5M")

            # Only configure credentials if explicitly provided
            if access_key and secret_key:
                builder = builder \
                    .config("spark.hadoop.fs.s3a.access.key", access_key) \
                    .config("spark.hadoop.fs.s3a.secret.key", secret_key)

                # Handle endpoint
                if endpoint:
                    builder = builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
                else:
                    region = s3_config.get("region", "ap-northeast-2")
                    builder = builder.config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")

                # Path-style access for LocalStack
                if endpoint and endpoint.startswith("http://"):
                    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
            else:
                # No explicit credentials - will use Spark's global credentials (IRSA)
                print("Using Spark's global S3 credentials for destination (IRSA)")

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
