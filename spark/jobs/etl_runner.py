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


def transform_sql(df: DataFrame, config: dict, additional_tables: dict = None) -> DataFrame:
    """
    Execute SQL query on DataFrame using temp view
    
    User writes SQL like: SELECT id, UPPER(name) as name_upper FROM input
    "input" refers to the upstream DataFrame (usually UNION ALL of all sources)
    
    For JOIN queries, additional_tables can be provided:
    {"user_join": df1, "order_join": df2}
    These will be registered as separate temp views for direct access.
    """
    from pyspark.sql import SparkSession
    
    sql_query = config.get("sql")
    if not sql_query:
        raise ValueError("SQL query is required in transform config")
    
    # Get or create Spark session
    spark = SparkSession.builder.getOrCreate()
    
    # Register additional named tables first (for JOIN support)
    if additional_tables:
        for table_name, table_df in additional_tables.items():
            table_df.createOrReplaceTempView(table_name)
            print(f"   Registered table '{table_name}' with {table_df.count()} rows")
    
    # Create temporary view named "input" (for backward compatibility)
    df.createOrReplaceTempView("input")
    
    # Execute user's SQL query
    result_df = spark.sql(sql_query)
    
    print(f"   Executed SQL: {sql_query[:100]}...")
    print(f"   Result schema: {result_df.schema}")
    
    return result_df


# Single-input transforms
TRANSFORMS = {
    "select-fields": transform_select_fields,
    "drop-columns": transform_drop_columns,
    "filter": transform_filter,
    "s3-select-fields": transform_s3_select_fields,
    "s3-filter": transform_s3_filter,
    "sql": transform_sql,
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
        # Add partitioning only if partition_column is explicitly specified
        partition_column = source_config.get("partition_column")
        if partition_column:
            num_partitions = source_config.get("num_partitions", 16)
            lower_bound = source_config.get("lower_bound", 1)
            upper_bound = source_config.get("upper_bound", 10000000)
            print(f"   [Partition] column='{partition_column}', partitions={num_partitions}")
            reader = reader \
                .option("numPartitions", num_partitions) \
                .option("partitionColumn", partition_column) \
                .option("lowerBound", str(lower_bound)) \
                .option("upperBound", str(upper_bound))
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
    
    Nested struct fields are flattened with underscore naming (address.city → address_city)
    to match the Preview behavior and Visual Transform expectations.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, ArrayType
    
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
        
        # Flatten nested structures to match pandas json_normalize behavior
        df = flatten_mongodb_schema(df)
        
    
    # Future NoSQL support
    # elif db_type == "dynamodb":
    #     df = spark.read \
    #         .format("dynamodb") \
    #         .option("tableName", collection) \
    #         .load()
    
    else:
        raise ValueError(f"Unsupported NoSQL database type: {db_type}")

    return df


def flatten_mongodb_schema(df: DataFrame, sep: str = "_") -> DataFrame:
    """
    Recursively flatten MongoDB schema to match pandas json_normalize behavior.
    Converts nested structs and arrays to flat columns with underscore naming.
    
    Examples:
    - {"address": {"city": "Seoul"}} → address_city column
    - {"address": {"geo": {"lat": 37.5}}} → address_geo_lat column (fully recursive)
    - {"projects": [{"name": "A"}]} → projects_name: ["A"] (array extraction)
    
    Args:
        df: Input DataFrame
        sep: Separator for nested field names (default: "_")
    
    Returns:
        Flattened DataFrame
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, ArrayType
    
    def flatten_struct_recursive(col_name: str, col_type: StructType, prefix: str = "", path_prefix: str = "") -> list:
        """Recursively flatten struct type fields
        
        Args:
            col_name: Current column name
            col_type: StructType to flatten
            prefix: Prefix for flattened column names (with separator)
            path_prefix: Dot-notation path prefix for Spark column access
        
        Returns:
            List of (full_path, flat_name, field_type) tuples
        
        Base cases:
            1. Return empty list if StructType has no fields
            2. Append to result and terminate if field is primitive type
        """
        # Base case 1: Return empty list for empty struct
        if not col_type.fields:
            return []
        
        flattened = []
        current_prefix = f"{prefix}{col_name}{sep}" if prefix else f"{col_name}{sep}"
        current_path = f"{path_prefix}{col_name}." if path_prefix else f"{col_name}."
        
        for field in col_type.fields:
            field_name = field.name
            field_type = field.dataType
            
            if isinstance(field_type, StructType):
                # Recursive case: Recurse into nested struct
                nested_flattened = flatten_struct_recursive(field_name, field_type, current_prefix, current_path)
                flattened.extend(nested_flattened)
            else:
                # Base case 2: Leaf node (primitive type) - append to result
                full_path = f"{current_path}{field_name}"
                flat_name = f"{current_prefix}{field_name}"
                flattened.append((full_path, flat_name, field_type))
        
        return flattened
    
    flattened_cols = []
    
    for field in df.schema.fields:
        col_name = field.name
        col_type = field.dataType
        
        if isinstance(col_type, StructType):
            # Struct field: flatten recursively
            struct_fields = flatten_struct_recursive(col_name, col_type)
            for path, alias, _ in struct_fields:
                # Build nested column access: col("address.geo.lat")
                flattened_cols.append(F.col(path).alias(alias))
        
        elif isinstance(col_type, ArrayType):
            # Array field: check if it's array of structs or primitives
            element_type = col_type.elementType
            
            if isinstance(element_type, StructType):
                # Array of structs: extract each field as a separate array column
                # Example: projects: [{"name": "A", "budget": 100}] 
                # → projects_name: ["A"], projects_budget: [100]
                for struct_field in element_type.fields:
                    field_name = struct_field.name
                    flattened_name = f"{col_name}{sep}{field_name}"
                    # Use transform to extract specific field from each array element
                    flattened_cols.append(
                        F.transform(
                            F.col(col_name),
                            lambda x: x[field_name]
                        ).alias(flattened_name)
                    )
            else:
                # Array of primitives: keep as-is
                flattened_cols.append(F.col(col_name))
        
        else:
            # Primitive field: keep as-is
            flattened_cols.append(F.col(col_name))
    
    return df.select(*flattened_cols)


def parse_log_files_with_regex(spark: SparkSession, s3_path: str, custom_regex: str) -> DataFrame:
    """
    Parse log files from S3 using custom regex pattern with named groups

    Args:
        spark: SparkSession
        s3_path: S3 path to log files (s3a://bucket/path/)
        custom_regex: Custom regex pattern with Python named groups

    Returns:
        DataFrame with fields matching regex named groups
    """
    import re
    from pyspark.sql import functions as F

    print(f"   Parsing logs from: {s3_path}")
    print(f"   Using custom regex pattern with named groups")

    # Read log files as text
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

    return parsed_df


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
    # Get bucket and path from source_config (Target Wizard) or connection (legacy)
    bucket = source_config.get("bucket") or connection.get("bucket")
    path = source_config.get("path") or connection.get("path")
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


def read_s3_file_source(spark: SparkSession, source_config: dict) -> DataFrame:
    """
    Read structured files (Parquet, CSV, JSON) from S3
    Used for reading processed datasets from catalog (Target → Target chaining)
    
    Args:
        spark: SparkSession
        source_config: Source configuration
            - path: S3 path (s3a://bucket/path/)
            - format: File format (parquet, csv, json)
            - s3_config: S3 credentials (optional, IRSA will be used if not provided)
    
    Returns:
        DataFrame with file contents
    """
    path = source_config.get("path") or source_config.get("s3Location")
    file_format = source_config.get("format", "parquet")

    # source_datasets: bucket + path 별도 필드
    # catalog datasets: path에 전체 경로 (s3a://bucket/path)
    bucket = source_config.get("bucket")
    connection = source_config.get("connection", {})
    if not bucket:
        bucket = connection.get("bucket")

    # bucket이 있고 path가 전체 경로가 아니면 조합
    if bucket and path and not path.startswith("s3"):
        path = f"s3a://{bucket}/{path}"
    elif not path and bucket:
        raise ValueError("S3 path is required for S3 file source")
    elif not path:
        raise ValueError("S3 path is required for S3 file source")

    # Convert s3:// to s3a:// if needed
    if path.startswith("s3://"):
        path = path.replace("s3://", "s3a://", 1)

    print(f"   Reading {file_format} files from: {path}")
    
    # Configure S3 credentials if provided (LocalStack or explicit AWS)
    s3_config = source_config.get("s3_config", {})
    
    access_key = s3_config.get("access_key_id") or s3_config.get("access_key")
    secret_key = s3_config.get("secret_access_key") or s3_config.get("secret_key")
    endpoint = s3_config.get("endpoint")
    
    if access_key and secret_key:
        # Explicit credentials provided
        spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
        
        if endpoint:
            spark.conf.set("spark.hadoop.fs.s3a.endpoint", endpoint)
        else:
            region = s3_config.get("region", "ap-northeast-2")
            spark.conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
        
        # Path-style access for LocalStack
        if endpoint and endpoint.startswith("http://"):
            spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    else:
        # No credentials - use Spark's global credentials (IRSA)
        print("   Using Spark's global S3 credentials (IRSA)")
    
    # Read based on format
    if file_format.lower() == "parquet":
        df = spark.read.parquet(path)
    elif file_format.lower() == "csv":
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(path)
    elif file_format.lower() == "json":
        df = spark.read.json(path)
    elif file_format.lower() == "log":
        # Parse log files with regex
        custom_regex = source_config.get("customRegex")
        if not custom_regex:
            raise ValueError("customRegex is required for log file format")

        df = parse_log_files_with_regex(spark, path, custom_regex)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    record_count = df.count()
    print(f"   ✅ Read {record_count} records from {file_format.upper()} files")
    print(f"   Schema:")
    df.printSchema()
    
    return df


def _extract_json_path(data: object, path: str):
    """
    Enhanced JSONPath-like extractor with recursive array handling.
    Supports:
    - Dot paths: $.data.items
    - Array index: $.data[0]
    - Array wildcard: $.data[*]
    - Nested wildcards: $.results[*].items (recursively applies path to each element)
    """
    if not path:
        return data

    if path.startswith("$."):
        path = path[2:]
    elif path.startswith("$"):
        path = path[1:]

    tokens = [p for p in path.split(".") if p]
    current = data

    for token_idx, token in enumerate(tokens):
        if current is None:
            return None

        if "[" in token and token.endswith("]"):
            base, bracket = token.split("[", 1)
            index_token = bracket[:-1]

            # Navigate to base field first if exists
            if base:
                if isinstance(current, dict):
                    current = current.get(base)
                else:
                    return None

            # Handle array indexing
            if index_token == "*":
                # Wildcard - apply remaining path to each element
                if not isinstance(current, list):
                    return None

                # Check if there are more tokens after this wildcard
                remaining_tokens = tokens[token_idx + 1:]

                if remaining_tokens:
                    # Recursively apply remaining path to each element
                    remaining_path = ".".join(remaining_tokens)
                    results = []
                    for item in current:
                        extracted = _extract_json_path(item, remaining_path)
                        if extracted is not None:
                            # If extracted is a list, extend; otherwise append
                            if isinstance(extracted, list):
                                results.extend(extracted)
                            else:
                                results.append(extracted)
                    return results if results else None
                else:
                    # No remaining path - return the list as-is
                    return current
            else:
                # Numeric index
                try:
                    idx = int(index_token)
                except ValueError:
                    return None
                if isinstance(current, list) and 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return None
        else:
            # Simple field access
            if isinstance(current, dict):
                current = current.get(token)
            else:
                return None

    return current


def read_api_source(spark: SparkSession, source_config: dict) -> DataFrame:
    """Read data from a RESTful API with basic pagination and JSON parsing."""
    import json
    import time
    import requests
    from pyspark.sql.types import StructType

    connection = source_config.get("connection", {})
    connection_config = connection.get("config", {})

    base_url = connection.get("base_url") or connection_config.get("base_url") or source_config.get("base_url")
    endpoint = source_config.get("endpoint") or ""
    method = (source_config.get("method") or "GET").upper()

    if not base_url:
        raise ValueError("API base_url is required in connection config")
    if not base_url.startswith("http://") and not base_url.startswith("https://"):
        base_url = f"https://{base_url}"
    if method != "GET":
        raise ValueError(f"Unsupported API method: {method}")

    headers = {}
    headers.update(connection_config.get("headers") or {})
    headers.setdefault("Accept", "application/json")

    auth_type = connection_config.get("auth_type", "none")
    auth_config = connection_config.get("auth_config") or {}

    if auth_type == "api_key":
        header_name = auth_config.get("header_name")
        api_key = auth_config.get("api_key")
        if header_name and api_key:
            headers[header_name] = api_key
    elif auth_type == "bearer":
        token = auth_config.get("token")
        if token:
            headers["Authorization"] = f"Bearer {token}"
    elif auth_type == "basic":
        username = auth_config.get("username")
        password = auth_config.get("password")
        if username and password:
            headers["Authorization"] = requests.auth._basic_auth_str(username, password)

    params = {}
    params.update(source_config.get("query_params") or {})

    incremental = source_config.get("incremental_config") or {}
    if incremental.get("enabled"):
        last_sync = incremental.get("last_sync_timestamp")
        start_from = incremental.get("start_from")
        timestamp_param = incremental.get("timestamp_param")

        if last_sync and timestamp_param:
            # Subsequent runs: use last_sync_timestamp
            params[timestamp_param] = last_sync
            print(f"   [Incremental] Using last_sync_timestamp: {last_sync}")
        elif start_from and timestamp_param:
            # First run with start_from: use start_from date
            params[timestamp_param] = start_from
            print(f"   [Incremental] First run, using start_from: {start_from}")
        # else: No timestamp parameter (fetch all data)

    pagination = source_config.get("pagination") or {}
    if isinstance(pagination, dict):
        pagination_type = pagination.get("type", "none")
        pagination_config = pagination.get("config", {})
    else:
        pagination_type = "none"
        pagination_config = {}

    response_path = source_config.get("response_path")
    all_data = []

    # Build full URL with proper formatting
    full_url = base_url.rstrip("/") + "/" + endpoint.lstrip("/") if endpoint else base_url
    print(f"   API URL: {full_url}")
    print(f"   Auth type: {auth_type}")
    print(f"   Pagination: {pagination_type}")
    if response_path:
        print(f"   Response path: {response_path}")

    def _request_api(request_params: dict):
        max_retries = 3
        attempt = 0
        while True:
            attempt += 1
            print(f"   Making API request: {full_url} with params={request_params}")
            response = requests.get(full_url, headers=headers, params=request_params, timeout=30)
            print(f"   Response status: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}")

            # Handle rate limiting (429)
            if response.status_code == 429 and attempt <= max_retries:
                retry_after = int(response.headers.get("Retry-After", "1"))
                time.sleep(retry_after)
                continue

            # Handle pagination limit errors (422, 404) - treat as end of data
            if response.status_code in [422, 404]:
                print(f"   API pagination limit reached (status={response.status_code}), stopping pagination")
                return None  # Signal no more data

            response.raise_for_status()
            return response

    def _parse_json(response):
        try:
            return response.json()
        except ValueError:
            content_type = response.headers.get("Content-Type", "unknown")
            snippet = response.text[:200].replace("\n", " ")
            raise ValueError(
                f"API response is not valid JSON (status={response.status_code}, "
                f"content_type={content_type}, body='{snippet}')"
            )

    def _extract_items(payload: object):
        if response_path:
            extracted = _extract_json_path(payload, response_path)
        else:
            extracted = payload

        if extracted is None:
            return []
        if isinstance(extracted, list):
            return extracted
        if isinstance(extracted, dict):
            return [extracted]
        return []

    if pagination_type == "offset_limit":
        offset_param = pagination_config.get("offset_param", "offset")
        limit_param = pagination_config.get("limit_param", "limit")
        page_size = int(pagination_config.get("page_size", 100))
        offset = int(pagination_config.get("start_offset", 0))

        while True:
            page_params = dict(params)
            page_params[offset_param] = offset
            page_params[limit_param] = page_size
            response = _request_api(page_params)
            if response is None:  # Pagination limit reached
                break
            items = _extract_items(_parse_json(response))
            if not items:
                break
            all_data.extend(items)
            if len(items) < page_size:
                break
            offset += page_size

    elif pagination_type == "page":
        page_param = pagination_config.get("page_param", "page")
        per_page_param = pagination_config.get("per_page_param", "per_page")
        page_size = int(pagination_config.get("page_size", 100))
        page = int(pagination_config.get("start_page", 1))

        while True:
            page_params = dict(params)
            page_params[page_param] = page
            page_params[per_page_param] = page_size
            response = _request_api(page_params)
            if response is None:  # Pagination limit reached
                break
            items = _extract_items(_parse_json(response))
            if not items:
                break
            all_data.extend(items)
            if len(items) < page_size:
                break
            page += 1

    elif pagination_type == "cursor":
        cursor_param = pagination_config.get("cursor_param", "cursor")
        next_cursor_path = pagination_config.get("next_cursor_path")
        cursor = pagination_config.get("start_cursor")

        while True:
            page_params = dict(params)
            if cursor:
                page_params[cursor_param] = cursor
            response = _request_api(page_params)
            if response is None:  # Pagination limit reached
                break
            payload = _parse_json(response)
            items = _extract_items(payload)
            if items:
                all_data.extend(items)
            next_cursor = None
            if next_cursor_path:
                next_cursor = _extract_json_path(payload, next_cursor_path)
            if not next_cursor:
                break
            cursor = next_cursor

    else:
        # No pagination - single request
        response = _request_api(params)
        if response is not None:
            all_data = _extract_items(_parse_json(response))
        else:
            all_data = []

    if not all_data:
        return spark.createDataFrame([], StructType([]))

    json_rdd = spark.sparkContext.parallelize([json.dumps(item) for item in all_data])
    df = spark.read.json(json_rdd)
    return df


# ============ SCD Type 2 Helper Functions ============

def detect_primary_keys(df: DataFrame) -> list:
    """Auto-detect primary key column(s) from DataFrame schema"""
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
    """Write DataFrame using SCD Type 2 (Slowly Changing Dimension) pattern"""
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable
    from datetime import datetime

    # Check if source is immutable (S3 logs, API responses)
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
    except Exception:
        table_exists = False
        print(f"   [SCD2] No existing table, creating new Delta table with SCD2 schema")

    if not table_exists:
        # First run: Just write the data with SCD2 columns
        new_data.write.format("delta").mode("overwrite").save(path)
        record_count = new_data.count()
        print(f"   [SCD2] ✅ Initial table created with {record_count} records")
        return

    # Build merge condition (match on primary keys and is_current=TRUE)
    merge_condition_parts = [f"target.{pk} = source.{pk}" for pk in primary_keys]
    merge_condition_parts.append("target.is_current = TRUE")
    merge_condition = " AND ".join(merge_condition_parts)

    # Build change detection condition (any non-key column changed)
    metadata_cols = ['valid_from', 'valid_to', 'is_current']
    data_columns = [col for col in df.columns if col not in primary_keys + metadata_cols]

    change_conditions = []
    for col in data_columns:
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

    merge_builder = merge_builder.whenMatchedUpdate(
        condition=change_condition,
        set={
            "valid_to": F.lit(current_time_str),
            "is_current": F.lit(False)
        }
    )

    merge_builder = merge_builder.whenNotMatchedInsertAll()
    merge_builder.execute()
    print(f"   [SCD2] Step 1: Expired changed records and inserted new records")

    # Step 2: Re-insert changed records as new current versions
    delta_table = DeltaTable.forPath(spark, path)

    expired_df = delta_table.toDF().filter(
        (F.col("valid_to") == current_time_str) & (F.col("is_current") == False)
    )

    expired_count = expired_df.count()

    if expired_count > 0:
        print(f"   [SCD2] Found {expired_count} changed records, inserting new versions")
        expired_keys = expired_df.select(primary_keys)
        updated_records = new_data.join(expired_keys, primary_keys, "inner")
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


# ============ Destination Writers ============

def write_s3_destination(df: DataFrame, dest_config: dict, job_name: str = "output", has_nosql_source: bool = False, source_types: list = None):
    """
    Write DataFrame to S3 as Delta Lake format

    Args:
        df: DataFrame to write
        dest_config: Destination configuration
        job_name: Job name for table naming
        has_nosql_source: Whether source includes NoSQL (for VOID type handling)
        source_types: List of source types (e.g., ['rdb', 's3', 'mongodb'])
    """
    path = dest_config.get("path")
    if not path:
        raise ValueError("Destination path is required")

    # Convert s3:// to s3a:// (Spark requires s3a scheme)
    if path.startswith("s3://"):
        path = path.replace("s3://", "s3a://", 1)

    # Ensure path ends with / before appending job name
    if not path.endswith("/"):
        path = path + "/"

    # Use glue_table_name if provided, otherwise use job_name
    glue_table_name = dest_config.get("glue_table_name")
    path = path + (glue_table_name or job_name)

    options = dest_config.get("options", {})
    compression = options.get("compression", "snappy")

    # Determine write mode
    incremental_config = dest_config.get("incremental_config", {})
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

    # Use SCD Type 2 for incremental loads to track full history
    # (S3 logs will use simple append inside write_scd2_merge)
    if incremental_config.get("enabled"):
        print(f"📊 Writing with incremental load (SCD Type 2 for RDB, append for S3)")

        # Get timestamp column for audit trail
        timestamp_col = incremental_config.get("timestamp_column")

        # Use SCD2 MERGE (auto-detects primary keys, handles S3 vs RDB)
        spark = writer_df.sparkSession
        write_scd2_merge(
            spark=spark,
            df=writer_df,
            path=path,
            primary_keys=None,  # Auto-detect
            timestamp_col=timestamp_col,
            source_types=source_types  # Pass source types for S3 detection
        )
    else:
        # Full load: overwrite mode
        print(f"📊 Writing with overwrite mode (full load)")
        mode = options.get("mode", "overwrite")

        writer = writer_df.write \
            .format("delta") \
            .mode(mode) \
            .option("compression", compression)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(path)
        print(f"✅ Data written to {path} (Delta Lake format)")

    # Note: Glue table registration is handled by Trino register_table() in Airflow DAG
    # This ensures proper Delta Lake metadata format for Trino compatibility


# ============ Main ETL Runner ============

def create_spark_session(config: dict) -> SparkSession:
    """Create SparkSession with S3 and Delta Lake configuration"""
    dest = config.get("destination", {})

    builder = SparkSession.builder.appName(f"ETL: {config.get('name', 'Unknown')}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

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
        # Dictionary to store source names by nodeId (for SQL JOIN support)
        source_names = {}

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

            # Build source description based on type
            if source_type == "api":
                connection = source_config.get("connection", {})
                connection_config = connection.get("config", {})
                base_url = connection.get("base_url") or connection_config.get("base_url") or source_config.get("base_url")
                endpoint = source_config.get("endpoint", "")
                source_desc = f"{base_url}/{endpoint}" if endpoint else base_url
            else:
                source_desc = source_config.get('table') or source_config.get('collection') or source_config.get('connection', {}).get('bucket', 'unknown')

            print(f"   [{node_id}] Reading from {source_type}: {source_desc}")
            if source_type == "rdb":
                df = read_rdb_source(spark, source_config)
            elif source_type in ["mongodb", "nosql"]:
                df = read_nosql_source(spark, source_config)
                has_nosql_source = True  # Mark that we have NoSQL source
            elif source_type == "api":
                df = read_api_source(spark, source_config)
            elif source_type == "s3":
                # Distinguish between S3 logs (with customRegex) and S3 files (without)
                if source_config.get("customRegex"):
                    # S3 log files with custom regex parsing
                    df = read_s3_logs_source(spark, source_config)
                else:
                    # S3 structured files (Parquet, CSV, JSON)
                    df = read_s3_file_source(spark, source_config)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")

            dataframes[node_id] = df
            # Store source name for SQL JOIN support (use name from config, fallback to node_id)
            source_name = source_config.get("name") or source_config.get("table") or source_config.get("collection") or node_id
            source_names[node_id] = source_name
            print(f"   [{node_id}] Schema (name: {source_name}):")
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
                # Single-input transform (or multi-input for SQL with UNION ALL)
                additional_tables = None  # For SQL transforms with multiple inputs
                
                if input_node_ids:
                    # Special handling for SQL transform with multiple inputs
                    if transform_type == "sql" and len(input_node_ids) > 1:
                        # UNION ALL multiple sources before SQL execution
                        print(f"   Combining {len(input_node_ids)} sources with UNION ALL...")
                        input_dfs = []
                        additional_tables = {}  # Build named tables for JOIN support
                        
                        for input_id in input_node_ids:
                            if input_id not in dataframes:
                                raise ValueError(f"Input node {input_id} not found for transform {node_id}")
                            df = dataframes[input_id]
                            input_dfs.append(df)
                            
                            # Add to additional_tables using source name
                            table_name = source_names.get(input_id, input_id)
                            additional_tables[table_name] = df
                            print(f"   → Table '{table_name}' prepared for SQL access")
                        
                        # Use unionByName with allowMissingColumns for schema alignment
                        input_df = input_dfs[0]
                        for df in input_dfs[1:]:
                            input_df = input_df.unionByName(df, allowMissingColumns=True)
                        
                        print(f"   UNION ALL complete. Combined schema has {len(input_df.columns)} columns")
                    else:
                        # Regular single input
                        input_df = dataframes[input_node_ids[0]]
                elif last_node_id:
                    input_df = dataframes[last_node_id]
                else:
                    raise ValueError(f"No input available for transform {node_id}")

                # Call transform function with additional_tables for SQL
                if transform_type == "sql" and additional_tables:
                    result_df = transform_sql(input_df, transform_config, additional_tables)
                else:
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

        # Extract source types for SCD Type 2 logic
        # Filter out None values to prevent issues in SCD2 logic
        source_types = [
            s.get("type") or s.get("source_type")
            for s in config.get("sources", [])
            if s.get("type") or s.get("source_type")
        ]

        # Fallback: if sources list is empty, try legacy single source
        if not source_types and config.get("source"):
            legacy_source_type = config["source"].get("type") or config["source"].get("source_type")
            if legacy_source_type:
                source_types = [legacy_source_type]

        print(f"💾 Writing to destination: {dest_type}")
        print(f"   Source types detected: {source_types}")

        if dest_type == "s3":
            write_s3_destination(final_df, dest_config, config.get("name", "output"), has_nosql_source, source_types)
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
    import sys
    import json

    # Parse config from command line
    if len(sys.argv) > 1:
        if sys.argv[1] == "--config-file":
            with open(sys.argv[2], "r") as f:
                config = json.load(f)
        elif sys.argv[1] == "--base64":
            import base64
            # Decode base64 config string
            decoded_bytes = base64.b64decode(sys.argv[2])
            decoded_str = decoded_bytes.decode("utf-8")
            config = json.loads(decoded_str)
        else:
            # Assume raw JSON string
            config = json.loads(sys.argv[1])
            
        run_etl(config)
    else:
        print("Usage: etl_runner.py <json_config> OR --base64 <b64_config> OR --config-file <path>")
        sys.exit(1)
