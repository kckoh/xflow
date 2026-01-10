"""
SQL Test API - Test SQL queries with DuckDB before execution
"""
from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any, Optional, Tuple
import duckdb
import pandas as pd
from bson import ObjectId

import database
from schemas.sql_test import (
    SourceInfo,
    SQLTestRequest,
    ColumnSchema,
    SourceSample,
    SQLTestResponse
)

router = APIRouter()


@router.post("/test", response_model=SQLTestResponse)
async def test_sql_query(request: SQLTestRequest):
    """
    Test SQL query with sample data using DuckDB
    
    Supports multiple sources combined with UNION ALL
    """
    import time
    start_time = time.time()
    limit = request.limit or 5
    
    try:
        # Get MongoDB database
        db = database.mongodb_client[database.DATABASE_NAME]
        
        # Load and combine data from all sources
        # Load 'limit' rows from each source so all sources appear in preview
        sample_df, source_samples, individual_sources = await _load_and_union_sources(request.sources, db, limit=limit)
        
        if sample_df is None or len(sample_df) == 0:
            raise HTTPException(
                status_code=400,
                detail="No data available from sources"
            )
        
        # Execute SQL with DuckDB
        con = duckdb.connect()
        
        # Register each individual source as a separate table (for JOIN/SQL Transform support)
        for source_info in individual_sources:
            table_name = source_info['dataset_name']
            table_df = source_info['dataframe']
            con.register(table_name, table_df)
        
        # Also register combined data as "input" table (for Visual Transform/backward compatibility)
        con.register('input', sample_df)
        
        # Execute user's SQL and apply limit
        # For UNION ALL, multiply limit by number of sources to show data from each source
        effective_limit = limit * len(request.sources)
        sql = request.sql
        # Only add limit if not present
        if "limit" not in sql.lower():
            sql = f"SELECT * FROM ({sql}) LIMIT {effective_limit}"
            
        result_df = con.execute(sql).df()
        
        # Extract schema
        schema = []
        for col in result_df.columns:
            dtype = str(result_df[col].dtype)
            # Map pandas dtypes to readable types
            if 'int' in dtype:
                col_type = 'integer'
            elif 'float' in dtype:
                col_type = 'double'
            elif 'bool' in dtype:
                col_type = 'boolean'
            elif 'datetime' in dtype:
                col_type = 'timestamp'
            else:
                col_type = 'string'
            
            schema.append(ColumnSchema(
                name=col,
                type=col_type,
                nullable=result_df[col].isnull().any()
            ))
        
        # Get sample rows
        sample_rows = result_df.to_dict('records')
        
        # Convert any non-serializable types for sample_rows
        for row in sample_rows:
            for key, value in row.items():
                # Handle nested structures (MongoDB arrays/objects) and numpy arrays
                if isinstance(value, (list, dict)) or (hasattr(value, '__iter__') and not isinstance(value, (str, bytes))):
                    row[key] = str(value)  # Convert to string for JSON serialization
                elif value is None:
                    row[key] = None
                elif not isinstance(value, (list, dict, type(None))):
                    try:
                        if pd.isna(value):
                            row[key] = None
                        elif isinstance(value, (pd.Timestamp, pd.DatetimeTZDtype)):
                            row[key] = str(value)
                    except (ValueError, TypeError):
                        # If pd.isna fails, just convert to string
                        row[key] = str(value)

        # Get before rows (combined source sample) - limit to same number as requested
        before_rows = sample_df.head(limit).to_dict('records')
        for row in before_rows:
            for key, value in row.items():
                # Handle nested structures (MongoDB arrays/objects) and numpy arrays
                if isinstance(value, (list, dict)) or (hasattr(value, '__iter__') and not isinstance(value, (str, bytes))):
                    row[key] = str(value)  # Convert to string for JSON serialization
                elif value is None:
                   row[key] = None
                elif not isinstance(value, (list, dict, type(None))):
                    try:
                        if pd.isna(value):
                            row[key] = None
                        elif isinstance(value, (pd.Timestamp, pd.DatetimeTZDtype)):
                            row[key] = str(value)
                    except (ValueError, TypeError):
                        # If pd.isna fails, just convert to string
                        row[key] = str(value)
        
        execution_time = int((time.time() - start_time) * 1000)
        
        return SQLTestResponse(
            valid=True,
            schema=schema,
            sample_rows=sample_rows,
            before_rows=before_rows,
            source_samples=source_samples,  # Include per-source samples
            execution_time_ms=execution_time
        )
        
    except duckdb.Error as e:
        # SQL execution error
        return SQLTestResponse(
            valid=False,
            error=f"SQL Error: {str(e)}"
        )
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        # Other errors
        import traceback
        traceback.print_exc()
        return SQLTestResponse(
            valid=False,
            error=f"Error: {str(e)}"
        )


async def _load_and_union_sources(
    sources_info: List[SourceInfo],
    db,
    limit: int = 100
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Load data from multiple sources and combine with UNION ALL.
    Aligns schemas by adding NULL columns where needed.
    
    Args:
        sources_info: List of source information (dataset_id and columns)
        db: MongoDB database instance
        limit: Max rows to load from each source
        
    Returns:
        Tuple of (combined DataFrame, list of per-source samples)
    """
    # Collect all unique columns across all sources
    all_columns = []
    for source in sources_info:
        for col in source.columns:
            if col not in all_columns:
                all_columns.append(col)
    
    combined_dfs = []
    source_samples = []  # Store individual source samples
    individual_sources = []  # Store individual source DataFrames for separate table registration
    
    for source in sources_info:
        # Get source dataset and connection
        source_dataset, connection = await _get_source_dataset_and_connection(
            source.source_dataset_id,
            db
        )
        
        # Load sample data
        df = await _load_sample_data(source_dataset, connection, limit=limit)
        
        if df is None or len(df) == 0:
            continue
        
        # Select only the columns specified for this source
        available_cols = [col for col in source.columns if col in df.columns]
        if available_cols:
            df_original = df[available_cols].copy()  # Keep original for source sample
        else:
            # If none of the requested columns exist, skip this source
            continue
        
        # Store individual source sample (before adding NULL columns)
        source_sample_rows = df_original.head(5).to_dict('records')
        for row in source_sample_rows:
            for key, value in row.items():
                # Handle nested structures (MongoDB arrays/objects) and numpy arrays
                if isinstance(value, (list, dict)) or (hasattr(value, '__iter__') and not isinstance(value, (str, bytes))):
                    row[key] = str(value)  # Convert to string for JSON serialization
                elif value is None:
                    row[key] = None
                elif not isinstance(value, (list, dict, type(None))):
                    try:
                        if pd.isna(value):
                            row[key] = None
                        elif isinstance(value, (pd.Timestamp, pd.DatetimeTZDtype)):
                            row[key] = str(value)
                    except (ValueError, TypeError):
                        # If pd.isna fails, just convert to string
                        row[key] = str(value)
        
        source_samples.append({
            "source_name": source_dataset.get("name", "Unknown Source"),
            "rows": source_sample_rows
        })
        
        # Store individual source DataFrame (before adding NULL columns for UNION ALL)
        # This allows each source to be registered as a separate table in DuckDB
        dataset_name = source_dataset.get("name", f"source_{source.source_dataset_id}")
        individual_sources.append({
            "dataset_name": dataset_name,
            "dataframe": df_original.copy()
        })
        
        # Add NULL columns for columns from other sources (for UNION ALL)
        df = df[available_cols]
        for col in all_columns:
            if col not in df.columns:
                df[col] = None
        
        # Reorder columns to match all_columns order
        df = df[all_columns]
        
        combined_dfs.append(df)
    
    if not combined_dfs:
        raise HTTPException(
            status_code=400,
            detail="No data available from any source"
        )
    
    # Combine all DataFrames (UNION ALL)
    combined_df = pd.concat(combined_dfs, ignore_index=True)
    
    return combined_df, source_samples, individual_sources


async def _load_sample_data(
    source_dataset: dict,
    connection: dict,
    limit: int = 1000
) -> pd.DataFrame:
    """
    Load sample data from source (default 1000 rows)
    Supports: PostgreSQL, MySQL, MongoDB, S3/Parquet
    """
    source_type = source_dataset.get("source_type")
    config = connection.get("config", {}) if connection else {}
    
    if source_type in ['postgres', 'postgresql']:
        # PostgreSQL
        import psycopg2
        
        conn_str = f"host={config.get('host')} port={config.get('port', 5432)} " \
                   f"dbname={config.get('database_name')} " \
                   f"user={config.get('user_name')} password={config.get('password')}"
        
        with psycopg2.connect(conn_str) as conn:
            query = f"SELECT * FROM {source_dataset.get('table')} LIMIT 1000"
            df = pd.read_sql(query, conn)
        
        return df
    
    elif source_type == 'mysql':
        # MySQL
        import pymysql
        
        conn = pymysql.connect(
            host=config.get('host'),
            port=int(config.get('port', 3306)),
            user=config.get('user_name'),
            password=config.get('password'),
            database=config.get('database_name')
        )
        
        query = f"SELECT * FROM {source_dataset.get('table')} LIMIT 1000"
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df
    
    elif source_type == 'mongodb':
        # MongoDB
        from pymongo import MongoClient
        
        client = MongoClient(config.get('uri'))
        db = client[config.get('database')]
        collection = db[source_dataset.get('collection')]
        
        # Get documents
        data = list(collection.find().limit(limit))
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Remove MongoDB _id if present
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
        
        # Convert dot notation columns to underscore (e.g., address.city -> address_city)
        # This prevents DuckDB from interpreting them as table references
        df.columns = [col.replace('.', '_') for col in df.columns]
        
        client.close()
        
        return df
    
    elif source_type == 's3':
        # S3 / Parquet (Catalog datasets or Source datasets)
        import duckdb

        # Check if this is a log file source (not supported for Run Test yet)
        file_format = source_dataset.get("format", "parquet")
        if file_format == "log":
            raise HTTPException(
                status_code=400,
                detail="S3 log file testing is not yet supported. Log files require regex parsing which will be validated during actual job execution. Please proceed to the next step."
            )

        # source_datasets: bucket + path 별도
        # catalog datasets: path에 전체 경로
        bucket = source_dataset.get("bucket")
        path = source_dataset.get("path")

        if bucket and path:
            # source_datasets 형태: bucket + path 조합
            path = f"s3://{bucket}/{path}"
        elif not path:
            raise ValueError("S3 dataset missing path")
            
        # Ensure path is DuckDB compatible
        # Spark paths like s3a:// should be converted or handled
        duck_path = path.replace("s3a://", "s3://")
        
        # Use DuckDB to read a sample from Parquet
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        
        # Configure S3 (Prioritize config, fallback to environment)
        import os
        
        # Configure S3 using Boto3 credential chain (Safe for LocalStack & Production IAM Roles)
        import boto3
        
        # 1. Resolve Region
        region = config.get("region") or os.getenv("AWS_REGION") or "us-east-1"
        
        # 2. Get Credentials (Prioritize config, then environment variables)
        access_key = config.get("access_key") or os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = config.get("secret_key") or os.getenv("AWS_SECRET_ACCESS_KEY")
        
        if access_key and secret_key:
            con.execute(f"SET s3_access_key_id='{access_key}';")
            con.execute(f"SET s3_secret_access_key='{secret_key}';")
        else:
            # Fetch from environment or IAM Role via Boto3
            session = boto3.Session()
            creds = session.get_credentials()
            if creds:
                frozen = creds.get_frozen_credentials()
                if frozen:
                    con.execute(f"SET s3_access_key_id='{frozen.access_key}';")
                    con.execute(f"SET s3_secret_access_key='{frozen.secret_key}';")
                    if frozen.token:
                        con.execute(f"SET s3_session_token='{frozen.token}';")
                    # Update access_key/secret_key for boto3 client later
                    access_key = frozen.access_key
                    secret_key = frozen.secret_key
            
            # Update region if session found one
            if session.region_name:
                region = session.region_name

        con.execute(f"SET s3_region='{region}';")

        # 3. Handle Endpoint (LocalStack/MinIO)
        endpoint = config.get("endpoint") or os.getenv("AWS_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL") or os.getenv("S3_ENDPOINT_URL")
        
        if endpoint:
            # Handle LocalStack / MinIO
            endpoint_url = endpoint.replace("http://", "").replace("https://", "")
            
            # If running outside docker (localhost) but container uses service name, 
            # we might need to be careful, but generally assume the env var is correct for the running context.
            con.execute(f"SET s3_endpoint='{endpoint_url}';")
            
            # Disable SSL for HTTP endpoints (common in LocalStack/MinIO)
            if "http://" in endpoint:
                con.execute("SET s3_use_ssl=false;")
                con.execute("SET s3_url_style='path';") # Force path style for MinIO/LocalStack
        
        # Use boto3 to list files if path is a directory (Robust for all envs)
        import boto3
        from botocore.client import Config
        from urllib.parse import urlparse

        try:
            # Parse bucket and prefix from path
            parsed = urlparse(duck_path)
            bucket_name = parsed.netloc
            prefix = parsed.path.lstrip('/')

             # Only enforce path style if using custom endpoint (LocalStack/MinIO)
            s3_config = None
            if endpoint:
                 s3_config = Config(s3={'addressing_style': 'path'}, signature_version='s3v4')

            # Create boto3 client - use credentials from config or environment
            s3_client = boto3.client(
                's3',
                endpoint_url=endpoint if endpoint else None,
                aws_access_key_id=access_key if access_key else None,
                aws_secret_access_key=secret_key if secret_key else None,
                region_name=region,
                use_ssl=False if endpoint and "http://" in endpoint else True,
                config=s3_config
            )
            
            # List objects
            print(f"[DEBUG] Listing objects in Bucket: {bucket_name}, Prefix: {prefix}")
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            
            parquet_files = []
            if 'Contents' in response:
                print(f"[DEBUG] Found {len(response['Contents'])} objects")
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.endswith('.parquet') and not key.endswith('/'):
                        parquet_files.append(f"s3://{bucket_name}/{key}")
            
            if parquet_files:
                files_str = ", ".join([f"'{f}'" for f in parquet_files])
                query = f"SELECT * FROM read_parquet([{files_str}]) LIMIT {limit}"
            else:
                 # Fallback
                 debug_info = f"Boto3 found 0 files. Config: endpoint={endpoint}, bucket={bucket_name}, prefix={prefix}"
                 print(f"[DEBUG] {debug_info}")
                 
                 if not duck_path.endswith('.parquet'):
                     if not duck_path.endswith('/'):
                         duck_path += '/'
                     duck_path += '**/*.parquet'
                 query = f"SELECT * FROM read_parquet('{duck_path}') LIMIT {limit}"

        except Exception as e:
            print(f"[DEBUG] Boto3 listing failed: {e}")
            # Fallback on error
            if not duck_path.endswith('.parquet'):
                 if not duck_path.endswith('/'):
                     duck_path += '/'
                 duck_path += '**/*.parquet'
            query = f"SELECT * FROM read_parquet('{duck_path}') LIMIT {limit}"

        try:
            df = con.execute(query).df()
        except Exception as e:
             extra_info = f" | {debug_info}" if 'debug_info' in locals() else ""
             raise HTTPException(status_code=500, detail=f"DuckDB Read Error: {str(e)}. Path: {duck_path}{extra_info}")
        
        return df
    
    elif source_type == 'api':
        import requests

        api_config = source_dataset.get("api", {})
        endpoint = api_config.get("endpoint")
        if not endpoint:
            raise HTTPException(status_code=400, detail="API endpoint is not configured")

        base_url = config.get("base_url", "")
        full_url = base_url.rstrip("/") + "/" + endpoint.lstrip("/")

        headers = config.get("headers", {}).copy() if config.get("headers") else {}
        auth_type = config.get("auth_type", "none")
        auth_config = config.get("auth_config", {})

        if auth_type == "api_key":
            header_name = auth_config.get("header_name")
            api_key = auth_config.get("api_key")
            if header_name and api_key:
                headers[header_name] = api_key
        elif auth_type == "bearer":
            token = auth_config.get("token")
            if token:
                headers["Authorization"] = f"Bearer {token}"

        auth = None
        if auth_type == "basic":
            username = auth_config.get("username")
            password = auth_config.get("password")
            if username and password:
                from requests.auth import HTTPBasicAuth
                auth = HTTPBasicAuth(username, password)

        params = api_config.get("query_params", {}).copy() if api_config.get("query_params") else {}
        params["limit"] = limit

        pagination = api_config.get("pagination", {})
        pagination_type = pagination.get("type", "none")
        pagination_config = pagination.get("config", {})

        if pagination_type == "offset_limit":
            offset_param = pagination_config.get("offset_param", "offset")
            limit_param = pagination_config.get("limit_param", "limit")
            params[offset_param] = 0
            params[limit_param] = limit
        elif pagination_type == "page":
            page_param = pagination_config.get("page_param", "page")
            per_page_param = pagination_config.get("per_page_param", "per_page")
            params[page_param] = 1
            params[per_page_param] = limit

        try:
            response = requests.get(full_url, headers=headers, auth=auth, params=params, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise HTTPException(status_code=500, detail=f"API request failed: {str(e)}")

        try:
            json_data = response.json()
        except ValueError:
            raise HTTPException(status_code=500, detail="API response is not valid JSON")

        response_path = api_config.get("response_path", "")
        if response_path:
            keys = response_path.replace("$.", "").split(".")
            current = json_data
            for key in keys:
                if isinstance(current, dict):
                    current = current.get(key)
                else:
                    break
            extracted_data = current
        else:
            extracted_data = json_data

        if isinstance(extracted_data, dict):
            extracted_data = [extracted_data]
        elif not isinstance(extracted_data, list):
            raise HTTPException(
                status_code=500,
                detail="Extracted data is not an array or object. Check your response_path setting.",
            )

        return pd.DataFrame(extracted_data[:limit])
    
    else:
        raise ValueError(f"Unsupported source type: {source_type}")


# ============================================================================
# Helper Functions
# ============================================================================

async def _get_source_dataset_and_connection(
    source_id: str,
    db
) -> Tuple[dict, Optional[dict]]:
    """
    Fetch source dataset and connection from MongoDB.
    Tries source_datasets first, then catalog datasets.
    
    Args:
        source_id: Source dataset ID (ObjectId as string)
        db: MongoDB database instance
        
    Returns:
        Tuple of (source_dataset dict, connection dict or None)
        
    Raises:
        HTTPException (404) if dataset not found
    """
    source_dataset = None
    connection = None
    
    # Try source_datasets first
    try:
        source_dataset = await db.source_datasets.find_one({"_id": ObjectId(source_id)})
        if source_dataset:
            connection_id = source_dataset.get("connection_id")
            if connection_id:
                connection = await db.connections.find_one({"_id": ObjectId(connection_id)})
    except Exception:
        pass
    
    # Try catalog datasets if not found
    if not source_dataset:
        try:
            catalog_dataset = await db.datasets.find_one({"_id": ObjectId(source_id)})
            if catalog_dataset:
                destination = catalog_dataset.get("destination", {})
                base_path = destination.get("path", "")
                name = catalog_dataset.get("name", "")
                
                # Construct full path
                full_path = base_path
                if base_path and name and not base_path.endswith(name):
                    if not base_path.endswith('/'):
                        full_path += '/'
                    full_path += name
                
                # Fallback: construct from URN if path is empty
                if not full_path and catalog_dataset.get("targets"):
                    target = catalog_dataset.get("targets")[0]
                    urn = target.get("urn", "")
                    if urn.startswith("urn:s3:"):
                        parts = urn.split(":")
                        if len(parts) >= 4:
                            bucket = parts[2]
                            key = ":".join(parts[3:]) or name
                            full_path = f"s3a://{bucket}/{key}"
                
                source_dataset = {
                    "id": str(catalog_dataset.get("_id")),
                    "name": catalog_dataset.get("name"),
                    "source_type": destination.get("type", "s3"),
                    "path": full_path,
                    "format": destination.get("format", "parquet"),
                    "is_catalog": True
                }
                connection = await db.connections.find_one({"type": "s3"})
        except Exception:
            pass
    
    if not source_dataset:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset not found: {source_id}"
        )
    
    return source_dataset, connection
