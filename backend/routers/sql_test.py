"""
SQL Test API - Test SQL queries with DuckDB before execution
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import duckdb
import pandas as pd
from bson import ObjectId

import database

router = APIRouter()


class SQLTestRequest(BaseModel):
    source_dataset_id: str
    sql: str
    limit: Optional[int] = 5


class ColumnSchema(BaseModel):
    name: str
    type: str
    nullable: bool = True


class SQLTestResponse(BaseModel):
    valid: bool
    schema: Optional[List[ColumnSchema]] = None
    sample_rows: Optional[List[Dict[str, Any]]] = None
    before_rows: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    execution_time_ms: Optional[int] = None


@router.post("/test", response_model=SQLTestResponse)
async def test_sql_query(request: SQLTestRequest):
    """
    Test SQL query with sample data using DuckDB
    
    Fast validation before running full Spark job
    """
    import time
    start_time = time.time()
    limit = request.limit or 5
    
    try:
        # Get MongoDB database
        db = database.mongodb_client[database.DATABASE_NAME]
        
        # 1. Get source dataset details (Try source_datasets first, then catalog datasets)
        source_dataset = None
        connection = None
        
        # Try source_datasets
        try:
            source_dataset = await db.source_datasets.find_one({"_id": ObjectId(request.source_dataset_id)})
        except:
            pass
            
        if source_dataset:
            # Get connection details for source_dataset
            connection_id = source_dataset.get("connection_id")
            if connection_id:
                connection = await db.connections.find_one({"_id": ObjectId(connection_id)})
        else:
            # Try catalog datasets (Dataset model)
            try:
                catalog_dataset = await db.datasets.find_one({"_id": ObjectId(request.source_dataset_id)})
                if catalog_dataset:
                    # Map catalog dataset to a common format for _load_sample_data
                    destination = catalog_dataset.get("destination", {})
                    source_dataset = {
                        "id": str(catalog_dataset.get("_id")),
                        "name": catalog_dataset.get("name"),
                        "source_type": destination.get("type", "s3"), # Usually s3 for catalog
                        "path": destination.get("path"),
                        "format": destination.get("format", "parquet"),
                        "is_catalog": True
                    }
                    # For catalog datasets in S3, we might not need a connection object if credentials are env-based
                    # but we look for a default S3 connection anyway
                    connection = await db.connections.find_one({"type": "s3"})
            except:
                pass

        if not source_dataset:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset not found: {request.source_dataset_id}"
            )
        
        # 3. Load sample data (using limit)
        sample_df = await _load_sample_data(source_dataset, connection, limit=100)
        
        if sample_df is None or len(sample_df) == 0:
            raise HTTPException(
                status_code=400,
                detail="No data available from source"
            )
        
        # 4. Execute SQL with DuckDB
        con = duckdb.connect()
        
        # Register sample data as "input" table
        con.register('input', sample_df)
        
        # Execute user's SQL and apply limit
        sql = request.sql
        # Only add limit if not present and if it's a simple select
        if "limit" not in sql.lower():
            sql = f"SELECT * FROM ({sql}) LIMIT {limit}"
            
        result_df = con.execute(sql).df()
        
        # 5. Extract schema
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
        
        # 6. Get sample rows
        sample_rows = result_df.to_dict('records')
        
        # Convert any non-serializable types for sample_rows
        for row in sample_rows:
            for key, value in row.items():
                if pd.isna(value):
                    row[key] = None
                elif isinstance(value, (pd.Timestamp, pd.DatetimeTZDtype)):
                    row[key] = str(value)

        # 7. Get before rows (source sample) - limit to same number as requested
        before_rows = sample_df.head(limit).to_dict('records')
        for row in before_rows:
            for key, value in row.items():
                if pd.isna(value):
                    row[key] = None
                elif isinstance(value, (pd.Timestamp, pd.DatetimeTZDtype)):
                    row[key] = str(value)
        
        execution_time = int((time.time() - start_time) * 1000)
        
        return SQLTestResponse(
            valid=True,
            schema=schema,
            sample_rows=sample_rows,
            before_rows=before_rows,
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
        
        client.close()
        
        return df
    
    elif source_type == 's3':
        # S3 / Parquet (Catalog datasets)
        import duckdb
        path = source_dataset.get("path")
        if not path:
            raise ValueError("S3 dataset missing path")
            
        # Ensure path is DuckDB compatible
        # Spark paths like s3a:// should be converted or handled
        duck_path = path.replace("s3a://", "s3://")
        
        # Use DuckDB to read a sample from Parquet
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        
        # Configure S3 if credentials are available in config
        if config.get("access_key"):
            con.execute(f"SET s3_access_key_id='{config.get('access_key')}';")
            con.execute(f"SET s3_secret_access_key='{config.get('secret_key')}';")
            if config.get("region"):
                con.execute(f"SET s3_region='{config.get('region')}';")
        
        # For MinIO / Custom S3
        if config.get("endpoint"):
            endpoint = config.get("endpoint").replace("http://", "").replace("https://", "")
            con.execute(f"SET s3_endpoint='{endpoint}';")
            if "http://" in config.get("endpoint"):
                con.execute("SET s3_use_ssl=false;")
        
        query = f"SELECT * FROM read_parquet('{duck_path}') LIMIT {limit}"
        df = con.execute(query).df()
        
        return df
    
    else:
        raise ValueError(f"Unsupported source type: {source_type}")
