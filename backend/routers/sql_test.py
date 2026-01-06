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


class ColumnSchema(BaseModel):
    name: str
    type: str
    nullable: bool = True


class SQLTestResponse(BaseModel):
    valid: bool
    schema: Optional[List[ColumnSchema]] = None
    sample_rows: Optional[List[Dict[str, Any]]] = None
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
    
    try:
        # Get MongoDB database
        db = database.mongodb_client[database.DATABASE_NAME]
        
        # 1. Get source dataset
        try:
            source_dataset = await db.source_datasets.find_one({"_id": ObjectId(request.source_dataset_id)})
            if not source_dataset:
                raise HTTPException(
                    status_code=404,
                    detail=f"Source dataset not found: {request.source_dataset_id}"
                )
        except Exception as e:
            raise HTTPException(
                status_code=404,
                detail=f"Source dataset not found: {request.source_dataset_id}"
            )
        
        # 2. Get connection details
        connection_id = source_dataset.get("connection_id")
        if not connection_id:
            raise HTTPException(
                status_code=400,
                detail="Source dataset has no connection_id"
            )
        
        connection = await db.connections.find_one({"_id": ObjectId(connection_id)})
        if not connection:
            raise HTTPException(
                status_code=404,
                detail=f"Connection not found: {connection_id}"
            )
        
        # 3. Load sample data (1000 rows)
        sample_df = await _load_sample_data(source_dataset, connection)
        
        if sample_df is None or len(sample_df) == 0:
            raise HTTPException(
                status_code=400,
                detail="No data available from source"
            )
        
        # 4. Execute SQL with DuckDB
        con = duckdb.connect()
        
        # Register sample data as "input" table
        con.register('input', sample_df)
        
        # Execute user's SQL
        result_df = con.execute(request.sql).df()
        
        # 5. Extract schema
        schema = []
        for col in result_df.columns:
            dtype = str(result_df[col].dtype)
            # Map pandas dtypes to readable types
            if 'int' in dtype:
                col_type = 'integer'
            elif 'float' in dtype:
                col_type = 'float'
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
        
        # 6. Get sample rows (first 5)
        sample_rows = result_df.head(5).to_dict('records')
        
        # Convert any non-serializable types
        for row in sample_rows:
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
        return SQLTestResponse(
            valid=False,
            error=f"Error: {str(e)}"
        )


async def _load_sample_data(
    source_dataset: dict,
    connection: dict
) -> pd.DataFrame:
    """
    Load sample data from source (1000 rows)
    Supports: PostgreSQL, MySQL, MongoDB
    """
    source_type = source_dataset.get("source_type")
    config = connection.get("config", {})
    
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
        
        # Get 1000 documents
        data = list(collection.find().limit(1000))
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Remove MongoDB _id if present
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
        
        client.close()
        
        return df
    
    else:
        raise ValueError(f"Unsupported source type: {source_type}")
