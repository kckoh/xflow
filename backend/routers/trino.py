from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, Dict, Any
from utils.trino_client import (
    execute_query,
    get_catalogs,
    get_schemas,
    get_tables,
    get_table_schema,
    preview_table,
)
from dependencies import get_user_session
import math
import re

router = APIRouter()


class QueryRequest(BaseModel):
    sql: str
    catalog: Optional[str] = None
    schema_name: Optional[str] = None


def clean_data(data: list[dict]) -> list[dict]:
    """NaN, Inf 값을 None으로 변환"""
    for row in data:
        for key, value in row.items():
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                row[key] = None
    return data


@router.post("/query")
async def run_query(
    request: QueryRequest,
    user_session: Optional[Dict[str, Any]] = Depends(get_user_session)
):
    """Trino SQL 쿼리 실행 (total count는 MongoDB row_count 사용)"""
    try:
        sql = request.sql.strip()
        original_limit = None
        total_count = None

        # Extract LIMIT from query
        limit_match = re.search(r'\bLIMIT\s+(\d+)\b', sql, re.IGNORECASE)
        if limit_match:
            original_limit = int(limit_match.group(1))

            # Try to get total_count from MongoDB if querying lakehouse tables
            # Extract table name from query (e.g., "FROM lakehouse.default.my_table")
            import re
            table_match = re.search(r'\bFROM\s+lakehouse\.default\.(\w+)', sql, re.IGNORECASE)
            if table_match:
                table_name = table_match.group(1)
                try:
                    from models import Dataset
                    from beanie import PydanticObjectId
                    
                    # Find dataset by glue_table_name
                    dataset = await Dataset.find_one({
                        "destination.glue_table_name": table_name
                    })
                    
                    if dataset and hasattr(dataset, 'row_count') and dataset.row_count:
                        total_count = dataset.row_count
                        print(f"Using MongoDB row_count for {table_name}: {total_count}")
                except Exception as e:
                    print(f"Failed to get row_count from MongoDB: {e}")
                    # Continue without total_count

        # Execute main query with original LIMIT
        data = execute_query(
            sql,
            catalog=request.catalog,
            schema=request.schema_name
        )
        data = clean_data(data)

        return {
            "data": data,
            "row_count": len(data),
            "total_count": total_count,
            "has_more": total_count is not None and len(data) < total_count if total_count else False
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/catalogs")
async def list_catalogs():
    """사용 가능한 카탈로그 목록 조회"""
    try:
        catalogs = get_catalogs()
        return {"catalogs": catalogs}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/catalogs/{catalog}/schemas")
async def list_schemas(catalog: str):
    """카탈로그의 스키마 목록 조회"""
    try:
        schemas = get_schemas(catalog)
        return {"schemas": schemas}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/catalogs/{catalog}/schemas/{schema}/tables")
async def list_tables(catalog: str, schema: str):
    """스키마의 테이블 목록 조회"""
    try:
        tables = get_tables(catalog, schema)
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/catalogs/{catalog}/schemas/{schema}/tables/{table}/schema")
async def get_schema(catalog: str, schema: str, table: str):
    """테이블 스키마 조회"""
    try:
        columns = get_table_schema(catalog, schema, table)
        return {"schema": columns}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/catalogs/{catalog}/schemas/{schema}/tables/{table}/preview")
async def preview(catalog: str, schema: str, table: str, limit: int = 100):
    """테이블 데이터 미리보기"""
    try:
        data = preview_table(catalog, schema, table, limit)
        data = clean_data(data)
        return {"data": data, "row_count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
