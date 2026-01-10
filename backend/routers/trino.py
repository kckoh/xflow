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
    """Trino SQL 쿼리 실행"""
    try:
        data = execute_query(
            request.sql,
            catalog=request.catalog,
            schema=request.schema_name
        )
        data = clean_data(data)
        return {"data": data, "row_count": len(data)}
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
