from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from utils.duckdb_client import execute_query, get_schema, preview_data
import math

router = APIRouter()


class QueryRequest(BaseModel):
    sql: str


def clean_data(data: list[dict]) -> list[dict]:
    """NaN, Inf 값을 None으로 변환"""
    for row in data:
        for key, value in row.items():
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                row[key] = None
    return data


@router.post("/query")
async def run_query(request: QueryRequest):
    """SQL 쿼리 실행"""
    try:
        data = execute_query(request.sql)
        data = clean_data(data)
        return {"data": data, "row_count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/schema")
async def get_table_schema(path: str):
    """
    S3 경로의 스키마 조회
    예: /api/duckdb/schema?path=s3://xflow-data-lake/new-one/*.parquet
    """
    try:
        schema = get_schema(path)
        return {"schema": schema}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/preview")
async def preview_table(path: str, limit: int = 100):
    """
    S3 경로의 데이터 미리보기
    예: /api/duckdb/preview?path=s3://xflow-data-lake/new-one/*.parquet&limit=10
    """
    try:
        data = preview_data(path, limit)
        return {"data": data, "row_count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/buckets")
async def list_s3_contents(bucket: str = "xflow-data-lake", prefix: str = ""):
    """S3 버킷 내용 조회"""
    try:
        path = f"s3://{bucket}/{prefix}*" if prefix else f"s3://{bucket}/*"
        data = execute_query(f"SELECT * FROM glob('{path}')")
        return {"files": data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
