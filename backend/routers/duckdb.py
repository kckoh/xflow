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


def get_s3_client():
    import boto3
    import os

    environment = os.getenv("ENVIRONMENT", "local")
    region = os.getenv("AWS_REGION", "ap-northeast-2")

    if environment == "production":
        # Production: Use IAM role credentials (no explicit keys needed)
        return boto3.client("s3", region_name=region)
    else:
        # Local: Use LocalStack
        return boto3.client(
            "s3",
            endpoint_url=os.getenv("AWS_ENDPOINT", "http://localstack-main:4566"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            region_name=region,
        )


@router.get("/buckets")
async def list_all_buckets():
    """모든 S3 버킷 목록 조회"""
    try:
        s3 = get_s3_client()
        response = s3.list_buckets()
        buckets = [b["Name"] for b in response.get("Buckets", [])]
        return {"buckets": buckets}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/buckets/{bucket}/files")
async def list_bucket_files(bucket: str, prefix: str = ""):
    """특정 버킷의 파일 목록 조회"""
    try:
        s3 = get_s3_client()
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = []
        for obj in response.get("Contents", []):
            files.append({
                "file": f"s3://{bucket}/{obj['Key']}",
                "size": obj["Size"],
            })

        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
