from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from typing import Optional, Dict, Any
from utils.duckdb_client import execute_query, get_schema, preview_data
from models import Dataset
import math
import re
from dependencies import get_user_session

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
async def run_query(
    request: QueryRequest,
    user_session: Optional[Dict[str, Any]] = Depends(get_user_session)
):
    """SQL 쿼리 실행 (권한 체크 포함, total count는 MongoDB row_count 사용)"""
    try:
        # Check permissions for datasets referenced in SQL

        if user_session:
            is_admin = user_session.get("is_admin", False)
            all_datasets = user_session.get("all_datasets", False)

            # Only check permissions if not admin and not all_datasets
            if not is_admin and not all_datasets:
                # Extract S3 paths from SQL
                s3_paths = re.findall(r's3://[\w\-]+/[\w\-]+', request.sql)

                if s3_paths:
                    dataset_access = user_session.get("dataset_access", [])
                    datasets = await Dataset.find_all().to_list()
                    dataset_id_to_name = {str(d.id): d.name for d in datasets}
                    allowed_dataset_names = [dataset_id_to_name.get(did) for did in dataset_access]

                    # Check each S3 path
                    for s3_path in s3_paths:
                        # Extract dataset name: s3://bucket/dataset_name/... -> dataset_name
                        parts = s3_path.replace('s3://', '').split('/')
                        if len(parts) > 1:
                            dataset_name = parts[1]

                            if dataset_name not in allowed_dataset_names:
                                raise HTTPException(
                                    status_code=403,
                                    detail=f"No permission to access dataset: {dataset_name}"
                                )

        # Try to get total_count from MongoDB if querying S3 datasets
        sql = request.sql.strip()
        original_limit = None
        total_count = None

        # Extract LIMIT from query
        limit_match = re.search(r'\bLIMIT\s+(\d+)\b', sql, re.IGNORECASE)
        if limit_match:
            original_limit = int(limit_match.group(1))
            
            # Extract S3 path to find dataset
            s3_match = re.search(r's3://[\w\-]+/([\w\-]+)', sql)
            if s3_match:
                dataset_name = s3_match.group(1)
                try:
                    # Find dataset by name
                    dataset = await Dataset.find_one({"name": dataset_name})
                    
                    if dataset and hasattr(dataset, 'row_count') and dataset.row_count:
                        total_count = dataset.row_count
                        print(f"Using MongoDB row_count for {dataset_name}: {total_count}")
                except Exception as e:
                    print(f"Failed to get row_count from MongoDB: {e}")
                    # Continue without total_count

        # Execute main query with original LIMIT
        data = execute_query(sql)
        data = clean_data(data)

        return {
            "data": data,
            "row_count": len(data),
            "total_count": total_count,
            "has_more": total_count is not None and len(data) < total_count if total_count else False
        }
    except HTTPException:
        raise
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
async def list_bucket_files(
    bucket: str,
    prefix: str = "",
    user_session: Optional[Dict[str, Any]] = Depends(get_user_session)
):
    """특정 버킷의 파일 목록 조회 (권한 체크 포함)"""
    try:
        s3 = get_s3_client()
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = []
        for obj in response.get("Contents", []):
            files.append({
                "file": f"s3://{bucket}/{obj['Key']}",
                "size": obj["Size"],
            })

        # Filter files by dataset permissions
        
        if user_session:
            is_admin = user_session.get("is_admin", False)
            all_datasets = user_session.get("all_datasets", False)
            
            # Only filter if not admin and not all_datasets
            if not is_admin and not all_datasets:
                dataset_access = user_session.get("dataset_access", [])
                
                # Get allowed dataset names
                datasets = await Dataset.find_all().to_list()
                dataset_id_to_name = {str(d.id): d.name for d in datasets}
                allowed_dataset_names = [dataset_id_to_name.get(did) for did in dataset_access if did in dataset_id_to_name]
                
                # Filter files
                filtered_files = []
                for file_obj in files:
                    # Extract dataset name from path: s3://bucket/dataset_name/file.parquet -> dataset_name
                    file_path = file_obj["file"]
                    parts = file_path.replace(f's3://{bucket}/', '').split('/')
                    
                    if len(parts) > 0:
                        dataset_name = parts[0]
                        
                        # Include if dataset is in allowed list
                        if dataset_name in allowed_dataset_names:
                            filtered_files.append(file_obj)
                
                files = filtered_files

        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
