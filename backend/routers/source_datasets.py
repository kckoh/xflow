from fastapi import APIRouter, HTTPException
from typing import List
from datetime import datetime
from bson import ObjectId
import os

import database
from schemas.source_dataset import (
    SourceDatasetCreate,
    SourceDatasetUpdate,
    SourceDatasetResponse,
)


async def get_s3_schema(bucket: str, path: str) -> List[dict]:
    """
    DuckDB를 사용하여 S3 Parquet 파일의 스키마 조회
    """
    try:
        import duckdb
        import boto3

        if not bucket or not path:
            return []

        # S3 경로 구성
        s3_path = f"s3://{bucket}/{path}"

        # 경로가 파일이 아니라 디렉토리면 /*.parquet 추가
        if not s3_path.endswith('.parquet'):
            if not s3_path.endswith('/'):
                s3_path += '/'
            s3_path += '*.parquet'

        # DuckDB 설정
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")

        # AWS 자격 증명 설정
        session = boto3.Session()
        creds = session.get_credentials()

        if creds:
            frozen = creds.get_frozen_credentials()
            con.execute(f"SET s3_access_key_id='{frozen.access_key}';")
            con.execute(f"SET s3_secret_access_key='{frozen.secret_key}';")
            if frozen.token:
                con.execute(f"SET s3_session_token='{frozen.token}';")

        region = session.region_name or os.getenv("AWS_REGION", "ap-northeast-2")
        con.execute(f"SET s3_region='{region}';")

        # LocalStack 엔드포인트 처리
        endpoint = os.getenv("AWS_ENDPOINT") or os.getenv("S3_ENDPOINT_URL")
        if endpoint:
            endpoint_url = endpoint.replace("http://", "").replace("https://", "")
            con.execute(f"SET s3_endpoint='{endpoint_url}';")
            if "http://" in endpoint:
                con.execute("SET s3_use_ssl=false;")
                con.execute("SET s3_url_style='path';")

        # 스키마만 조회 (LIMIT 0)
        query = f"SELECT * FROM read_parquet('{s3_path}') LIMIT 0"
        result = con.execute(query)

        # 컬럼 정보 추출 - description 사용
        schema = []
        for col_info in result.description:
            schema.append({
                "name": col_info[0],
                "type": col_info[1]
            })

        con.close()
        return schema

    except Exception as e:
        print(f"Failed to get S3 schema for {bucket}/{path}: {e}")
        return []

router = APIRouter(prefix="/api/source-datasets", tags=["source-datasets"])


def get_db():
    return database.mongodb_client[database.DATABASE_NAME]


@router.post("", response_model=SourceDatasetResponse)
async def create_source_dataset(dataset: SourceDatasetCreate):
    """Create a new source dataset"""
    db = get_db()
    now = datetime.utcnow()

    dataset_data = {
        **dataset.model_dump(),
        "created_at": now,
        "updated_at": now,
    }

    result = await db.source_datasets.insert_one(dataset_data)
    dataset_data["id"] = str(result.inserted_id)

    return dataset_data


@router.get("", response_model=List[SourceDatasetResponse])
async def get_source_datasets():
    """Get all source datasets"""
    db = get_db()
    cursor = db.source_datasets.find()
    datasets = []

    async for doc in cursor:
        doc["id"] = str(doc["_id"])
        del doc["_id"]

        # S3 타입인 경우 DuckDB로 스키마 조회
        if doc.get("source_type") == "s3":
            # 저장된 columns가 없거나 비어있으면 S3에서 조회
            if not doc.get("columns"):
                bucket = doc.get("bucket")
                path = doc.get("path")
                if bucket and path:
                    s3_schema = await get_s3_schema(bucket, path)
                    if s3_schema:
                        doc["columns"] = s3_schema

        datasets.append(doc)

    return datasets


@router.get("/{dataset_id}", response_model=SourceDatasetResponse)
async def get_source_dataset(dataset_id: str):
    """Get a specific source dataset"""
    db = get_db()

    try:
        doc = await db.source_datasets.find_one({"_id": ObjectId(dataset_id)})
    except:
        raise HTTPException(status_code=400, detail="Invalid dataset ID format")

    if not doc:
        raise HTTPException(status_code=404, detail="Source dataset not found")

    doc["id"] = str(doc["_id"])
    del doc["_id"]

    # S3 타입인 경우 DuckDB로 스키마 조회
    if doc.get("source_type") == "s3":
        if not doc.get("columns"):
            bucket = doc.get("bucket")
            path = doc.get("path")
            if bucket and path:
                s3_schema = await get_s3_schema(bucket, path)
                if s3_schema:
                    doc["columns"] = s3_schema

    return doc


@router.put("/{dataset_id}", response_model=SourceDatasetResponse)
async def update_source_dataset(dataset_id: str, dataset: SourceDatasetUpdate):
    """Update a source dataset"""
    db = get_db()

    try:
        obj_id = ObjectId(dataset_id)
    except:
        raise HTTPException(status_code=400, detail="Invalid dataset ID format")

    existing = await db.source_datasets.find_one({"_id": obj_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Source dataset not found")

    update_data = {k: v for k, v in dataset.model_dump().items() if v is not None}
    update_data["updated_at"] = datetime.utcnow()

    await db.source_datasets.update_one({"_id": obj_id}, {"$set": update_data})

    updated = await db.source_datasets.find_one({"_id": obj_id})
    updated["id"] = str(updated["_id"])
    del updated["_id"]
    return updated


@router.delete("/{dataset_id}")
async def delete_source_dataset(dataset_id: str):
    """Delete a source dataset"""
    db = get_db()

    try:
        obj_id = ObjectId(dataset_id)
    except:
        raise HTTPException(status_code=400, detail="Invalid dataset ID format")

    result = await db.source_datasets.delete_one({"_id": obj_id})

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Source dataset not found")

    return {"message": "Source dataset deleted successfully"}
