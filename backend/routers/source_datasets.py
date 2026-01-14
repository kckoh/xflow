from fastapi import APIRouter, HTTPException
from typing import List, Optional
from datetime import datetime
from bson import ObjectId
import os

import database
from schemas.source_dataset import (
    SourceDatasetCreate,
    SourceDatasetUpdate,
    SourceDatasetResponse,
)
from dependencies import sessions


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

    if dataset.source_type == "api" and not dataset.api:
        raise HTTPException(status_code=400, detail="API source requires api config")
    if dataset.source_type != "api" and dataset.api:
        raise HTTPException(status_code=400, detail="api config is only allowed for api source_type")

    dataset_data = {
        **dataset.model_dump(),
        "created_at": now,
        "updated_at": now,
    }
    
    # Extract schema from S3 when creating S3 source dataset (one-time operation)
    if dataset.source_type == "s3" and not dataset_data.get("columns"):
        bucket = dataset_data.get("bucket")
        path = dataset_data.get("path")
        if bucket and path:
            try:
                s3_schema = await get_s3_schema(bucket, path)
                if s3_schema:
                    dataset_data["columns"] = s3_schema
                    print(f"Extracted schema from S3 for {dataset.name}: {len(s3_schema)} columns")
            except Exception as e:
                print(f"Warning: Failed to extract schema from S3: {e}")
                # Continue without schema - user can add manually later

    result = await db.source_datasets.insert_one(dataset_data)
    dataset_data["id"] = str(result.inserted_id)

    return dataset_data



@router.get("", response_model=List[SourceDatasetResponse])
async def get_source_datasets(session_id: Optional[str] = None):
    """Get all source datasets, filtered by user permissions"""
    db = get_db()
    cursor = db.source_datasets.find()
    datasets = []

    async for doc in cursor:
        doc["id"] = str(doc["_id"])
        del doc["_id"]

        # Use stored columns/schema from MongoDB (fast!)
        # Schema is saved when source dataset is created
        if not doc.get("columns"):
            # Fallback to schema field if columns not present
            doc["columns"] = doc.get("schema", [])

        datasets.append(doc)

    # Filter by user permissions if session_id is provided
    if session_id and session_id in sessions:
        user_session = sessions[session_id]
        is_admin = user_session.get("is_admin", False)

        # Admin can see all datasets
        if not is_admin:
            all_datasets_access = user_session.get("all_datasets", False)

            if not all_datasets_access:
                # Get allowed dataset IDs from session
                allowed_dataset_ids = user_session.get("dataset_access", [])

                # Filter datasets to only those the user can access
                datasets = [d for d in datasets if d["id"] in allowed_dataset_ids]

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

    new_source_type = dataset.source_type or existing.get("source_type")
    if new_source_type == "api" and dataset.api is None and not existing.get("api"):
        raise HTTPException(status_code=400, detail="API source requires api config")
    if new_source_type != "api" and dataset.api:
        raise HTTPException(status_code=400, detail="api config is only allowed for api source_type")

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


@router.post("/{dataset_id}/preview")
async def preview_api_source(dataset_id: str, request_body: dict):
    """
    Preview API source data by making a test request
    Used to infer schema for API sources
    """
    db = database.mongodb_client[database.DATABASE_NAME]

    try:
        obj_id = ObjectId(dataset_id)
    except:
        raise HTTPException(status_code=400, detail="Invalid dataset ID format")

    # Get source dataset
    source_dataset = await db.source_datasets.find_one({"_id": obj_id})
    if not source_dataset:
        raise HTTPException(status_code=404, detail="Source dataset not found")

    # Only for API sources
    if source_dataset.get("source_type") != "api":
        raise HTTPException(
            status_code=400,
            detail="Preview is only supported for API sources",
        )

    # Get connection
    connection_id = source_dataset.get("connection_id")
    if not connection_id:
        raise HTTPException(status_code=400, detail="Connection ID is missing")

    try:
        conn_obj_id = ObjectId(connection_id)
    except:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    connection = await db.connections.find_one({"_id": conn_obj_id})
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    # Get API config
    api_config = source_dataset.get("api", {})
    endpoint = api_config.get("endpoint")
    if not endpoint:
        raise HTTPException(status_code=400, detail="API endpoint is not configured")

    # Build request
    import requests

    base_url = connection["config"].get("base_url", "")
    full_url = base_url.rstrip("/") + "/" + endpoint.lstrip("/")

    # Auth headers
    headers = connection["config"].get("headers", {}).copy() if connection["config"].get("headers") else {}
    auth_type = connection["config"].get("auth_type", "none")
    auth_config = connection["config"].get("auth_config", {})

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

    # Query params (limit for preview)
    limit = request_body.get("limit", 10)
    params = api_config.get("query_params", {}).copy() if api_config.get("query_params") else {}

    # Add pagination for preview (limit to 10)
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

    # Make request
    try:
        response = requests.get(full_url, headers=headers, auth=auth, params=params, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"API request failed: {str(e)}")

    # Parse response
    try:
        json_data = response.json()
    except ValueError:
        raise HTTPException(status_code=500, detail="API response is not valid JSON")

    # Extract data using response_path
    response_path = api_config.get("response_path", "")
    if response_path:
        # Simple JSONPath extraction
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

    # Ensure it's a list
    if not isinstance(extracted_data, list):
        if isinstance(extracted_data, dict):
            extracted_data = [extracted_data]
        else:
            raise HTTPException(
                status_code=500,
                detail="Extracted data is not an array or object. Check your response_path setting.",
            )

    return {"data": extracted_data[:limit], "count": len(extracted_data[:limit])}
