from fastapi import APIRouter, HTTPException
from typing import List
from datetime import datetime
from bson import ObjectId
import os

import database
import json
import pandas as pd
from kafka import KafkaConsumer
from pydantic import BaseModel
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


def _infer_json_schema(records: List[dict]) -> List[dict]:
    if not records:
        return []

    df = pd.json_normalize(records, sep="_")
    schema = []
    for col in df.columns:
        series = df[col]
        sample = None
        if not series.empty:
            non_null = series.dropna()
            if not non_null.empty:
                sample = non_null.iloc[0]

        if isinstance(sample, bool):
            col_type = "boolean"
        elif isinstance(sample, int):
            col_type = "int"
        elif isinstance(sample, float):
            col_type = "double"
        else:
            col_type = "string"

        schema.append({"name": col, "type": col_type})
    return schema


def _consume_kafka_records(bootstrap_servers: str, topic: str, limit: int = 1) -> List[dict]:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
        request_timeout_ms=3000,
        api_version_auto_timeout_ms=3000,
        fetch_max_wait_ms=500,
        max_poll_records=limit,
        value_deserializer=lambda v: v.decode("utf-8") if v else None,
    )
    records = []
    try:
        for msg in consumer:
            if not msg.value:
                continue
            try:
                payload = json.loads(msg.value)
            except Exception:
                continue
            if isinstance(payload, dict):
                records.append(payload)
            elif isinstance(payload, list):
                records.extend([p for p in payload if isinstance(p, dict)])
            if len(records) >= limit:
                break
    finally:
        consumer.close()
    return records[:limit]


def get_kafka_schema(bootstrap_servers: str, topic: str, limit: int = 1) -> dict:
    records = _consume_kafka_records(bootstrap_servers, topic, limit=limit)
    return {
        "schema": _infer_json_schema(records),
        "sample": records,
    }


class KafkaSchemaRequest(BaseModel):
    connection_id: str
    topic: str
    sample_size: int = 1


class KafkaTopicsRequest(BaseModel):
    connection_id: str


def get_kafka_topics(bootstrap_servers: str) -> List[str]:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        api_version_auto_timeout_ms=5000,
        request_timeout_ms=5000,
        consumer_timeout_ms=5000,
    )
    try:
        topics = consumer.topics() or set()
        return sorted(topics)
    finally:
        consumer.close()

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
    elif dataset.source_type == "kafka" and not dataset_data.get("columns"):
        connection_id = dataset_data.get("connection_id")
        topic = dataset_data.get("topic")
        if connection_id and topic:
            try:
                conn = await db.connections.find_one({"_id": ObjectId(connection_id)})
                if conn:
                    bootstrap_servers = conn.get("config", {}).get("bootstrap_servers")
                    if bootstrap_servers:
                        kafka_schema = get_kafka_schema(bootstrap_servers, topic)
                        if kafka_schema.get("schema"):
                            dataset_data["columns"] = kafka_schema["schema"]
            except Exception as e:
                print(f"Warning: Failed to extract Kafka schema: {e}")

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

        # Use stored columns/schema from MongoDB (fast!)
        # Schema is saved when source dataset is created
        if not doc.get("columns"):
            # Fallback to schema field if columns not present
            doc["columns"] = doc.get("schema", [])

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
    elif doc.get("source_type") == "kafka":
        if not doc.get("columns"):
            connection_id = doc.get("connection_id")
            topic = doc.get("topic")
            if connection_id and topic:
                try:
                    conn = await db.connections.find_one({"_id": ObjectId(connection_id)})
                    if conn:
                        bootstrap_servers = conn.get("config", {}).get("bootstrap_servers")
                        if bootstrap_servers:
                            kafka_schema = get_kafka_schema(bootstrap_servers, topic)
                            if kafka_schema.get("schema"):
                                doc["columns"] = kafka_schema["schema"]
                except Exception as e:
                    print(f"Warning: Failed to fetch Kafka schema: {e}")

    return doc


@router.post("/kafka/schema")
async def fetch_kafka_schema(request: KafkaSchemaRequest):
    """Fetch Kafka topic schema from a sample message."""
    db = get_db()

    try:
        conn = await db.connections.find_one({"_id": ObjectId(request.connection_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")

    bootstrap_servers = conn.get("config", {}).get("bootstrap_servers")
    if not bootstrap_servers:
        raise HTTPException(status_code=400, detail="Connection missing bootstrap_servers")

    if not request.topic:
        raise HTTPException(status_code=400, detail="Topic is required")

    result = get_kafka_schema(bootstrap_servers, request.topic, limit=request.sample_size)
    if not result.get("schema"):
        raise HTTPException(status_code=400, detail="Failed to infer schema from topic")

    return result


@router.post("/kafka/topics")
async def fetch_kafka_topics(request: KafkaTopicsRequest):
    """Fetch Kafka topic list from a connection."""
    db = get_db()

    try:
        conn = await db.connections.find_one({"_id": ObjectId(request.connection_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid connection ID format")

    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")

    bootstrap_servers = conn.get("config", {}).get("bootstrap_servers")
    if not bootstrap_servers:
        raise HTTPException(status_code=400, detail="Connection missing bootstrap_servers")

    topics = get_kafka_topics(bootstrap_servers)
    return {"topics": topics}


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
