from fastapi import APIRouter, HTTPException
from beanie import PydanticObjectId
from bson import ObjectId
import logging
import re
import os

import database
from models import Dataset, JobRun
from datetime import datetime
from services.kafka_streaming_service import KafkaStreamingService
from utils.kafka import has_kafka_source
from utils.trino_client import register_delta_table

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/streaming", tags=["streaming"])


def _get_db():
    return database.mongodb_client[database.DATABASE_NAME]


def _find_node(nodes, predicate):
    for node in nodes or []:
        if predicate(node):
            return node
    return None


def _normalize_dataset_id(dataset_id: str) -> str:
    try:
        return str(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid dataset id")


@router.post("/jobs/{dataset_id}/start")
async def start_streaming_job(dataset_id: str):
    dataset_id = _normalize_dataset_id(dataset_id)
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if dataset.job_type != "streaming":
        if has_kafka_source(dataset.nodes or []):
            dataset.job_type = "streaming"
            await dataset.save()
        else:
            raise HTTPException(status_code=400, detail="Dataset is not a streaming job")

    source_node = _find_node(
        dataset.nodes,
        lambda n: n.get("data", {}).get("nodeCategory") == "source",
    )
    transform_node = _find_node(
        dataset.nodes,
        lambda n: n.get("data", {}).get("transformType") == "sql",
    )

    if not source_node:
        raise HTTPException(status_code=400, detail="Missing source node")

    source_dataset_id = source_node.get("data", {}).get("sourceDatasetId")
    if not source_dataset_id:
        raise HTTPException(status_code=400, detail="Missing source dataset ID")

    db = _get_db()
    source_dataset = await db.source_datasets.find_one({"_id": ObjectId(source_dataset_id)})
    if not source_dataset:
        raise HTTPException(status_code=404, detail="Source dataset not found")

    if source_dataset.get("source_type") != "kafka":
        raise HTTPException(status_code=400, detail="Source dataset is not Kafka")

    connection_id = source_dataset.get("connection_id")
    if not connection_id:
        raise HTTPException(status_code=400, detail="Missing Kafka connection")

    connection = await db.connections.find_one({"_id": ObjectId(connection_id)})
    if not connection:
        raise HTTPException(status_code=404, detail="Kafka connection not found")

    bootstrap_servers = connection.get("config", {}).get("bootstrap_servers")
    if not bootstrap_servers:
        raise HTTPException(status_code=400, detail="Kafka bootstrap_servers missing")

    topic = source_dataset.get("topic")
    if not topic:
        raise HTTPException(status_code=400, detail="Kafka topic missing")

    source_schema = source_dataset.get("columns") or source_node.get("data", {}).get("columns", [])
    query = transform_node.get("data", {}).get("query") if transform_node else None
    source_format = source_dataset.get("format") or "json"
    auto_schema = source_dataset.get("auto_schema")
    if auto_schema is None:
        auto_schema = source_format == "json"

    destination = dataset.destination or {}
    base_path = destination.get("path")
    if base_path:
        if base_path.startswith("s3://"):
            base_path = base_path.replace("s3://", "s3a://", 1)
        if not base_path.endswith("/"):
            base_path += "/"
        destination["checkpoint_path"] = f"{base_path}_checkpoints/{dataset_id}/"

    group_id = f"xflow-stream-{dataset_id}"
    job_config = {
        "id": str(dataset.id),
        "name": dataset.name,
        "kafka": {
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
            "group_id": group_id,
        },
        "source_schema": source_schema,
        "query": query,
        "format": source_format,
        "auto_schema": auto_schema,
        "destination": destination,
        "custom_regex": source_dataset.get("customRegex") or source_dataset.get("custom_regex"),
    }

    app_name = f"kafka-stream-{dataset_id}"
    KafkaStreamingService.submit_job(job_config, app_name)

    # Trino 테이블 등록 (Delta Lake)
    glue_table_name = destination.get("glue_table_name")
    if not glue_table_name and destination.get("path"):
        # Auto-generate glue_table_name from dataset name
        safe_name = re.sub(r'[^a-z0-9_]', '_', dataset.name.lower())
        glue_table_name = safe_name
        destination["glue_table_name"] = glue_table_name
        # Update dataset with generated table name
        dataset.destination = destination
        await dataset.save()
    
    if glue_table_name and destination.get("path"):
        s3_path = destination["path"]
        # Ensure path ends with table name
        if not s3_path.endswith(glue_table_name):
            s3_path = s3_path.rstrip("/") + "/" + glue_table_name
        
        logger.info(f"[Streaming] Registering Trino table: {glue_table_name}")
        try:
            if os.getenv("ENVIRONMENT") == "local":
                # Skip Trino registration in local environment to avoid network crashes
                success = True
                logger.info(f"[Streaming] Trino registration SKIPPED (local environment)")
            else:
                success = register_delta_table(glue_table_name, s3_path)
            
            if success:
                logger.info(f"[Streaming] ✅ Trino table registered: {glue_table_name}")
            else:
                logger.warning(f"[Streaming] ⚠️ Trino registration may have failed")
        except Exception as e:
            logger.error(f"[Streaming] ❌ Trino registration error: {e}")
            # Don't fail the streaming job if Trino registration fails

    # Create JobRun record for tracking
    job_run = JobRun(
        dataset_id=dataset_id,
        status="running",
        started_at=datetime.utcnow(),
        error_message=None
    )
    await job_run.insert()

    return {
        "status": "started",
        "app_name": app_name,
        "group_id": group_id,
        "trino_table": glue_table_name if glue_table_name else None,
        "run_id": str(job_run.id)
    }


@router.post("/jobs/{dataset_id}/stop")
async def stop_streaming_job(dataset_id: str):
    dataset_id = _normalize_dataset_id(dataset_id)
    app_name = f"kafka-stream-{dataset_id}"
    group_id = f"xflow-stream-{dataset_id}"
    stopped = KafkaStreamingService.stop_job(app_name)

    # Update JobRun status if stopped
    if stopped:
        # Find latest running job for this dataset
        active_runs = await JobRun.find(
            JobRun.dataset_id == dataset_id,
            JobRun.status == "running"
        ).sort("-started_at").limit(1).to_list()
        
        if active_runs:
            run = active_runs[0]
            run.status = "success"  # User manually stopped it (finished)
            run.finished_at = datetime.utcnow()
            await run.save()

    return {
        "status": "stopped" if stopped else "unknown",
        "app_name": app_name,
        "group_id": group_id,
    }


@router.get("/jobs/{dataset_id}/status")
async def get_streaming_status(dataset_id: str):
    dataset_id = _normalize_dataset_id(dataset_id)
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if dataset.job_type != "streaming" and not has_kafka_source(dataset.nodes or []):
        return {"status": "stopped", "state": "NOT_STREAMING"}

    app_name = f"kafka-stream-{dataset_id}"
    group_id = f"xflow-stream-{dataset_id}"
    status = KafkaStreamingService.get_job_status(app_name)
    state = (status.get("state") or "UNKNOWN").upper()

    running_states = {"RUNNING", "SUBMITTED", "PENDING"}
    is_running = state in running_states

    return {
        "status": "running" if is_running else "stopped",
        "state": state,
        "app_name": app_name,
        "group_id": group_id,
    }
