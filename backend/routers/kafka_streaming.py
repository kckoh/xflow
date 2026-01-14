from fastapi import APIRouter, HTTPException
from beanie import PydanticObjectId
from bson import ObjectId

import database
from models import Dataset
from services.kafka_streaming_service import KafkaStreamingService

router = APIRouter(prefix="/api/streaming", tags=["streaming"])


def _get_db():
    return database.mongodb_client[database.DATABASE_NAME]


def _find_node(nodes, predicate):
    for node in nodes or []:
        if predicate(node):
            return node
    return None


def _has_kafka_source(nodes) -> bool:
    for node in nodes or []:
        data = node.get("data", {}) if isinstance(node, dict) else {}
        source_type = (data.get("sourceType") or "").lower()
        platform = (data.get("platform") or "").lower()
        if source_type == "kafka" or "kafka" in platform:
            return True
    return False


@router.post("/jobs/{dataset_id}/start")
async def start_streaming_job(dataset_id: str):
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if dataset.job_type != "streaming":
        if _has_kafka_source(dataset.nodes or []):
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

    destination = dataset.destination or {}
    base_path = destination.get("path")
    if base_path:
        if base_path.startswith("s3://"):
            base_path = base_path.replace("s3://", "s3a://", 1)
        if not base_path.endswith("/"):
            base_path += "/"
        destination["checkpoint_path"] = f"{base_path}_checkpoints/{dataset_id}/"

    job_config = {
        "id": str(dataset.id),
        "name": dataset.name,
        "kafka": {
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
        },
        "source_schema": source_schema,
        "query": query,
        "destination": destination,
    }

    app_name = f"kafka-stream-{dataset_id}"
    KafkaStreamingService.submit_job(job_config, app_name)

    return {"status": "started", "app_name": app_name}


@router.post("/jobs/{dataset_id}/stop")
async def stop_streaming_job(dataset_id: str):
    app_name = f"kafka-stream-{dataset_id}"
    stopped = KafkaStreamingService.stop_job(app_name)
    return {"status": "stopped" if stopped else "unknown", "app_name": app_name}


@router.get("/jobs/{dataset_id}/status")
async def get_streaming_status(dataset_id: str):
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if dataset.job_type != "streaming" and not _has_kafka_source(dataset.nodes or []):
        return {"status": "stopped", "state": "NOT_STREAMING"}

    app_name = f"kafka-stream-{dataset_id}"
    status = KafkaStreamingService.get_job_status(app_name)
    state = (status.get("state") or "UNKNOWN").upper()

    running_states = {"RUNNING", "SUBMITTED", "PENDING"}
    is_running = state in running_states

    return {
        "status": "running" if is_running else "stopped",
        "state": state,
        "app_name": app_name,
    }
