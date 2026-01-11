import logging
import os
from datetime import datetime

from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException, status

from models import Dataset, Connection
from services.spark_service import SparkService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/{dataset_id}/start")
async def start_streaming_job(dataset_id: str):
    """Start a Spark Streaming Job for a Kafka Dataset"""
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if dataset.job_type != "streaming":
         # Fallback check
         if not (dataset.sources and dataset.sources[0].get("type") == "kafka"):
            raise HTTPException(status_code=400, detail="Not a streaming dataset")

    # Build Streaming Config
    if not dataset.sources:
            logger.error(f"[ERROR] Dataset {dataset_id} has no sources")
            raise HTTPException(status_code=400, detail="Dataset has no sources")
            
    source = dataset.sources[0]
    conn_id = source.get("connection_id")
    
    logger.info(f"[DEBUG] Starting Streaming Job for Dataset {dataset_id}: Source Config={source}")

    conn = await Connection.get(PydanticObjectId(conn_id))
    if not conn:
            logger.error(f"[ERROR] Source connection not found: {conn_id}")
            raise HTTPException(status_code=400, detail="Source connection not found")
    
    # Determine Runner Type
    # For now, if type is kafka, use kafka runner.
    runner_type = "kafka"
    
    config = {
        "job_id": str(dataset.id),
        "connection_id": str(conn.id),
        "topic": source.get("config", {}).get("topic"),
        "columns": source.get("config", {}).get("columns"),
        "bootstrap_servers": conn.config.get("bootstrap_servers"),
        "security_protocol": conn.config.get("security_protocol"),
        "sasl_mechanism": conn.config.get("sasl_mechanism"),
        "sasl_username": conn.config.get("sasl_username"),
        "sasl_password": conn.config.get("sasl_password"),
        "destination": dataset.destination,
        "target_path": dataset.destination.get("path")
    }
    
    app_name = f"flow-{str(dataset.id)}"
    
    try:
        submission_id = SparkService.submit_job(config, app_name, runner_type=runner_type)
        
        # Save submission_id for Local Mode stopping
        if not dataset.ui_params:
            dataset.ui_params = {}
        dataset.ui_params["spark_submission_id"] = submission_id
        
    except Exception as e:
        logger.error(f"Failed to submit Spark job: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    dataset.status = "active"
    dataset.updated_at = datetime.utcnow()
    await dataset.save()
    
    return {"message": "Streaming job started", "status": "active"}

@router.post("/{dataset_id}/stop")
async def stop_streaming_job(dataset_id: str):
    """Stop a Spark Streaming Job"""
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    app_name = f"flow-{str(dataset.id)}"
    submission_id = dataset.ui_params.get("spark_submission_id") if dataset.ui_params else None
    
    try:
        SparkService.stop_job(app_name, submission_id=submission_id)
    except Exception as e:
        logger.warning(f"Failed to stop job (might be already stopped): {e}")
        # Continue to update status anyway

    dataset.status = "paused"
    dataset.updated_at = datetime.utcnow()
    await dataset.save()

    return {"message": "Streaming job stopped", "status": "paused"}
