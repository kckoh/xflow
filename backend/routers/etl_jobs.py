import os
from datetime import datetime
from typing import List

import httpx
from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException, status

from models import ETLJob, JobRun, Connection, Dataset
from schemas.etl_job import ETLJobCreate, ETLJobUpdate, ETLJobResponse
from services.lineage_service import sync_pipeline_to_dataset
# OpenSearch Dual Write
from utils.indexers import index_single_etl_job, delete_etl_job_from_index

router = APIRouter()

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver.airflow:8080/api/v1")
AIRFLOW_AUTH = ("admin", "admin")
# DAG ID: "etl_job_dag" for local, "etl_job_dag_k8s" for production (EKS)
AIRFLOW_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "etl_job_dag")


@router.post("", response_model=ETLJobResponse, status_code=status.HTTP_201_CREATED)
async def create_etl_job(job: ETLJobCreate):
    """Create a new ETL job configuration"""
    # Check if job name exists
    existing_job = await ETLJob.find_one(ETLJob.name == job.name)
    if existing_job:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job name already exists"
        )

    # Handle both multiple sources (new) and single source (legacy)
    sources_data = []
    if job.sources:
        sources_data = [s.model_dump() for s in job.sources]
    elif job.source:
        sources_data = [job.source.model_dump()]

    # Validate all source connections exist
    for source_item in sources_data:
        if source_item.get("connection_id"):
            try:
                source = await Connection.get(PydanticObjectId(source_item["connection_id"]))
                if not source:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Source connection not found: {source_item['connection_id']}"
                    )
            except Exception:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid source connection ID: {source_item.get('connection_id')}"
                )

    # Create new ETL job
    new_job = ETLJob(
        name=job.name,
        description=job.description,
        sources=sources_data,
        source=sources_data[0] if sources_data else {},  # Legacy compatibility
        transforms=[t.model_dump() for t in job.transforms],
        destination=job.destination.model_dump(),
        schedule=job.schedule,
        status="draft",
        nodes=job.nodes or [],
        edges=job.edges or [],
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    await new_job.insert()
    
    # Dual Write: OpenSearch에 인덱싱
    await index_single_etl_job(new_job)
    
    # Sync to Dataset (Lineage)
    await sync_pipeline_to_dataset(new_job)

    return ETLJobResponse(
        id=str(new_job.id),
        name=new_job.name,
        description=new_job.description,
        sources=new_job.sources,
        source=new_job.source,
        transforms=new_job.transforms,
        destination=new_job.destination,
        schedule=new_job.schedule,
        status=new_job.status,
        nodes=new_job.nodes,
        edges=new_job.edges,
        created_at=new_job.created_at,
        updated_at=new_job.updated_at,
        import_ready=False,
    )


@router.get("", response_model=List[ETLJobResponse])
async def list_etl_jobs(import_ready: bool = None):
    """Get all ETL jobs with their active status, optionally filtered by import_ready flag"""
    # Build query filter
    if import_ready is not None:
        jobs = await ETLJob.find(ETLJob.import_ready == import_ready).to_list()
    else:
        jobs = await ETLJob.find_all().to_list()

    # Pre-fetch Datasets to map is_active status
    datasets = await Dataset.find_all().to_list()
    status_map = {d.job_id: d.is_active for d in datasets if d.job_id}

    return [
        ETLJobResponse(
            id=str(job.id),
            name=job.name,
            description=job.description,
            sources=job.sources,
            source=job.source,
            transforms=job.transforms,
            destination=job.destination,
            schedule=job.schedule,
            status=job.status,
            nodes=job.nodes,
            edges=job.edges,
            created_at=job.created_at,
            updated_at=job.updated_at,
            is_active=status_map.get(str(job.id), False),
            import_ready=getattr(job, 'import_ready', False)
        )
        for job in jobs
    ]


@router.get("/{job_id}", response_model=ETLJobResponse)
async def get_etl_job(job_id: str):
    """Get a specific ETL job by ID"""
    try:
        job = await ETLJob.get(PydanticObjectId(job_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Job not found")

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return ETLJobResponse(
        id=str(job.id),
        name=job.name,
        description=job.description,
        sources=job.sources,
        source=job.source,
        transforms=job.transforms,
        destination=job.destination,
        schedule=job.schedule,
        status=job.status,
        nodes=job.nodes,
        edges=job.edges,
        created_at=job.created_at,
        updated_at=job.updated_at,
        import_ready=getattr(job, 'import_ready', False),
    )


@router.put("/{job_id}", response_model=ETLJobResponse)
async def update_etl_job(job_id: str, job_update: ETLJobUpdate):
    """Update an ETL job configuration"""
    try:
        job = await ETLJob.get(PydanticObjectId(job_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Job not found")

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Update fields if provided
    if job_update.name is not None:
        job.name = job_update.name
    if job_update.description is not None:
        job.description = job_update.description

    # Handle both multiple sources (new) and single source (legacy)
    if job_update.sources is not None:
        job.sources = [s.model_dump() for s in job_update.sources]
        job.source = job.sources[0] if job.sources else {}  # Legacy compatibility
    elif job_update.source is not None:
        job.source = job_update.source.model_dump()
        job.sources = [job.source]  # Also update sources array

    if job_update.transforms is not None:
        job.transforms = [t.model_dump() for t in job_update.transforms]
    if job_update.destination is not None:
        job.destination = job_update.destination.model_dump()
    if job_update.schedule is not None:
        job.schedule = job_update.schedule
    if job_update.status is not None:
        job.status = job_update.status
    if job_update.import_ready is not None:
        job.import_ready = job_update.import_ready
    if job_update.nodes is not None:
        job.nodes = job_update.nodes
    if job_update.edges is not None:
        job.edges = job_update.edges

    job.updated_at = datetime.utcnow()
    await job.save()
    
    # Dual Write: OpenSearch 업데이트
    await index_single_etl_job(job)

    # Sync to Dataset (Lineage)
    await sync_pipeline_to_dataset(job)

    return ETLJobResponse(
        id=str(job.id),
        name=job.name,
        description=job.description,
        sources=job.sources,
        source=job.source,
        transforms=job.transforms,
        destination=job.destination,
        schedule=job.schedule,
        status=job.status,
        nodes=job.nodes,
        edges=job.edges,
        created_at=job.created_at,
        updated_at=job.updated_at,
        import_ready=getattr(job, 'import_ready', False),
    )



@router.patch("/{job_id}/nodes/{node_id}/metadata")
async def update_node_metadata(job_id: str, node_id: str, metadata: dict):
    """Update metadata for a specific node in an ETL job.
    
    Used by Domain to sync description/tags back to ETL Job.
    
    Args:
        job_id: ETL Job ID
        node_id: Node ID within the job
        metadata: { table: { description, tags }, columns: { colName: { description, tags } } }
    """
    try:
        job = await ETLJob.get(PydanticObjectId(job_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Job not found")

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Find and update the specific node
    node_found = False
    for i, node in enumerate(job.nodes or []):
        if node.get("id") == node_id:
            # Merge metadata
            if "data" not in job.nodes[i]:
                job.nodes[i]["data"] = {}
            if "metadata" not in job.nodes[i]["data"]:
                job.nodes[i]["data"]["metadata"] = {}
            
            # Update table metadata
            if "table" in metadata:
                if "table" not in job.nodes[i]["data"]["metadata"]:
                    job.nodes[i]["data"]["metadata"]["table"] = {}
                job.nodes[i]["data"]["metadata"]["table"].update(metadata["table"])
            
            # Update column metadata
            if "columns" in metadata:
                if "columns" not in job.nodes[i]["data"]["metadata"]:
                    job.nodes[i]["data"]["metadata"]["columns"] = {}
                job.nodes[i]["data"]["metadata"]["columns"].update(metadata["columns"])
            
            node_found = True
            break

    if not node_found:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found in job")

    job.updated_at = datetime.utcnow()
    await job.save()

    return {"message": "Metadata updated successfully", "job_id": job_id, "node_id": node_id}


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_etl_job(job_id: str):
    """Delete an ETL job"""
    try:
        job = await ETLJob.get(PydanticObjectId(job_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Job not found")

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    await job.delete()
    
    # Dual Write: OpenSearch에서 삭제
    await delete_etl_job_from_index(job_id)
    
    return None


@router.post("/{job_id}/run")
async def run_etl_job(job_id: str):
    """Trigger an ETL job execution"""
    try:
        job = await ETLJob.get(PydanticObjectId(job_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Job not found")

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Create job run record
    job_run = JobRun(
        job_id=job_id,
        status="pending",
        started_at=datetime.utcnow(),
    )
    await job_run.insert()

    # Trigger Airflow DAG
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{AIRFLOW_BASE_URL}/dags/{AIRFLOW_DAG_ID}/dagRuns",
                json={
                    "conf": {"job_id": job_id},
                    "dag_run_id": f"job_{job_id}_{job_run.id}",
                },
                auth=AIRFLOW_AUTH,
                timeout=10.0,
            )

            if response.status_code in [200, 201]:
                airflow_data = response.json()
                job_run.airflow_run_id = airflow_data.get("dag_run_id")
                job_run.status = "running"
                await job_run.save()
            else:
                job_run.status = "failed"
                job_run.error_message = f"Airflow API error: {response.text}"
                await job_run.save()
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to trigger Airflow DAG: {response.text}"
                )

    except httpx.RequestError as e:
        job_run.status = "failed"
        job_run.error_message = f"Connection error: {str(e)}"
        await job_run.save()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to connect to Airflow: {str(e)}"
        )

    return {
        "message": "Job triggered successfully",
        "job_id": job_id,
        "run_id": str(job_run.id),
        "airflow_run_id": job_run.airflow_run_id,
    }
