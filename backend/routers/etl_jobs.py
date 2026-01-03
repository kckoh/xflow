import os
from datetime import datetime
from typing import List

import httpx
from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException, status

from models import ETLJob, JobRun, Connection, Dataset
from schemas.etl_job import ETLJobCreate, ETLJobUpdate, ETLJobResponse
from services.lineage_service import sync_pipeline_to_dataset

router = APIRouter()


async def get_table_size_gb(connection: Connection, table_name: str) -> float:
    """Calculate table size in GB from database connection"""
    config = connection.config
    db_type = connection.type

    try:
        if db_type == "postgres":
            import psycopg2
            conn = psycopg2.connect(
                host=config.get("host"),
                port=config.get("port", 5432),
                database=config.get("database_name"),
                user=config.get("user_name"),
                password=config.get("password"),
            )
            cursor = conn.cursor()
            cursor.execute(
                "SELECT pg_total_relation_size(%s) / (1024.0 * 1024.0 * 1024.0)",
                (table_name,)
            )
            result = cursor.fetchone()
            size_gb = float(result[0]) if result and result[0] else 0.0
            cursor.close()
            conn.close()

        elif db_type == "mysql":
            import pymysql
            conn = pymysql.connect(
                host=config.get("host"),
                port=config.get("port", 3306),
                database=config.get("database_name"),
                user=config.get("user_name"),
                password=config.get("password"),
            )
            cursor = conn.cursor()
            cursor.execute("""
                SELECT (data_length + index_length) / (1024 * 1024 * 1024)
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """, (config.get("database_name"), table_name))
            result = cursor.fetchone()
            size_gb = float(result[0]) if result and result[0] else 0.0
            cursor.close()
            conn.close()

        elif db_type == "mongodb":
            from pymongo import MongoClient
            host = config.get("host")
            port = config.get("port", 27017)
            user = config.get("user_name")
            password = config.get("password")
            db_name = config.get("database_name")

            if user and password:
                mongo_url = f"mongodb://{user}:{password}@{host}:{port}"
            else:
                mongo_url = f"mongodb://{host}:{port}"

            client = MongoClient(mongo_url)
            db = client[db_name]
            stats = db.command("collStats", table_name)
            size_bytes = stats.get("size", 0) + stats.get("totalIndexSize", 0)
            size_gb = size_bytes / (1024 * 1024 * 1024)
            client.close()
        else:
            return 1.0

        print(f"Table {table_name} ({db_type}) size: {size_gb:.2f} GB")
        return max(size_gb, 0.1)  # Minimum 0.1 GB

    except Exception as e:
        print(f"Failed to get table size for {table_name}: {e}")
        return 1.0  # Default 1GB on error

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

    # Validate all source connections and calculate table sizes
    total_size_gb = 0.0
    for source_item in sources_data:
        if source_item.get("connection_id"):
            try:
                connection = await Connection.get(PydanticObjectId(source_item["connection_id"]))
                if not connection:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Source connection not found: {source_item['connection_id']}"
                    )
                # Calculate table size if table_name exists
                table_name = source_item.get("table_name")
                if table_name and connection.type in ["postgres", "mysql", "mongodb"]:
                    size_gb = await get_table_size_gb(connection, table_name)
                    source_item["size_gb"] = size_gb
                    total_size_gb += size_gb
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid source connection ID: {source_item.get('connection_id')}"
                )

    # Create new ETL job with estimated size for Spark auto-scaling
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
        estimated_size_gb=total_size_gb if total_size_gb > 0 else 1.0,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    await new_job.insert()
    
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
