import os
from datetime import datetime
from typing import List

import httpx
from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException, status
from models import Connection, Dataset, ETLJob, JobRun
from schemas.etl_job import ETLJobCreate, ETLJobResponse, ETLJobUpdate
from services.lineage_service import sync_pipeline_to_dataset

# OpenSearch Dual Write
from utils.indexers import delete_etl_job_from_index, index_single_etl_job
from utils.schedule_converter import generate_schedule

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
                (table_name,),
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
            cursor.execute(
                """
                SELECT (data_length + index_length) / (1024 * 1024 * 1024)
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """,
                (config.get("database_name"), table_name),
            )
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


AIRFLOW_BASE_URL = os.getenv(
    "AIRFLOW_BASE_URL", "http://airflow-webserver.airflow:8080/api/v1"
)
AIRFLOW_AUTH = ("admin", "admin")
# DAG ID: "etl_job_dag" for local, "dataset_dag_k8s" for production (EKS)
AIRFLOW_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "dataset_dag_k8s")


@router.post("", response_model=ETLJobResponse, status_code=status.HTTP_201_CREATED)
async def create_etl_job(job: ETLJobCreate):
    """Create a new ETL job configuration"""
    # Check if job name exists
    existing_job = await ETLJob.find_one(ETLJob.name == job.name)
    if existing_job:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Job name already exists"
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
                connection = await Connection.get(
                    PydanticObjectId(source_item["connection_id"])
                )
                if not connection:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Source connection not found: {source_item['connection_id']}",
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
                    detail=f"Invalid source connection ID: {source_item.get('connection_id')}",
                )

    # Create new ETL job with estimated size for Spark auto-scaling
    schedule = None
    if job.schedule_frequency:
        schedule = generate_schedule(job.schedule_frequency, job.ui_params)

    new_job = ETLJob(
        name=job.name,
        description=job.description,
        dataset_type=job.dataset_type,
        job_type=job.job_type,
        sources=sources_data,
        source=sources_data[0] if sources_data else {},  # Legacy compatibility
        transforms=[t.model_dump() for t in job.transforms],
        targets=job.targets or [],
        destination=job.destination.model_dump(),
        # Schedule info
        schedule=schedule,
        schedule_frequency=job.schedule_frequency,
        ui_params=job.ui_params,
        incremental_config=job.incremental_config,
        status="draft",
        nodes=job.nodes or [],
        edges=job.edges or [],
        estimated_size_gb=total_size_gb if total_size_gb > 0 else 1.0,
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
        dataset_type=new_job.dataset_type,
        job_type=new_job.job_type,
        sources=new_job.sources,
        source=new_job.source,
        transforms=new_job.transforms,
        targets=new_job.targets,
        destination=new_job.destination,
        schedule=new_job.schedule,
        schedule_frequency=new_job.schedule_frequency,
        ui_params=new_job.ui_params,
        incremental_config=new_job.incremental_config,
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
            dataset_type=getattr(job, "dataset_type", "source"),
            job_type=getattr(job, "job_type", "batch"),
            sources=job.sources,
            source=job.source,
            transforms=job.transforms,
            targets=getattr(job, "targets", []),
            destination=job.destination,
            schedule=job.schedule,
            schedule_frequency=job.schedule_frequency,
            ui_params=job.ui_params,
            incremental_config=job.incremental_config,
            status=job.status,
            nodes=job.nodes,
            edges=job.edges,
            created_at=job.created_at,
            updated_at=job.updated_at,
            is_active=status_map.get(str(job.id), False),
            import_ready=getattr(job, "import_ready", False),
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
        dataset_type=getattr(job, "dataset_type", "source"),
        job_type=job.job_type,
        sources=job.sources,
        source=job.source,
        transforms=job.transforms,
        targets=getattr(job, "targets", []),
        destination=job.destination,
        schedule=job.schedule,
        schedule_frequency=job.schedule_frequency,
        ui_params=job.ui_params,
        incremental_config=job.incremental_config,
        status=job.status,
        nodes=job.nodes,
        edges=job.edges,
        created_at=job.created_at,
        updated_at=job.updated_at,
        import_ready=getattr(job, "import_ready", False),
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
    if job_update.targets is not None:
        job.targets = job_update.targets
    if job_update.destination is not None:
        job.destination = job_update.destination.model_dump()

    # Handle schedule updates
    if job_update.schedule_frequency == "":
        # Explicitly clear schedule if frequency is empty string
        job.schedule_frequency = None
        job.ui_params = None
        job.schedule = None
    elif job_update.schedule_frequency is not None or job_update.ui_params is not None:
        # Use existing values if not provided in update
        freq = (
            job_update.schedule_frequency
            if job_update.schedule_frequency is not None
            else job.schedule_frequency
        )
        params = (
            job_update.ui_params if job_update.ui_params is not None else job.ui_params
        )

        job.schedule_frequency = freq
        job.ui_params = params
        job.schedule = generate_schedule(freq, params)

    if job_update.status is not None:
        job.status = job_update.status

    if job_update.incremental_config is not None:
        job.incremental_config = job_update.incremental_config
    if job_update.import_ready is not None:
        job.import_ready = job_update.import_ready
    if job_update.nodes is not None:
        job.nodes = job_update.nodes
    if job_update.edges is not None:
        job.edges = job_update.edges
    if job_update.dataset_type is not None:
        job.dataset_type = job_update.dataset_type
    if job_update.job_type is not None:
        job.job_type = job_update.job_type

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
        dataset_type=getattr(job, "dataset_type", "source"),
        job_type=job.job_type,
        sources=job.sources,
        source=job.source,
        transforms=job.transforms,
        targets=job.targets,
        destination=job.destination,
        schedule=job.schedule,
        schedule_frequency=job.schedule_frequency,
        ui_params=job.ui_params,
        incremental_config=job.incremental_config,
        status=job.status,
        nodes=job.nodes,
        edges=job.edges,
        created_at=job.created_at,
        updated_at=job.updated_at,
        import_ready=getattr(job, "import_ready", False),
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

    return {
        "message": "Metadata updated successfully",
        "job_id": job_id,
        "node_id": node_id,
    }


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

    # Cascading Delete: Delete associated Dataset from Catalog
    from services import catalog_service

    dataset = await Dataset.find_one(Dataset.job_id == job_id)
    if dataset:
        await catalog_service.delete_dataset(str(dataset.id))

    return None


@router.post("/{job_id}/activate")
async def activate_etl_job(job_id: str):
    """Activate schedule for an ETL job"""
    try:
        job = await ETLJob.get(PydanticObjectId(job_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Job not found")

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if not job.schedule:
        raise HTTPException(
            status_code=400, detail="Cannot activate job without a schedule"
        )

    job.status = "active"
    job.updated_at = datetime.utcnow()
    await job.save()

    return {"message": "Job schedule activated", "status": "active"}


@router.post("/{job_id}/deactivate")
async def deactivate_etl_job(job_id: str):
    """Pause schedule for an ETL job"""
    try:
        job = await ETLJob.get(PydanticObjectId(job_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Job not found")

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    job.status = "paused"
    job.updated_at = datetime.utcnow()
    await job.save()

    return {"message": "Job schedule paused", "status": "paused"}


@router.post("/{job_id}/run")
async def run_etl_job(job_id: str):
    """Trigger an ETL job execution or activate schedule"""
    try:
        job = await ETLJob.get(PydanticObjectId(job_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Job not found")

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # [Logic for Action Button]
    # Case 1: Job has a schedule → Activate schedule, don't run immediately
    if job.schedule:
        if job.status != "active":
            job.status = "active"
            job.updated_at = datetime.utcnow()
            await job.save()
            return {
                "message": "Schedule activated. Job will run according to schedule.",
                "job_id": job_id,
                "schedule": job.schedule,
                "schedule_activated": True,
            }
        else:
            return {
                "message": "Schedule is already active",
                "job_id": job_id,
                "schedule": job.schedule,
                "schedule_activated": False,
            }

    # Case 2: No schedule → Run immediately (one-time execution)
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
                    detail=f"Failed to trigger Airflow DAG: {response.text}",
                )

    except httpx.RequestError as e:
        job_run.status = "failed"
        job_run.error_message = f"Connection error: {str(e)}"
        await job_run.save()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to connect to Airflow: {str(e)}",
        )

    return {
        "message": "Job triggered successfully (one-time execution)",
        "job_id": job_id,
        "run_id": str(job_run.id),
        "airflow_run_id": job_run.airflow_run_id,
    }
