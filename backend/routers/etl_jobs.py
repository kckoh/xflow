from datetime import datetime
from typing import List

import httpx
from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException, status

from models import ETLJob, JobRun, RDBSource
from schemas.etl_job import ETLJobCreate, ETLJobUpdate, ETLJobResponse

router = APIRouter()

AIRFLOW_BASE_URL = "http://airflow-webserver:8080/api/v1"
AIRFLOW_AUTH = ("admin", "admin")


@router.post("/", response_model=ETLJobResponse, status_code=status.HTTP_201_CREATED)
async def create_etl_job(job: ETLJobCreate):
    """Create a new ETL job configuration"""
    # Check if job name exists
    existing_job = await ETLJob.find_one(ETLJob.name == job.name)
    if existing_job:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job name already exists"
        )

    # Validate source connection exists
    if job.source.type == "rdb":
        try:
            source = await RDBSource.get(PydanticObjectId(job.source.connection_id))
            if not source:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Source connection not found"
                )
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid source connection ID"
            )

    # Create new ETL job
    new_job = ETLJob(
        name=job.name,
        description=job.description,
        source=job.source.model_dump(),
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

    return ETLJobResponse(
        id=str(new_job.id),
        name=new_job.name,
        description=new_job.description,
        source=new_job.source,
        transforms=new_job.transforms,
        destination=new_job.destination,
        schedule=new_job.schedule,
        status=new_job.status,
        nodes=new_job.nodes,
        edges=new_job.edges,
        created_at=new_job.created_at,
        updated_at=new_job.updated_at,
    )


@router.get("/", response_model=List[ETLJobResponse])
async def list_etl_jobs():
    """Get all ETL jobs"""
    jobs = await ETLJob.find_all().to_list()
    return [
        ETLJobResponse(
            id=str(job.id),
            name=job.name,
            description=job.description,
            source=job.source,
            transforms=job.transforms,
            destination=job.destination,
            schedule=job.schedule,
            status=job.status,
            created_at=job.created_at,
            updated_at=job.updated_at,
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
        source=job.source,
        transforms=job.transforms,
        destination=job.destination,
        schedule=job.schedule,
        status=job.status,
        nodes=job.nodes,
        edges=job.edges,
        created_at=job.created_at,
        updated_at=job.updated_at,
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
    if job_update.source is not None:
        job.source = job_update.source.model_dump()
    if job_update.transforms is not None:
        job.transforms = [t.model_dump() for t in job_update.transforms]
    if job_update.destination is not None:
        job.destination = job_update.destination.model_dump()
    if job_update.schedule is not None:
        job.schedule = job_update.schedule
    if job_update.status is not None:
        job.status = job_update.status
    if job_update.nodes is not None:
        job.nodes = job_update.nodes
    if job_update.edges is not None:
        job.edges = job_update.edges

    job.updated_at = datetime.utcnow()
    await job.save()

    return ETLJobResponse(
        id=str(job.id),
        name=job.name,
        description=job.description,
        source=job.source,
        transforms=job.transforms,
        destination=job.destination,
        schedule=job.schedule,
        status=job.status,
        nodes=job.nodes,
        edges=job.edges,
        created_at=job.created_at,
        updated_at=job.updated_at,
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
                f"{AIRFLOW_BASE_URL}/dags/etl_job_dag/dagRuns",
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
