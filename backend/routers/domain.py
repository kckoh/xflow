from typing import List
from fastapi import APIRouter, HTTPException, status
from beanie import PydanticObjectId

from models import ETLJob, Dataset
from schemas.domain import DomainJobListResponse, JobExecutionResponse, DatasetNodeResponse

router = APIRouter()


@router.get("/jobs", response_model=List[DomainJobListResponse])
async def list_domain_jobs(import_ready: bool = True):
    """Get ETL jobs that are ready to be imported into domain (import_ready=true by default)"""
    jobs = await ETLJob.find(ETLJob.import_ready == import_ready).to_list()

    return [
        DomainJobListResponse(
            id=str(job.id),
            name=job.name,
            description=job.description,
            source_count=len(job.sources) if job.sources else 0,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )
        for job in jobs
    ]


@router.get("/jobs/{job_id}/execution", response_model=JobExecutionResponse)
async def get_job_execution(job_id: str):
    """Get the latest execution result (Dataset) for a specific ETL job"""
    try:
        # Find the most recent Dataset for this job
        dataset = await Dataset.find_one(
            Dataset.job_id == job_id,
            sort=[("created_at", -1)]
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch execution result: {str(e)}"
        )

    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No execution result found for this job"
        )

    return JobExecutionResponse(
        id=str(dataset.id),
        name=dataset.name,
        description=dataset.description,
        job_id=dataset.job_id,
        sources=[DatasetNodeResponse(**node.model_dump()) for node in dataset.sources],
        transforms=[DatasetNodeResponse(**node.model_dump()) for node in dataset.transforms],
        targets=[DatasetNodeResponse(**node.model_dump()) for node in dataset.targets],
        is_active=dataset.is_active,
        created_at=dataset.created_at,
        updated_at=dataset.updated_at,
    )
