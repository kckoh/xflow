from typing import List

from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException

from models import JobRun, Dataset
from schemas.job_run import JobRunResponse, JobRunListResponse

router = APIRouter()


@router.get("", response_model=List[JobRunListResponse])
async def list_job_runs(dataset_id: str = None, limit: int = 50):
    """Get all job runs, optionally filtered by dataset_id"""
    if dataset_id:
        runs = await JobRun.find(JobRun.dataset_id == dataset_id).sort(-JobRun.started_at).limit(limit).to_list()
    else:
        runs = await JobRun.find_all().sort(-JobRun.started_at).limit(limit).to_list()

    result = []
    for run in runs:
        # Get dataset name
        dataset_name = None
        try:
            dataset = await Dataset.get(PydanticObjectId(run.dataset_id))
            if dataset:
                dataset_name = dataset.name
        except Exception:
            pass

        # Calculate duration
        duration = None
        if run.started_at and run.finished_at:
            duration = (run.finished_at - run.started_at).total_seconds()

        result.append(JobRunListResponse(
            id=str(run.id),
            dataset_id=run.dataset_id,
            dataset_name=dataset_name,
            status=run.status,
            started_at=run.started_at,
            finished_at=run.finished_at,
            duration_seconds=duration,
        ))

    return result


@router.get("/{run_id}", response_model=JobRunResponse)
async def get_job_run(run_id: str):
    """Get a specific job run by ID"""
    try:
        run = await JobRun.get(PydanticObjectId(run_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Job run not found")

    if not run:
        raise HTTPException(status_code=404, detail="Job run not found")

    # Get dataset name
    dataset_name = None
    try:
        dataset = await Dataset.get(PydanticObjectId(run.dataset_id))
        if dataset:
            dataset_name = dataset.name
    except Exception:
        pass

    # Calculate duration
    duration = None
    if run.started_at and run.finished_at:
        duration = (run.finished_at - run.started_at).total_seconds()

    return JobRunResponse(
        id=str(run.id),
        dataset_id=run.dataset_id,
        dataset_name=dataset_name,
        status=run.status,
        started_at=run.started_at,
        finished_at=run.finished_at,
        duration_seconds=duration,
        error_message=run.error_message,
        airflow_run_id=run.airflow_run_id,
    )
