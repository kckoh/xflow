from typing import List
from datetime import datetime

from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException

from models import JobRun, Dataset
from schemas.job_run import JobRunResponse, JobRunListResponse, JobRunUpdate
from utils.s3_utils import calculate_s3_size_bytes

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


@router.patch("/{run_id}", response_model=JobRunResponse)
async def update_job_run(run_id: str, update: JobRunUpdate):
    """
    Update job run status.
    When status is 'success', automatically calculates S3 file size and updates Dataset.
    """
    try:
        run = await JobRun.get(PydanticObjectId(run_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Job run not found")

    if not run:
        raise HTTPException(status_code=404, detail="Job run not found")

    # Update status
    run.status = update.status
    
    if update.error_message:
        run.error_message = update.error_message
    
    # Set timestamps based on status
    if update.status == "running" and not run.started_at:
        run.started_at = datetime.utcnow()
    elif update.status in ["success", "failed"]:
        run.finished_at = datetime.utcnow()
    
    await run.save()

    # If success, calculate S3 file size and update Dataset
    if update.status == "success":
        try:
            dataset = await Dataset.get(PydanticObjectId(run.dataset_id))
            if dataset and dataset.destination:
                s3_path = dataset.destination.get("path")
                
                if s3_path:
                    try:
                        # Calculate S3 file size
                        file_size_bytes = await calculate_s3_size_bytes(s3_path)
                        
                        # Update Dataset with actual size
                        dataset.actual_size_bytes = file_size_bytes
                        dataset.updated_at = datetime.utcnow()
                        await dataset.save()
                        
                        print(f"Updated Dataset {dataset.name} with actual size: {file_size_bytes} bytes")
                    except Exception as e:
                        # Log error but don't fail the job run update
                        print(f"Warning: Failed to calculate S3 size for {s3_path}: {str(e)}")
        except Exception as e:
            print(f"Warning: Failed to update Dataset size: {str(e)}")

    # Get dataset name for response
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

