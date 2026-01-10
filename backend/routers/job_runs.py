from typing import List, Dict
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


@router.get("/bulk", response_model=Dict[str, List[JobRunListResponse]])
async def get_bulk_job_runs(dataset_ids: str, limit: int = 10):
    """
    Get job runs for multiple datasets in one request
    
    Args:
        dataset_ids: Comma-separated dataset IDs (e.g., "id1,id2,id3")
        limit: Maximum number of runs per dataset (default: 10)
    
    Returns:
        Dictionary mapping dataset_id to list of job runs
        {
            "dataset_id_1": [run1, run2, ...],
            "dataset_id_2": [run1, run2, ...],
            ...
        }
    """
    try:
        # Parse dataset IDs
        ids = [id.strip() for id in dataset_ids.split(',') if id.strip()]
        
        if not ids:
            return {}
        
        # Fetch all runs for these datasets in one query
        all_runs = await JobRun.find(
            {"dataset_id": {"$in": ids}}
        ).sort(-JobRun.started_at).to_list()
        
        # Group runs by dataset_id
        runs_by_dataset = {}
        for run in all_runs:
            dataset_id = run.dataset_id
            if dataset_id not in runs_by_dataset:
                runs_by_dataset[dataset_id] = []
            
            # Calculate duration
            duration = None
            if run.started_at and run.finished_at:
                duration = (run.finished_at - run.started_at).total_seconds()
            
            runs_by_dataset[dataset_id].append(JobRunListResponse(
                id=str(run.id),
                dataset_id=run.dataset_id,
                dataset_name=None,  # Not needed for JobsPage
                status=run.status,
                started_at=run.started_at,
                finished_at=run.finished_at,
                duration_seconds=duration,
            ))
        
        # Limit runs per dataset and sort
        for dataset_id in runs_by_dataset:
            runs_by_dataset[dataset_id] = sorted(
                runs_by_dataset[dataset_id],
                key=lambda x: x.started_at if x.started_at else datetime.min,
                reverse=True
            )[:limit]
        
        return runs_by_dataset
        
    except Exception as e:
        print(f"Bulk job runs error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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

