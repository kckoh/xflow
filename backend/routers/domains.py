from fastapi import APIRouter, HTTPException, Body, status, Query
from typing import List, Optional, Dict, Any
from datetime import datetime
from beanie import PydanticObjectId

from models import Domain, ETLJob, Dataset
from schemas.domain import (
    DomainCreate,
    DomainGraphUpdate,
    DomainJobListResponse,
    JobExecutionResponse,
    DatasetNodeResponse
)

router = APIRouter()

# --- Domain CRUD Endpoints ---

@router.get("", response_model=List[Domain])
async def get_all_domains():
    """
    Get all domains (list view).
    """
    domains = await Domain.find_all().to_list()
    return domains


# --- ETL Job Import Endpoints (must be before /{id} to avoid route conflict) ---

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




@router.get("/jobs", response_model=List[DomainJobListResponse])
async def list_domain_jobs(import_ready: bool = Query(True)):
    """Get ETL jobs that are ready to be imported into domain (import_ready=true by default)"""
    try:
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
    except Exception as e:
        print(f"Error in list_domain_jobs: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch jobs: {str(e)}"
        )

@router.get("/{id}", response_model=Domain)
async def get_domain(id: str):
    """
    Get a specific domain by ID (detail view).
    """
    domain = await Domain.get(id)
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")
    return domain


@router.post("", response_model=Domain, status_code=status.HTTP_201_CREATED)
async def create_domain(domain_data: DomainCreate):
    """
    Create a new domain.
    now expects the frontend to provide the initial graph layout (nodes/edges).
    """
    # 1. Base Domain Data
    domain_dict = {
        "name": domain_data.name,
        "type": domain_data.type,
        "owner": domain_data.owner,
        "tags": domain_data.tags,
        "description": domain_data.description,
        "nodes": domain_data.nodes or [],
        "edges": domain_data.edges or []
    }

    # 2. Logic to hydrate nodes/edges from job_ids (Legacy/Fallback)
    # If frontend sends job_ids but NO nodes, we might ideally warn or just do nothing.
    # For now, we assume frontend sends the calculated nodes.
    # If both are empty, it's just an empty domain.

    new_domain = Domain(**domain_dict)
    await new_domain.insert()
    return new_domain

@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_domain(id: str):
    """
    Delete a domain.
    """
    domain = await Domain.get(id)
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")
    
    await domain.delete()

@router.post("/{id}/graph", response_model=Domain)
async def save_domain_graph(id: str, graph_data: DomainGraphUpdate):
    """
    Upsert graph state (nodes, edges) for a domain.
    """
    domain = await Domain.get(id)
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")
    
    domain.nodes = graph_data.nodes
    domain.edges = graph_data.edges
    domain.updated_at = datetime.utcnow()

    await domain.save()
    return domain
