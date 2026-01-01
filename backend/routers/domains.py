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


@router.get("/{id}", response_model=Domain)
async def get_domain(id: str):
    """
    Get a specific domain by ID (detail view).
    """
    domain = await Domain.get(id)
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")
    return domain


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

@router.post("", response_model=Domain, status_code=status.HTTP_201_CREATED)
async def create_domain(domain_data: DomainCreate):
    """
    Create a new domain.
    """
    # Check if domain with same name exists? (Optional, maybe allow dupes for now)
    
    # 1. Base Domain Data
    domain_dict = {
        "name": domain_data.name,
        "type": domain_data.type,
        "owner": domain_data.owner,
        "tags": domain_data.tags,
        "description": domain_data.description,
        "nodes": [],
        "edges": []
    }

    # 2. Logic to hydrate nodes/edges from job_ids
    if domain_data.job_ids:
        jobs = await ETLJob.find({"_id": {"$in": [PydanticObjectId(jid) for jid in domain_data.job_ids]}}).to_list()
        
        # Simple hydration strategy: Add Job nodes. 
        # For a full graph, we might need to verify what the frontend expects.
        # Based on DomainImportModal, it usually adds SchemaNodes (tables) and EtlStepNodes (jobs).
        # For MVP, let's assume we simply link the jobs or create nodes representing these jobs/tables.
        # However, without shared logic, it's hard to replicate exact frontend graph positioning.
        # We will add them as nodes with a default layout or just data.
        
        current_y = 50
        for job in jobs:
            # Add Job Node
            job_node = {
                "id": str(job.id),
                "type": "etlStepNode", # Assuming this is the node type for jobs
                "position": {"x": 250, "y": current_y},
                "data": {
                    "label": job.name,
                    "jobId": str(job.id),
                    "status": "active" # dummy
                }
            }
            domain_dict["nodes"].append(job_node)
            
            # We could also add Source/Target nodes if we want to show the lineage
            # But let's keep it simple for now: "Domain has Jobs"
            current_y += 150

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
