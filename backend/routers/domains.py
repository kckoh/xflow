from fastapi import APIRouter, HTTPException, Body, status, Query
from typing import List, Optional, Dict, Any
from datetime import datetime
from beanie import PydanticObjectId

from models import Domain, ETLJob, Dataset
from schemas.domain import (
    DomainCreate,
    DomainUpdate,
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

@router.put("/{id}", response_model=Domain)
async def update_domain(id: str, update_data: DomainUpdate):
    """
    Update domain metadata (name, description, tags, etc.).
    """
    domain = await Domain.get(id)
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")

    # Update fields that are provided
    update_dict = update_data.model_dump(exclude_unset=True)
    
    if not update_dict:
        return domain

    for key, value in update_dict.items():
        setattr(domain, key, value)
    
    domain.updated_at = datetime.utcnow()
    await domain.save()
    return domain

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


# --- Attachment Endpoints ---

from fastapi import UploadFile, File
from models import Attachment
import uuid
from utils.aws_client import get_aws_client

@router.post("/{id}/files", response_model=Domain)
async def upload_attachment(id: str, file: UploadFile = File(...)):
    """
    Upload a file to S3 and attach it to the domain.
    """
    domain = await Domain.get(id)
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")

    try:
        s3 = get_aws_client("s3")
        bucket_name = "xflow-data" # ToDo: Move to config
        
        # Ensure bucket exists (First time setup for LocalStack)
        try:
            s3.head_bucket(Bucket=bucket_name)
        except Exception:
            try:
                # Create bucket if it doesn't exist
                region = s3.meta.region_name
                if region and region != 'us-east-1':
                    s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                else:
                    s3.create_bucket(Bucket=bucket_name)
                    
                print(f"Created bucket: {bucket_name}")
            except Exception as e:
                print(f"Failed to create bucket: {e}")
                # Fallthrough to let upload try (or fail with specific error)

        file_ext = file.filename.split('.')[-1] if '.' in file.filename else ""
        file_uuid = str(uuid.uuid4())
        s3_key = f"domains/{id}/docs/{file_uuid}_{file.filename}"
        
        # Upload to S3
        s3.upload_fileobj(
            file.file,
            bucket_name,
            s3_key,
            ExtraArgs={'ContentType': file.content_type}
        )
        
        # Construct S3 URL (virtual-hosted style)
        # s3_url = f"https://{bucket_name}.s3.{s3.meta.region_name}.amazonaws.com/{s3_key}"
        # Or simple s3:// style for internal use, but frontend might need http url.
        # Let's verify if user wants presigned url or direct access.
        # For now, let's store the s3:// path mostly or a constructed public URL if public.
        # Assuming private bucket, let's store the key or s3 uri.
        # But UI needs to download it. So generating a presigned URL on GET would be best,
        # but for simplicity, let's store the s3 URI and maybe generate presigned link on frontend request?
        # Actually, simpler: return the S3 key, and have a 'download' endpoint.
        # But user asked for S3 attachment.
        # Let's store the full S3 URI.
        s3_uri = f"s3://{bucket_name}/{s3_key}"

        # Create Attachment Metadata
        attachment = Attachment(
            id=file_uuid,
            name=file.filename,
            url=s3_uri,
            size=file.size if file.size else 0, # UploadFile might not have size set immediately
            type=file.content_type or "application/octet-stream",
            uploaded_at=datetime.utcnow()
        )
        
        # Add to Domain
        if domain.attachments is None:
            domain.attachments = []
        domain.attachments.append(attachment)
        domain.updated_at = datetime.utcnow()
        
        await domain.save()
        return domain

    except Exception as e:
        print(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"File upload failed: {str(e)}")

@router.delete("/{id}/files/{file_id}", response_model=Domain)
async def delete_attachment(id: str, file_id: str):
    """
    Delete an attachment from S3 and the domain.
    """
    domain = await Domain.get(id)
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")

    # Find attachment
    attachment = next((a for a in domain.attachments if a.id == file_id), None)
    if not attachment:
        raise HTTPException(status_code=404, detail="File attachment not found")

    try:
        # Delete from S3
        # Extract Key from s3://bucket/key
        # url: s3://xflow-data/domains/...
        if attachment.url.startswith("s3://"):
            bucket_name = attachment.url.split("/")[2]
            key = "/".join(attachment.url.split("/")[3:])
            
            s3 = get_aws_client("s3")
            s3.delete_object(Bucket=bucket_name, Key=key)
        
        # Remove from List
        domain.attachments = [a for a in domain.attachments if a.id != file_id]
        domain.updated_at = datetime.utcnow()
        
        await domain.save()
        return domain

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File deletion failed: {str(e)}")

@router.get("/{id}/files/{file_id}/download")
async def get_attachment_url(id: str, file_id: str):
    """
    Generate a presigned URL for downloading the attachment.
    """
    domain = await Domain.get(id)
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")

    attachment = next((a for a in domain.attachments if a.id == file_id), None)
    if not attachment:
        raise HTTPException(status_code=404, detail="File attachment not found")
        
    try:
        if attachment.url.startswith("s3://"):
            bucket_name = attachment.url.split("/")[2]
            key = "/".join(attachment.url.split("/")[3:])
            
            s3 = get_aws_client("s3")
            url = s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': key},
                ExpiresIn=3600 # 1 hour
            )
            return {"url": url}
        else:
            return {"url": attachment.url} # Return as is if not S3 (e.g. http link)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate download URL: {str(e)}")
