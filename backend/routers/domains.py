from fastapi import APIRouter, HTTPException, Body, status, Query, Depends
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
# OpenSearch Dual Write
from utils.indexers import index_single_domain, delete_domain_from_index
from dependencies import get_user_session

router = APIRouter()

# --- Domain CRUD Endpoints ---

@router.get("")
async def get_all_domains(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search term for domain name"),
    user_session: Optional[Dict[str, Any]] = Depends(get_user_session)
):
    """
    Get all domains (list view) with pagination.
    Filters domains based on user's dataset_access permissions.
    Only shows domains where user has access to at least one dataset.
    """
    domains = await Domain.find_all().to_list()
    
    # Filter domains based on dataset permissions    
    if user_session:
        is_admin = user_session.get("is_admin", False)
        
        if not is_admin:
            dataset_access = user_session.get("dataset_access", [])
            
            # Get all datasets to map names to IDs
            datasets = await Dataset.find_all().to_list()
            dataset_id_to_name = {str(d.id): d.name for d in datasets}
            allowed_dataset_names = set(dataset_id_to_name.get(did) for did in dataset_access if did in dataset_id_to_name)
            
            # Filter domains
            filtered_domains = []
            for domain in domains:
                # Extract dataset names from domain nodes
                has_accessible_dataset = False
                
                if domain.nodes:
                    for node in domain.nodes:
                        node_data = node.get("data", {})
                        node_name = node_data.get("name") or node_data.get("label", "")
                        
                        # Extract dataset name (remove prefix like "(S3) ")
                        if node_name and ') ' in node_name:
                            node_name = node_name.split(') ')[1]
                        
                        # Check if this dataset is accessible
                        if node_name in allowed_dataset_names:
                            has_accessible_dataset = True
                            break
                
                # Only include domain if user has access to at least one dataset
                if has_accessible_dataset:
                    filtered_domains.append(domain)
            
            domains = filtered_domains
    
    # Apply search filter
    if search:
        search_lower = search.lower()
        domains = [d for d in domains if search_lower in d.name.lower()]
    
    # Sort by updated_at descending (newest first)
    domains.sort(key=lambda d: d.updated_at or d.created_at, reverse=True)
    
    # Pagination
    total = len(domains)
    total_pages = (total + limit - 1) // limit  # ceil division
    start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    paginated_domains = domains[start_idx:end_idx]
    
    return {
        "items": paginated_domains,
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": total_pages
    }


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
async def list_domain_jobs(
    import_ready: bool = Query(True),
    user_session: Optional[Dict[str, Any]] = Depends(get_user_session)
):
    """Get ETL jobs that are ready to be imported into domain (import_ready=true by default)
    Filters by user's dataset_access permissions if authenticated."""
    try:
        # Get all import-ready jobs
        jobs = await ETLJob.find(ETLJob.import_ready == import_ready).to_list()
        
        # Filter jobs based on dataset_access permissions
        if user_session:
            is_admin = user_session.get("is_admin", False)
            dataset_access = user_session.get("dataset_access", [])
            
            # Admin sees all jobs
            if not is_admin and dataset_access is not None:
                # Get all datasets to map job_id to dataset_id
                datasets = await Dataset.find_all().to_list()
                job_to_dataset = {d.job_id: str(d.id) for d in datasets if d.job_id}
                
                # Filter jobs: only include if user has access to the dataset
                filtered_jobs = []
                for job in jobs:
                    job_id_str = str(job.id)
                    dataset_id = job_to_dataset.get(job_id_str)
                    
                    # Include job if:
                    # 1. User has access to its dataset, OR
                    # 2. No dataset exists yet for this job (allow import to create it)
                    if dataset_id is None or dataset_id in dataset_access:
                        filtered_jobs.append(job)
                
                jobs = filtered_jobs

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
    
    # Dual Write: OpenSearch에 인덱싱
    await index_single_domain(new_domain)
    
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
    
    # Dual Write: OpenSearch에서 삭제
    await delete_domain_from_index(id)

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
    
    # Dual Write: OpenSearch 업데이트
    await index_single_domain(domain)
    
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
    
    # Dual Write: OpenSearch 업데이트 (노드/컬럼 변경 시)
    await index_single_domain(domain)
    
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
        # Check File Size (Limit 10MB)
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)
        
        if file_size > 10 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="File size exceeds 10MB limit")

        s3 = get_aws_client("s3")
        bucket_name = "xflow-data" # ToDo: Move to config
        
        # Ensure bucket exists
        try:
            s3.head_bucket(Bucket=bucket_name)
        except Exception as e:
            # Team Lead Directive: Do not auto-create bucket. Raise error if missing.
            print(f"S3 Bucket '{bucket_name}' not found or inaccessible: {e}")
            raise HTTPException(status_code=500, detail="Server Configuration Error: S3 Storage not ready.")

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

from fastapi import UploadFile, File, Request
from models import Attachment
import uuid
from utils.aws_client import get_aws_client
from utils.limiter import limiter

# ... (Previous code)

@router.get("/{id}/files/{file_id}/download")
@limiter.limit("30/minute") # Team Lead Directive: 30 downloads per minute
async def get_attachment_url(id: str, file_id: str, request: Request):
    """
    Download attachment directly (Proxied to bypass 403/CORS issues)
    """
    domain = await Domain.get(id)
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")

    attachment = next((a for a in domain.attachments if a.id == file_id), None)
    if not attachment:
        raise HTTPException(status_code=404, detail="File attachment not found")
        
    try:
        from fastapi.responses import StreamingResponse
        import mimetypes

        if attachment.url.startswith("s3://"):
            bucket_name = attachment.url.split("/")[2]
            key = "/".join(attachment.url.split("/")[3:])
            
            s3 = get_aws_client("s3")
            response = s3.get_object(Bucket=bucket_name, Key=key)
            
            # Use generator to stream content
            def iterfile():
                yield from response['Body']

            filename = attachment.name or "download"
            # URL encode filename for Content-Disposition header
            from urllib.parse import quote
            encoded_filename = quote(filename)

            return StreamingResponse(
                iterfile(),
                media_type=attachment.type or "application/octet-stream",
                headers={
                    "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
                }
            )
        else:
            # If not S3, just redirect (fallback)
            from fastapi.responses import RedirectResponse
            return RedirectResponse(url=attachment.url)
            
    except Exception as e:
        print(f"Download Proxy Failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to download file: {str(e)}")
