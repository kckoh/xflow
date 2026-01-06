from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from datetime import datetime


class DomainDatasetListResponse(BaseModel):
    """Simplified Dataset info for domain import list (pipelines)"""
    id: str
    name: str
    description: Optional[str] = None
    source_count: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ETLJobNodeResponse(BaseModel):
    """Node in ETLJob execution result (source/transform/target for lineage)"""
    nodeId: str
    type: str
    schema: List[dict]
    config: dict
    inputNodeIds: List[str] = []
    urn: Optional[str] = None


class DatasetExecutionResponse(BaseModel):
    """Full execution result with sources/targets/transforms from ETLJob (lineage)"""
    id: str
    name: str
    description: Optional[str] = None
    dataset_id: Optional[str] = None
    sources: List[ETLJobNodeResponse]
    transforms: List[ETLJobNodeResponse]
    targets: List[ETLJobNodeResponse]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class AttachmentSchema(BaseModel):
    id: str
    name: str
    url: str
    size: int
    type: str
    uploaded_at: datetime

class DomainCreate(BaseModel):
    name: str
    type: str
    owner: Optional[str] = None
    tags: List[str] = []
    description: Optional[str] = None
    docs: Optional[str] = None
    attachments: List[AttachmentSchema] = []
    job_ids: List[str] = []
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []

class DomainUpdate(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    owner: Optional[str] = None
    tags: Optional[List[str]] = None
    description: Optional[str] = None
    docs: Optional[str] = None
    attachments: Optional[List[AttachmentSchema]] = None

class DomainGraphUpdate(BaseModel):
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]