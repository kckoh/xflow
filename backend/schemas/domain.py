from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from datetime import datetime


class DomainJobListResponse(BaseModel):
    """Simplified job info for domain import list"""
    id: str
    name: str
    description: Optional[str] = None
    source_count: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class DatasetNodeResponse(BaseModel):
    """Node in job execution result (source/transform/target)"""
    nodeId: str
    type: str
    schema: List[dict]
    config: dict
    inputNodeIds: List[str] = []
    urn: Optional[str] = None


class JobExecutionResponse(BaseModel):
    """Full execution result with sources/targets/transforms"""
    id: str
    name: str
    description: Optional[str] = None
    job_id: Optional[str] = None
    sources: List[DatasetNodeResponse]
    transforms: List[DatasetNodeResponse]
    targets: List[DatasetNodeResponse]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class DomainCreate(BaseModel):
    name: str
    type: str
    owner: Optional[str] = None
    tags: List[str] = []
    description: Optional[str] = None
    docs: Optional[str] = None
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

class DomainGraphUpdate(BaseModel):
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]