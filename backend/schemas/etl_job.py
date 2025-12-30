from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime


class SourceConfig(BaseModel):
    nodeId: Optional[str] = None  # Node ID for graph reference
    type: str  # rdb
    connection_id: str  # RDBSource ID
    table: Optional[str] = None
    query: Optional[str] = None  # Custom SQL query


class TransformConfig(BaseModel):
    nodeId: Optional[str] = None  # Node ID for graph reference
    type: str  # select-fields, drop-columns, filter, union
    config: dict  # Type-specific configuration
    inputNodeIds: Optional[List[str]] = None  # For multi-input transforms like union


class DestinationConfig(BaseModel):
    type: str = "s3"  # s3
    path: str  # s3a://bucket/path
    format: str = "parquet"
    options: dict = {}  # compression, partitionBy, etc.


class ETLJobCreate(BaseModel):
    name: str
    description: Optional[str] = None
    # Multiple sources support (new)
    sources: Optional[List[SourceConfig]] = None
    # Legacy single source (backward compatibility)
    source: Optional[SourceConfig] = None
    transforms: List[TransformConfig] = []
    destination: DestinationConfig
    # Visual Editor state
    nodes: Optional[List[dict]] = None
    edges: Optional[List[dict]] = None


class ETLJobUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    # Multiple sources support (new)
    sources: Optional[List[SourceConfig]] = None
    # Legacy single source (backward compatibility)
    source: Optional[SourceConfig] = None
    transforms: Optional[List[TransformConfig]] = None
    destination: Optional[DestinationConfig] = None
    status: Optional[str] = None
    # Visual Editor state
    nodes: Optional[List[dict]] = None
    edges: Optional[List[dict]] = None


class ETLJobResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    # Multiple sources support (new)
    sources: Optional[List[dict]] = None
    # Legacy single source (backward compatibility)
    source: Optional[dict] = None
    transforms: List[dict]
    destination: dict
    schedule_enabled: bool = False
    schedule_cron: Optional[str] = None
    schedule_timezone: str = "UTC"
    status: str
    created_at: datetime
    updated_at: datetime
    # Visual Editor state
    nodes: Optional[List[dict]] = None
    edges: Optional[List[dict]] = None

    class Config:
        from_attributes = True

