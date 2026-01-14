from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


class SourceConfig(BaseModel):
    nodeId: Optional[str] = None  # Node ID for graph reference
    type: str  # rdb, mongodb, nosql, s3
    connection_id: str  # Connection ID
    table: Optional[str] = None  # For RDB
    collection: Optional[str] = None  # For NoSQL (MongoDB)
    query: Optional[str] = None  # Custom SQL query
    customRegex: Optional[str] = None  # For S3 log parsing with named groups


class TransformConfig(BaseModel):
    nodeId: Optional[str] = None  # Node ID for graph reference
    type: str  # select-fields, drop-columns, filter, union
    config: dict  # Type-specific configuration
    inputNodeIds: Optional[List[str]] = None  # For multi-input transforms like union


class DestinationConfig(BaseModel):
    type: str = "s3"  # s3
    path: str  # s3a://bucket/path
    format: str = "parquet"
    glue_table_name: Optional[str] = None
    options: Dict[str, Any] = Field(default_factory=dict)  # compression, partitionBy, etc.


class DatasetCreate(BaseModel):
    name: str
    description: Optional[str] = None
    dataset_type: str = "source"  # "source" or "target"
    job_type: str = "batch"  # "batch" or "cdc"
    # Multiple sources support (new)
    sources: Optional[List[SourceConfig]] = None
    # Legacy single source (backward compatibility)
    source: Optional[SourceConfig] = None
    transforms: List[TransformConfig] = []
    targets: Optional[List[dict]] = None
    destination: DestinationConfig
    schedule: Optional[str] = None  # Created by backend
    schedule_frequency: Optional[str] = None
    ui_params: Optional[dict] = None
    incremental_config: Optional[dict] = None

    # Visual Editor state
    nodes: Optional[List[dict]] = None
    edges: Optional[List[dict]] = None


class DatasetUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    dataset_type: Optional[str] = None  # "source" or "target"
    job_type: Optional[str] = None  # "batch" or "cdc"
    # Multiple sources support (new)
    sources: Optional[List[SourceConfig]] = None
    # Legacy single source (backward compatibility)
    source: Optional[SourceConfig] = None
    transforms: Optional[List[TransformConfig]] = None
    targets: Optional[List[dict]] = None
    destination: Optional[DestinationConfig] = None

    schedule: Optional[str] = None
    schedule_frequency: Optional[str] = None
    ui_params: Optional[dict] = None
    incremental_config: Optional[dict] = None

    status: Optional[str] = None
    import_ready: Optional[bool] = None
    # Visual Editor state
    nodes: Optional[List[dict]] = None
    edges: Optional[List[dict]] = None


class DatasetResponse(DatasetCreate):
    id: str
    name: str
    description: Optional[str] = None
    owner: Optional[str] = "system"
    # Multiple sources support (new)
    sources: Optional[List[dict]] = None
    # Legacy single source (backward compatibility)
    source: Optional[dict] = None
    transforms: List[dict]
    destination: dict

    schedule: Optional[str] = None
    schedule_frequency: Optional[str] = None
    ui_params: Optional[dict] = None
    incremental_config: Optional[dict] = None
    last_sync_timestamp: Optional[datetime] = None

    status: str
    created_at: datetime
    updated_at: datetime
    # Visual Editor state
    nodes: Optional[List[dict]] = None
    edges: Optional[List[dict]] = None
    is_active: bool = False  # Derived from ETLJob model
    import_ready: bool = False

    class Config:
        from_attributes = True
