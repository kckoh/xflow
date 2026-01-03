from datetime import datetime
from typing import Optional, List, Dict, Any
from beanie import Document, Link
from pydantic import Field, BaseModel

class User(Document):
    """
    User document for MongoDB.
    Beanie automatically handles the _id field as a PydanticObjectId.
    """
    email: str = Field(..., unique=True, index=True)
    password: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "users"  # Collection name in MongoDB
        indexes = [
            "email",  # Create index on email for faster lookups
        ]


class Connection(Document):
    """
    Generic Connection document for storing connection info for various sources.
    Supports RDB (PostgreSQL, MySQL), NoSQL (MongoDB), Object Storage (S3), etc.
    """
    name: str
    description: Optional[str] = None
    type: str  # postgres / mysql / s3 / mongodb / etc.
    
    # Generic configuration storage
    # - RDB: { host, port, database, user, password, ... }
    # - S3: { bucket, access_key, secret_key, region, ... }
    config: dict = Field(default_factory=dict)
    
    status: str = "disconnected"  # connected / error / disconnected
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "connections"


class Transform(Document):
    """
    Transform document for storing ETL transformation configurations.
    Supports multiple transform types: select-fields, filter, join, etc.
    Note: source_id and source_table are managed by Pipeline, not Transform.
    """
    name: str  # Transform name
    transform_type: str  # Transform type: "select-fields", "filter", "join", etc.
    config: dict = Field(default_factory=dict)  # Type-specific configuration

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "transforms"


class ETLJob(Document):
    """
    ETL Job document for storing ETL pipeline configurations.
    Defines source, transforms, and destination for data processing.
    """
    name: str
    description: Optional[str] = None

    # Multiple sources support (new)
    sources: List[dict] = Field(default_factory=list)
    # Example: [{"nodeId": "1", "type": "rdb", "connection_id": "...", "table": "products"}]

    # Legacy single source (backward compatibility)
    source: dict = Field(default_factory=dict)
    # Example: {"type": "rdb", "connection_id": "...", "table": "products"}

    transforms: List[dict] = Field(default_factory=list)
    # Example: [{"nodeId": "3", "type": "union", "config": {}, "inputNodeIds": ["1", "2"]}]

    destination: dict = Field(default_factory=dict)
    # Example: {"type": "s3", "path": "s3a://bucket/path", "format": "parquet"}

    schedule: Optional[str] = None  # Cron expression or @interval string
    schedule_frequency: Optional[str] = None  # daily, weekly, interval, etc.
    ui_params: Optional[dict] = None  # Raw UI parameters for restoration
    
    status: str = "draft"  # draft, active, paused
    
    # Incremental Load Support
    last_sync_timestamp: Optional[datetime] = None
    incremental_config: Optional[dict] = None
    # Example: {"enabled": true, "timestamp_column": "updated_at", "mode": "append"}

    # Import ready flag - true when job execution is complete and ready to import
    import_ready: bool = False

    # Estimated total source size in GB (for Spark executor auto-scaling)
    estimated_size_gb: float = 1.0

    # Visual Editor state (for UI restoration)
    nodes: Optional[List[dict]] = None
    edges: Optional[List[dict]] = None

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "etl_jobs"


class JobRun(Document):
    """
    Job Run document for tracking ETL job executions.
    """
    job_id: str  # Reference to ETLJob
    status: str = "pending"  # pending, running, success, failed
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error_message: Optional[str] = None
    airflow_run_id: Optional[str] = None  # Airflow DAG run ID

    class Settings:
        name = "job_runs"



class Attachment(BaseModel):
    id: str  # UUID
    name: str  # Original filename
    url: str  # S3 URL or Path
    size: int  # Bytes
    type: str  # MIME type
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)

class Domain(Document):
    name: str
    type: str  # e.g., 'marketing', 'sales'
    owner: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    description: Optional[str] = None
    docs: Optional[str] = None
    attachments: List[Attachment] = Field(default_factory=list)
    nodes: Optional[List[Dict[str, Any]]] = None
    edges: Optional[List[Dict[str, Any]]] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "domains"


# Dataset Model (MongoDB - Replaces Neo4j Models)
class DatasetNode(BaseModel):
    nodeId: str
    urn: str # Global Unique Identifier
    type: str # rdb, s3, filter, etc.
    schema: List[dict] = Field(default_factory=list)
    config: dict = Field(default_factory=dict)
    inputNodeIds: List[str] = Field(default_factory=list)

class Dataset(Document):
    """
    MongoDB Document representing a Logical Dataset / Pipeline.
    Designed to be synced from ETLJob.
    """
    name: str # Pipeline Name
    description: Optional[str] = None # Pipeline Description
    job_id: Optional[str] = None # Reference to the ETLJob that defined this dataset
    
    # 1. Inputs (Sources)
    sources: List[DatasetNode] = Field(default_factory=list)
    
    # 2. Transformations
    transforms: List[DatasetNode] = Field(default_factory=list)
    
    # 3. Outputs (Targets)
    targets: List[DatasetNode] = Field(default_factory=list)
    
    is_active: bool = False
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "datasets"
        indexes = [
            "job_id",
            "sources.urn",
            "targets.urn"
        ]