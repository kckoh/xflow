from datetime import datetime
from typing import Any, Dict, List, Optional

from beanie import Document, Link
from pydantic import BaseModel, Field


class User(Document):
    """
    User document for MongoDB.
    Beanie automatically handles the _id field as a PydanticObjectId.
    """

    email: str = Field(..., unique=True, index=True)
    password: Optional[str] = None
    name: Optional[str] = None

    # Admin role
    is_admin: bool = False

    # Dataset access control
    dataset_access: List[str] = Field(default_factory=list)  # Dataset IDs user can access
    all_datasets: bool = False  # If true, user can access all datasets

    # Feature-level permissions (applied to accessible datasets)
    can_manage_datasets: bool = False    # Create/Edit/Delete datasets and ETL pipelines
    can_run_query: bool = True           # Run queries (default: true)

    # DEPRECATED - kept for backward compatibility, will be removed in future
    etl_access: bool = False
    domain_edit_access: bool = False

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


class Dataset(Document):
    """
    Dataset document for storing ETL pipeline configurations.
    Defines source, transforms, and destination for data processing.
    """

    name: str
    description: Optional[str] = None
    dataset_type: str = "source"  # "source" or "target"
    job_type: str = "batch"  # "batch" or "cdc"

    # Multiple sources support (new)
    sources: List[dict] = Field(default_factory=list)
    # Example: [{"nodeId": "1", "type": "rdb", "connection_id": "...", "table": "products"}]

    # Legacy single source (backward compatibility)
    source: dict = Field(default_factory=dict)
    # Example: {"type": "rdb", "connection_id": "...", "table": "products"}

    transforms: List[dict] = Field(default_factory=list)
    targets: List[dict] = Field(default_factory=list)
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

    # Actual size of files stored in S3 (in bytes)
    # Updated after successful job runs
    actual_size_bytes: Optional[int] = None
    
    # Row count from quality checks
    # Updated when quality checks are run
    row_count: Optional[int] = None

    # Visual Editor state (for UI restoration)
    nodes: Optional[List[dict]] = None
    edges: Optional[List[dict]] = None

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "datasets"


class JobRun(Document):
    """
    Job Run document for tracking Dataset executions.
    """

    dataset_id: str  # Reference to Dataset
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


# ETLJob Model (MongoDB - Replaces Neo4j Models)
class ETLJobNode(BaseModel):
    nodeId: str
    urn: str  # Global Unique Identifier
    type: str  # rdb, s3, filter, etc.
    schema: List[dict] = Field(default_factory=list)
    config: dict = Field(default_factory=dict)
    inputNodeIds: List[str] = Field(default_factory=list)


class ETLJob(Document):
    """
    MongoDB Document representing a Logical ETL Job / Pipeline.
    Designed to be synced from Dataset.
    """

    name: str  # Pipeline Name
    description: Optional[str] = None  # Pipeline Description
    dataset_id: Optional[str] = None  # Reference to the Dataset that defined this ETL job

    # 1. Inputs (Sources)
    sources: List[ETLJobNode] = Field(default_factory=list)

    # 2. Transformations
    transforms: List[ETLJobNode] = Field(default_factory=list)

    # 3. Outputs (Targets)
    targets: List[ETLJobNode] = Field(default_factory=list)

    is_active: bool = False

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "etl_jobs"
        indexes = ["dataset_id", "sources.urn", "targets.urn"]


class QualityCheck(BaseModel):
    """Individual quality check result"""

    name: str  # "null_check", "duplicate_check", etc.
    column: Optional[str] = None  # Column name (if applicable)
    passed: bool  # Did it pass the threshold?
    value: float  # Actual value (e.g., 0.5 = 0.5% nulls)
    threshold: float  # Threshold to pass (e.g., 5.0 = 5%)
    message: Optional[str] = None


class QualityResult(Document):
    """
    Quality check result for a Dataset.
    Each run creates a new document, allowing historical tracking.
    """

    dataset_id: str  # Reference to Dataset._id
    s3_path: str  # S3 path that was checked

    # Summary metrics
    row_count: int = 0
    column_count: int = 0
    null_counts: Dict[str, int] = Field(default_factory=dict)  # { column: null_count }
    duplicate_count: int = 0

    # Overall score (0-100)
    overall_score: float = 0.0

    # Detailed check results
    checks: List[QualityCheck] = Field(default_factory=list)

    # Metadata
    status: str = "pending"  # pending / running / completed / failed
    error_message: Optional[str] = None
    run_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    duration_ms: int = 0

    class Settings:
        name = "quality_results"
        indexes = ["dataset_id", "run_at"]

