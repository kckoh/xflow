from datetime import datetime
from typing import Optional, List
from beanie import Document
from pydantic import Field
from neomodel import (
    StructuredNode, StringProperty, RelationshipTo, RelationshipFrom, 
    UniqueIdProperty, JSONProperty
)

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


class RDBSource(Document):
    """
    RDB Source document for storing relational database connection info.
    Supports PostgreSQL, MySQL, MariaDB, etc.
    For AWS/LocalStack deployment configurations.
    """
    # Basic connection info
    name: str # 별명
    description: Optional[str] = None
    type: str  # postgres / mysql / mariadb / oracle
    host: str
    port: int
    database_name: str
    user_name: str
    password: str  # Should be encrypted in production
    status: str = "disconnected"  # connected / fail / disconnected
    
    # AWS/LocalStack deployment configuration (required)
    cloud_config: dict = Field(default_factory=dict)
    # Example structure (based on AWS Glue Connection):
    # {
    #   "vpc_id": "vpc-123abc",
    #   "subnet_id": "subnet-456def",
    #   "security_group_ids": ["sg-789ghi"],
    #   "availability_zone": "us-east-1a",
    #   "iam_role_arn": "arn:aws:iam::123456789012:role/GlueRole"
    # }
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "rdb_sources"


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
    source: dict = Field(default_factory=dict)
    # Example: {"type": "rdb", "connection_id": "...", "table": "products"}

    transforms: List[dict] = Field(default_factory=list)
    # Example: [{"type": "drop-columns", "config": {"columns": ["category"]}}]

    destination: dict = Field(default_factory=dict)
    # Example: {"type": "s3", "path": "s3a://bucket/path", "format": "parquet"}

    schedule: Optional[str] = None  # Cron expression or None for manual trigger
    status: str = "draft"  # draft, active, paused

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


class ETLJob(Document):
    """
    ETL Job document for storing ETL pipeline configurations.
    Defines source, transforms, and destination for data processing.
    """
    name: str
    description: Optional[str] = None
    source: dict = Field(default_factory=dict)
    # Example: {"type": "rdb", "connection_id": "...", "table": "products"}

    transforms: List[dict] = Field(default_factory=list)
    # Example: [{"type": "drop-columns", "config": {"columns": ["category"]}}]

    destination: dict = Field(default_factory=dict)
    # Example: {"type": "s3", "path": "s3a://bucket/path", "format": "parquet"}

    schedule: Optional[str] = None  # Cron expression or None for manual trigger
    status: str = "draft"  # draft, active, paused

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

# Neo4j Models
class Column(StructuredNode):
    """
    Represents a Column in a Table.
    """
    uid = UniqueIdProperty() # Auto-generated or manual unique ID
    name = StringProperty(required=True)
    type = StringProperty(default="string")
    description = StringProperty()
    
    # Relationships
    flows_to = RelationshipTo('Column', 'FLOWS_TO')
    belongs_to = RelationshipFrom('Table', 'HAS_COLUMN')

class Table(StructuredNode):
    """
    Represents a Dataset/Table.
    """
    mongo_id = StringProperty(unique_index=True, required=True)
    name = StringProperty(required=True)
    platform = StringProperty(default="hive")
    layer = StringProperty()
    description = StringProperty()
    urn = StringProperty()
    domain = StringProperty()
    
    # Dynamic properties payload if needed
    properties = JSONProperty()

    # Relationships
    has_columns = RelationshipTo(Column, 'HAS_COLUMN')
    flows_to = RelationshipTo('Table', 'FLOWS_TO') # Table-level lineage