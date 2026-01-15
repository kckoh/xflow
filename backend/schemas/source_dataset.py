from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime

from schemas.api_source import APISourceDatasetConfig


class SourceDatasetCreate(BaseModel):
    name: str
    description: Optional[str] = None
    owner: Optional[str] = None
    source_type: str  # postgres, mongodb, s3
    connection_id: str

    # PostgreSQL specific
    table: Optional[str] = None

    # MongoDB specific
    collection: Optional[str] = None

    # S3 specific
    bucket: Optional[str] = None
    path: Optional[str] = None
    format: Optional[str] = None  # "log", "parquet", "csv", "json"

    # Schema information
    columns: Optional[List[dict]] = None

    # API specific
    api: Optional[APISourceDatasetConfig] = None

    # Kafka specific
    topic: Optional[str] = None


class SourceDatasetUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    owner: Optional[str] = None
    source_type: Optional[str] = None
    connection_id: Optional[str] = None

    # PostgreSQL specific
    table: Optional[str] = None

    # MongoDB specific
    collection: Optional[str] = None

    # S3 specific
    bucket: Optional[str] = None
    path: Optional[str] = None
    format: Optional[str] = None  # "log", "parquet", "csv", "json"

    # Schema information
    columns: Optional[List[dict]] = None

    # API specific
    api: Optional[APISourceDatasetConfig] = None

    # Kafka specific
    topic: Optional[str] = None


class SourceDatasetResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    owner: Optional[str] = None
    source_type: str
    connection_id: str

    # Type-specific fields
    table: Optional[str] = None
    collection: Optional[str] = None
    bucket: Optional[str] = None
    path: Optional[str] = None
    format: Optional[str] = None  # "log", "parquet", "csv", "json"

    # Schema information
    columns: Optional[List[dict]] = None

    # API specific
    api: Optional[APISourceDatasetConfig] = None

    # Kafka specific
    topic: Optional[str] = None

    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
