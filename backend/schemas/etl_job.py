from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime


class SourceConfig(BaseModel):
    type: str  # rdb
    connection_id: str  # RDBSource ID
    table: Optional[str] = None
    query: Optional[str] = None  # Custom SQL query


class TransformConfig(BaseModel):
    type: str  # select-fields, drop-columns, filter
    config: dict  # Type-specific configuration


class DestinationConfig(BaseModel):
    type: str = "s3"  # s3
    path: str  # s3a://bucket/path
    format: str = "parquet"
    options: dict = {}  # compression, partitionBy, etc.


class ETLJobCreate(BaseModel):
    name: str
    description: Optional[str] = None
    source: SourceConfig
    transforms: List[TransformConfig] = []
    destination: DestinationConfig
    schedule: Optional[str] = None  # Cron expression or None for manual


class ETLJobUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    source: Optional[SourceConfig] = None
    transforms: Optional[List[TransformConfig]] = None
    destination: Optional[DestinationConfig] = None
    schedule: Optional[str] = None
    status: Optional[str] = None


class ETLJobResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    source: dict
    transforms: List[dict]
    destination: dict
    schedule: Optional[str] = None
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
