from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class APIAuthConfig(BaseModel):
    header_name: Optional[str] = None
    api_key: Optional[str] = None
    token: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


class APIConnectionConfig(BaseModel):
    base_url: str
    auth_type: str = "none"  # none, api_key, bearer, basic
    auth_config: Optional[APIAuthConfig] = None
    headers: Optional[Dict[str, str]] = None


class PaginationConfig(BaseModel):
    type: str = "none"  # none, offset_limit, page, cursor
    config: Dict[str, Any] = Field(default_factory=dict)


class IncrementalConfig(BaseModel):
    enabled: bool = False
    timestamp_field: Optional[str] = None
    timestamp_param: Optional[str] = None
    last_sync_timestamp: Optional[str] = None
    format: Optional[str] = None  # e.g. ISO 8601


class APISourceDatasetConfig(BaseModel):
    endpoint: str
    method: str = "GET"
    query_params: Optional[Dict[str, Any]] = None
    pagination: Optional[PaginationConfig] = None
    response_path: Optional[str] = None
    incremental_config: Optional[IncrementalConfig] = None

