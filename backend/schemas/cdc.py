"""
CDC (Change Data Capture) Schemas
Pydantic models for CDC connector management
"""
from pydantic import BaseModel
from typing import List, Optional


class CDCTableConfig(BaseModel):
    """Configuration for a table to enable CDC"""
    connection_id: str
    database: str
    schema_name: str = "public"
    table_name: str


class CDCConnectorRequest(BaseModel):
    """Request to create a CDC connector"""
    connector_name: str
    source_type: str  # "postgresql" or "mongodb"
    host: str
    port: int
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    tables: List[str]


class CDCConnectorResponse(BaseModel):
    """Response after creating a CDC connector"""
    name: str
    status: str
    tables: List[str]
