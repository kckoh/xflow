from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime

class ColumnItem(BaseModel):
    """
    Detailed schema information for a column.
    """
    name: str 
    type: str # e.g. INT, STRING
    description: Optional[str] = None
    is_partition: bool = False
    tags: List[str] = [] # Added Column Tags

class CatalogItem(BaseModel):
    """
    Summary of a dataset for the list view (Catalog Page).
    """
    id: str = Field(...)
    name: str # Table name
    urn: str
    platform: str = "hive"
    layer: Optional[str] = None 
    owner: Optional[str] = "Unknown"
    tags: List[str] = []
    description: Optional[str] = None
    properties: Dict[str, Any] = {}

class DatasetDetail(CatalogItem):
    """
    Full details of a dataset, including schema.
    """
    schema: List[ColumnItem] = Field(default=[]) 

class DatasetUpdate(BaseModel):
    """
    Editable detailed metadata.
    """
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: Optional[List[str]] = None
    domain: Optional[str] = None
    schema: Optional[List[ColumnItem]] = None

class DatasetCreate(BaseModel):
    """
    Payload for creating a new Logical Dataset (Metadata only).
    """
    name: str = Field(..., description="Unique dataset/table name")
    description: Optional[str] = None
    platform: str = "hive"
    domain: str = Field("General", description="Business Domain")
    owner: str = "User"
    tags: List[str] = []
    schema: Optional[List[ColumnItem]] = Field(default=[]) 
    # No s3_path mandatory
