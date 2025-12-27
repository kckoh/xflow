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
    
    class Config:
        populate_by_name = True

class CatalogItem(BaseModel):
    """
    Summary of a dataset for the list view (Catalog Page).
    """
    id: str = Field(...)
    name: str # Table name
    urn: str
    platform: str = "hive"
    layer: Optional[str] = None # Derived from properties.layer
    owner: Optional[str] = "Unknown"
    tags: List[str] = []
    description: Optional[str] = None
    properties: Dict[str, Any] = {}
    
    class Config:
        populate_by_name = True

class DatasetDetail(CatalogItem):
    """
    Full details of a dataset, including schema.
    """
    schema_: List[ColumnItem] = Field(default=[]) 
    
    # We can alias 'columns' to 'schema' if we want frontend to see 'columns'
    # For now, let's expose 'schema' as 'schema' in JSON or 'columns'
    columns: List[ColumnItem] = Field(default=[]) 

    class Config:
        populate_by_name = True

class DatasetUpdate(BaseModel):
    """
    Editable detailed metadata.
    """
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: Optional[List[str]] = None
