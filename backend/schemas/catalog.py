from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime

class CatalogItem(BaseModel):
    """
    Summary of a dataset for the list view (Catalog Page).
    """
    id: str = Field(..., alias="_id")
    name: str # Table name
    type: str # e.g. 'Table', 'Topic'
    platform: Optional[str] = None # e.g. 'MySQL', 'Kafka'
    owner: str # Display name of the owner
    updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    tags: List[str] = []
    description: Optional[str] = None
    
    class Config:
        populate_by_name = True

class ColumnItem(BaseModel):
    """
    Detailed schema information for a column.
    Sources from MongoDB 'columns' collection.
    """
    name: str = Field(..., alias="column_name")
    type: str = Field(..., alias="data_type")
    description: Optional[str] = None
    is_pii: bool = False
    tags: List[str] = []
    
    # Optional stats or extended metadata
    nullable: Optional[bool] = None
    is_primary_key: Optional[bool] = None
    
    class Config:
        populate_by_name = True

class DatasetDetail(CatalogItem):
    """
    Full details of a dataset, including schema.
    Combines 'tables' and 'columns' collections from MongoDB.
    """
    columns: List[ColumnItem] = []
    
    # Additional metadata fields that might be in the detail view
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    documentation_url: Optional[List[str]] = None
