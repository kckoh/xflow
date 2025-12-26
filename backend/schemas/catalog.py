from typing import List, Optional
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
    tags: List[str] = []
    
    class Config:
        populate_by_name = True
