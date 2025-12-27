"""
Select Fields Transform Schemas
Request and response models for select-fields transform
"""

from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime


class SelectFieldsCreate(BaseModel):
    """Request schema for creating a select-fields transform"""
    name: Optional[str] = "Select Fields" 
    selected_columns: List[str]


class SelectFieldsResponse(BaseModel):
    """Response schema for select-fields transform"""
    id: str
    name: str
    transform_type: str
    selected_columns: List[str]  # Extracted from config for convenience
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
