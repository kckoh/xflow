"""
Transform API Schemas
Request and response models for Transform operations
"""

from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class TransformCreate(BaseModel):
    """Request schema for creating a transform"""
    name: Optional[str] = "Select Fields"  # 기본값, 나중에 수정 가능
    source_id: str
    source_table: str
    transform_type: str  # Required: "select-fields" (MVP), more types later
    selected_columns: List[str]


class TransformResponse(BaseModel):
    """Response schema for transform"""
    id: str
    name: str
    source_id: str
    source_table: str
    transform_type: str
    selected_columns: List[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
