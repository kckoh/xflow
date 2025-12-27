"""
RDB Transform Schemas
Schemas for RDB transformation operations (Select Fields, etc.)
"""

from typing import List
from pydantic import BaseModel, Field


class ColumnInfo(BaseModel):
    """Column information schema"""
    name: str
    type: str
    nullable: bool
