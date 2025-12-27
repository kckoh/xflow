"""
RDB Tables Schemas
Schemas for RDB table and column information
"""

from typing import List
from pydantic import BaseModel


class RDBTableListResponse(BaseModel):
    """RDB table list response"""
    source_id: str
    tables: List[str]


class RDBColumnInfo(BaseModel):
    """RDB column information schema"""
    name: str
    type: str
    nullable: bool
