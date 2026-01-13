from pydantic import BaseModel
from typing import List, Optional


class RoleCreate(BaseModel):
    """Schema for creating a new role"""
    name: str
    description: Optional[str] = None
    is_admin: bool = False
    can_manage_datasets: bool = False
    can_run_query: bool = True
    dataset_permissions: List[str] = []
    all_datasets: bool = False


class RoleUpdate(BaseModel):
    """Schema for updating a role"""
    name: Optional[str] = None
    description: Optional[str] = None
    is_admin: Optional[bool] = None
    can_manage_datasets: Optional[bool] = None
    can_run_query: Optional[bool] = None
    dataset_permissions: Optional[List[str]] = None
    all_datasets: Optional[bool] = None


class RoleResponse(BaseModel):
    """Schema for role response"""
    id: str
    name: str
    description: Optional[str] = None
    is_admin: bool = False
    can_manage_datasets: bool = False
    can_run_query: bool = True
    dataset_permissions: List[str] = []
    all_datasets: bool = False
    created_at: Optional[str] = None

    class Config:
        from_attributes = True
