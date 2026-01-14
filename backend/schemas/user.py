from pydantic import BaseModel, EmailStr
from typing import List, Optional


# Role schemas
class RoleCreate(BaseModel):
    """Schema for creating a new role"""
    name: str
    description: Optional[str] = None
    dataset_etl_access: bool = False
    query_ai_access: bool = False
    dataset_access: List[str] = []
    all_datasets: bool = False


class RoleUpdate(BaseModel):
    """Schema for updating a role"""
    name: Optional[str] = None
    description: Optional[str] = None
    dataset_etl_access: Optional[bool] = None
    query_ai_access: Optional[bool] = None
    dataset_access: Optional[List[str]] = None
    all_datasets: Optional[bool] = None


class RoleResponse(BaseModel):
    """Schema for role response"""
    id: str
    name: str
    description: Optional[str] = None
    dataset_etl_access: bool = False
    query_ai_access: bool = False
    dataset_access: List[str] = []
    all_datasets: bool = False
    created_at: Optional[str] = None

    class Config:
        from_attributes = True


class UserCreate(BaseModel):
    email: str
    password: str


class UserLogin(BaseModel):
    email: str
    password: str


class UserResponse(BaseModel):
    id: str  # MongoDB ObjectId is string
    email: str

    class Config:
        from_attributes = True


# Admin schemas
class UserCreateAdmin(BaseModel):
    """Schema for admin creating a new user"""
    email: str
    password: str
    name: Optional[str] = None
    is_admin: bool = False
    role_id: Optional[str] = None
    etl_access: bool = False
    domain_edit_access: bool = False
    dataset_access: List[str] = []
    all_datasets: bool = False


class UserUpdateAdmin(BaseModel):
    """Schema for admin updating a user"""
    email: Optional[str] = None
    password: Optional[str] = None  # Optional - only update if provided
    name: Optional[str] = None
    is_admin: Optional[bool] = None
    role_id: Optional[str] = None
    etl_access: Optional[bool] = None
    domain_edit_access: Optional[bool] = None
    dataset_access: Optional[List[str]] = None
    all_datasets: Optional[bool] = None


class UserResponseAdmin(BaseModel):
    """Schema for admin user list/detail response (no password)"""
    id: str
    email: str
    name: Optional[str] = None
    is_admin: bool = False
    role_id: Optional[str] = None
    role_name: Optional[str] = None  # For display purposes
    etl_access: bool = False
    domain_edit_access: bool = False
    dataset_access: List[str] = []
    all_datasets: bool = False
    created_at: Optional[str] = None

    class Config:
        from_attributes = True
