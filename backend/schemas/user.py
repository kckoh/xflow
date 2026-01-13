from pydantic import BaseModel, EmailStr
from typing import List, Optional


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
    
    # RBAC (NEW)
    role_ids: List[str] = []  # List of Role IDs to assign
    
    # Legacy fields (for custom override)
    is_admin: bool = False
    
    # Dataset access control
    dataset_access: List[str] = []
    all_datasets: bool = False
    
    # Feature-level permissions
    can_manage_datasets: bool = False
    can_run_query: bool = True



class UserUpdateAdmin(BaseModel):
    """Schema for admin updating a user"""
    email: Optional[str] = None
    password: Optional[str] = None
    name: Optional[str] = None
    
    # RBAC (NEW)
    role_ids: Optional[List[str]] = None
    
    # Legacy fields (for custom override)
    is_admin: Optional[bool] = None
    
    # Dataset access control
    dataset_access: Optional[List[str]] = None
    all_datasets: Optional[bool] = None
    
    # Feature-level permissions
    can_manage_datasets: Optional[bool] = None
    can_run_query: Optional[bool] = None



class UserResponseAdmin(BaseModel):
    """Schema for admin user list/detail response (no password)"""
    id: str
    email: str
    name: Optional[str] = None
    
    # RBAC (NEW)
    role_ids: List[str] = []
    roles: Optional[List[dict]] = None  # Populated role objects
    
    # Legacy fields
    is_admin: bool = False
    
    # Dataset access control
    dataset_access: List[str] = []
    all_datasets: bool = False
    
    # Feature-level permissions
    can_manage_datasets: bool = False
    can_run_query: bool = True
    
    created_at: Optional[str] = None

    class Config:
        from_attributes = True

