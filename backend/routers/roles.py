"""
Role Management Router - Admin-only endpoints for role management
"""
from datetime import datetime
from typing import List

from fastapi import APIRouter, HTTPException, status
from models import Role
from schemas.role import RoleCreate, RoleUpdate, RoleResponse
from routers.admin import check_admin

router = APIRouter()


@router.get("/roles", response_model=List[RoleResponse])
async def get_roles(session_id: str):
    """Get all roles (admin only)"""
    check_admin(session_id)
    roles = await Role.find_all().to_list()
    
    return [
        RoleResponse(
            id=str(role.id),
            name=role.name,
            description=role.description,
            is_admin=role.is_admin,
            can_manage_datasets=role.can_manage_datasets,
            can_run_query=role.can_run_query,
            dataset_permissions=role.dataset_permissions,
            all_datasets=role.all_datasets,
            created_at=role.created_at.isoformat() if role.created_at else None
        )
        for role in roles
    ]


@router.post("/roles", response_model=RoleResponse, status_code=status.HTTP_201_CREATED)
async def create_role(role_data: RoleCreate, session_id: str):
    """Create a new role (admin only)"""
    check_admin(session_id)
    
    # Check if role name already exists
    existing = await Role.find_one(Role.name == role_data.name)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role name already exists"
        )
    
    new_role = Role(
        name=role_data.name,
        description=role_data.description,
        is_admin=role_data.is_admin,
        can_manage_datasets=role_data.can_manage_datasets,
        can_run_query=role_data.can_run_query,
        dataset_permissions=role_data.dataset_permissions,
        all_datasets=role_data.all_datasets,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    
    await new_role.insert()
    
    return RoleResponse(
        id=str(new_role.id),
        name=new_role.name,
        description=new_role.description,
        is_admin=new_role.is_admin,
        can_manage_datasets=new_role.can_manage_datasets,
        can_run_query=new_role.can_run_query,
        dataset_permissions=new_role.dataset_permissions,
        all_datasets=new_role.all_datasets,
        created_at=new_role.created_at.isoformat() if new_role.created_at else None
    )


@router.put("/roles/{role_id}", response_model=RoleResponse)
async def update_role(role_id: str, role_data: RoleUpdate, session_id: str):
    """Update a role (admin only)"""
    check_admin(session_id)
    from bson import ObjectId
    
    role = await Role.get(ObjectId(role_id))
    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )
    
    update_data = role_data.model_dump(exclude_unset=True)
    
    # Check name uniqueness if name is being updated
    if "name" in update_data and update_data["name"] != role.name:
        existing = await Role.find_one(Role.name == update_data["name"])
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Role name already exists"
            )
    
    for key, value in update_data.items():
        setattr(role, key, value)
    
    role.updated_at = datetime.utcnow()
    await role.save()
    
    return RoleResponse(
        id=str(role.id),
        name=role.name,
        description=role.description,
        is_admin=role.is_admin,
        can_manage_datasets=role.can_manage_datasets,
        can_run_query=role.can_run_query,
        dataset_permissions=role.dataset_permissions,
        all_datasets=role.all_datasets,
        created_at=role.created_at.isoformat() if role.created_at else None
    )


@router.delete("/roles/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_role(role_id: str, session_id: str):
    """Delete a role (admin only)"""
    check_admin(session_id)
    from bson import ObjectId
    
    role = await Role.get(ObjectId(role_id))
    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )
    
    # Check if any users have this role
    from models import User
    users_with_role = await User.find(User.role_ids == role_id).to_list()
    if users_with_role:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot delete role: {len(users_with_role)} user(s) still assigned to this role"
        )
    
    await role.delete()
    return None


@router.get("/roles/{role_id}", response_model=RoleResponse)
async def get_role(role_id: str, session_id: str):
    """Get a specific role (admin only)"""
    check_admin(session_id)
    from bson import ObjectId
    
    role = await Role.get(ObjectId(role_id))
    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )
    
    return RoleResponse(
        id=str(role.id),
        name=role.name,
        description=role.description,
        is_admin=role.is_admin,
        can_manage_datasets=role.can_manage_datasets,
        can_run_query=role.can_run_query,
        dataset_permissions=role.dataset_permissions,
        all_datasets=role.all_datasets,
        created_at=role.created_at.isoformat() if role.created_at else None
    )
