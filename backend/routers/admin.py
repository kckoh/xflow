"""
Admin Router - Admin-only endpoints for user management
All endpoints require admin authentication
"""
from datetime import datetime
from typing import List

from fastapi import APIRouter, HTTPException, status, Depends
from models import User, Role
from schemas.user import UserCreateAdmin, UserUpdateAdmin, UserResponseAdmin, RoleCreate, RoleUpdate, RoleResponse, BulkAddDataset
from dependencies import sessions

router = APIRouter()


# Dependency to require admin access
async def require_admin(session_id: str):
    """Check if the current user is an admin"""
    if session_id not in sessions:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    
    user_session = sessions[session_id]
    if not user_session.get("is_admin", False):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    
    return user_session


@router.get("/users", response_model=List[UserResponseAdmin])
async def get_users(admin: dict = Depends(require_admin)):
    """Get all users (admin only)"""
    from bson import ObjectId

    users = await User.find_all().to_list()

    # Fetch all roles for mapping
    roles = await Role.find_all().to_list()
    role_map = {str(role.id): role.name for role in roles}

    return [
        UserResponseAdmin(
            id=str(user.id),
            email=user.email,
            name=user.name,
            is_admin=user.is_admin,
            role_id=user.role_id,
            role_name=role_map.get(user.role_id) if user.role_id else None,
            etl_access=user.etl_access,
            domain_edit_access=user.domain_edit_access,
            dataset_access=user.dataset_access,
            all_datasets=user.all_datasets,
            created_at=user.created_at.isoformat() if user.created_at else None
        )
        for user in users
    ]


@router.post("/users", response_model=UserResponseAdmin, status_code=status.HTTP_201_CREATED)
async def create_user(user_data: UserCreateAdmin, admin: dict = Depends(require_admin)):
    """Create a new user (admin only)"""
    from bson import ObjectId

    # Check if email already exists
    existing = await User.find_one(User.email == user_data.email)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Validate role_id if provided
    role_name = None
    if user_data.role_id:
        role = await Role.get(ObjectId(user_data.role_id))
        if not role:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid role_id"
            )
        role_name = role.name

    # Create new user
    new_user = User(
        email=user_data.email,
        password=user_data.password,
        name=user_data.name or user_data.email.split("@")[0],
        is_admin=user_data.is_admin,
        role_id=user_data.role_id,
        etl_access=user_data.etl_access,
        domain_edit_access=user_data.domain_edit_access,
        dataset_access=user_data.dataset_access,
        all_datasets=user_data.all_datasets,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )

    await new_user.insert()

    return UserResponseAdmin(
        id=str(new_user.id),
        email=new_user.email,
        name=new_user.name,
        is_admin=new_user.is_admin,
        role_id=new_user.role_id,
        role_name=role_name,
        etl_access=new_user.etl_access,
        domain_edit_access=new_user.domain_edit_access,
        dataset_access=new_user.dataset_access,
        all_datasets=new_user.all_datasets,
        created_at=new_user.created_at.isoformat() if new_user.created_at else None
    )


@router.put("/users/{user_id}", response_model=UserResponseAdmin)
async def update_user(user_id: str, user_data: UserUpdateAdmin, admin: dict = Depends(require_admin)):
    """Update a user (admin only)"""
    from bson import ObjectId

    # Find user
    user = await User.get(ObjectId(user_id))
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Update only provided fields
    update_data = user_data.model_dump(exclude_unset=True)

    # Check email uniqueness if email is being updated
    if "email" in update_data and update_data["email"] != user.email:
        existing = await User.find_one(User.email == update_data["email"])
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )

    # Validate role_id if provided
    if "role_id" in update_data and update_data["role_id"]:
        role = await Role.get(ObjectId(update_data["role_id"]))
        if not role:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid role_id"
            )

    # Apply updates
    for key, value in update_data.items():
        setattr(user, key, value)

    user.updated_at = datetime.utcnow()
    await user.save()

    # Get role name for response
    role_name = None
    if user.role_id:
        role = await Role.get(ObjectId(user.role_id))
        if role:
            role_name = role.name

    return UserResponseAdmin(
        id=str(user.id),
        email=user.email,
        name=user.name,
        is_admin=user.is_admin,
        role_id=user.role_id,
        role_name=role_name,
        etl_access=user.etl_access,
        domain_edit_access=user.domain_edit_access,
        dataset_access=user.dataset_access,
        all_datasets=user.all_datasets,
        created_at=user.created_at.isoformat() if user.created_at else None
    )


@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(user_id: str, admin: dict = Depends(require_admin)):
    """Delete a user (admin only)"""
    from bson import ObjectId

    # Find user
    user = await User.get(ObjectId(user_id))
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Prevent admin from deleting themselves
    if str(user.id) == admin.get("user_id"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )

    await user.delete()
    return None


# Role endpoints
@router.get("/roles", response_model=List[RoleResponse])
async def get_roles(session_id: str):
    """
    Get all roles (authenticated users only)
    Changed from admin-only to allow all authenticated users for permission assignment in wizards
    """
    # Check authentication (but not admin)
    if session_id not in sessions:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    
    roles = await Role.find_all().to_list()

    return [
        RoleResponse(
            id=str(role.id),
            name=role.name,
            description=role.description,
            dataset_etl_access=role.dataset_etl_access,
            query_ai_access=role.query_ai_access,
            dataset_access=role.dataset_access,
            all_datasets=role.all_datasets,
            created_at=role.created_at.isoformat() if role.created_at else None
        )
        for role in roles
    ]


@router.post("/roles", response_model=RoleResponse, status_code=status.HTTP_201_CREATED)
async def create_role(role_data: RoleCreate, admin: dict = Depends(require_admin)):
    """Create a new role (admin only)"""
    # Check if name already exists
    existing = await Role.find_one(Role.name == role_data.name)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role name already exists"
        )

    # Create new role
    new_role = Role(
        name=role_data.name,
        description=role_data.description,
        dataset_etl_access=role_data.dataset_etl_access,
        query_ai_access=role_data.query_ai_access,
        dataset_access=role_data.dataset_access,
        all_datasets=role_data.all_datasets,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )

    await new_role.insert()

    return RoleResponse(
        id=str(new_role.id),
        name=new_role.name,
        description=new_role.description,
        dataset_etl_access=new_role.dataset_etl_access,
        query_ai_access=new_role.query_ai_access,
        dataset_access=new_role.dataset_access,
        all_datasets=new_role.all_datasets,
        created_at=new_role.created_at.isoformat() if new_role.created_at else None
    )


@router.put("/roles/{role_id}", response_model=RoleResponse)
async def update_role(role_id: str, role_data: RoleUpdate, admin: dict = Depends(require_admin)):
    """Update a role (admin only)"""
    from bson import ObjectId

    # Find role
    role = await Role.get(ObjectId(role_id))
    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )

    # Update only provided fields
    update_data = role_data.model_dump(exclude_unset=True)

    # Check name uniqueness if name is being updated
    if "name" in update_data and update_data["name"] != role.name:
        existing = await Role.find_one(Role.name == update_data["name"])
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Role name already exists"
            )

    # Apply updates
    for key, value in update_data.items():
        setattr(role, key, value)

    role.updated_at = datetime.utcnow()
    await role.save()

    return RoleResponse(
        id=str(role.id),
        name=role.name,
        description=role.description,
        dataset_etl_access=role.dataset_etl_access,
        query_ai_access=role.query_ai_access,
        dataset_access=role.dataset_access,
        all_datasets=role.all_datasets,
        created_at=role.created_at.isoformat() if role.created_at else None
    )


@router.delete("/roles/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_role(role_id: str, admin: dict = Depends(require_admin)):
    """Delete a role (admin only)"""
    from bson import ObjectId

    # Find role
    role = await Role.get(ObjectId(role_id))
    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )

    # Check if any users are using this role
    users_with_role = await User.find(User.role_id == str(role.id)).to_list()
    if users_with_role:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot delete role: {len(users_with_role)} user(s) are assigned to this role"
        )

    await role.delete()
    return None


@router.post("/roles/{role_id}/add-dataset")
async def add_dataset_to_role(role_id: str, dataset_id: str, admin: dict = Depends(require_admin)):
    """Add a dataset to role's dataset_access list"""
    from bson import ObjectId

    # Find role
    role = await Role.get(ObjectId(role_id))
    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )

    # Add dataset_id if not already present
    if dataset_id not in role.dataset_access:
        role.dataset_access.append(dataset_id)
        role.updated_at = datetime.utcnow()
        await role.save()

    return {"message": "Dataset added to role successfully"}


@router.post("/roles/bulk-add-dataset")
async def bulk_add_dataset_to_roles(
    data: BulkAddDataset,
    admin: dict = Depends(require_admin)
):
    """
    Add a dataset to multiple roles' dataset_access lists
    Admin only
    """
    from bson import ObjectId

    updated_count = 0
    for role_id in data.role_ids:
        try:
            role = await Role.get(ObjectId(role_id))
            if role and data.dataset_id not in role.dataset_access:
                role.dataset_access.append(data.dataset_id)
                role.updated_at = datetime.utcnow()
                await role.save()
                updated_count += 1
        except Exception as e:
            print(f"Error updating role {role_id}: {e}")
            continue

    return {
        "message": f"Dataset added to {updated_count} role(s)",
        "updated_count": updated_count
    }
