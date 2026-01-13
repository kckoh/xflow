"""
Admin Router - Admin-only endpoints for user management
All endpoints require admin authentication
"""
from datetime import datetime
from typing import List

from fastapi import APIRouter, HTTPException, status
from models import User
from schemas.user import UserCreateAdmin, UserUpdateAdmin, UserResponseAdmin
from dependencies import sessions

router = APIRouter()


# Helper function to check admin access
def check_admin(session_id: str):
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
async def get_users(session_id: str):
    """Get all users (admin only)"""
    check_admin(session_id)
    users = await User.find_all().to_list()
    
    # Populate role information
    from models import Role
    from bson import ObjectId
    
    result = []
    for user in users:
        # Fetch roles for this user
        roles = []
        if user.role_ids:
            role_object_ids = [ObjectId(rid) for rid in user.role_ids]
            role_docs = await Role.find({"_id": {"$in": role_object_ids}}).to_list()
            roles = [
                {
                    "id": str(role.id),
                    "name": role.name,
                    "description": role.description
                }
                for role in role_docs
            ]
        
        result.append(UserResponseAdmin(
            id=str(user.id),
            email=user.email,
            name=user.name,
            role_ids=user.role_ids,
            roles=roles,
            is_admin=user.is_admin,
            dataset_access=user.dataset_access,
            all_datasets=user.all_datasets,
            can_manage_datasets=user.can_manage_datasets,
            can_run_query=user.can_run_query,
            created_at=user.created_at.isoformat() if user.created_at else None
        ))
    
    return result


@router.post("/users", response_model=UserResponseAdmin, status_code=status.HTTP_201_CREATED)
async def create_user(user_data: UserCreateAdmin, session_id: str):
    """Create a new user (admin only)"""
    check_admin(session_id)
    # Check if email already exists
    existing = await User.find_one(User.email == user_data.email)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    # Create new user
    new_user = User(
        email=user_data.email,
        password=user_data.password,
        name=user_data.name or user_data.email.split("@")[0],
        role_ids=user_data.role_ids,
        is_admin=user_data.is_admin,
        dataset_access=user_data.dataset_access,
        all_datasets=user_data.all_datasets,
        can_manage_datasets=user_data.can_manage_datasets,
        can_run_query=user_data.can_run_query,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    
    await new_user.insert()
    
    return UserResponseAdmin(
        id=str(new_user.id),
        email=new_user.email,
        name=new_user.name,
        role_ids=new_user.role_ids,
        roles=None,  # Not populated in creation response
        is_admin=new_user.is_admin,
        dataset_access=new_user.dataset_access,
        all_datasets=new_user.all_datasets,
        can_manage_datasets=new_user.can_manage_datasets,
        can_run_query=new_user.can_run_query,
        created_at=new_user.created_at.isoformat() if new_user.created_at else None
    )


@router.put("/users/{user_id}", response_model=UserResponseAdmin)
async def update_user(user_id: str, user_data: UserUpdateAdmin, session_id: str):
    """Update a user (admin only)"""
    check_admin(session_id)
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
    
    # Apply updates
    for key, value in update_data.items():
        setattr(user, key, value)
    
    user.updated_at = datetime.utcnow()
    await user.save()
    
    return UserResponseAdmin(
        id=str(user.id),
        email=user.email,
        name=user.name,
        role_ids=user.role_ids,
        roles=None,  # Not populated in update response
        is_admin=user.is_admin,
        dataset_access=user.dataset_access,
        all_datasets=user.all_datasets,
        can_manage_datasets=user.can_manage_datasets,
        can_run_query=user.can_run_query,
        created_at=user.created_at.isoformat() if user.created_at else None
    )



@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(user_id: str, session_id: str):
    """Delete a user (admin only)"""
    admin = check_admin(session_id)
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


@router.get("/users/public")
async def get_public_users(session_id: str):
    """Get minimal user list for dataset sharing (non-admin accessible)"""
    # Check authentication only (not admin)
    if session_id not in sessions:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    
    users = await User.find_all().to_list()
    
    # Return only minimal public information
    return [
        {
            "id": str(user.id),
            "name": user.name or user.email.split("@")[0],
            "email": user.email
        }
        for user in users
    ]
