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
    
    return [
        UserResponseAdmin(
            id=str(user.id),
            email=user.email,
            name=user.name,
            is_admin=user.is_admin,
            etl_access=user.etl_access,
            domain_edit_access=user.domain_edit_access,
            dataset_access=user.dataset_access,
            all_datasets=user.all_datasets,
            created_at=user.created_at.isoformat() if user.created_at else None
        )
        for user in users
    ]


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
        is_admin=user_data.is_admin,
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
        etl_access=new_user.etl_access,
        domain_edit_access=new_user.domain_edit_access,
        dataset_access=new_user.dataset_access,
        all_datasets=new_user.all_datasets,
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
        is_admin=user.is_admin,
        etl_access=user.etl_access,
        domain_edit_access=user.domain_edit_access,
        dataset_access=user.dataset_access,
        all_datasets=user.all_datasets,
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
