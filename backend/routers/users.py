from fastapi import APIRouter, HTTPException
from models import User
from schemas.user import UserResponse

router = APIRouter()


@router.get("/", response_model=list[UserResponse])
async def get_users():
    """Get all users"""
    users = []
    async for user in User.find():
        users.append(user)
    return users


@router.get("/{user_id}", response_model=UserResponse)
async def get_user(user_id: str):  # Changed from int to str
    """Get a single user by ID"""
    user = await User.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user