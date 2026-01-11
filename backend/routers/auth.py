import uuid
from datetime import datetime

from dependencies import sessions
from fastapi import APIRouter, Header, HTTPException, status
from models import User
from schemas.user import UserCreate, UserLogin

router = APIRouter()


@router.post("/signup", status_code=status.HTTP_201_CREATED)
async def signup(user: UserCreate):
    """Create a new user account"""
    # Check if user exists (Beanie query)
    existing_user = await User.find_one(User.email == user.email)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Create new user
    new_user = User(
        email=user.email,
        password=user.password,
        name=user.email.split("@")[0],  # Default name from email
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )

    # Save to database (async)
    await new_user.insert()

    # Return response
    return {
        "id": str(new_user.id),  # Convert ObjectId to string
        "email": new_user.email,
        "message": "User created successfully",
    }


@router.post("/login")
async def login(user: UserLogin):
    """Login with email and password"""
    # Find user by email and password (Beanie query)
    db_user = await User.find_one(
        User.email == user.email,
        User.password == user.password
    )

    if not db_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    # Create session with full user info
    # Special admin email check
    is_admin = db_user.is_admin or db_user.email == "admin@xflows.net"
    
    session_id = str(uuid.uuid4())
    sessions[session_id] = {
        "user_id": str(db_user.id),
        "email": db_user.email,
        "name": db_user.name or db_user.email.split("@")[0],
        "is_admin": is_admin,
        "etl_access": db_user.etl_access,
        "domain_edit_access": db_user.domain_edit_access,
        "dataset_access": db_user.dataset_access,
        "all_datasets": db_user.all_datasets,
        "can_manage_datasets": db_user.can_manage_datasets,
        "can_run_query": db_user.can_run_query,
    }
    
    return {
        "session_id": session_id,
        "user": sessions[session_id]
    }


@router.get("/me")
async def get_current_user(session_id: str):
    """Get current user from session"""
    if session_id not in sessions:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return sessions[session_id]


@router.post("/logout")
def logout(session_id: str = Header(alias="X-Session-ID")):
    """Logout and destroy session"""
    if session_id in sessions:
        del sessions[session_id]
    return {"message": "Logged out successfully"}
