import uuid
from datetime import datetime
from bson import ObjectId

from dependencies import set_session_data, get_session_data, delete_session_data
from fastapi import APIRouter, Header, HTTPException, status
from models import User, Role
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

    # Fetch role information if user has a role
    role_permissions = {
        "dataset_etl_access": False,
        "query_ai_access": False,
        "dataset_access": [],
        "all_datasets": False,
    }

    if db_user.role_id and not is_admin:
        try:
            role = await Role.get(ObjectId(db_user.role_id))
            if role:
                role_permissions = {
                    "dataset_etl_access": role.dataset_etl_access,
                    "query_ai_access": role.query_ai_access,
                    "dataset_access": role.dataset_access,
                    "all_datasets": role.all_datasets,
                }
        except Exception as e:
            print(f"Error fetching role: {e}")

    session_id = str(uuid.uuid4())
    session_data = {
        "user_id": str(db_user.id),
        "email": db_user.email,
        "name": db_user.name or db_user.email.split("@")[0],
        "is_admin": is_admin,
        "role_id": db_user.role_id,
        # User-level permissions (backward compatibility)
        "etl_access": db_user.etl_access or role_permissions["dataset_etl_access"],
        "domain_edit_access": db_user.domain_edit_access,
        "dataset_access": db_user.dataset_access if db_user.dataset_access else role_permissions["dataset_access"],
        "all_datasets": db_user.all_datasets or role_permissions["all_datasets"],
        # Role-level permissions
        "role_dataset_etl_access": role_permissions["dataset_etl_access"],
        "role_query_ai_access": role_permissions["query_ai_access"],
    }

    # Store session in Redis
    await set_session_data(session_id, session_data)

    return {
        "session_id": session_id,
        "user": session_data
    }


@router.get("/me")
async def get_current_user(session_id: str):
    """Get current user from session"""
    session_data = await get_session_data(session_id)
    if not session_data:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return session_data


@router.post("/logout")
async def logout(session_id: str = Header(alias="X-Session-ID")):
    """Logout and destroy session"""
    await delete_session_data(session_id)
    return {"message": "Logged out successfully"}
