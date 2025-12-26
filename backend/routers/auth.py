import uuid

from backend.database import get_db
from backend.dependencies import sessions
from backend.models import User
from backend.schemas.user import UserCreate, UserLogin
from fastapi import APIRouter, Depends, Header, HTTPException, status

router = APIRouter()


@router.post("/signup", status_code=status.HTTP_201_CREATED)
async def signup(user: UserCreate, db=Depends(get_db)):
    existing_user = await db.users.find_one({"email": user.email})
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered"
        )

    # Create new user
    new_user = User(email=user.email, password=user.password)

    # Add to database
    insert_result = await db.users.insert_one(
        new_user.model_dump(by_alias=True, exclude_unset=True)
    )

    # return status code
    return {
        "id": str(insert_result.inserted_id),
        "email": new_user.email,
        "message": "User created successfully",
    }


@router.post("/login")
async def login(user: UserLogin, db=Depends(get_db)):
    db_user = await db.users.find_one(
        {"email": user.email, "password": user.password}
    )

    if not db_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    session_id = str(uuid.uuid4())
    sessions[session_id] = {
        "user_id": str(db_user["_id"]),
        "email": db_user["email"],
    }
    return {"session_id": session_id}


@router.get("/me")
def get_current_user(session_id: str):
    if session_id not in sessions:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return sessions[session_id]


@router.post("/logout")
def logout(session_id: str = Header(alias="X-Session-ID")):
    if session_id in sessions:
        del sessions[session_id]
    return {"message": "Logged out successfully"}
