import uuid

from backend.database import get_db
from backend.dependencies import sessions
from fastapi import APIRouter, Depends, Header, HTTPException, status
from backend.models import User
from backend.schemas.user import UserCreate, UserLogin
from sqlalchemy.orm import Session

router = APIRouter()


@router.post("/signup", status_code=status.HTTP_201_CREATED)
def signup(user: UserCreate, db: Session = Depends(get_db)):
    existing_user = db.query(User).filter(User.email == user.email).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered"
        )

    # Create new user
    new_user = User(email=user.email, password=user.password)

    # Add to database
    db.add(new_user)
    db.commit()
    db.refresh(new_user)  # Get the ID that was auto-generated

    # return status code
    return {
        "id": new_user.id,
        "email": new_user.email,
        "message": "User created successfully",
    }


@router.post("/login")
def login(user: UserLogin, db: Session = Depends(get_db)):
    db_user = (
        db.query(User)
        .filter(User.email == user.email, User.password == user.password)
        .first()
    )

    if not db_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    session_id = str(uuid.uuid4())
    sessions[session_id] = {"user_id": db_user.id, "email": db_user.email}
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
