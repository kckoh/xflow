from fastapi import HTTPException, Header
from sqlalchemy.orm import Session
from backend.database import get_db
from backend.models import User
from fastapi import Depends

# Move sessions here so it's accessible everywhere
sessions = {}


def get_current_user_id(session_id: str = Header(alias="X-Session-ID")) -> int:
    """
    Dependency to get current user ID from session.
    Usage: user_id: int = Depends(get_current_user_id)
    """
    if not session_id or session_id not in sessions:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return sessions[session_id]["user_id"]


def get_current_user(
    session_id: str = Header(alias="X-Session-ID"),
    db: Session = Depends(get_db)
) -> User:
    """
    Dependency to get current user from session.
    Usage: current_user: User = Depends(get_current_user)
    """
    if not session_id or session_id not in sessions:
        raise HTTPException(status_code=401, detail="Not authenticated")

    user_id = sessions[session_id]["user_id"]
    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return user
