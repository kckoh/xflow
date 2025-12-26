from bson import ObjectId
from fastapi import Depends, Header, HTTPException, status
from database import get_db
from models import User

# Move sessions here so it's accessible everywhere
sessions = {}


def get_current_user_id(session_id: str = Header(alias="X-Session-ID")) -> str:
    """
    Dependency to get current user ID from session.
    Usage: user_id: int = Depends(get_current_user_id)
    """
    if not session_id or session_id not in sessions:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    return sessions[session_id]["user_id"]


async def get_current_user(
    session_id: str = Header(alias="X-Session-ID"),
    db=Depends(get_db)
) -> User:
    """
    Dependency to get current user from session.
    Usage: current_user: User = Depends(get_current_user)
    """
    if not session_id or session_id not in sessions:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    user_id = sessions[session_id]["user_id"]
    try:
        object_id = ObjectId(user_id)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid user id"
        ) from exc

    user = await db.users.find_one({"_id": object_id})

    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    return User.model_validate(user)
