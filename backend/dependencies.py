from fastapi import HTTPException, Header, Query
from models import User
from typing import Optional, Dict, Any

# Move sessions here so it's accessible everywhere
sessions = {}


def get_user_session(
    session_id: Optional[str] = Query(None, description="Session ID for authentication")
) -> Optional[Dict[str, Any]]:
    """
    FastAPI Dependency to get user session from session_id query parameter.
    Returns None if session_id is not provided or invalid.
    
    Usage:
        @router.get("/")
        async def endpoint(user_session: Optional[Dict] = Depends(get_user_session)):
            if user_session:
                is_admin = user_session.get("is_admin", False)
    """
    if session_id and session_id in sessions:
        return sessions[session_id]
    return None



def get_current_user_id(session_id: str = Header(alias="X-Session-ID")) -> str:
    """
    Dependency to get current user ID from session.
    Usage: user_id: str = Depends(get_current_user_id)
    """
    if not session_id or session_id not in sessions:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return sessions[session_id]["user_id"]


async def get_current_user(
    session_id: str = Header(alias="X-Session-ID")
) -> User:
    """
    Dependency to get current user from session.
    Usage: current_user: User = Depends(get_current_user)
    """
    if not session_id or session_id not in sessions:
        raise HTTPException(status_code=401, detail="Not authenticated")

    user_id = sessions[session_id]["user_id"]

    # Beanie query - find by id (async)
    user = await User.get(user_id)

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return user
