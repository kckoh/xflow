from fastapi import HTTPException, Header, Query
from models import User
from typing import Optional, Dict, Any
from utils.redis_client import redis_client

# Fallback in-memory sessions (used if Redis unavailable)
sessions = {}

# Session config
SESSION_PREFIX = "session:"
SESSION_EXPIRY = 86400  # 24 hours


async def get_session_data(session_id: str) -> Optional[Dict[str, Any]]:
    """Get session data from Redis, fallback to memory"""
    try:
        data = await redis_client.get_json(f"{SESSION_PREFIX}{session_id}")
        if data:
            return data
    except Exception:
        pass
    # Fallback to in-memory
    return sessions.get(session_id)


async def set_session_data(session_id: str, data: Dict[str, Any]) -> bool:
    """Set session data in Redis and memory"""
    sessions[session_id] = data  # Always keep in memory as fallback
    try:
        return await redis_client.set_json(
            f"{SESSION_PREFIX}{session_id}", data, ex=SESSION_EXPIRY
        )
    except Exception:
        return True  # Memory fallback succeeded


async def delete_session_data(session_id: str) -> bool:
    """Delete session from Redis and memory"""
    sessions.pop(session_id, None)
    try:
        await redis_client.delete(f"{SESSION_PREFIX}{session_id}")
    except Exception:
        pass
    return True


async def get_user_session(
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
    if session_id:
        return await get_session_data(session_id)
    return None


async def get_current_user_id(session_id: str = Header(alias="X-Session-ID")) -> str:
    """
    Dependency to get current user ID from session.
    Usage: user_id: str = Depends(get_current_user_id)
    """
    session_data = await get_session_data(session_id)
    if not session_id or not session_data:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return session_data["user_id"]


async def get_current_user(
    session_id: str = Header(alias="X-Session-ID")
) -> User:
    """
    Dependency to get current user from session.
    Usage: current_user: User = Depends(get_current_user)
    """
    session_data = await get_session_data(session_id)
    if not session_id or not session_data:
        raise HTTPException(status_code=401, detail="Not authenticated")

    user_id = session_data["user_id"]

    # Beanie query - find by id (async)
    user = await User.get(user_id)

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return user
