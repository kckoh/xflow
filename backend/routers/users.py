from bson import ObjectId
from backend.database import get_db
from fastapi import APIRouter, Depends, HTTPException, status
from backend.schemas.user import UserResponse

router = APIRouter()


@router.get("/", response_model=list[UserResponse])
async def get_users(db=Depends(get_db)):
    users = []
    async for user in db.users.find({}):
        users.append(UserResponse(id=str(user["_id"]), email=user["email"]))
    return users


@router.get("/{user_id}", response_model=UserResponse)
async def get_user(user_id: str, db=Depends(get_db)):
    try:
        object_id = ObjectId(user_id)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid user id"
        ) from exc

    user = await db.users.find_one({"_id": object_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return UserResponse(id=str(user["_id"]), email=user["email"])
