from pydantic import BaseModel, EmailStr


class UserCreate(BaseModel):
    email: str
    password: str


class UserLogin(BaseModel):
    email: str
    password: str


class UserResponse(BaseModel):
    id: str  # MongoDB ObjectId is string
    email: str

    class Config:
        from_attributes = True
