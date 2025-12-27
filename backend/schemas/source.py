from typing import Optional
from pydantic import BaseModel


class SourceCreate(BaseModel):
    name: str
    description: Optional[str] = None
    type: str  # rdb / nosql / log / api
    host: str
    port: int
    database_name: str
    user_name: str
    password: str
    cloud_config: dict = {}


class SourceUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    host: Optional[str] = None
    cloud_config: dict = {}
    port: Optional[int] = None
    database_name: Optional[str] = None
    user_name: Optional[str] = None
    password: Optional[str] = None


class SourceResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    type: str
    host: str
    port: int
    database_name: str
    user_name: str
    status: str
    cloud_config: dict

    class Config:
        from_attributes = True
