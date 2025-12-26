from datetime import datetime
from typing import Optional, Dict
from pydantic import BaseModel, Field
from bson import ObjectId


class PyObjectId(ObjectId):
    """Custom ObjectId type for Pydantic"""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")


class User(BaseModel):
    """User model for MongoDB"""

    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    email: str = Field(..., index=True)
    password: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class FileProcessingHistory(BaseModel):
    """File processing history model for MongoDB"""

    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    object_path: str = Field(..., index=True)
    table_name: str = Field(..., index=True)
    ddl_statement: str
    partition_values: Optional[Dict] = None
    num_columns: Optional[int] = None
    num_rows: Optional[int] = None
    status: str = Field(..., index=True)  # 'success' or 'failed'
    error_message: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
