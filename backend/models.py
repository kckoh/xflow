from datetime import datetime
from typing import Optional
from beanie import Document
from pydantic import Field


class User(Document):
    """
    User document for MongoDB.
    Beanie automatically handles the _id field as a PydanticObjectId.
    """
    email: str = Field(..., unique=True, index=True)
    password: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "users"  # Collection name in MongoDB
        indexes = [
            "email",  # Create index on email for faster lookups
        ]


class Source(Document):
    """
    Source document for storing database connection info.
    Supports both LocalStack and AWS deployment configurations.
    """
    # Basic connection info
    name: str = Field(..., index=True)
    description: Optional[str] = None
    type: str  # rdb / nosql / log / api
    host: str
    port: int
    database_name: str
    user_name: str
    password: str  # Should be encrypted in production
    status: str = "disconnected"  # connected / fail / disconnected
    
    # AWS/LocalStack deployment configuration (required)
    cloud_config: dict = Field(default_factory=dict)
    # Example structure (based on AWS Glue Connection):
    # {
    #   "vpc_id": "vpc-123abc",
    #   "subnet_id": "subnet-456def",
    #   "security_group_ids": ["sg-789ghi"],
    #   "availability_zone": "us-east-1a",
    #   "iam_role_arn": "arn:aws:iam::123456789012:role/GlueRole"
    # }
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "sources"
        indexes = ["name"]
