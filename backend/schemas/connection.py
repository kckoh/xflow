from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime

class ConnectionCreate(BaseModel):
    name: str
    description: Optional[str] = None
    type: str  # postgres, mysql, s3, mongodb, etc.
    config: Dict[str, Any]  # Flexible configuration for any connection type

class ConnectionUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None

class ConnectionResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    type: str
    config: Dict[str, Any]
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
