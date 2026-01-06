from typing import Optional
from pydantic import BaseModel
from datetime import datetime


class JobRunResponse(BaseModel):
    id: str
    dataset_id: str
    dataset_name: Optional[str] = None
    status: str  # pending, running, success, failed
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    airflow_run_id: Optional[str] = None

    class Config:
        from_attributes = True


class JobRunListResponse(BaseModel):
    id: str
    dataset_id: str
    dataset_name: Optional[str] = None
    status: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    class Config:
        from_attributes = True
