"""
Quality Check Schemas
"""

from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel


class QualityRunRequest(BaseModel):
    """Request body for running quality check"""
    s3_path: str                          # S3 path to check
    job_id: Optional[str] = None          # Optional ETL Job ID
    null_threshold: float = 5.0           # Max null % per column
    duplicate_threshold: float = 1.0      # Max duplicate %


class QualityCheckResponse(BaseModel):
    """Individual check result"""
    name: str
    column: Optional[str]
    passed: bool
    value: float
    threshold: float
    message: Optional[str]


class QualityResultResponse(BaseModel):
    """Quality result response"""
    id: str
    dataset_id: str
    job_id: Optional[str]
    s3_path: str
    row_count: int
    column_count: int
    null_counts: dict
    duplicate_count: int
    overall_score: float
    checks: List[QualityCheckResponse]
    status: str
    error_message: Optional[str]
    run_at: datetime
    completed_at: Optional[datetime]
    duration_ms: int
