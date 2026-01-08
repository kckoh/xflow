"""
Quality Check Schemas
"""

from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel


class QualityRunRequest(BaseModel):
    """Request body for running quality check"""
    s3_path: str                          # 검사할 S3 경로
    dataset_id: Optional[str] = None      # 연관된 Dataset (pipeline) ID (선택)
    null_threshold: float = 5.0           # Null 허용 범위 (5.0% 이상부터 감점)
    duplicate_threshold: float = 1.0      # 중복 허용 범위 (1.0% 이상부터 감점)


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
    dataset_id: str                        # Dataset (pipeline) ID
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
