"""
AWS Athena Query Response Schemas
쿼리 실행, 상태, 결과 응답 스키마
"""
from typing import List, Optional
from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    """Athena 쿼리 실행 요청"""
    query: str = Field(..., description="SQL query to execute")


class QueryExecutionResponse(BaseModel):
    """Athena 쿼리 실행 응답"""
    query_execution_id: str = Field(..., description="쿼리 실행 ID")
    query: str = Field(..., description="실행된 SQL 쿼리")


class QueryStatusResponse(BaseModel):
    """Athena 쿼리 상태 응답"""
    query_execution_id: str = Field(..., description="쿼리 실행 ID")
    state: str = Field(..., description="쿼리 상태 (QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED)")
    state_change_reason: str = Field(default="", description="상태 변경 이유")
    submission_date_time: Optional[str] = Field(None, description="쿼리 제출 시간")
    completion_date_time: Optional[str] = Field(None, description="쿼리 완료 시간")


class QueryResultsResponse(BaseModel):
    """Athena 쿼리 결과 응답"""
    columns: List[str] = Field(..., description="컬럼 이름 목록")
    data: List[dict] = Field(..., description="결과 데이터 (각 행은 딕셔너리)")
    row_count: int = Field(..., description="결과 행 개수")
