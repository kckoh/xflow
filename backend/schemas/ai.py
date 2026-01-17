"""
AI Query Assistant 관련 스키마
Text-to-SQL 기능을 위한 Request/Response 모델
"""
from pydantic import BaseModel, Field
from typing import Optional, List


class GenerateSQLRequest(BaseModel):
    """
    SQL 생성 요청 스키마
    """
    question: str = Field(
        description="자연어 질문 (예: '매출 top 10 고객 보여줘')"
    )
    prompt_type: str = Field(
        default='query_page',
        description="프롬프트 타입 (query_page, field_transform, sql_transform, partition)"
    )
    metadata: Optional[dict] = Field(
        default=None,
        description="컨텍스트별 메타데이터 (컬럼 정보, 소스 정보 등)"
    )
    context: Optional[str] = Field(
        default=None,
        description="추가 컨텍스트 정보 (선택)"
    )
    engine: Optional[str] = Field(
        default='trino',
        description="쿼리 엔진 (duckdb | trino)"
    )


class GenerateSQLResponse(BaseModel):
    """
    SQL 생성 응답 스키마
    """
    sql: str = Field(description="생성된 SQL 쿼리")
    schema_context: str = Field(description="사용된 스키마 정보")


class SchemaSearchResult(BaseModel):
    """
    스키마 검색 결과 항목
    """
    name: str = Field(description="테이블/데이터셋 이름")
    doc_type: str = Field(description="문서 타입 (domain/etl_job)")
    description: Optional[str] = Field(default=None, description="설명")
    columns: List[str] = Field(default_factory=list, description="컬럼 목록")
    score: float = Field(description="검색 점수")


class SchemaSearchResponse(BaseModel):
    """
    스키마 검색 응답 스키마
    """
    total: int = Field(description="총 검색 결과 수")
    results: List[SchemaSearchResult] = Field(description="검색 결과 목록")
    context: str = Field(description="LLM용 컨텍스트 문자열")
