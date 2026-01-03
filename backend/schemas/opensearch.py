"""
Search 관련 스키마
OpenSearch 인덱싱 및 검색용
"""
from pydantic import BaseModel, Field
from typing import Optional, Literal, List
from datetime import datetime


class DomainDocument(BaseModel):
    """
    OpenSearch에 저장되는 Domain/ETL Job 문서 스키마
    """
    doc_id: str = Field(description="MongoDB 문서 ID")
    doc_type: Literal['domain', 'etl_job'] = Field(description="문서 타입")
    name: str = Field(description="이름")
    description: Optional[str] = Field(default=None, description="설명")
    type: Optional[str] = Field(default=None, description="타입 (domain의 경우)")
    owner: Optional[str] = Field(default=None, description="소유자")
    tags: Optional[List[str]] = Field(default=None, description="태그 목록")
    status: Optional[str] = Field(default=None, description="상태 (etl_job의 경우)")
    # 노드/컬럼 메타데이터
    node_descriptions: Optional[List[str]] = Field(default=None, description="노드 설명 목록")
    node_tags: Optional[List[str]] = Field(default=None, description="노드 태그 목록")
    column_names: Optional[List[str]] = Field(default=None, description="컬럼명 목록")
    column_descriptions: Optional[List[str]] = Field(default=None, description="컬럼 설명 목록")
    column_tags: Optional[List[str]] = Field(default=None, description="컬럼 태그 목록")
    # 타임스탬프
    created_at: Optional[datetime] = Field(default=None, description="생성 시간")
    updated_at: Optional[datetime] = Field(default=None, description="수정 시간")
    last_indexed: Optional[datetime] = Field(default=None, description="마지막 인덱싱 시간")


class IndexingResult(BaseModel):
    """
    인덱싱 결과 스키마
    """
    domains: int = Field(default=0, description="Domain 인덱싱 문서 수")
    etl_jobs: int = Field(default=0, description="ETL Job 인덱싱 문서 수")
    total: int = Field(description="총 인덱싱 문서 수")


class ReindexRequest(BaseModel):
    """
    재인덱싱 요청 스키마
    """
    delete_existing: bool = Field(
        default=True,
        description="기존 인덱스 삭제 여부"
    )


class ReindexResult(BaseModel):
    """
    재인덱싱 결과 스키마
    """
    success: bool = Field(description="성공 여부")
    domains_indexed: int = Field(default=0, description="Domain 인덱싱 수")
    etl_jobs_indexed: int = Field(default=0, description="ETL Job 인덱싱 수")
    total: int = Field(description="총 인덱싱 수")
    message: Optional[str] = Field(default=None, description="메시지")


class DomainSearchResult(BaseModel):
    """
    Domain/ETL Job 검색 결과 스키마
    """
    total: int = Field(description="총 검색 결과 수")
    results: List[DomainDocument] = Field(description="검색 결과 문서들")


class StatusResponse(BaseModel):
    """
    OpenSearch 상태 응답 스키마
    """
    status: Literal['healthy', 'degraded', 'unhealthy'] = Field(
        description="전체 상태"
    )
    opensearch_connected: bool = Field(
        description="OpenSearch 연결 상태"
    )
    domain_index_exists: bool = Field(
        default=False,
        description="Domain 인덱스 존재 여부"
    )
    total_documents: int = Field(
        description="총 문서 수"
    )
    domain_documents: int = Field(
        default=0,
        description="Domain 문서 수"
    )
    etl_job_documents: int = Field(
        default=0,
        description="ETL Job 문서 수"
    )
