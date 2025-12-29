"""
Search 관련 스키마
OpenSearch 인덱싱 및 검색용
"""
from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime


class CatalogDocument(BaseModel):
    """
    OpenSearch에 저장되는 Catalog 문서 스키마
    """
    source: Literal['s3', 'mongodb'] = Field(
        description="데이터 소스"
    )
    source_type: str = Field(
        description="리소스 타입 (table, collection)"
    )
    database: str = Field(
        description="데이터베이스 이름"
    )
    resource_name: str = Field(
        description="테이블/컬렉션/노드 이름"
    )
    field_name: str = Field(
        description="필드/컬럼 이름"
    )
    field_type: str = Field(
        description="필드 타입"
    )
    description: Optional[str] = Field(
        default=None,
        description="설명"
    )
    location: Optional[str] = Field(
        default=None,
        description="S3 경로 등"
    )
    tag: Optional[list[str]] = Field(
        default=None,
        description="태그 목록"
    )
    owner: Optional[str] = Field(
        default=None,
        description="소유자"
    )
    domain: Optional[str] = Field(
        default=None,
        description="비즈니스 도메인"
    )
    last_indexed: datetime = Field(
        description="마지막 인덱싱 시간"
    )


class IndexingResult(BaseModel):
    """
    인덱싱 결과 스키마
    """
    s3: int = Field(description="S3 인덱싱 문서 수")
    mongodb: int = Field(description="MongoDB 인덱싱 문서 수")
    total: int = Field(description="총 인덱싱 문서 수")


class SearchQuery(BaseModel):
    """
    검색 쿼리 스키마
    """
    q: str = Field(
        description="검색어",
        min_length=1
    )
    source: Optional[Literal['s3', 'mongodb']] = Field(
        default=None,
        description="특정 소스만 검색"
    )
    limit: int = Field(
        default=20,
        ge=1,
        le=100,
        description="결과 개수 제한"
    )


class SearchResult(BaseModel):
    """
    검색 결과 스키마
    """
    total: int = Field(description="총 검색 결과 수")
    results: list[CatalogDocument] = Field(description="검색 결과 문서들")


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
    index_exists: bool = Field(
        description="인덱스 존재 여부"
    )
    total_documents: int = Field(
        description="총 문서 수"
    )
    s3_documents: int = Field(
        description="S3 소스 문서 수"
    )
    mongodb_documents: int = Field(
        description="MongoDB 소스 문서 수"
    )
