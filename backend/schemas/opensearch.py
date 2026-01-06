"""
Search 관련 스키마
OpenSearch 인덱싱 및 검색용
"""
from pydantic import BaseModel, Field
from typing import Optional, Literal, List
from datetime import datetime


class DatasetDocument(BaseModel):
    """
    OpenSearch에 저장되는 Dataset 문서 스키마
    """
    doc_id: str = Field(description="MongoDB 문서 ID")
    doc_type: Literal['dataset'] = Field(description="문서 타입")
    name: str = Field(description="이름")
    description: Optional[str] = Field(default=None, description="설명")
    status: Optional[str] = Field(default=None, description="상태")
    s3_path: Optional[str] = Field(default=None, description="S3 경로")
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


# Backward compatibility alias
DomainDocument = DatasetDocument


class IndexingResult(BaseModel):
    """
    인덱싱 결과 스키마
    """
    datasets: int = Field(default=0, description="Dataset 인덱싱 문서 수")
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
    datasets_indexed: int = Field(default=0, description="Dataset 인덱싱 수")
    total: int = Field(description="총 인덱싱 수")
    message: Optional[str] = Field(default=None, description="메시지")


class DatasetSearchResult(BaseModel):
    """
    Dataset 검색 결과 스키마
    """
    total: int = Field(description="총 검색 결과 수")
    results: List[DatasetDocument] = Field(description="검색 결과 문서들")


# Backward compatibility alias
DomainSearchResult = DatasetSearchResult


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
        description="인덱스 존재 여부"
    )
    total_documents: int = Field(
        description="총 문서 수"
    )
    dataset_documents: int = Field(
        default=0,
        description="Dataset 문서 수"
    )
