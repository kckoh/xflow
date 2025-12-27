"""
AWS Glue Catalog Response Schemas
데이터베이스, 테이블 메타데이터 응답 스키마
"""
from typing import List, Optional
from pydantic import BaseModel, Field


class DatabaseSchema(BaseModel):
    """Glue 데이터베이스 스키마"""
    name: str = Field(..., description="데이터베이스 이름")
    description: str = Field(default="", description="데이터베이스 설명")
    location_uri: str = Field(default="", description="S3 위치")
    create_time: Optional[str] = Field(None, description="생성 시간 (ISO format)")


class DatabaseListResponse(BaseModel):
    """데이터베이스 목록 응답"""
    databases: List[DatabaseSchema]


class TableColumn(BaseModel):
    """테이블 컬럼 정보"""
    name: str = Field(..., description="컬럼 이름")
    type: str = Field(..., description="데이터 타입 (예: string, bigint, double)")
    comment: str = Field(default="", description="컬럼 설명")


class PartitionKey(BaseModel):
    """파티션 키 정보"""
    name: str = Field(..., description="파티션 키 이름")
    type: str = Field(..., description="데이터 타입")
    comment: str = Field(default="", description="파티션 키 설명")


class TableListItem(BaseModel):
    """테이블 목록 아이템"""
    name: str = Field(..., description="테이블 이름")
    database_name: str = Field(..., description="데이터베이스 이름")
    create_time: Optional[str] = Field(None, description="생성 시간")
    update_time: Optional[str] = Field(None, description="수정 시간")
    table_type: str = Field(default="", description="테이블 타입 (예: EXTERNAL_TABLE)")
    storage_location: str = Field(default="", description="S3 저장 위치")
    column_count: int = Field(..., description="컬럼 개수")


class TableListResponse(BaseModel):
    """테이블 목록 응답"""
    tables: List[TableListItem]


class TableSchema(BaseModel):
    """테이블 스키마 상세 정보"""
    name: str = Field(..., description="테이블 이름")
    database_name: str = Field(..., description="데이터베이스 이름")
    table_type: str = Field(default="", description="테이블 타입")
    create_time: Optional[str] = Field(None, description="생성 시간")
    update_time: Optional[str] = Field(None, description="수정 시간")
    storage_location: str = Field(default="", description="S3 저장 위치")
    input_format: str = Field(default="", description="입력 포맷")
    output_format: str = Field(default="", description="출력 포맷")
    columns: List[TableColumn] = Field(..., description="컬럼 목록")
    partition_keys: List[PartitionKey] = Field(default_factory=list, description="파티션 키 목록")


class SyncS3Response(BaseModel):
    """S3 동기화 응답"""
    message: str = Field(..., description="응답 메시지")
    crawlers_started: List[str] = Field(..., description="시작된 크롤러 목록")
    total_crawlers: int = Field(..., description="총 크롤러 개수")
