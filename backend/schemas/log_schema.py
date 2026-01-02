from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class ApacheLogSchema(BaseModel):
    """
    Apache Combined Log Format 스키마

    형식:
    127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08"

    필드:
    1. client_ip: 클라이언트 IP 주소
    2. timestamp: 요청 시각
    3. http_method: HTTP 메서드 (GET, POST, PUT, DELETE 등)
    4. path: 요청 경로
    5. http_version: HTTP 버전
    6. status_code: HTTP 상태 코드
    7. bytes_sent: 응답 크기 (bytes)
    8. referrer: Referer 헤더
    9. user_agent: User-Agent 헤더
    """
    client_ip: str = Field(..., description="클라이언트 IP 주소")
    timestamp: str = Field(..., description="요청 시각 (Apache 형식)")
    http_method: str = Field(..., description="HTTP 메서드")
    path: str = Field(..., description="요청 경로")
    http_version: str = Field(..., description="HTTP 버전")
    status_code: int = Field(..., description="HTTP 상태 코드")
    bytes_sent: int = Field(0, description="응답 크기 (bytes)")
    referrer: Optional[str] = Field(None, description="Referer 헤더")
    user_agent: Optional[str] = Field(None, description="User-Agent 헤더")

    class Config:
        json_schema_extra = {
            "example": {
                "client_ip": "178.128.69.202",
                "timestamp": "31/Dec/2025:10:15:23 +0000",
                "http_method": "POST",
                "path": "/api/logs/test-connection?bucket=xflow-datalake&path=S3",
                "http_version": "HTTP/1.1",
                "status_code": 200,
                "bytes_sent": 1234,
                "referrer": "http://localhost:5173/etl/job/123",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
            }
        }


class LogFormatGuide(BaseModel):
    """로그 형식 가이드"""
    format_name: str = "Apache Combined Log Format"
    format_pattern: str = '%h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-Agent}i"'
    example: str = '127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08"'
    description: str = "Industry standard Apache web server log format"
    fields: list = [
        "client_ip: Client IP address",
        "ident: Identity (usually -)",
        "user: Authenticated user (usually -)",
        "timestamp: Request timestamp [DD/Mon/YYYY:HH:MM:SS +ZONE]",
        "request: Full HTTP request line (method + path + version)",
        "status_code: HTTP status code",
        "bytes_sent: Response size in bytes",
        "referrer: Referer header",
        "user_agent: User-Agent header"
    ]


class TestConnectionRequest(BaseModel):
    bucket: str = Field(..., description="S3 bucket name")
    path: str = Field(..., description="S3 path/prefix")


class PreviewLogsRequest(BaseModel):
    bucket: str = Field(..., description="S3 bucket name")
    path: str = Field(..., description="S3 path/prefix")
    limit: int = Field(5, description="Number of logs to preview")


class TransformLogsRequest(BaseModel):
    source_bucket: str = Field(..., description="Source S3 bucket")
    source_path: str = Field(..., description="Source S3 path/prefix")
    target_bucket: Optional[str] = Field(None, description="Target S3 bucket (optional if target_path is s3:// URI)")
    target_path: str = Field(..., description="Target S3 path/prefix (can be s3://bucket/path format)")
    selected_fields: Optional[List[str]] = Field(None, description="Fields to include")
    filters: Optional[Dict[str, Any]] = Field(None, description="Transform filters")
