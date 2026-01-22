"""
S3 Log Parsing Schemas
Schemas for S3 log preview and AI regex generation
"""
from pydantic import BaseModel
from typing import List, Dict, Any, Optional


class S3LogPreviewRequest(BaseModel):
    source_dataset_id: str
    custom_regex: str
    selected_fields: List[str]
    filters: Dict[str, Any]
    limit: Optional[int] = 5


class S3LogPreviewResponse(BaseModel):
    valid: bool
    before_rows: Optional[List[Dict[str, Any]]] = None
    after_rows: Optional[List[Dict[str, Any]]] = None
    total_records: Optional[int] = None
    filtered_records: Optional[int] = None
    error: Optional[str] = None


class RegexTestRequest(BaseModel):
    source_dataset_id: str
    custom_regex: str
    limit: Optional[int] = 5


class S3LogTestRequest(BaseModel):
    """Test S3 log parsing without saving source dataset"""
    connection_id: str
    bucket: str
    path: str
    custom_regex: str
    limit: Optional[int] = 5


class RegexTestResponse(BaseModel):
    valid: bool
    sample_logs: Optional[List[str]] = None
    parsed_rows: Optional[List[Dict[str, Any]]] = None
    fields_extracted: Optional[List[str]] = None
    total_lines: Optional[int] = None
    parsed_lines: Optional[int] = None
    error: Optional[str] = None


class GenerateRegexRequest(BaseModel):
    """Generate regex pattern from S3 logs using AI"""
    connection_id: str
    bucket: str
    path: str
    limit: Optional[int] = 5


class GenerateRegexResponse(BaseModel):
    success: bool
    regex_pattern: Optional[str] = None
    sample_logs: Optional[List[str]] = None
    error: Optional[str] = None
