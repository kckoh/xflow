"""
SQL Test API Schemas
"""
from pydantic import BaseModel
from typing import List, Dict, Any, Optional


class SourceInfo(BaseModel):
    """Information about a single source dataset"""
    source_dataset_id: str
    columns: List[str]  # Columns to select from this source


class SQLTestRequest(BaseModel):
    """Request model for SQL test endpoint"""
    sources: List[SourceInfo]  # Support multiple sources for UNION ALL
    sql: str
    limit: Optional[int] = 5


class ColumnSchema(BaseModel):
    """Schema information for a single column"""
    name: str
    type: str
    nullable: bool = True


class SourceSample(BaseModel):
    """Sample data from a single source"""
    source_name: str
    rows: List[Dict[str, Any]]


class SparkWarning(BaseModel):
    """Warning about Spark SQL compatibility"""
    function: str
    spark_equivalent: str
    message: str


class SQLTestResponse(BaseModel):
    """Response model for SQL test endpoint"""
    valid: bool
    schema: Optional[List[ColumnSchema]] = None
    sample_rows: Optional[List[Dict[str, Any]]] = None
    before_rows: Optional[List[Dict[str, Any]]] = None
    source_samples: Optional[List[SourceSample]] = None  # Per-source samples
    spark_warnings: Optional[List[SparkWarning]] = None  # DuckDB -> Spark compatibility warnings
    sql_conversions: Optional[List[str]] = None  # Spark SQL -> DuckDB conversions made
    executed_sql: Optional[str] = None  # Actual SQL executed (after conversion)
    error: Optional[str] = None
    execution_time_ms: Optional[int] = None
