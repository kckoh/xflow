from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional
from pydantic import BaseModel
from urllib.parse import unquote
import logging

from database import get_database
from models import FileProcessingHistory
from services.table_manager_service import table_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/data-lake", tags=["data-lake"])


# ============ Request/Response Models ============

class MinIOEventRecord(BaseModel):
    """MinIO event notification record"""
    s3: dict


class MinIOEvent(BaseModel):
    """MinIO event notification payload"""
    EventName: str
    Key: str
    Records: List[MinIOEventRecord]


class ProcessFileRequest(BaseModel):
    """Request to process a specific file"""
    object_path: str


class DiscoverTablesRequest(BaseModel):
    """Request to discover and process tables"""
    prefix: Optional[str] = ""


class RebuildTableRequest(BaseModel):
    """Request to rebuild a table"""
    table_name: str


class ProcessingHistoryResponse(BaseModel):
    """Processing history response"""
    id: Optional[str]
    object_path: str
    table_name: str
    ddl_statement: str
    partition_values: Optional[dict]
    num_columns: Optional[int]
    num_rows: Optional[int]
    status: str
    error_message: Optional[str]
    created_at: Optional[str]

    class Config:
        from_attributes = True


# ============ Endpoints ============

@router.post("/webhook/minio")
async def minio_webhook(event: dict):
    """
    Receive MinIO event notifications and process uploaded files

    MinIO sends events when files are uploaded. This endpoint:
    1. Receives the event
    2. Extracts file path
    3. Triggers automatic table creation/partition sync
    """
    try:
        logger.info(f"Received MinIO event: {event}")

        # MinIO event structure:
        # {
        #   "EventName": "s3:ObjectCreated:Put",
        #   "Key": "datalake/sales/year=2024/month=12/data.parquet",
        #   "Records": [...]
        # }

        # Extract file path from event
        records = event.get("Records", [])
        if not records:
            raise HTTPException(status_code=400, detail="No records in event")

        results = []
        for record in records:
            # Get object key (file path)
            s3_info = record.get("s3", {})
            object_info = s3_info.get("object", {})
            object_key = object_info.get("key", "")

            if not object_key:
                logger.warning("No object key in record")
                continue

            # URL decode the object key (MinIO sends URL-encoded paths)
            object_key = unquote(object_key)

            # Only process Parquet files
            if not object_key.endswith('.parquet'):
                logger.info(f"Skipping non-Parquet file: {object_key}")
                continue

            # Remove bucket name from path if present
            # MinIO event may include bucket name like "datalake/sales/..."
            # We only want "sales/..."
            if object_key.startswith('datalake/'):
                object_key = object_key[len('datalake/'):]

            logger.info(f"Processing file from event: {object_key}")

            # Process the file
            result = await table_manager.process_parquet_file(object_key)
            results.append(result)

        return {
            "status": "success",
            "processed_files": len(results),
            "results": results
        }

    except Exception as e:
        logger.error(f"Error processing MinIO event: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/process-file")
async def process_file(request: ProcessFileRequest):
    """
    Manually trigger processing of a specific file

    Args:
        request: Contains object_path (e.g., "sales/year=2024/month=12/data.parquet")

    Returns:
        Processing result
    """
    try:
        logger.info(f"Manual processing requested for: {request.object_path}")
        result = await table_manager.process_parquet_file(request.object_path)
        return result
    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/discover-tables")
async def discover_tables(request: DiscoverTablesRequest):
    """
    Discover all Parquet files in MinIO and process them

    Args:
        request: Optional prefix to filter files (e.g., "sales/")

    Returns:
        List of processing results
    """
    try:
        logger.info(f"Discovering tables with prefix: {request.prefix}")
        results = table_manager.discover_and_process_tables(prefix=request.prefix)
        return {
            "status": "success",
            "processed_files": len(results),
            "results": results
        }
    except Exception as e:
        logger.error(f"Error discovering tables: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rebuild-table")
async def rebuild_table(request: RebuildTableRequest):
    """
    Drop and recreate a table from existing files in MinIO

    Args:
        request: Contains table_name

    Returns:
        Rebuild result
    """
    try:
        logger.info(f"Rebuilding table: {request.table_name}")
        result = table_manager.rebuild_table(request.table_name)
        return result
    except Exception as e:
        logger.error(f"Error rebuilding table: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history", response_model=List[ProcessingHistoryResponse])
async def get_processing_history(
    limit: int = 100,
    table_name: Optional[str] = None,
    status: Optional[str] = None
):
    """
    Get processing history

    Args:
        limit: Maximum number of records to return (default: 100)
        table_name: Filter by table name (optional)
        status: Filter by status - 'success' or 'failed' (optional)

    Returns:
        List of processing history records
    """
    try:
        db = get_database()

        # Build filter query
        query_filter = {}
        if table_name:
            query_filter["table_name"] = table_name
        if status:
            query_filter["status"] = status

        # Query MongoDB (async)
        history_cursor = db.file_processing_history.find(query_filter).sort("created_at", -1).limit(limit)
        history = await history_cursor.to_list(length=limit)

        # Convert to response model
        return [
            ProcessingHistoryResponse(
                id=str(h["_id"]),
                object_path=h["object_path"],
                table_name=h["table_name"],
                ddl_statement=h["ddl_statement"],
                partition_values=h.get("partition_values"),
                num_columns=h.get("num_columns"),
                num_rows=h.get("num_rows"),
                status=h["status"],
                error_message=h.get("error_message"),
                created_at=h["created_at"].isoformat() if h.get("created_at") else None
            )
            for h in history
        ]

    except Exception as e:
        logger.error(f"Error getting history: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables")
async def get_created_tables():
    """
    Get list of tables that were automatically created

    Returns:
        List of unique table names with their creation info
    """
    try:
        db = get_database()

        # Get distinct table names with their first creation record using aggregation
        pipeline = [
            {"$match": {"status": "success"}},
            {"$sort": {"created_at": 1}},
            {"$group": {
                "_id": "$table_name",
                "first_record": {"$first": "$$ROOT"}
            }},
            {"$replaceRoot": {"newRoot": "$first_record"}}
        ]

        tables_cursor = db.file_processing_history.aggregate(pipeline)
        tables = await tables_cursor.to_list(length=None)

        return [
            {
                "table_name": t["table_name"],
                "created_at": t["created_at"].isoformat() if t.get("created_at") else None,
                "ddl_statement": t["ddl_statement"],
                "num_columns": t.get("num_columns"),
                "partition_values": t.get("partition_values")
            }
            for t in tables
        ]

    except Exception as e:
        logger.error(f"Error getting tables: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
