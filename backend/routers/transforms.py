"""
Transform API Router
Column listing for transform configuration
"""

from typing import List
from fastapi import APIRouter, HTTPException, status
from beanie import PydanticObjectId

from models import RDBSource
from services.database_connector import DatabaseConnector
from schemas.rdb_transform import ColumnInfo

router = APIRouter()


# ============ Column Listing API ============

@router.get("/rdb-sources/{source_id}/tables/{table_name}/columns", response_model=List[ColumnInfo])
async def get_table_columns(source_id: str, table_name: str):
    """
    Get columns for a specific table in an RDB source
    """
    # Get RDBSource
    try:
        source = await RDBSource.get(PydanticObjectId(source_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Source not found")
    
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    
    try:
        connector = DatabaseConnector(
            db_type=source.type,
            host=source.host,
            port=source.port,
            database=source.database_name,
            user=source.user_name,
            password=source.password,
        )
        
        columns = connector.get_columns(table_name)
        
        return [
            ColumnInfo(
                name=col['name'],
                type=col['type'],
                nullable=col.get('nullable', True)
            )
            for col in columns
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get columns: {str(e)}")
