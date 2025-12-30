"""
Metadata API Router
Table, Collection, or File listing for various connections
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, status
from beanie import PydanticObjectId

from models import Connection
from services.database_connector import DatabaseConnector
from schemas.rdb_tables import RDBTableListResponse, RDBColumnInfo

router = APIRouter()

# ============ Table/Dataset Listing API ============

@router.get("/{connection_id}/tables", response_model=RDBTableListResponse)
async def get_connection_tables(connection_id: str):
    """Get tables/datasets from a connection"""
    try:
        conn = await Connection.get(PydanticObjectId(connection_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Connection not found")

    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")

    # Currently only supports RDB types via DatabaseConnector
    if conn.type in ['postgres', 'mysql', 'mariadb', 'oracle']:
        try:
            connector = DatabaseConnector(
                db_type=conn.type,
                host=conn.config.get('host'),
                port=int(conn.config.get('port', 5432)),
                database=conn.config.get('database_name'),
                user=conn.config.get('user_name'),
                password=conn.config.get('password'),
            )
            
            tables = connector.get_tables()
            
            # Update connection status
            await conn.update({"$set": {"status": "connected"}})
            
            return RDBTableListResponse(
                source_id=connection_id,
                tables=tables
            )
        except Exception as e:
            await conn.update({"$set": {"status": "error"}})
            raise HTTPException(status_code=500, detail=f"Failed to get tables: {str(e)}")
            
    else:
        raise HTTPException(status_code=400, detail=f"Metadata listing not supported yet for type: {conn.type}")


# ============ Column/Schema Listing API ============

@router.get("/{connection_id}/tables/{table_name}/columns", response_model=List[RDBColumnInfo])
async def get_table_columns(connection_id: str, table_name: str):
    """
    Get columns for a specific table/dataset
    """
    try:
        conn = await Connection.get(PydanticObjectId(connection_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    if conn.type in ['postgres', 'mysql', 'mariadb', 'oracle']:
        try:
            connector = DatabaseConnector(
                db_type=conn.type,
                host=conn.config.get('host'),
                port=int(conn.config.get('port', 5432)),
                database=conn.config.get('database_name'),
                user=conn.config.get('user_name'),
                password=conn.config.get('password'),
            )
            
            columns = connector.get_columns(table_name)
            
            return [
                RDBColumnInfo(
                    name=col['name'],
                    type=col['type'],
                    nullable=col.get('nullable', True)
                )
                for col in columns
            ]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to get columns: {str(e)}")
    
    else:
         raise HTTPException(status_code=400, detail=f"Schema listing not supported yet for type: {conn.type}")
