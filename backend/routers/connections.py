from datetime import datetime
from typing import List

from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException, status
from models import Connection
from schemas.connection import ConnectionCreate, ConnectionResponse, ConnectionUpdate
from services.connection_tester import ConnectionTester

router = APIRouter()

@router.post("/test", status_code=status.HTTP_200_OK)
async def test_connection_config(connection: ConnectionCreate):
    """
    Test a connection configuration without saving it.
    Input matches ConnectionCreate schema but we only use type and config.
    """
    is_success, message = ConnectionTester.test_connection(connection.type, connection.config)
    
    if not is_success:
        raise HTTPException(status_code=400, detail=message)
    
    return {"success": True, "message": message}

@router.post("/", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(connection: ConnectionCreate):
    """Create a new connection"""
    # Check if name exists
    existing = await Connection.find_one(Connection.name == connection.name)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Connection name already exists"
        )

    new_conn = Connection(
        name=connection.name,
        description=connection.description,
        type=connection.type,
        config=connection.config,
        status="disconnected",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    
    await new_conn.insert()
    
    return ConnectionResponse(
        id=str(new_conn.id),
        name=new_conn.name,
        description=new_conn.description,
        type=new_conn.type,
        config=new_conn.config,
        status=new_conn.status,
        created_at=new_conn.created_at,
        updated_at=new_conn.updated_at
    )

@router.get("/", response_model=List[ConnectionResponse])
async def list_connections():
    """List all connections"""
    conns = await Connection.find_all().to_list()
    return [
        ConnectionResponse(
            id=str(c.id),
            name=c.name,
            description=c.description,
            type=c.type,
            config=c.config,
            status=c.status,
            created_at=c.created_at,
            updated_at=c.updated_at
        ) 
        for c in conns
    ]

@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(connection_id: str):
    """Delete a connection"""
    conn = await Connection.get(PydanticObjectId(connection_id))
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    await conn.delete()
    return None
