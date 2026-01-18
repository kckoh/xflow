from datetime import datetime
from typing import List

from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException, status
from models import Connection
from schemas.connection import ConnectionCreate, ConnectionResponse, ConnectionUpdate
from schemas.api_source import APIConnectionConfig
from services.connection_tester import ConnectionTester

router = APIRouter()


def _validate_api_connection_config(config: dict) -> APIConnectionConfig:
    try:
        parsed = APIConnectionConfig(**config)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid API connection config: {exc}")

    auth_type = parsed.auth_type
    auth_config = parsed.auth_config

    if auth_type == "api_key":
        if not auth_config or not auth_config.header_name or not auth_config.api_key:
            raise HTTPException(status_code=400, detail="API key auth requires header_name and api_key")
    elif auth_type == "bearer":
        if not auth_config or not auth_config.token:
            raise HTTPException(status_code=400, detail="Bearer auth requires token")
    elif auth_type == "basic":
        if not auth_config or not auth_config.username or not auth_config.password:
            raise HTTPException(status_code=400, detail="Basic auth requires username and password")
    elif auth_type != "none":
        raise HTTPException(status_code=400, detail=f"Unsupported auth_type: {auth_type}")

    return parsed


@router.post("/test", status_code=status.HTTP_200_OK)
async def test_connection_config(connection: ConnectionCreate):
    """
    Test a connection configuration without saving it.
    Input matches ConnectionCreate schema but we only use type and config.
    """
    if connection.type == "api":
        _validate_api_connection_config(connection.config)

    is_success, message = await ConnectionTester.test_connection(connection.type, connection.config)
    
    if not is_success:
        raise HTTPException(status_code=400, detail=message)
    
    return {"success": True, "message": message}

@router.post("", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(connection: ConnectionCreate):
    """Create a new connection"""
    if connection.type == "api":
        _validate_api_connection_config(connection.config)

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

@router.get("", response_model=List[ConnectionResponse])
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
