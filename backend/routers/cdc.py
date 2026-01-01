"""
CDC (Change Data Capture) Router
Manages Debezium connectors for real-time data streaming
"""
from fastapi import APIRouter, HTTPException
from typing import List
import httpx
from schemas.cdc import CDCTableConfig, CDCConnectorRequest, CDCConnectorResponse

router = APIRouter(prefix="/api/cdc", tags=["CDC"])

# Kafka Connect URL (Debezium runs on Kafka Connect)
KAFKA_CONNECT_URL = "http://kafka-connect:8083"


@router.get("/connectors")
async def list_connectors():
    """List all CDC connectors"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{KAFKA_CONNECT_URL}/connectors")
            if response.status_code == 200:
                connectors = response.json()
                # Get status for each connector
                result = []
                for name in connectors:
                    status_resp = await client.get(f"{KAFKA_CONNECT_URL}/connectors/{name}/status")
                    if status_resp.status_code == 200:
                        result.append(status_resp.json())
                return result
            return []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list connectors: {str(e)}")


@router.get("/connectors/{connector_name}")
async def get_connector_status(connector_name: str):
    """Get status of a specific connector"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status")
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                raise HTTPException(status_code=404, detail="Connector not found")
            else:
                raise HTTPException(status_code=response.status_code, detail="Failed to get connector status")
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Connection error: {str(e)}")


@router.post("/connectors", response_model=CDCConnectorResponse)
async def create_connector(request: CDCConnectorRequest):
    """Create a new CDC connector for PostgreSQL or MongoDB"""
    
    # Build connector config based on source type
    if request.source_type == "postgresql":
        config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "database.hostname": request.host,
            "database.port": str(request.port),
            "database.user": request.username or "postgres",
            "database.password": request.password or "postgres",
            "database.dbname": request.database,
            "topic.prefix": request.connector_name,
            "plugin.name": "pgoutput",
            "table.include.list": ",".join(request.tables)
        }
    elif request.source_type == "mongodb":
        # Build MongoDB connection string
        if request.username and request.password:
            conn_string = f"mongodb://{request.username}:{request.password}@{request.host}:{request.port}/?authSource=admin"
        else:
            conn_string = f"mongodb://{request.host}:{request.port}/?replicaSet=rs0"
        
        config = {
            "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
            "tasks.max": "1",
            "mongodb.connection.string": conn_string,
            "topic.prefix": request.connector_name,
            "collection.include.list": ",".join(request.tables)
        }
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported source type: {request.source_type}")
    
    # Create the connector
    connector_payload = {
        "name": request.connector_name,
        "config": config
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{KAFKA_CONNECT_URL}/connectors",
                json=connector_payload,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 201:
                return CDCConnectorResponse(
                    name=request.connector_name,
                    status="CREATED",
                    tables=request.tables
                )
            elif response.status_code == 409:
                raise HTTPException(status_code=409, detail="Connector already exists")
            else:
                raise HTTPException(
                    status_code=response.status_code, 
                    detail=f"Failed to create connector: {response.text}"
                )
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Connection error: {str(e)}")


@router.put("/connectors/{connector_name}/tables")
async def update_connector_tables(connector_name: str, tables: List[str]):
    """Update the tables monitored by a connector"""
    try:
        async with httpx.AsyncClient() as client:
            # Get current config
            config_resp = await client.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/config")
            if config_resp.status_code != 200:
                raise HTTPException(status_code=404, detail="Connector not found")
            
            config = config_resp.json()
            
            # Update table list based on connector type
            if "io.debezium.connector.postgresql" in config.get("connector.class", ""):
                config["table.include.list"] = ",".join(tables)
            elif "io.debezium.connector.mongodb" in config.get("connector.class", ""):
                config["collection.include.list"] = ",".join(tables)
            
            # Update connector
            response = await client.put(
                f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/config",
                json=config,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                return {"status": "updated", "tables": tables}
            else:
                raise HTTPException(status_code=response.status_code, detail="Failed to update connector")
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Connection error: {str(e)}")


@router.delete("/connectors/{connector_name}")
async def delete_connector(connector_name: str):
    """Delete a CDC connector (stops CDC for that source)"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.delete(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}")
            
            if response.status_code == 204:
                return {"status": "deleted", "connector": connector_name}
            elif response.status_code == 404:
                raise HTTPException(status_code=404, detail="Connector not found")
            else:
                raise HTTPException(status_code=response.status_code, detail="Failed to delete connector")
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Connection error: {str(e)}")


@router.post("/connectors/{connector_name}/restart")
async def restart_connector(connector_name: str):
    """Restart a CDC connector"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/restart")
            
            if response.status_code in [200, 204]:
                return {"status": "restarted", "connector": connector_name}
            elif response.status_code == 404:
                raise HTTPException(status_code=404, detail="Connector not found")
            else:
                raise HTTPException(status_code=response.status_code, detail="Failed to restart connector")
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Connection error: {str(e)}")
