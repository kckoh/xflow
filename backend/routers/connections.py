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

@router.post("", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(connection: ConnectionCreate):
    """Create a new connection"""
    # Check for duplicate configuration based on core identity fields
    # We allow duplicate names but prevent duplicate connections to the same actual data source
    existing_conns = await Connection.find(Connection.type == connection.type).to_list()
    
    for conn in existing_conns:
        # RDB Identity: Host + Port + Database
        if connection.type in ['postgres', 'mysql', 'mariadb']:
            if (conn.config.get('host') == connection.config.get('host') and
                str(conn.config.get('port')) == str(connection.config.get('port')) and
                conn.config.get('database_name') == connection.config.get('database_name')):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Connection to {connection.config.get('host')}:{connection.config.get('port')}/{connection.config.get('database_name')} already exists."
                )
        
        # MongoDB Identity: URI + Database
        elif connection.type == 'mongodb':
             if (conn.config.get('uri') == connection.config.get('uri') and
                 conn.config.get('database') == connection.config.get('database')):
                 raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Connection to MongoDB '{connection.config.get('database')}' already exists."
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

@router.get("/{connection_id}/topics")
async def get_kafka_topics(connection_id: str):
    """Get list of topics from Kafka cluster"""
    conn = await Connection.get(PydanticObjectId(connection_id))
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    if conn.type != 'kafka':
        raise HTTPException(status_code=400, detail="Connection is not a Kafka connection")
    
    try:
        from kafka import KafkaAdminClient
        
        bootstrap_servers = conn.config.get('bootstrap_servers')
        if not bootstrap_servers:
            raise HTTPException(status_code=400, detail="Bootstrap servers not configured")
        
        # Convert localhost to kafka for Docker network
        if 'localhost' in bootstrap_servers:
            bootstrap_servers = bootstrap_servers.replace('localhost', 'kafka')
        
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers.split(','),
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000
        )
        
        # Get topics list
        topics = admin_client.list_topics()
        admin_client.close()
        
        # Filter out internal topics
        topics = [t for t in topics if not t.startswith('__')]
        
        return {"topics": topics}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch topics: {str(e)}")

@router.post("/{connection_id}/topics/{topic}/schema")
async def infer_kafka_schema(connection_id: str, topic: str):
    """Infer schema from Kafka topic messages"""
    conn = await Connection.get(PydanticObjectId(connection_id))
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    if conn.type != 'kafka':
        raise HTTPException(status_code=400, detail="Connection is not a Kafka connection")
    
    try:
        from kafka import KafkaConsumer
        import json
        
        bootstrap_servers = conn.config.get('bootstrap_servers')
        if not bootstrap_servers:
            raise HTTPException(status_code=400, detail="Bootstrap servers not configured")
        
        # Convert localhost to kafka for Docker network
        if 'localhost' in bootstrap_servers:
            bootstrap_servers = bootstrap_servers.replace('localhost', 'kafka')
        
        # Create consumer to read first message
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(','),
            auto_offset_reset='earliest',
            max_poll_records=1,
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: m.decode('utf-8')
        )
        
        try:
            # Read first message
            for message in consumer:
                data = json.loads(message.value)
                
                # Infer schema from JSON
                schema = []
                for key, value in data.items():
                    field_type = infer_type(value)
                    schema.append({
                        "name": key,
                        "type": field_type
                    })
                
                consumer.close()
                return {"schema": schema, "sample": data}
            
            # No messages found
            raise HTTPException(status_code=400, detail="Topic is empty or no messages available")
            
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Message is not valid JSON")
        finally:
            consumer.close()
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to infer schema: {str(e)}")

def infer_type(value):
    """Infer data type from Python value"""
    if isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "integer"
    elif isinstance(value, float):
        return "double"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        return "array"
    elif isinstance(value, dict):
        return "object"
    else:
        return "string"

@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(connection_id: str):
    """Delete a connection"""
    conn = await Connection.get(PydanticObjectId(connection_id))
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    await conn.delete()
    return None
