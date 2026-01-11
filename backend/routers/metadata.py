"""
Metadata API Router
Table, Collection, or File listing for various connections
"""

from typing import List, Optional, Any, Dict
from fastapi import APIRouter, HTTPException, status
from beanie import PydanticObjectId
import json

from models import Connection
from services.database_connector import DatabaseConnector
from schemas.rdb_tables import RDBTableListResponse, RDBColumnInfo

router = APIRouter()

# Helper function for Kafka Configuration
def _get_kafka_config(conn: Connection) -> Dict[str, Any]:
    bootstrap_servers = conn.config.get('bootstrap_servers')
    if isinstance(bootstrap_servers, str):
        bootstrap_servers = [s.strip() for s in bootstrap_servers.split(',')]
    
    config = {
        'bootstrap_servers': bootstrap_servers,
        'request_timeout_ms': 5000,
    }

    security_protocol = conn.config.get('security_protocol', 'PLAINTEXT')
    if security_protocol != 'PLAINTEXT':
        config['security_protocol'] = security_protocol
        if 'SASL' in security_protocol:
            config['sasl_mechanism'] = conn.config.get('sasl_mechanism', 'PLAIN')
            config['sasl_plain_username'] = conn.config.get('sasl_username')
            config['sasl_plain_password'] = conn.config.get('sasl_password')
    
    return config

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


# ============ MongoDB Collection/Schema API ============

@router.get("/{connection_id}/collections")
async def get_mongodb_collections(connection_id: str):
    """
    Get MongoDB collections from a connection.
    Returns list of collection names.
    """
    try:
        conn = await Connection.get(PydanticObjectId(connection_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    
    if conn.type == 'mongodb':
        try:
            from services.mongodb_connector import MongoDBConnector
            
            with MongoDBConnector(
                uri=conn.config.get('uri'),
                database=conn.config.get('database')
            ) as connector:
                collections = connector.get_collections()
                
                # Update connection status
                await conn.update({"$set": {"status": "connected"}})
                
                return {
                    "source_id": connection_id,
                    "collections": collections
                }
        except Exception as e:
            await conn.update({"$set": {"status": "error"}})
            raise HTTPException(status_code=500, detail=f"Failed to get collections: {str(e)}")
    else:
        raise HTTPException(status_code=400, detail=f"Not a MongoDB connection (type: {conn.type})")


@router.get("/{connection_id}/collections/{collection_name}/schema")
async def get_collection_schema(
    connection_id: str,
    collection_name: str,
    sample_size: int = 1000
):
    """
    Infer schema from a MongoDB collection by sampling documents.
    """
    try:
        conn = await Connection.get(PydanticObjectId(connection_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    if conn.type == 'mongodb':
        try:
            from services.mongodb_connector import MongoDBConnector
            
            with MongoDBConnector(
                uri=conn.config.get('uri'),
                database=conn.config.get('database')
            ) as connector:
                schema = connector.infer_schema(collection_name, sample_size)
                return schema
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to infer schema: {str(e)}")
    else:
        raise HTTPException(status_code=400, detail=f"Not a MongoDB connection (type: {conn.type})")


# ============ Kafka Topic/Schema API ============

@router.get("/{connection_id}/topics")
async def get_kafka_topics(connection_id: str):
    """
    Get Kafka topics from a connection.
    """
    try:
        conn = await Connection.get(PydanticObjectId(connection_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Connection not found")

    if conn.type == 'kafka':
        try:
            from kafka import KafkaAdminClient
            
            config = _get_kafka_config(conn)
            admin_client = KafkaAdminClient(**config)
            
            topics = admin_client.list_topics()
            
            # Filter internal topics (starting with __)
            topics = [t for t in topics if not t.startswith("__")]
            
            admin_client.close()
            
            await conn.update({"$set": {"status": "connected"}})
            
            return {
                "source_id": connection_id,
                "topics": topics
            }
        except Exception as e:
            await conn.update({"$set": {"status": "error"}})
            raise HTTPException(status_code=500, detail=f"Failed to get Kafka topics: {str(e)}")
    else:
        raise HTTPException(status_code=400, detail=f"Not a Kafka connection (type: {conn.type})")

@router.get("/{connection_id}/topics/{topic}/schema")
async def get_kafka_topic_schema(connection_id: str, topic: str):
    """
    Infer schema from a Kafka topic by sampling the latest message.
    Assumes JSON format.
    """
    try:
        conn = await Connection.get(PydanticObjectId(connection_id))
    except Exception:
         raise HTTPException(status_code=404, detail="Connection not found")
    
    if conn.type == 'kafka':
        try:
            from kafka import KafkaConsumer, TopicPartition
            
            config = _get_kafka_config(conn)
            # Consumer needs group_id for some operations, but simple consumption is fine without if using assign/seek
            # Remove request_timeout_ms from consumer config if it conflicts or use defaults
            # Consumer config is slightly different from AdminClient
            consumer_config = {
                 'bootstrap_servers': config['bootstrap_servers'],
                 'auto_offset_reset': 'earliest',
                 'enable_auto_commit': False,
                 'consumer_timeout_ms': 5000 # Stop iteration if no message after 5s
            }
            if 'security_protocol' in config:
                consumer_config['security_protocol'] = config['security_protocol']
                consumer_config['sasl_mechanism'] = config.get('sasl_mechanism')
                consumer_config['sasl_plain_username'] = config.get('sasl_plain_username')
                consumer_config['sasl_plain_password'] = config.get('sasl_plain_password')

            consumer = KafkaConsumer(**consumer_config)
            
            # Manually assign partition 0 to read latest message
            tp = TopicPartition(topic, 0)
            consumer.assign([tp])
            
            # Seek to end
            consumer.seek_to_end(tp)
            end_offset = consumer.position(tp)
            
            if end_offset == 0:
                 return [] # Empty topic
            
            # Seek to last message (or few messages back)
            start_offset = max(0, end_offset - 5) # Sample last 5 messages
            consumer.seek(tp, start_offset)
            
            messages = consumer.poll(timeout_ms=5000)
            
            consumer.close()
            
            if not messages:
                return []

            # Infer schema from the last valid JSON message
            schema = []
            
            for record in reversed(messages.get(tp, [])):
                try:
                    if record.value:
                        data = json.loads(record.value.decode('utf-8'))
                        for key, value in data.items():
                             schema_type = "string"
                             if isinstance(value, int):
                                 schema_type = "integer"
                             elif isinstance(value, float):
                                 schema_type = "double"
                             elif isinstance(value, bool):
                                 schema_type = "boolean"
                             elif isinstance(value, dict):
                                 schema_type = "struct"
                             elif isinstance(value, list):
                                 schema_type = "array"
                                 
                             schema.append({
                                 "name": key,
                                 "type": schema_type,
                                 "nullable": True
                             })
                        break # Found a valid JSON, stop
                except Exception:
                    continue # Not JSON, try next message
            
            return schema

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to infer Kafka schema: {str(e)}")
    else:
        raise HTTPException(status_code=400, detail=f"Not a Kafka connection (type: {conn.type})")
