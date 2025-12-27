import os
from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
from neo4j import GraphDatabase
from models import User, RDBSource, Transform


# MongoDB connection from environment variables
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://mongo:mongo@localhost:27017")
DATABASE_NAME = os.getenv("MONGODB_DATABASE", "mydb")

# Neo4j connection from environment variables
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# Global clients
mongodb_client = None
neo4j_driver = None


async def init_db():
    """
    Initialize MongoDB connection and Beanie ODM and Neo4j connection.
    Call this on application startup.
    """
    global mongodb_client, neo4j_driver

    # MongoDB (Motor)
    mongodb_client = AsyncIOMotorClient(MONGODB_URL)

    # Initialize Beanie with document models

    await init_beanie(
        database=mongodb_client[DATABASE_NAME],
        document_models=[User, RDBSource, Transform]
    )
    print(f"✅ Connected to MongoDB at {MONGODB_URL}")

    # Neo4j (Official Driver)
    try:
        neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        neo4j_driver.verify_connectivity()
        print(f"✅ Connected to Neo4j at {NEO4J_URI}")
    except Exception as e:
        print(f"❌ Failed to connect to Neo4j: {e}")


async def close_db():
    """
    Close MongoDB connection and Neo4j connection.
    Call this on application shutdown.
    """
    global mongodb_client, neo4j_driver
    
    if mongodb_client:
        mongodb_client.close()
        print("Closed MongoDB connection")
        
    if neo4j_driver:
        neo4j_driver.close()
        print("Closed Neo4j connection")
