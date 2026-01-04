import os
from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
# from neo4j import GraphDatabase
from models import User, Connection, Transform, ETLJob, JobRun, Dataset, Domain, QualityResult


# MongoDB connection from environment variables
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://mongo:mongo@localhost:27017")
DATABASE_NAME = os.getenv("MONGODB_DATABASE", "mydb")

# Neo4j connection from environment variables
# Global clients
mongodb_client = None

async def init_db():
    """
    Initialize MongoDB connection and Beanie ODM.
    Call this on application startup.
    """
    global mongodb_client

    # MongoDB (Motor)
    mongodb_client = AsyncIOMotorClient(MONGODB_URL)

    # Initialize Beanie with document models
    await init_beanie(
        database=mongodb_client[DATABASE_NAME],
        document_models=[User, Connection, Transform, ETLJob, JobRun, Dataset, Domain, QualityResult]
    )
    print(f"✅ Connected to MongoDB at {MONGODB_URL}")


async def close_db():
    """
    Close MongoDB connection.
    Call this on application shutdown.
    """
    global mongodb_client
    
    if mongodb_client:
        mongodb_client.close()
        print("Closed MongoDB connection")
