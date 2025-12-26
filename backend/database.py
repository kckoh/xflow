from motor.motor_asyncio import AsyncIOMotorClient
from backend.config import settings

# MongoDB connection
mongodb_client: AsyncIOMotorClient = None
database = None


async def connect_to_mongodb():
    """Connect to MongoDB on startup"""
    global mongodb_client, database

    # Build MongoDB connection URL
    mongodb_url = f"mongodb://{settings.mongodb_user}:{settings.mongodb_password}@{settings.mongodb_host}:{settings.mongodb_port}"

    mongodb_client = AsyncIOMotorClient(mongodb_url)
    database = mongodb_client[settings.mongodb_db]

    print(f"✅ Connected to MongoDB: {settings.mongodb_db}")


async def close_mongodb_connection():
    """Close MongoDB connection on shutdown"""
    global mongodb_client

    if mongodb_client:
        mongodb_client.close()
        print("❌ Closed MongoDB connection")


def get_database():
    """Get MongoDB database instance"""
    return database
