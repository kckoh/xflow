from motor.motor_asyncio import AsyncIOMotorClient
from backend.config import settings

# MongoDB connection
mongodb_client: AsyncIOMotorClient = None
database = None


async def connect_to_mongodb():
    """Connect to MongoDB on startup"""
    global mongodb_client, database

    # Build MongoDB connection URL
    if settings.mongodb_user and settings.mongodb_password:
        mongodb_url = (
            f"mongodb://{settings.mongodb_user}:{settings.mongodb_password}"
            f"@{settings.mongodb_host}:{settings.mongodb_port}"
        )
        if settings.mongodb_auth_source:
            mongodb_url = f"{mongodb_url}/?authSource={settings.mongodb_auth_source}"
    else:
        mongodb_url = f"mongodb://{settings.mongodb_host}:{settings.mongodb_port}"

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


def get_db():
    """FastAPI dependency alias for MongoDB database instance"""
    return get_database()
