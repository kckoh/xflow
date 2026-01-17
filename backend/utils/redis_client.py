"""
Redis client for FastAPI
Usage:
    from utils.redis_client import redis_client

    # Set value
    await redis_client.set("key", "value", ex=3600)  # expires in 1 hour

    # Get value
    value = await redis_client.get("key")

    # Delete
    await redis_client.delete("key")

    # JSON (for sessions)
    await redis_client.set_json("session:123", {"user_id": "abc"})
    data = await redis_client.get_json("session:123")
"""

import os
import json
from typing import Optional, Any
import redis.asyncio as redis


class RedisClient:
    def __init__(self):
        self.client: Optional[redis.Redis] = None

    async def connect(self):
        """Connect to Redis"""
        if self.client is None:
            self.client = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis-master"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                password=os.getenv("REDIS_PASSWORD", "redis123"),
                decode_responses=True,
            )
        return self.client

    async def disconnect(self):
        """Close Redis connection"""
        if self.client:
            await self.client.close()
            self.client = None

    async def get(self, key: str) -> Optional[str]:
        """Get string value"""
        client = await self.connect()
        return await client.get(key)

    async def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """Set string value with optional expiration (seconds)"""
        client = await self.connect()
        return await client.set(key, value, ex=ex)

    async def delete(self, key: str) -> int:
        """Delete key"""
        client = await self.connect()
        return await client.delete(key)

    async def exists(self, key: str) -> bool:
        """Check if key exists"""
        client = await self.connect()
        return await client.exists(key) > 0

    async def get_json(self, key: str) -> Optional[Any]:
        """Get JSON value"""
        value = await self.get(key)
        if value:
            return json.loads(value)
        return None

    async def set_json(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """Set JSON value"""
        return await self.set(key, json.dumps(value), ex=ex)

    async def keys(self, pattern: str = "*") -> list:
        """Get all keys matching pattern"""
        client = await self.connect()
        return await client.keys(pattern)


# Singleton instance
redis_client = RedisClient()
