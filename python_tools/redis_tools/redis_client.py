from typing import Any
import redis.asyncio as redis


class RedisConnection:
    _redis: redis.Redis
    def __init__(self, redis: redis.Redis):
        self._redis = redis

    async def get(self, key: str) -> Any:
        return await self._redis.get(name=key)

    async def set(self, key: str, value: str) -> None:
        await self._redis.set(name=key, value=value)

    async def execute_command(self, *args) -> None:
        await self._redis.execute_command(*args)


class RedisClient:
    _conn: RedisConnection
    def __init__(self, url: str):
        self.create_pool(url)

    def create_pool(self, url: str):
        pool = redis.ConnectionPool.from_url(url, max_connections=10, encoding="utf-8", decode_responses=True)
        client = redis.Redis.from_pool(pool)
        self._conn = RedisConnection(client)

    def get(self) -> RedisConnection:
        return self._conn
