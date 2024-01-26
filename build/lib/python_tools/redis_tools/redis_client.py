from typing import Set, Awaitable
import redis.asyncio as redis


class RedisConnection:
    _redis: redis.Redis

    def __init__(self, redis: redis.Redis):
        self._redis = redis

    def get(self, key: str) -> Awaitable:
        return self._redis.get(name=key)

    def set(self, *, key: str, value: str) -> Awaitable:
        return self._redis.set(name=key, value=value)

    def hset(self, *, name: str, key: str, value: str) -> Awaitable[int] | int:
        return self._redis.hset(name=name, key=key, value=value)

    def hget(self, *, name: str, key: str) -> Awaitable[str | None] | str | None:
        return self._redis.hget(name=name, key=key)

    def hdel(self, *, name: str, keys: list[str | int]) -> Awaitable[int] | int:
        return self._redis.hdel(name, *keys)

    def add_to_set(self, name: str, values: list[str]) -> Awaitable[int] | int:
        return self._redis.sadd(name, *values)

    def remove_from_set(self, name: str, values: list[str]) -> Awaitable[int] | int:
        return self._redis.srem(name, *values)

    def get_set_values(self, name: str) -> Awaitable[Set] | Set:
        return self._redis.smembers(name)

    def delete(self, *keys: list[str]) -> Awaitable:
        return self._redis.delete(*keys)

    async def execute_command(self, *args) -> None:
        await self._redis.execute_command(*args)


class RedisClient:
    _conn: RedisConnection
    _url: str

    def __init__(self, url: str):
        self._url = url

    def create_pool(self):
        pool = redis.ConnectionPool.from_url(
            self._url, max_connections=10, encoding="utf-8", decode_responses=True
        )
        client = redis.Redis.from_pool(pool)
        self._conn = RedisConnection(client)

    def get(self) -> RedisConnection:
        return self._conn
