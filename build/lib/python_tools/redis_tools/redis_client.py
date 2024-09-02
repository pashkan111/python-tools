from typing import Set, Awaitable, Any
import redis.asyncio as redis
import logging

ResponseT = Awaitable | Any


class RedisConnection:
    _redis: redis.Redis

    def __init__(self, redis: redis.Redis):
        self._redis = redis

    def ping(self) -> ResponseT:
        return self._redis.ping()

    def get(self, key: str) -> ResponseT:
        return self._redis.get(name=key)

    def set(self, *, key: str, value: str) -> ResponseT:
        return self._redis.set(name=key, value=value)

    def hset(self, *, name: str, key: str, value: str) -> Awaitable[int] | int:
        return self._redis.hset(name=name, key=key, value=value)

    def hget(self, *, name: str, key: str) -> Awaitable[str | None] | str | None:
        return self._redis.hget(name=name, key=key)

    def hdel(self, *, name: str, keys: list[str | int]) -> Awaitable[int] | int:
        return self._redis.hdel(name, *keys)

    def add_to_set(self, *, name: str, values: list[str]) -> Awaitable[int] | int:
        return self._redis.sadd(name, *values)

    def remove_from_set(self, *, name: str, values: list[str]) -> Awaitable[int] | int:
        return self._redis.srem(name, *values)

    def get_set_values(self, name: str) -> Awaitable[Set] | Set:
        return self._redis.smembers(name)

    def delete(self, *keys: list[str]) -> ResponseT:
        return self._redis.delete(*keys)

    async def execute_command(self, *args) -> None:
        await self._redis.execute_command(*args)

    def xadd(self, *, channel: str, data: dict[str, Any]) -> ResponseT:
        return self._redis.xadd(channel, data)

    def xread(
        self,
        *,
        streams: dict[str, Any],
        count: int | None = None,
        block: int | None = None,
    ) -> ResponseT:
        return self._redis.xread(streams, count, block)

    def keys(self, pattern: str = '*', **kwargs) -> ResponseT:
        return self._redis.keys(pattern)


class RedisClient:
    _conn: RedisConnection
    _url: str

    def __init__(self, url: str):
        self._url = url

    def create_pool(self):
        try:
            pool = redis.ConnectionPool.from_url(self._url, max_connections=10, encoding="utf-8", decode_responses=True)
            client = redis.Redis(connection_pool=pool)
            self._conn = RedisConnection(client)
            logging.info("Successfully connected to Redis.")
        except redis.ConnectionError as e:
            logging.error(f"Failed to connect to Redis: {e}")
            raise

    def get(self) -> RedisConnection:
        return self._conn
