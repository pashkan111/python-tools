import asyncpg
from typing import AsyncIterator, Any
from contextlib import asynccontextmanager
from asyncpg.transaction import Transaction
from typing import AsyncGenerator


class PostgresConnection:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[asyncpg.Connection]:
        connection = await self.pool.acquire()
        try:
            yield connection
        finally:
            await self.pool.release(connection)

    async def _close_pool(self):
        await self.pool.close()

    async def fetch(
        self, *, query: str, conn: asyncpg.Connection | None = None, args: list[Any] | None = None, **kwargs
    ) -> list[asyncpg.Record]:
        args = args or []
        if conn:
            return await conn.fetch(query, *args)
        async with self._connection() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(
        self, *, query: str, conn: asyncpg.Connection | None = None, args: list[Any] | None = None, **kwargs
    ) -> asyncpg.Record | None:
        args = args or []
        if conn:
            return await conn.fetchrow(query, *args)
        async with self._connection() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(
        self, *, query: str, conn: asyncpg.Connection | None = None, args: list[Any] | None = None, **kwargs
    ) -> Any:
        args = args or []
        if conn:
            return await conn.fetchval(query, *args)
        async with self._connection() as conn:
            return await conn.fetchval(query, *args)

    async def execute(
        self, *, command: str, conn: asyncpg.Connection | None = None, args: list[Any] | None = None, **kwargs
    ) -> Any:
        args = args or []
        if conn:
            return await conn.execute(command, *args)
        async with self._connection() as conn:
            return await conn.execute(command, *args)

    @asynccontextmanager
    async def transaction(
        self, isolation: str = "read_committed"
    ) -> AsyncGenerator[tuple[Transaction, asyncpg.Connection], None]:
        async with self._connection() as conn:
            transaction = conn.transaction(isolation=isolation)
            await transaction.start()
            yield transaction, conn


class Pg:
    connection: PostgresConnection

    def __init__(
        self,
        dsn: str,
        min_size: int = 10,
        max_size: int = 10,
        max_queries: int = 50000,
        max_inactive_connection_lifetime: float = 300.0,
    ):
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.max_queries = max_queries
        self.max_inactive_connection_lifetime = max_inactive_connection_lifetime

    async def create_pool(self) -> None:
        pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=self.min_size,
            max_size=self.max_size,
            max_queries=self.max_queries,
            max_inactive_connection_lifetime=self.max_inactive_connection_lifetime,
        )
        if not pool:
            raise Exception
        self.connection = PostgresConnection(pool)

    def get(self) -> PostgresConnection:
        return self.connection
