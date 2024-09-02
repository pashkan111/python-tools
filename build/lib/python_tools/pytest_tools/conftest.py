import pytest
import os
import asyncio
import pytest_asyncio
from python_tools.postgres_tools import PostgresConnection
from python_tools.redis_tools import RedisConnection
from .tools import drop_tables, create_tables
from typing import AsyncGenerator
from faker import Faker
import importlib
from typing import TypedDict


class EnvVariables(TypedDict):
    db_schema_path: str
    db_drop_schema_path: str
    db_config_path: str
    redis_config_path: str


DEFAULT_VARS = EnvVariables(
    db_schema_path="database/schema.sql",
    db_drop_schema_path="database/drop_schema.sql",
    db_config_path="database.db_config",
    redis_config_path="redis.config",
)


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session):
    os.environ["ENVIRON"] = "test"
    os.environ["DB_SCHEMA"] = DEFAULT_VARS["db_schema_path"]
    os.environ["DB_DROP_SCHEMA"] = DEFAULT_VARS["db_drop_schema_path"]
    os.environ["DB_CONFIG"] = DEFAULT_VARS["db_config_path"]
    os.environ["REDIS_CONFIG"] = DEFAULT_VARS["db_config_path"]


@pytest_asyncio.fixture
async def pg(pool: PostgresConnection) -> AsyncGenerator[PostgresConnection, None]:
    await drop_tables(pool, os.environ["DB_DROP_SCHEMA"])
    await create_tables(pool, os.environ["DB_SCHEMA"])
    yield pool
    await drop_tables(pool, os.environ["DB_DROP_SCHEMA"])
    await pool._close_pool()


@pytest_asyncio.fixture
async def pool() -> PostgresConnection:
    db_config = importlib.import_module(os.environ["DB_CONFIG"])
    pg = db_config.pg
    await pg.create_pool()
    return pg.get()


@pytest.fixture(scope="session")
def faker():
    return Faker()


@pytest_asyncio.fixture
async def redis() -> AsyncGenerator[RedisConnection, None]:
    redis_config = importlib.import_module(os.environ["REDIS_CONFIG"])
    redis_client = redis_config.redis_client
    redis_client.create_pool()
    client = redis_client.get()
    await client.execute_command("FLUSHDB")
    yield client
    await client.execute_command("FLUSHDB")
    await client._redis.close()


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    """Create an instance of the default event loop for each test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
