import pytest
import os
import pytest_asyncio
from python_tools.postgres_tools import PostgresConnection
from .tools import drop_tables, create_tables
from typing import AsyncGenerator
from faker import Faker
import importlib
from typing import TypedDict


class EnvVariables(TypedDict):
    db_schema_path: str
    db_drop_schema_path: str
    db_config_path: str


@pytest.fixture(scope="session")
def environ_variables() -> EnvVariables:
    return EnvVariables(
        db_schema_path='database/schema.sql',
        db_drop_schema_path='database/drop_schema.sql',
        db_config_path='database.db_config'
    )


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session, environ_variables: EnvVariables):
    os.environ['ENVIRON'] = 'test'
    os.environ['DB_SCHEMA'] = environ_variables['db_schema_path']
    os.environ['DB_DROP_SCHEMA'] = environ_variables['db_drop_schema_path']
    os.environ['DB_CONFIG'] = environ_variables['db_config_path']


@pytest_asyncio.fixture
async def pg(pool: PostgresConnection) -> AsyncGenerator[PostgresConnection, None]:
    await drop_tables(pool, os.environ['DB_DROP_SCHEMA'])
    await create_tables(pool, os.environ['DB_SCHEMA'])
    yield pool
    await drop_tables(pool, os.environ['DB_DROP_SCHEMA'])
    await pool._close_pool()


@pytest_asyncio.fixture
async def pool() -> PostgresConnection:
    db_config = importlib.import_module(os.environ['DB_CONFIG'])
    pg = db_config.pg
    await pg.create_pool()
    return pg.get()


@pytest.fixture(scope="session")
def faker():
    return Faker()
