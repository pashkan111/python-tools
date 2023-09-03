import pytest
import os
import pytest_asyncio
from python_tools.postgres_tools import PostgresConnection
from .tools import drop_tables, create_tables
from typing import AsyncGenerator
from faker import Faker
import importlib


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session):
    os.environ['ENVIRON'] = 'test'


@pytest_asyncio.fixture
async def pg(pool: PostgresConnection) -> AsyncGenerator[PostgresConnection, None]:
    await drop_tables(pool)
    await create_tables(pool)
    yield pool
    await drop_tables(pool)
    await pool._close_pool()


@pytest_asyncio.fixture
async def pool() -> PostgresConnection:
    db_config = importlib.import_module('database.db_config')
    pg = db_config.pg
    await pg.create_pool()
    return pg.get()


@pytest.fixture(scope="session")
def faker():
    return Faker()
