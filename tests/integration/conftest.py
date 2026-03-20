"""
tests/integration/conftest.py

Integration test fixtures — real PostgreSQL via ledger-test-db (port 5433).

autouse=True clean_db truncates all tables before every test.
This guarantees test isolation without needing separate schemas.
"""
from __future__ import annotations

import pytest_asyncio

from src.event_store import EventStore
from src.upcasting.registry import UpcasterRegistry


@pytest_asyncio.fixture(autouse=True)
async def clean_db(db_pool):
    """Truncate all event store tables before each integration test."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE
                outbox,
                events,
                event_streams,
                projection_checkpoints
            RESTART IDENTITY CASCADE
            """
        )
    yield


@pytest_asyncio.fixture
async def store(db_pool) -> EventStore:
    """Real EventStore backed by the test database."""
    registry = UpcasterRegistry()
    return EventStore(
        pool                = db_pool,
        upcaster_registry   = registry,
        outbox_destinations = ["test-destination"],
    )


@pytest_asyncio.fixture
async def raw_conn(db_pool):
    """Raw asyncpg connection for direct SQL queries in tests."""
    async with db_pool.acquire() as conn:
        yield conn
