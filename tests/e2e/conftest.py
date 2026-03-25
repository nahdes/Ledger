"""tests/e2e/conftest.py — fixtures for end-to-end tests."""
from __future__ import annotations  # ✓ Correct (with double underscores)
import os
import asyncpg
import pytest_asyncio
from dotenv import load_dotenv
from src.event_store import EventStore
from src.upcasting.registry import UpcasterRegistry

load_dotenv()

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test",
)

@pytest_asyncio.fixture(scope="function")  # ← Explicitly match pytest-asyncio setting
async def db_pool():
    pool = await asyncpg.create_pool(dsn=TEST_DATABASE_URL, min_size=2, max_size=10)
    yield pool
    await pool.close()

@pytest_asyncio.fixture(autouse=True)
async def clean_db(db_pool):
    """Truncate all event store tables before each e2e test."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE
                outbox, events, event_streams, projection_checkpoints
            RESTART IDENTITY CASCADE
            """
        )
    yield

@pytest_asyncio.fixture
async def store(db_pool) -> EventStore:
    """Real EventStore backed by the test database."""
    return EventStore(
        pool=db_pool,
        upcaster_registry=UpcasterRegistry(),
        outbox_destinations=["test"],
    )