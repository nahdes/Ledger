from __future__ import annotations
import os
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest_asyncio
from dotenv import load_dotenv

load_dotenv()

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://ledger:ledger_dev_secret@localhost:5433/ledger_test",
)

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def make_app_id() -> str:
    return f"APEX-{uuid.uuid4().hex[:6].upper()}"


def make_session_id(agent_type: str = "cre") -> str:
    return f"sess-{agent_type}-{uuid.uuid4().hex[:8]}"


@pytest_asyncio.fixture
async def db_pool():
    pool = await asyncpg.create_pool(dsn=TEST_DATABASE_URL, min_size=2, max_size=10)
    yield pool
    await pool.close()