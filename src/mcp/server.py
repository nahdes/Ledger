"""
src/mcp/server.py

Phase 5: MCP Server entry point.
Exposes The Ledger as an MCP server with 8 tools (command side) and
6 resources (query side) for AI agent consumption.

Start with:
    python -m src.mcp.server

Or import and run in your own asyncio app:
    from src.mcp.server import create_server
    server = await create_server(store, pool, daemon)
"""
from __future__ import annotations

import asyncio
import logging
import os

import asyncpg
from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.stdio import stdio_server

from src.event_store import EventStore
from src.mcp.resources import register_resources
from src.mcp.tools import register_tools
from src.projections.agent_performance import AgentPerformanceLedgerProjection
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.compliance_audit import ComplianceAuditViewProjection
from src.projections.daemon import ProjectionDaemon
from src.upcasting.registry import default_registry
from src.upcasting.upcasters import *  # noqa: F401,F403 — registers upcasters

load_dotenv()
logger = logging.getLogger(__name__)


async def create_server(
    store: EventStore,
    pool: asyncpg.Pool,
    daemon: ProjectionDaemon,
) -> Server:
    """
    Build and return a configured MCP Server with all tools and resources
    registered. The daemon is passed through to health resource queries.
    """
    app = Server("apex-ledger")
    register_tools(app, store)
    register_resources(app, store, pool, daemon)
    return app


async def main() -> None:
    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://ledger:ledger_dev_secret@localhost:5432/ledger",
    )
    pool = await asyncpg.create_pool(dsn=database_url, min_size=2, max_size=10)

    store = EventStore(
        pool=pool,
        upcaster_registry=default_registry,
        outbox_destinations=["redis-streams"],
    )

    projections = [
        ApplicationSummaryProjection(),
        AgentPerformanceLedgerProjection(),
        ComplianceAuditViewProjection(),
    ]
    daemon = ProjectionDaemon(store=store, pool=pool, projections=projections)

    # Start daemon as background task
    asyncio.create_task(daemon.run_forever(poll_interval_ms=500))
    logger.info("ProjectionDaemon started")

    server = await create_server(store, pool, daemon)
    logger.info("MCP server starting on stdio")

    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())