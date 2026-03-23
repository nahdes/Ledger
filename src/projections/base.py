"""
src/projections/base.py

Base class for all projections. A projection handles a set of event types
and updates a read model table. The ProjectionDaemon routes events to
subscribed projections and manages checkpointing.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any
import asyncpg


class Projection(ABC):
    """Base class for all async projections."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique projection name — must match projection_checkpoints.projection_name."""

    @property
    @abstractmethod
    def event_types(self) -> set[str]:
        """Set of event types this projection handles. Others are ignored."""

    @abstractmethod
    async def handle(self, event: Any, conn: asyncpg.Connection) -> None:
        """
        Process one event and update the read model.
        Called inside a transaction managed by the daemon.
        Must be idempotent — replaying the same event must produce the same state.
        """

    async def initialize(self, conn: asyncpg.Connection) -> None:
        """
        Called once when the daemon starts. Create tables, indexes, etc.
        Default is a no-op since tables are created by schema migration.
        """