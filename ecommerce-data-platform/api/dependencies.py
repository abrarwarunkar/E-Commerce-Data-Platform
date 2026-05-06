"""
Database connection pool and dependency injection for FastAPI.
Data Lineage: api/dependencies.py -> all API routers
"""

import os
from typing import AsyncGenerator

import asyncpg
from fastapi import Depends

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ecommerce:ecommerce_secret@postgres:5432/ecommerce_dw",
)

_pool: asyncpg.Pool | None = None


async def init_db_pool() -> None:
    """Initialize the async connection pool on startup."""
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )


async def close_db_pool() -> None:
    """Close the connection pool on shutdown."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    """FastAPI dependency — yields a DB connection from the pool."""
    async with _pool.acquire() as conn:
        yield conn
