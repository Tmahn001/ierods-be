"""Shared asyncpg connection pool.
Opening a fresh connection on every 30-second tick would exhaust PostgreSQL
within an hour, so every caller goes through get_pool()."""
import asyncpg
from config import DATABASE_URL

_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool


async def get_pool() -> asyncpg.Pool:
    if _pool is None:
        await init_pool()
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
