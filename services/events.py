"""Audit-trail helper used by every service. Kept in its own module so the
failure detector and recovery planner do not import each other."""
from datetime import datetime, timezone

from websocket.manager import ws_manager


async def log_event(conn, session_id: int | None, level: str, message: str):
    await conn.execute(
        "INSERT INTO session_events (session_id, level, message) VALUES ($1,$2,$3)",
        session_id, level, message
    )
    await ws_manager.broadcast("LOG_ENTRY", {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "message": message,
    })
