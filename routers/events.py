from fastapi import APIRouter, Query

from database import get_pool

router = APIRouter()


@router.get("")
async def list_events(limit: int = Query(default=100, le=2000), sessionId: int | None = None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        if sessionId is None:
            sessionId = await conn.fetchval(
                "SELECT session_id FROM exam_sessions ORDER BY is_active DESC, session_id DESC LIMIT 1")
        rows = await conn.fetch(
            f"""SELECT event_time, level, message FROM session_events
                WHERE session_id = $1 ORDER BY event_time DESC, id DESC LIMIT {int(limit)}""",
            sessionId)
    return [{"timestamp": r["event_time"].isoformat(), "level": r["level"], "message": r["message"]}
            for r in rows]
