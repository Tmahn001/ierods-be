"""Ingest login and submission events from the examination server.

This is the single most important integration point in the system: it is how
IERODS learns that a student sat down and started their exam."""
import asyncio
from datetime import datetime, timezone

from config import POLL_INTERVAL_SEC
from database import get_pool
from websocket.manager import ws_manager


async def session_log_loop():
    while True:
        try:
            await poll_examination_server()
        except Exception as e:
            print(f"[session_log] error: {e}")
        await asyncio.sleep(POLL_INTERVAL_SEC)


async def poll_examination_server():
    """
    INTEGRATION POINT.

    In production this reads the OAU examination server's session table and
    calls apply_login_event / apply_submit_event for every new row.

    For development and for the demo, events arrive instead through
    POST /api/students/session-event, so this loop only reconciles state.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT session_id FROM exam_sessions WHERE is_active = TRUE"
        )
        if not session:
            return
        # Reconciliation: a station that is OCCUPIED with nobody IN_SESSION or
        # DISRUPTED on it is stale (e.g. an admin override). Free it.
        stale = await conn.fetch(
            """UPDATE workstations w SET status = 'AVAILABLE'
               WHERE w.status = 'OCCUPIED'
                 AND NOT EXISTS (
                   SELECT 1 FROM students s
                   WHERE s.current_station = w.station_id
                     AND s.status IN ('IN_SESSION','DISRUPTED'))
               RETURNING w.station_id"""
        )
    for row in stale:
        await ws_manager.broadcast("WORKSTATION_UPDATE", {
            "stationId": row["station_id"], "status": "AVAILABLE",
            "currentMatric": None, "sessionStartTime": None,
        })


async def apply_login_event(conn, matric: str, station_id: str, session_id: int) -> dict:
    """Called when the exam server reports a student authenticated on a machine."""
    now = datetime.now(timezone.utc)
    # A relocated student keeps their original session_start: the exam server
    # holds the checkpoint, so elapsed time carries over (Section 6.4).
    await conn.execute(
        """UPDATE students
           SET status = 'IN_SESSION',
               current_station = $2,
               session_start = COALESCE(session_start, $3)
           WHERE matric_number = $1""",
        matric, station_id, now
    )
    await conn.execute(
        "UPDATE workstations SET status = 'OCCUPIED' WHERE station_id = $1",
        station_id
    )
    await conn.execute(
        """UPDATE exam_sessions
           SET first_admission_at = COALESCE(first_admission_at, $2)
           WHERE session_id = $1""",
        session_id, now
    )
    start = await conn.fetchval(
        "SELECT session_start FROM students WHERE matric_number = $1", matric
    )
    payload = {
        "stationId": station_id, "status": "OCCUPIED",
        "currentMatric": matric, "sessionStartTime": start.isoformat(),
    }
    await ws_manager.broadcast("WORKSTATION_UPDATE", payload)
    return payload


async def apply_submit_event(conn, matric: str, session_id: int) -> dict:
    """Called when the exam server reports a submission."""
    now = datetime.now(timezone.utc)
    # Read the station BEFORE clearing it; an UPDATE ... RETURNING would
    # return the post-update (NULL) value and never free the seat.
    old_station = await conn.fetchval(
        "SELECT current_station FROM students WHERE matric_number = $1", matric
    )
    await conn.execute(
        """UPDATE students
           SET status = 'COMPLETED', submission_time = $2, current_station = NULL
           WHERE matric_number = $1""",
        matric, now
    )
    if old_station:
        # Only free the seat if it is still healthy; a FAILED seat stays FAILED.
        freed = await conn.fetchval(
            """UPDATE workstations SET status = 'AVAILABLE'
               WHERE station_id = $1 AND status IN ('OCCUPIED','DEGRADED')
               RETURNING station_id""",
            old_station
        )
        if freed:
            await ws_manager.broadcast("WORKSTATION_UPDATE", {
                "stationId": freed, "status": "AVAILABLE",
                "currentMatric": None, "sessionStartTime": None,
            })
    await conn.execute(
        "UPDATE exam_sessions SET last_submission_at = $2 WHERE session_id = $1",
        session_id, now
    )
    return {"matricNumber": matric, "station": old_station, "submittedAt": now.isoformat()}
