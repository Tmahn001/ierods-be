from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Literal

from config import HEARTBEAT_TIMEOUT_SEC
from database import get_pool
from websocket.manager import ws_manager
from services.failure_detector import handle_new_failures
from utils import row_to_dict

router = APIRouter()

# currentMatric and sessionStartTime are NOT columns on workstations.
# They come from this LEFT JOIN against the students table.
WORKSTATION_QUERY = """
SELECT w.station_id, w.hall_id, w.row_number, w.seat_number, w.ups_circuit_id,
       w.processor_score, w.status, w.last_heartbeat,
       s.matric_number AS current_matric, s.session_start AS session_start_time
FROM workstations w
LEFT JOIN students s
  ON s.current_station = w.station_id AND s.status IN ('IN_SESSION','DISRUPTED')
"""
ORDER = " ORDER BY w.hall_id, w.row_number, w.seat_number"


class HeartbeatIn(BaseModel):
    stationId: str
    statusCode: Literal["OK", "SLOW", "TIMEOUT"] = "OK"
    responseMs: int | None = None


class StatusPatch(BaseModel):
    status: Literal["AVAILABLE", "OCCUPIED", "FAILED", "DEGRADED", "OFFLINE"]
    reason: str | None = Field(default=None, max_length=200)


@router.get("")
async def list_workstations():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(WORKSTATION_QUERY + ORDER)
    return [row_to_dict(r) for r in rows]


@router.get("/hall/{hall_id}")
async def list_hall(hall_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(WORKSTATION_QUERY + " WHERE w.hall_id = $1" + ORDER, hall_id)
    return [row_to_dict(r) for r in rows]


@router.get("/{station_id}")
async def get_workstation(station_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(WORKSTATION_QUERY + " WHERE w.station_id = $1", station_id)
    if not row:
        raise HTTPException(404, "Unknown station")
    return row_to_dict(row)


@router.post("/heartbeat")
async def heartbeat(body: HeartbeatIn):
    """Receives a ping result for one workstation. In production the pinger
    posts here every 30s per machine. A TIMEOUT is logged but does not refresh
    last_heartbeat, so the monitor will mark the machine FAILED in due course."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM workstations WHERE station_id = $1", body.stationId)
        if not exists:
            raise HTTPException(404, "Unknown station")
        await conn.execute(
            "INSERT INTO heartbeat_log (station_id, status_code, response_ms) VALUES ($1,$2,$3)",
            body.stationId, body.statusCode, body.responseMs
        )
        if body.statusCode == "TIMEOUT":
            return {"stationId": body.stationId, "recorded": True, "refreshed": False}

        # Refresh the heartbeat. Recovery from FAILED is left to the monitor
        # (Section 6.1 step 3) so a single ping does not flap a machine.
        # SLOW marks a free machine DEGRADED; OK clears DEGRADED.
        row = await conn.fetchrow(
            """UPDATE workstations SET
                 last_heartbeat = NOW(),
                 status = CASE
                   WHEN $2 = 'SLOW' AND status = 'AVAILABLE' THEN 'DEGRADED'
                   WHEN $2 = 'OK'   AND status = 'DEGRADED'  THEN 'AVAILABLE'
                   ELSE status END
               WHERE station_id = $1
               RETURNING station_id, status, last_heartbeat""",
            body.stationId, body.statusCode
        )
    await ws_manager.broadcast("WORKSTATION_UPDATE", {
        "stationId": row["station_id"], "status": row["status"],
        "lastHeartbeat": row["last_heartbeat"].isoformat(),
    })
    return {"stationId": row["station_id"], "status": row["status"], "recorded": True, "refreshed": True}


@router.patch("/{station_id}/status")
async def override_status(station_id: str, body: StatusPatch):
    """Admin manual override. Marking a machine FAILED runs the failure
    detector so disrupted students are identified; the heartbeat is cleared so
    the monitor does not immediately restore it."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT station_id, hall_id, ups_circuit_id, status FROM workstations WHERE station_id = $1",
            station_id)
        if not row:
            raise HTTPException(404, "Unknown station")
        if body.status in ("FAILED", "OFFLINE"):
            await conn.execute(
                "UPDATE workstations SET status = $2, last_heartbeat = NULL WHERE station_id = $1",
                station_id, body.status)
        else:
            await conn.execute(
                "UPDATE workstations SET status = $2, last_heartbeat = NOW() WHERE station_id = $1",
                station_id, body.status)
        session_id = await conn.fetchval("SELECT session_id FROM exam_sessions WHERE is_active = TRUE")
        if session_id:
            await conn.execute(
                "INSERT INTO session_events (session_id, level, message) VALUES ($1,'WARN',$2)",
                session_id,
                f"Admin override: {station_id} {row['status']} → {body.status}"
                + (f" ({body.reason})" if body.reason else ""))

    if body.status == "FAILED" and row["status"] != "FAILED":
        await handle_new_failures([{
            "station_id": station_id, "hall_id": row["hall_id"], "ups_circuit_id": row["ups_circuit_id"],
        }])
    else:
        await ws_manager.broadcast("WORKSTATION_UPDATE", {"stationId": station_id, "status": body.status})
    return {"stationId": station_id, "status": body.status}


@router.post("/{station_id}/simulate-silence")
async def simulate_silence(station_id: str):
    """DEV ONLY. Backdates last_heartbeat past the timeout so the next
    heartbeat cycle detects the station as FAILED through the normal path.
    Equivalent to the Section 10 step-12 check."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        ok = await conn.fetchval(
            """UPDATE workstations
               SET last_heartbeat = NOW() - make_interval(secs => $2::float8)
               WHERE station_id = $1 RETURNING station_id""",
            station_id, float(HEARTBEAT_TIMEOUT_SEC + 60))
    if not ok:
        raise HTTPException(404, "Unknown station")
    return {"stationId": station_id, "note": "will be detected on next heartbeat cycle"}
