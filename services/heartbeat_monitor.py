"""Every 30s: accumulate time counters, mark silent machines FAILED, restore
machines whose heartbeat came back."""
import asyncio
from datetime import datetime, timezone

from config import POLL_INTERVAL_SEC, HEARTBEAT_TIMEOUT_SEC, HEARTBEAT_SIMULATE
from database import get_pool
from websocket.manager import ws_manager
from services.failure_detector import handle_new_failures


async def heartbeat_loop():
    while True:
        try:
            await run_heartbeat_cycle()
        except Exception as e:
            print(f"[heartbeat] error: {e}")
        await asyncio.sleep(POLL_INTERVAL_SEC)


async def run_heartbeat_cycle():
    pool = await get_pool()
    now = datetime.now(timezone.utc)

    async with pool.acquire() as conn:
        # 1. Accumulate time counters for the utilisation metric (Ch.3 Eq. 3.11).
        #    Only while a session is active, so idle overnight hours do not
        #    dilute the ratio.
        active = await conn.fetchval("SELECT session_id FROM exam_sessions WHERE is_active = TRUE")
        if active:
            await conn.execute(
                """UPDATE workstations
                   SET cumulative_available_sec = cumulative_available_sec + $1
                   WHERE status IN ('AVAILABLE','OCCUPIED','DEGRADED')""",
                POLL_INTERVAL_SEC
            )
            await conn.execute(
                """UPDATE workstations
                   SET cumulative_occupied_sec = cumulative_occupied_sec + $1
                   WHERE status = 'OCCUPIED'""",
                POLL_INTERVAL_SEC
            )

        # 1b. Dev stand-in for the ping service (see config.HEARTBEAT_SIMULATE).
        if HEARTBEAT_SIMULATE:
            await conn.execute(
                """UPDATE workstations SET last_heartbeat = $1
                   WHERE status != 'OFFLINE'
                     AND last_heartbeat IS NOT NULL
                     AND last_heartbeat >= $1::timestamptz - make_interval(secs => $2::float8)""",
                now, float(HEARTBEAT_TIMEOUT_SEC)
            )

        # 2. Mark machines that have gone silent
        newly_failed = await conn.fetch(
            """UPDATE workstations
               SET status = 'FAILED'
               WHERE status IN ('AVAILABLE','OCCUPIED','DEGRADED')
                 AND last_heartbeat IS NOT NULL
                 AND last_heartbeat < $1::timestamptz - make_interval(secs => $2::float8)
               RETURNING station_id, hall_id, ups_circuit_id""",
            now, float(HEARTBEAT_TIMEOUT_SEC)
        )

        # 3. Restore machines whose heartbeat came back
        recovered = await conn.fetch(
            """UPDATE workstations w
               SET status = CASE
                     WHEN EXISTS (
                       SELECT 1 FROM students s
                       WHERE s.current_station = w.station_id AND s.status = 'IN_SESSION'
                     ) THEN 'OCCUPIED' ELSE 'AVAILABLE' END
               WHERE w.status = 'FAILED'
                 AND w.last_heartbeat >= $1::timestamptz - make_interval(secs => $2::float8)
               RETURNING w.station_id, w.status, w.last_heartbeat""",
            now, float(HEARTBEAT_TIMEOUT_SEC)
        )

    for row in recovered:
        await ws_manager.broadcast("WORKSTATION_UPDATE", {
            "stationId": row["station_id"],
            "status": row["status"],
            "lastHeartbeat": row["last_heartbeat"].isoformat() if row["last_heartbeat"] else None,
        })

    if newly_failed:
        await handle_new_failures([dict(r) for r in newly_failed])
