"""Classify SINGLE vs CIRCUIT failures, suppress login storms, identify
disrupted students from the session log."""
from datetime import datetime, timezone

from database import get_pool
from websocket.manager import ws_manager
from services.events import log_event

LOGIN_STORM_WINDOW_MIN = 2
LOGIN_STORM_MIN_STATIONS = 5
CIRCUIT_FAILURE_MIN_STATIONS = 5


async def handle_new_failures(failed: list[dict]):
    """`failed` rows carry station_id, hall_id, ups_circuit_id.
    The stations are already marked FAILED by the caller."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT session_id, exam_duration_min FROM exam_sessions WHERE is_active = TRUE"
        )
        if not session:
            return None

        station_ids = [f["station_id"] for f in failed]

        # Suppress login storms: a burst of silence right after a batch was
        # admitted is network saturation, not hardware failure. Keyed off the
        # admission timestamp, which is always written, so the window can
        # always be computed.
        recent_batch = await conn.fetchval(
            """SELECT COUNT(*) FROM batches
               WHERE session_id = $1
                 AND admitted_at > NOW() - make_interval(mins => $2::int)""",
            session["session_id"], LOGIN_STORM_WINDOW_MIN
        )
        if recent_batch and len(failed) >= LOGIN_STORM_MIN_STATIONS:
            await conn.execute(
                """UPDATE workstations w SET status = CASE
                     WHEN EXISTS (SELECT 1 FROM students s
                                  WHERE s.current_station = w.station_id
                                    AND s.status = 'IN_SESSION') THEN 'OCCUPIED'
                     ELSE 'AVAILABLE' END,
                   last_heartbeat = NOW()
                   WHERE station_id = ANY($1) AND status = 'FAILED'""",
                station_ids
            )
            await log_event(conn, session["session_id"], "WARN",
                f"Login storm suppressed: {len(station_ids)} stations silent "
                f"within {LOGIN_STORM_WINDOW_MIN} min of batch admission")
            return None

        circuits = {f["ups_circuit_id"] for f in failed}
        failure_type = ("CIRCUIT"
                        if len(circuits) == 1 and len(failed) >= CIRCUIT_FAILURE_MIN_STATIONS
                        else "SINGLE")

        affected = await conn.fetch(
            """SELECT matric_number, current_station, session_start
               FROM students
               WHERE current_station = ANY($1) AND status = 'IN_SESSION'""",
            station_ids
        )
        affected_matrics = [r["matric_number"] for r in affected]

        if affected_matrics:
            await conn.execute(
                """UPDATE students SET status = 'DISRUPTED',
                          disruption_count = disruption_count + 1
                   WHERE matric_number = ANY($1)""",
                affected_matrics
            )

        event = await conn.fetchrow(
            """INSERT INTO failure_events
                 (session_id, station_ids, failure_type, affected_matrics)
               VALUES ($1, $2, $3, $4)
               RETURNING event_id, detected_at""",
            session["session_id"], station_ids, failure_type, affected_matrics
        )

        now = datetime.now(timezone.utc)
        duration = session["exam_duration_min"]
        affected_payload = []
        for r in affected:
            elapsed = (now - r["session_start"]).total_seconds() / 60 if r["session_start"] else 0
            affected_payload.append({
                "matricNumber": r["matric_number"],
                "stationId": r["current_station"],
                "elapsedMinutes": round(elapsed),
                "remainingMinutes": max(0, round(duration - elapsed)),
            })

        await log_event(conn, session["session_id"], "ERROR",
            f"{failure_type} failure: {len(station_ids)} station(s) down "
            f"({', '.join(station_ids[:4])}{'…' if len(station_ids) > 4 else ''}), "
            f"{len(affected_matrics)} student(s) disrupted")

    for f in failed:
        await ws_manager.broadcast("WORKSTATION_UPDATE", {
            "stationId": f["station_id"], "status": "FAILED",
        })

    payload = {
        "eventId": event["event_id"],
        "stationIds": station_ids,
        "failureType": failure_type,
        "detectedAt": event["detected_at"].isoformat(),
        "resolvedAt": None,
        "affectedStudents": affected_payload,
        "recoveryPlan": None,
    }
    await ws_manager.broadcast("FAILURE_DETECTED", payload)
    return payload
