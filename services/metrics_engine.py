"""Compute T, U and FIF per Chapter 3 equations 3.10-3.12 and broadcast every 30s.

Throughput follows Eq. 3.10, not a rolling-hour count. Utilisation uses the
cumulative counters on workstations, not an instantaneous snapshot. FIF is
None before any failure has been resolved; never zero, which would read as a
perfect score."""
import asyncio
from datetime import datetime, timezone

from config import POLL_INTERVAL_SEC, MANUAL_RECOVERY_BASELINE_MIN
from database import get_pool
from websocket.manager import ws_manager
from services.batch_scheduler import compute_recommendation


async def metrics_loop():
    while True:
        try:
            await compute_and_broadcast()
        except Exception as e:
            print(f"[metrics] error: {e}")
        await asyncio.sleep(POLL_INTERVAL_SEC)


async def compute_metrics(conn, session=None) -> dict | None:
    """Pure computation, no snapshot insert. Used by the loop and by
    GET /api/metrics/live."""
    if session is None:
        session = await conn.fetchrow("SELECT * FROM exam_sessions WHERE is_active = TRUE")
    if not session:
        return None
    sid = session["session_id"]
    now = datetime.now(timezone.utc)

    occupied = await conn.fetchval("SELECT COUNT(*) FROM workstations WHERE status='OCCUPIED'")
    available = await conn.fetchval("SELECT COUNT(*) FROM workstations WHERE status='AVAILABLE'")
    failed = await conn.fetchval("SELECT COUNT(*) FROM workstations WHERE status='FAILED'")

    completed = await conn.fetchval(
        "SELECT COUNT(*) FROM students WHERE session_id=$1 AND status='COMPLETED'", sid)
    in_session = await conn.fetchval(
        "SELECT COUNT(*) FROM students WHERE session_id=$1 AND status IN ('IN_SESSION','DISRUPTED')", sid)
    queue_depth = max(0, session["total_enrolled"] - completed - in_session)

    # Throughput — Chapter 3 Eq. 3.10:
    #   T = N_completed / (T_last_submission − T_first_admission)
    first_adm = session["first_admission_at"]
    last_sub = session["last_submission_at"]
    if first_adm and last_sub and completed > 0:
        hours = max((last_sub - first_adm).total_seconds() / 3600, 1 / 60)
        throughput = completed / hours
    else:
        throughput = 0.0

    # Utilisation — Chapter 3 Eq. 3.11:
    #   U = Σ occupied_time(w) / Σ available_time(w)
    totals = await conn.fetchrow(
        """SELECT COALESCE(SUM(cumulative_occupied_sec),0)  AS occ,
                  COALESCE(SUM(cumulative_available_sec),0) AS avail
           FROM workstations"""
    )
    occ, avail = float(totals["occ"]), float(totals["avail"])
    utilisation = (occ / avail) if avail > 0 else 0.0

    # FIF — Chapter 3 Eq. 3.12: D_IERODS / D_manual. NULL until a failure resolves.
    avg_resolution = await conn.fetchval(
        """SELECT AVG(EXTRACT(EPOCH FROM (resolved_at - detected_at))/60)
           FROM failure_events WHERE session_id=$1 AND resolved_at IS NOT NULL""",
        sid
    )
    fif = (float(avg_resolution) / MANUAL_RECOVERY_BASELINE_MIN) if avg_resolution is not None else None

    elapsed_min = int((now - session["started_at"]).total_seconds() / 60) if session["started_at"] else 0

    return {
        "sessionId": sid,
        "throughputRate": round(throughput, 1),
        "utilisationRatio": round(utilisation, 4),
        "failureImpactFactor": round(fif, 3) if fif is not None else None,
        "queueDepth": queue_depth,
        "studentsInSession": in_session,
        "studentsCompleted": completed,
        "stationsOccupied": occupied,
        "stationsAvailable": available,
        "stationsFailed": failed,
        "elapsedSessionMinutes": elapsed_min,
    }


async def compute_and_broadcast():
    pool = await get_pool()
    async with pool.acquire() as conn:
        metrics = await compute_metrics(conn)
        if not metrics:
            return
        raw_fif = None
        if metrics["failureImpactFactor"] is not None:
            raw_fif = metrics["failureImpactFactor"]
        await conn.execute(
            """INSERT INTO metrics_snapshots
                 (session_id, throughput_rate, utilisation_ratio, failure_impact_factor,
                  queue_depth, students_in_session, students_completed,
                  stations_occupied, stations_available, stations_failed)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
            metrics["sessionId"], metrics["throughputRate"], metrics["utilisationRatio"], raw_fif,
            metrics["queueDepth"], metrics["studentsInSession"], metrics["studentsCompleted"],
            metrics["stationsOccupied"], metrics["stationsAvailable"], metrics["stationsFailed"]
        )
        recommendation = await compute_recommendation(conn)

    await ws_manager.broadcast("METRICS_UPDATE", metrics)
    if recommendation:
        await ws_manager.broadcast("BATCH_RECOMMENDATION", recommendation)
