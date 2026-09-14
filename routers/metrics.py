from fastapi import APIRouter, HTTPException, Query

from config import MANUAL_RECOVERY_BASELINE_MIN
from database import get_pool
from services.metrics_engine import compute_metrics
from utils import row_to_dict

router = APIRouter()


@router.get("/live")
async def live():
    pool = await get_pool()
    async with pool.acquire() as conn:
        m = await compute_metrics(conn)
    if m is None:
        raise HTTPException(409, "No active session")
    return m


@router.get("/history")
async def history(sessionId: int | None = Query(default=None), limit: int = Query(default=2000, le=20000)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        if sessionId is None:
            sessionId = await conn.fetchval(
                "SELECT session_id FROM exam_sessions ORDER BY is_active DESC, session_id DESC LIMIT 1")
        rows = await conn.fetch(
            f"""SELECT recorded_at, throughput_rate, utilisation_ratio, failure_impact_factor,
                       queue_depth, students_in_session, students_completed,
                       stations_occupied, stations_available, stations_failed
                FROM metrics_snapshots WHERE session_id = $1
                ORDER BY recorded_at ASC LIMIT {int(limit)}""",
            sessionId)
    return {"sessionId": sessionId, "points": [row_to_dict(r) for r in rows]}


@router.get("/summary/{session_id}")
async def summary(session_id: int):
    """End-of-day report figures per Chapter 3 Eq. 3.10-3.12 plus Dead Air."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        session = await conn.fetchrow("SELECT * FROM exam_sessions WHERE session_id = $1", session_id)
        if not session:
            raise HTTPException(404, "Unknown session")
        counts = await conn.fetchrow(
            """SELECT
                 COUNT(*) FILTER (WHERE status='COMPLETED')      AS completed,
                 COUNT(*) FILTER (WHERE status='RESIT_REQUIRED') AS resits,
                 COUNT(*) FILTER (WHERE status='NOT_ADMITTED')   AS not_admitted,
                 COUNT(*) FILTER (WHERE status IN ('IN_SESSION','DISRUPTED')) AS in_session,
                 COUNT(*) FILTER (WHERE disruption_count > 0)    AS disrupted_once,
                 COUNT(*) AS roster
               FROM students WHERE session_id = $1""", session_id)
        failures = await conn.fetchrow(
            """SELECT COUNT(*) AS total,
                      COUNT(*) FILTER (WHERE failure_type='CIRCUIT') AS circuit,
                      COUNT(*) FILTER (WHERE resolved_at IS NULL) AS open,
                      AVG(EXTRACT(EPOCH FROM (resolved_at - detected_at))/60) AS avg_resolution_min,
                      COALESCE(SUM(array_length(station_ids,1)),0) AS stations_hit
               FROM failure_events WHERE session_id = $1""", session_id)
        batches = await conn.fetchrow(
            """SELECT COUNT(*) AS n, COALESCE(SUM(idle_gap_sec),0) AS dead_air_sec,
                      COALESCE(AVG(announced_size),0) AS avg_size
               FROM batches WHERE session_id = $1""", session_id)
        last = await conn.fetchrow(
            """SELECT utilisation_ratio, throughput_rate, failure_impact_factor
               FROM metrics_snapshots WHERE session_id = $1 ORDER BY recorded_at DESC LIMIT 1""",
            session_id)
        peaks = await conn.fetchrow(
            """SELECT MAX(students_in_session) AS peak_in_session, MAX(queue_depth) AS peak_queue,
                      MAX(stations_failed) AS peak_failed
               FROM metrics_snapshots WHERE session_id = $1""", session_id)

    first_adm, last_sub = session["first_admission_at"], session["last_submission_at"]
    makespan_min = ((last_sub - first_adm).total_seconds() / 60) if first_adm and last_sub else None
    completed = counts["completed"]
    throughput = (completed / (makespan_min / 60)) if makespan_min and makespan_min > 0 else 0.0
    avg_res = failures["avg_resolution_min"]
    fif = (float(avg_res) / MANUAL_RECOVERY_BASELINE_MIN) if avg_res is not None else None

    return {
        "session": row_to_dict(session),
        "students": row_to_dict(counts),
        "makespanMinutes": round(makespan_min, 1) if makespan_min is not None else None,
        "throughputRate": round(throughput, 1),
        "utilisationRatio": last["utilisation_ratio"] if last else None,
        "failureImpactFactor": round(fif, 3) if fif is not None else None,
        "manualRecoveryBaselineMin": MANUAL_RECOVERY_BASELINE_MIN,
        "failures": {
            "total": failures["total"], "circuit": failures["circuit"], "open": failures["open"],
            "stationsHit": int(failures["stations_hit"]),
            "avgResolutionMin": round(float(avg_res), 1) if avg_res is not None else None,
        },
        "batches": {
            "count": batches["n"], "deadAirSec": int(batches["dead_air_sec"]),
            "avgAnnouncedSize": round(float(batches["avg_size"]), 1),
        },
        "peaks": row_to_dict(peaks) if peaks else {},
    }
