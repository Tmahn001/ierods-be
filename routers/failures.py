from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Query

from database import get_pool
from services.recovery_planner import generate_recovery_plan, _as_list
from utils import row_to_dict

router = APIRouter()


async def _build_events(conn, open_only: bool) -> list[dict]:
    session = await conn.fetchrow(
        "SELECT session_id, exam_duration_min FROM exam_sessions WHERE is_active = TRUE")
    if not session:
        return []
    where = "AND f.resolved_at IS NULL" if open_only else ""
    rows = await conn.fetch(
        f"""SELECT f.event_id, f.station_ids, f.failure_type, f.detected_at, f.resolved_at,
                   f.affected_matrics,
                   p.plan_id, p.assignments, p.resit_flags, p.confidence_score,
                   p.estimated_disruption_min, p.generated_at, p.approved_at
            FROM failure_events f
            LEFT JOIN recovery_plans p ON p.event_id = f.event_id
            WHERE f.session_id = $1 {where}
            ORDER BY f.detected_at DESC""",
        session["session_id"])

    now = datetime.now(timezone.utc)
    duration = session["exam_duration_min"]
    out = []
    for r in rows:
        students = await conn.fetch(
            "SELECT matric_number, current_station, session_start FROM students WHERE matric_number = ANY($1)",
            r["affected_matrics"])
        affected = []
        for s in students:
            elapsed = (now - s["session_start"]).total_seconds() / 60 if s["session_start"] else 0
            affected.append({
                "matricNumber": s["matric_number"],
                "stationId": s["current_station"],
                "elapsedMinutes": round(elapsed),
                "remainingMinutes": max(0, round(duration - elapsed)),
            })
        plan = None
        if r["plan_id"] is not None:
            plan = {
                "planId": r["plan_id"],
                "eventId": r["event_id"],
                "assignments": _as_list(r["assignments"]),
                "resitFlags": _as_list(r["resit_flags"]),
                "estimatedDisruptionMin": r["estimated_disruption_min"],
                "confidenceScore": r["confidence_score"],
                "generatedAt": r["generated_at"].isoformat(),
                "approvedAt": r["approved_at"].isoformat() if r["approved_at"] else None,
            }
        out.append({
            "eventId": r["event_id"],
            "stationIds": list(r["station_ids"]),
            "failureType": r["failure_type"],
            "detectedAt": r["detected_at"].isoformat(),
            "resolvedAt": r["resolved_at"].isoformat() if r["resolved_at"] else None,
            "affectedStudents": affected,
            "recoveryPlan": plan,
        })
    return out


@router.get("")
async def list_failures(open: bool = Query(default=False)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await _build_events(conn, open_only=open)


@router.post("/{event_id}/plan")
async def make_plan(event_id: int):
    try:
        return await generate_recovery_plan(event_id)
    except ValueError as e:
        raise HTTPException(409, str(e))
