"""Produce and execute relocation plans with soft-locked seats.

Two things to get right here. JSONB columns must receive json.dumps output,
not str() of a list. And the plan soft-locks the seats it allocates so two
simultaneous failures cannot both promise the same machine."""
import json
from datetime import datetime, timezone

from database import get_pool
from websocket.manager import ws_manager
from services.events import log_event


def _as_list(value) -> list:
    """asyncpg returns JSONB as text unless a codec is registered."""
    if value is None:
        return []
    if isinstance(value, (list, dict)):
        return value
    return json.loads(value)


def hall_of(station_id: str) -> str:
    """'ICT-A-R03-S12' -> 'ICT-A'. Falls back to the whole id if unparseable."""
    parts = station_id.split("-")
    return "-".join(parts[:2]) if len(parts) >= 2 else station_id


async def generate_recovery_plan(event_id: int) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            event = await conn.fetchrow(
                "SELECT * FROM failure_events WHERE event_id = $1", event_id
            )
            session = await conn.fetchrow(
                """SELECT session_id, exam_duration_min, min_processor_score
                   FROM exam_sessions WHERE is_active = TRUE"""
            )
            if not event or not session:
                raise ValueError("No active session or unknown event")
            if event["resolved_at"] is not None:
                raise ValueError("Failure event already resolved")

            duration = session["exam_duration_min"]
            min_score = session["min_processor_score"]
            now = datetime.now(timezone.utc)

            students = await conn.fetch(
                """SELECT matric_number, current_station, session_start
                   FROM students WHERE matric_number = ANY($1)""",
                event["affected_matrics"]
            )
            # Deepest into the exam recovers first: most to lose
            students = sorted(
                students,
                key=lambda r: (now - (r["session_start"] or now)).total_seconds(),
                reverse=True
            )

            # Seats already promised by other open plans are excluded
            reserved_elsewhere = await conn.fetchval(
                """SELECT COALESCE(array_agg(s), '{}')
                   FROM recovery_plans rp, unnest(rp.reserved_stations) s
                   WHERE rp.approved_at IS NULL AND rp.event_id != $1""",
                event_id
            ) or []

            # Hard Constraint 3 (Ch.3 Eq. 3.4): never relocate a student to a
            # hardware-incompatible station.
            free = await conn.fetch(
                """SELECT station_id, hall_id, processor_score
                   FROM workstations
                   WHERE status = 'AVAILABLE'
                     AND station_id != ALL($1::varchar[])
                     AND processor_score >= $2
                   ORDER BY processor_score DESC, hall_id, row_number, seat_number""",
                reserved_elsewhere, min_score
            )

            assignments, resits, reserved = [], [], []
            pool_iter = list(free)

            for s in students:
                start = s["session_start"] or now
                elapsed = round((now - start).total_seconds() / 60)
                remaining = max(0, duration - elapsed)
                original_hall = hall_of(s["current_station"] or "")

                # Prefer a seat in the same hall to avoid physical relocation
                same_hall = [w for w in pool_iter if w["hall_id"] == original_hall]
                chosen = same_hall[0] if same_hall else (pool_iter[0] if pool_iter else None)

                if chosen is None:
                    resits.append({
                        "matricNumber": s["matric_number"],
                        "reason": "NO_FREE_STATION",
                        "elapsedMinutes": elapsed,
                    })
                    continue

                pool_iter.remove(chosen)
                reserved.append(chosen["station_id"])
                hall_change = chosen["hall_id"] != original_hall
                assignments.append({
                    "matricNumber": s["matric_number"],
                    "fromStation": s["current_station"],
                    "targetHall": chosen["hall_id"],
                    "instruction": (
                        f"Reseat at any free station in {chosen['hall_id']}"
                        + (" — REQUIRES HALL CHANGE" if hall_change else "")
                    ),
                    "requiresHallChange": hall_change,
                    "elapsedMinutes": elapsed,
                    "remainingMinutes": remaining,
                })

            hall_changes = sum(1 for a in assignments if a["requiresHallChange"])
            estimated = 3 + (hall_changes * 2) + (10 if resits else 0)
            confidence = 0.95 if not resits else 0.60

            plan = await conn.fetchrow(
                """INSERT INTO recovery_plans
                     (event_id, assignments, resit_flags, reserved_stations,
                      confidence_score, estimated_disruption_min)
                   VALUES ($1, $2::jsonb, $3::jsonb, $4, $5, $6)
                   ON CONFLICT (event_id) DO UPDATE
                     SET assignments = EXCLUDED.assignments,
                         resit_flags = EXCLUDED.resit_flags,
                         reserved_stations = EXCLUDED.reserved_stations,
                         confidence_score = EXCLUDED.confidence_score,
                         estimated_disruption_min = EXCLUDED.estimated_disruption_min,
                         generated_at = NOW()
                   RETURNING plan_id, generated_at""",
                event_id,
                json.dumps(assignments),
                json.dumps(resits),
                reserved,
                confidence,
                estimated,
            )

            await log_event(conn, session["session_id"], "INFO",
                f"Recovery plan #{plan['plan_id']} for event #{event_id}: "
                f"{len(assignments)} relocation(s), {len(resits)} resit flag(s), "
                f"est. {estimated} min")

    payload = {
        "planId": plan["plan_id"],
        "eventId": event_id,
        "assignments": assignments,
        "resitFlags": resits,
        "estimatedDisruptionMin": estimated,
        "confidenceScore": confidence,
        "generatedAt": plan["generated_at"].isoformat(),
        "approvedAt": None,
    }
    await ws_manager.broadcast("RECOVERY_PLAN_READY", payload)
    return payload


async def approve_recovery_plan(plan_id: int, approved_by: str = "admin") -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            plan = await conn.fetchrow(
                """UPDATE recovery_plans
                   SET approved_at = NOW(), approved_by = $2
                   WHERE plan_id = $1 AND approved_at IS NULL
                   RETURNING *""",
                plan_id, approved_by
            )
            if not plan:
                raise ValueError("Plan not found or already approved")
            assignments = _as_list(plan["assignments"])
            resits = _as_list(plan["resit_flags"])

            # Relocated students return to IN_SESSION with their clock preserved.
            # session_start is deliberately NOT reset: the exam server holds the
            # checkpoint, so elapsed time carries over. current_station is
            # updated when the exam server reports their login on the new seat.
            for a in assignments:
                await conn.execute(
                    "UPDATE students SET status = 'IN_SESSION' WHERE matric_number = $1",
                    a["matricNumber"]
                )
            for r in resits:
                await conn.execute(
                    "UPDATE students SET status = 'RESIT_REQUIRED', current_station = NULL "
                    "WHERE matric_number = $1",
                    r["matricNumber"]
                )

            await conn.execute(
                "UPDATE failure_events SET resolved_at = NOW() WHERE event_id = $1",
                plan["event_id"]
            )
            session_id = await conn.fetchval(
                "SELECT session_id FROM failure_events WHERE event_id = $1", plan["event_id"]
            )
            await log_event(conn, session_id, "INFO",
                f"Recovery plan #{plan_id} approved by {approved_by}: "
                f"{len(assignments)} student(s) reseated, {len(resits)} flagged for resit")

    await ws_manager.broadcast("FAILURE_RESOLVED", {"eventId": plan["event_id"]})
    return {"planId": plan_id, "approvedAt": plan["approved_at"].isoformat()}
