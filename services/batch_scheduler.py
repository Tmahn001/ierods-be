"""Compute B_optimal (Ch.3 Eq. 3.8), decide admission readiness, record
admissions. HOW MANY seats to fill lives here; WHOSE names to call comes from
priority_queue."""
from datetime import datetime, timezone

from config import BATCH_READY_THRESHOLD
from services.priority_queue import compute_priority_order


async def compute_recommendation(conn) -> dict | None:
    session = await conn.fetchrow(
        "SELECT * FROM exam_sessions WHERE is_active = TRUE"
    )
    if not session:
        return None

    sid = session["session_id"]

    free = await conn.fetchval(
        "SELECT COUNT(*) FROM workstations WHERE status = 'AVAILABLE'"
    )
    completed = await conn.fetchval(
        "SELECT COUNT(*) FROM students WHERE session_id = $1 AND status = 'COMPLETED'", sid
    )
    in_session = await conn.fetchval(
        "SELECT COUNT(*) FROM students WHERE session_id = $1 AND status IN ('IN_SESSION','DISRUPTED')", sid
    )

    # The one formula that governs queue depth (Section 0).
    queue_depth = max(0, session["total_enrolled"] - completed - in_session)

    # B_optimal = floor(free_stations × (1 − expected_failure_rate))  — Ch.3 Eq. 3.8
    failure_rate = session["expected_failure_rate"]
    safe_size = int(free * (1 - failure_rate))
    buffer = free - safe_size
    safe_size = min(safe_size, queue_depth)

    # Dead Air: how long has capacity been sitting idle since the last admission?
    last_admit = await conn.fetchval(
        "SELECT MAX(admitted_at) FROM batches WHERE session_id = $1", sid
    )
    idle_gap = 0
    if last_admit and free >= BATCH_READY_THRESHOLD:
        idle_gap = int((datetime.now(timezone.utc) - last_admit).total_seconds())

    ready = free >= BATCH_READY_THRESHOLD and queue_depth > 0 and safe_size > 0

    # WHICH specific students to call next, ranked by Priority Value — Ch.3 Eq. 3.9.
    ranked = await compute_priority_order(conn, sid)
    admission_order = ranked[:safe_size] if safe_size > 0 else []

    if queue_depth == 0:
        reasoning = f"{free} station(s) free. No students left to admit."
    else:
        reasoning = (
            f"{free} station(s) free. Holding {buffer} back as failure buffer "
            f"({failure_rate:.0%} expected rate). Safe to admit {safe_size}. "
            f"{queue_depth} student(s) still to be processed."
        )
        if free < BATCH_READY_THRESHOLD:
            reasoning += f" Waiting for at least {BATCH_READY_THRESHOLD} free stations."

    return {
        "readyToAdmit": ready,
        "recommendedSize": safe_size,
        "freeStations": free,
        "bufferHeldBack": buffer,
        "queueDepth": queue_depth,
        "idleGapSec": idle_gap,
        "admissionOrder": admission_order,
        "reasoning": reasoning,
    }


async def record_admission(conn, size: int, admitted_by: str = "admin") -> dict:
    session = await conn.fetchrow("SELECT session_id FROM exam_sessions WHERE is_active = TRUE")
    if not session:
        raise ValueError("No active session")
    sid = session["session_id"]

    free = await conn.fetchval("SELECT COUNT(*) FROM workstations WHERE status = 'AVAILABLE'")
    last_admit = await conn.fetchval(
        "SELECT MAX(admitted_at) FROM batches WHERE session_id = $1", sid
    )
    idle_gap = int((datetime.now(timezone.utc) - last_admit).total_seconds()) if last_admit else 0

    batch_no = await conn.fetchval(
        "SELECT COALESCE(MAX(batch_number),0)+1 FROM batches WHERE session_id = $1", sid
    )
    row = await conn.fetchrow(
        """INSERT INTO batches
             (session_id, batch_number, announced_size,
              free_stations_at_admission, idle_gap_sec, admitted_by)
           VALUES ($1,$2,$3,$4,$5,$6)
           RETURNING batch_id, batch_number, announced_size, admitted_at,
                     free_stations_at_admission, idle_gap_sec""",
        sid, batch_no, size, free, idle_gap, admitted_by
    )
    return {
        "batchId": row["batch_id"],
        "batchNumber": row["batch_number"],
        "announcedSize": row["announced_size"],
        "admittedAt": row["admitted_at"].isoformat(),
        "freeStationsAtAdmission": row["free_stations_at_admission"],
        "idleGapSec": row["idle_gap_sec"],
    }
