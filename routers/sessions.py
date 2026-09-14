from datetime import date
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from database import get_pool
from websocket.manager import ws_manager
from services.events import log_event
from utils import row_to_dict

router = APIRouter()

SESSION_COLS = """session_id, course_code, exam_date, total_enrolled, exam_duration_min,
  expected_failure_rate, min_processor_score, pv_alpha, pv_beta, pv_gamma, hall_ids,
  started_at, ended_at, first_admission_at, last_submission_at, is_active"""


class SessionIn(BaseModel):
    courseCode: str = Field(min_length=1, max_length=15)
    examDate: date
    totalEnrolled: int = Field(ge=0)
    examDurationMin: int = Field(gt=0)
    hallIds: list[str] = Field(min_length=1)
    expectedFailureRate: float = Field(default=0.11, ge=0, lt=1)
    minProcessorScore: float = Field(default=0.0, ge=0, le=1)
    pvAlpha: float = 0.5
    pvBeta: float = 0.3
    pvGamma: float = 0.2


def _session(row) -> dict:
    return row_to_dict(row)


@router.get("")
async def list_sessions():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"SELECT {SESSION_COLS} FROM exam_sessions ORDER BY session_id DESC")
    return [_session(r) for r in rows]


@router.get("/active")
async def active_session():
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT {SESSION_COLS} FROM exam_sessions WHERE is_active = TRUE")
    return _session(row) if row else None


@router.get("/{session_id}")
async def get_session(session_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT {SESSION_COLS} FROM exam_sessions WHERE session_id = $1", session_id)
    if not row:
        raise HTTPException(404, "Unknown session")
    return _session(row)


@router.post("", status_code=201)
async def create_session(body: SessionIn):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""INSERT INTO exam_sessions
                 (course_code, exam_date, total_enrolled, exam_duration_min,
                  expected_failure_rate, min_processor_score, pv_alpha, pv_beta, pv_gamma, hall_ids)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
               RETURNING {SESSION_COLS}""",
            body.courseCode.upper().strip(), body.examDate, body.totalEnrolled, body.examDurationMin,
            body.expectedFailureRate, body.minProcessorScore, body.pvAlpha, body.pvBeta, body.pvGamma,
            body.hallIds,
        )
    return _session(row)


@router.post("/{session_id}/start")
async def start_session(session_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            other = await conn.fetchval(
                "SELECT session_id FROM exam_sessions WHERE is_active = TRUE AND session_id != $1", session_id)
            if other:
                raise HTTPException(409, f"Session {other} is already active; end it first")
            row = await conn.fetchrow(
                f"""UPDATE exam_sessions
                   SET is_active = TRUE, started_at = COALESCE(started_at, NOW()), ended_at = NULL
                   WHERE session_id = $1 RETURNING {SESSION_COLS}""",
                session_id)
            if not row:
                raise HTTPException(404, "Unknown session")
            # Utilisation (Eq. 3.11) is a per-session quantity: reset the
            # cumulative counters so yesterday's hours do not leak in.
            await conn.execute(
                "UPDATE workstations SET cumulative_occupied_sec = 0, cumulative_available_sec = 0")
            n_students = await conn.fetchval(
                "SELECT COUNT(*) FROM students WHERE session_id = $1", session_id)
            await log_event(conn, session_id, "INFO",
                f"Session started: {row['course_code']} — {row['total_enrolled']} enrolled, "
                f"{n_students} on roster, {row['exam_duration_min']} min exam, "
                f"halls {', '.join(row['hall_ids'])}")
    return _session(row)


@router.post("/{session_id}/end")
async def end_session(session_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""UPDATE exam_sessions SET is_active = FALSE, ended_at = NOW()
               WHERE session_id = $1 RETURNING {SESSION_COLS}""",
            session_id)
        if not row:
            raise HTTPException(404, "Unknown session")
        await log_event(conn, session_id, "INFO", f"Session ended: {row['course_code']}")
    await ws_manager.broadcast("METRICS_UPDATE", None)
    return _session(row)
