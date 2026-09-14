import csv
import io
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query
from pydantic import BaseModel
from typing import Literal

from database import get_pool
from services.session_log_reader import apply_login_event, apply_submit_event
from services.events import log_event
from utils import row_to_dict

router = APIRouter()

STUDENT_QUERY = """
SELECT matric_number, course_code, status, current_station,
       session_start AS session_start_time, submission_time, disruption_count, queued_at,
       CASE WHEN session_start IS NOT NULL AND status IN ('IN_SESSION','DISRUPTED')
            THEN EXTRACT(EPOCH FROM (NOW() - session_start))/60
            WHEN session_start IS NOT NULL AND submission_time IS NOT NULL
            THEN EXTRACT(EPOCH FROM (submission_time - session_start))/60
            ELSE NULL END AS elapsed_minutes
FROM students
"""


class SessionEventIn(BaseModel):
    matricNumber: str
    stationId: str | None = None
    event: Literal["LOGIN", "SUBMIT"]


@router.get("")
async def list_students(
    status: str | None = Query(default=None),
    limit: int = Query(default=5000, le=20000),
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        sid = await conn.fetchval("SELECT session_id FROM exam_sessions WHERE is_active = TRUE")
        params: list = [sid]
        where = "WHERE session_id = $1"
        if status:
            where += " AND status = $2"
            params.append(status.upper())
        rows = await conn.fetch(
            f"{STUDENT_QUERY} {where} ORDER BY queued_at, matric_number LIMIT {int(limit)}", *params)
    out = []
    for r in rows:
        d = row_to_dict(r)
        if d.get("elapsedMinutes") is not None:
            d["elapsedMinutes"] = round(d["elapsedMinutes"])
        out.append(d)
    return out


@router.post("/import")
async def import_roster(
    file: UploadFile = File(...),
    sessionId: int | None = Form(default=None),
):
    """Multipart CSV with a header row: matric_number,course_code.
    Rows are attached to the given session (or the active session, or the
    newest session if none is active). queued_at = load time, which is the
    WT(s,t) proxy used by the priority queue."""
    raw = await file.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(400, "CSV must be UTF-8")

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise HTTPException(400, "Empty CSV")
    fields = {f.strip().lower(): f for f in reader.fieldnames}
    if "matric_number" not in fields:
        raise HTTPException(400, "CSV needs a matric_number column")

    pool = await get_pool()
    async with pool.acquire() as conn:
        if sessionId is None:
            sessionId = await conn.fetchval(
                """SELECT session_id FROM exam_sessions
                   ORDER BY is_active DESC, session_id DESC LIMIT 1""")
        session = await conn.fetchrow(
            "SELECT session_id, course_code, total_enrolled FROM exam_sessions WHERE session_id = $1", sessionId)
        if not session:
            raise HTTPException(404, "No session to import into; create one first")

        rows, seen = [], set()
        for line in reader:
            matric = (line.get(fields["matric_number"]) or "").strip().upper()
            if not matric or matric in seen:
                continue
            seen.add(matric)
            course = (line.get(fields.get("course_code", ""), "") or session["course_code"]).strip().upper()
            rows.append((matric, session["session_id"], course))

        async with conn.transaction():
            # Upsert so re-importing a corrected roster is safe.
            await conn.executemany(
                """INSERT INTO students (matric_number, session_id, course_code)
                   VALUES ($1,$2,$3)
                   ON CONFLICT (matric_number) DO UPDATE
                     SET session_id = EXCLUDED.session_id, course_code = EXCLUDED.course_code""",
                rows)
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM students WHERE session_id = $1", session["session_id"])
            await log_event(conn, session["session_id"], "INFO",
                f"Roster imported: {len(rows)} student(s) from {file.filename}")

    return {
        "sessionId": session["session_id"],
        "imported": len(rows),
        "rosterSize": total,
        "totalEnrolled": session["total_enrolled"],
        "warning": (None if total == session["total_enrolled"]
                    else f"Roster has {total} students but session total_enrolled is {session['total_enrolled']}"),
    }


@router.post("/session-event")
async def session_event(body: SessionEventIn):
    """Dev/demo entry point for the examination-server feed (Section 6.2).
    In production the session_log_reader polls the exam server instead."""
    matric = body.matricNumber.strip().upper()
    pool = await get_pool()
    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT session_id FROM exam_sessions WHERE is_active = TRUE")
        if not session:
            raise HTTPException(409, "No active session")
        sid = session["session_id"]

        student = await conn.fetchrow(
            "SELECT matric_number, status, current_station FROM students WHERE matric_number = $1 AND session_id = $2",
            matric, sid)
        if not student:
            raise HTTPException(404, f"{matric} is not on the roster for the active session")

        async with conn.transaction():
            if body.event == "LOGIN":
                if not body.stationId:
                    raise HTTPException(400, "stationId is required for LOGIN")
                if student["status"] in ("COMPLETED",):
                    raise HTTPException(409, f"{matric} has already submitted")
                station = await conn.fetchrow(
                    "SELECT station_id, status FROM workstations WHERE station_id = $1", body.stationId)
                if not station:
                    raise HTTPException(404, "Unknown station")
                if station["status"] in ("FAILED", "OFFLINE"):
                    raise HTTPException(409, f"{body.stationId} is {station['status']}")
                occupant = await conn.fetchval(
                    """SELECT matric_number FROM students
                       WHERE current_station = $1 AND status IN ('IN_SESSION','DISRUPTED')
                         AND matric_number != $2""",
                    body.stationId, matric)
                if occupant:
                    raise HTTPException(409, f"{body.stationId} is occupied by {occupant}")
                result = await apply_login_event(conn, matric, body.stationId, sid)
                await log_event(conn, sid, "INFO",
                    f"{matric} logged in on {body.stationId}"
                    + (" (relocated)" if student["status"] == "IN_SESSION" and student["current_station"] != body.stationId else ""))
                return {"ok": True, "event": "LOGIN", **result}

            # SUBMIT
            if student["status"] not in ("IN_SESSION", "DISRUPTED"):
                raise HTTPException(409, f"{matric} is {student['status']}, not in session")
            result = await apply_submit_event(conn, matric, sid)
            await log_event(conn, sid, "INFO", f"{matric} submitted from {student['current_station']}")
            return {"ok": True, "event": "SUBMIT", **result}
