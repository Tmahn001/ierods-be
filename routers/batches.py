from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from database import get_pool
from services.batch_scheduler import compute_recommendation, record_admission
from services.events import log_event
from websocket.manager import ws_manager
from utils import row_to_dict

router = APIRouter()


class AdmitIn(BaseModel):
    size: int = Field(gt=0)
    admittedBy: str = "admin"


@router.get("/recommendation")
async def recommendation():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rec = await compute_recommendation(conn)
    if rec is None:
        raise HTTPException(409, "No active session")
    return rec


@router.post("/admit", status_code=201)
async def admit(body: AdmitIn):
    """Records that the administrator called `size` students into the hall.
    IERODS does not seat anyone: the exam server reports each login later."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                batch = await record_admission(conn, body.size, body.admittedBy)
            except ValueError as e:
                raise HTTPException(409, str(e))
            sid = await conn.fetchval("SELECT session_id FROM exam_sessions WHERE is_active = TRUE")
            gap = batch["idleGapSec"]
            await log_event(conn, sid, "INFO",
                f"Batch #{batch['batchNumber']}: {body.size} student(s) called in "
                f"({batch['freeStationsAtAdmission']} free"
                + (f", {gap // 60}m{gap % 60:02d}s since last batch" if gap else "") + ")")
        rec = await compute_recommendation(conn)
    if rec:
        await ws_manager.broadcast("BATCH_RECOMMENDATION", rec)
    return batch


@router.get("")
async def list_batches():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT b.batch_id, b.batch_number, b.announced_size, b.admitted_at,
                      b.free_stations_at_admission, b.idle_gap_sec, b.admitted_by
               FROM batches b JOIN exam_sessions e ON e.session_id = b.session_id
               WHERE e.is_active = TRUE ORDER BY b.batch_number DESC""")
    return [row_to_dict(r) for r in rows]
