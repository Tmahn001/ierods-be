from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services.recovery_planner import approve_recovery_plan

router = APIRouter()


class ApproveIn(BaseModel):
    approvedBy: str = "admin"


@router.post("/{plan_id}/approve")
async def approve(plan_id: int, body: ApproveIn | None = None):
    try:
        return await approve_recovery_plan(plan_id, (body.approvedBy if body else "admin"))
    except ValueError as e:
        raise HTTPException(409, str(e))
