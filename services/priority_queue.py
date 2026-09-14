"""Implements Chapter 3, Section 3.7.4, Equation 3.9: the per-student Priority
Value that decides WHICH waiting students are called forward next.

Known limitation: the system has no physical queue sensor (Section 0).
WT(s,t) is approximated as time elapsed since the student's record was loaded
from the pre-session roster (queued_at). It is a sound basis for ranking, not
a measurement of time spent physically standing outside the hall."""
from datetime import datetime, timezone


async def compute_priority_order(conn, session_id: int) -> list[dict]:
    """Ranks all NOT_ADMITTED students in a session by Priority Value (Eq. 3.9).

    PV(s,t) = alpha*WT(s,t) + beta*(1/D(s)) + gamma*COMPAT(s, W_available(t))

    D(s) is constant across a session (one course per session), so the 1/D(s)
    term is the same for every student in a given call. It still matters when
    comparing sessions of different exam length and is kept per the formula.
    """
    session = await conn.fetchrow(
        """SELECT exam_duration_min, min_processor_score, pv_alpha, pv_beta, pv_gamma
           FROM exam_sessions WHERE session_id = $1""",
        session_id
    )
    if not session:
        return []

    duration = max(1, session["exam_duration_min"])
    min_score = session["min_processor_score"]
    alpha, beta, gamma = session["pv_alpha"], session["pv_beta"], session["pv_gamma"]

    # COMPAT term: is there currently at least one available, hardware-compatible
    # workstation for this course? Uses min_processor_score, consistent with the
    # recovery planner (Section 6.4).
    compatible_free = await conn.fetchval(
        "SELECT COUNT(*) FROM workstations WHERE status = 'AVAILABLE' AND processor_score >= $1",
        min_score
    )
    compat_term = 1.0 if compatible_free > 0 else 0.0

    waiting = await conn.fetch(
        """SELECT matric_number, queued_at FROM students
           WHERE session_id = $1 AND status = 'NOT_ADMITTED'
           ORDER BY queued_at ASC, matric_number ASC""",
        session_id
    )

    now = datetime.now(timezone.utc)
    scored = []
    for row in waiting:
        wt_min = max(0.0, (now - row["queued_at"]).total_seconds() / 60)
        pv = alpha * wt_min + beta * (1.0 / duration) + gamma * compat_term
        scored.append({
            "matricNumber": row["matric_number"],
            "waitMinutes": round(wt_min, 1),
            "priorityValue": round(pv, 4),
        })

    # Stable sort: ties keep roster order.
    scored.sort(key=lambda r: r["priorityValue"], reverse=True)
    return scored


def is_compatible(processor_score: float, min_required: float) -> bool:
    return processor_score >= min_required
