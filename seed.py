"""Populates a small dev/test fixture (Section 10, Phase 1):

  * 250 stations across 2 halls (ICT-A, ICT-B), 12 UPS circuits
  * one CHM 102 session with 500 students, started and active

Intentionally smaller than the 800-seat Section 9 simulation: this is for
exercising the live dashboard, not for scale.

Usage:  python seed.py [--no-start] [--stations N] [--students N]
"""
import argparse
import asyncio
import random
from datetime import date, datetime, timezone

import asyncpg
from config import DATABASE_URL

HALLS = ["ICT-A", "ICT-B"]
SEATS_PER_ROW = 25


def build_stations(n_stations: int, n_circuits: int, rng: random.Random):
    per_hall = n_stations // len(HALLS)
    rows = []
    idx = 0
    for hall in HALLS:
        for i in range(per_hall):
            row = i // SEATS_PER_ROW + 1
            seat = i % SEATS_PER_ROW + 1
            circuit = f"UPS-{(idx * n_circuits // n_stations) + 1:02d}"
            # Mostly modern machines, a tail of older hardware
            score = 1.0 if rng.random() < 0.8 else round(rng.uniform(0.55, 0.9), 2)
            rows.append((f"{hall}-R{row:02d}-S{seat:02d}", hall, row, seat, circuit, score))
            idx += 1
    return rows


async def main(args):
    rng = random.Random(42)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        async with conn.transaction():
            await conn.execute(
                """TRUNCATE session_events, metrics_snapshots, recovery_plans, failure_events,
                   heartbeat_log, batches, students, exam_sessions, workstations
                   RESTART IDENTITY CASCADE""")

            stations = build_stations(args.stations, 12, rng)
            # last_heartbeat is left NULL on purpose: the monitor only marks a
            # station FAILED once it has heard from it at least once, so a dev
            # box with no real pinger does not see all 250 machines "die" 90s in.
            await conn.executemany(
                """INSERT INTO workstations
                     (station_id, hall_id, row_number, seat_number, ups_circuit_id, processor_score)
                   VALUES ($1,$2,$3,$4,$5,$6)""",
                stations)

            sid = await conn.fetchval(
                """INSERT INTO exam_sessions
                     (course_code, exam_date, total_enrolled, exam_duration_min,
                      expected_failure_rate, min_processor_score, hall_ids,
                      started_at, is_active)
                   VALUES ('CHM102', $1, $2, 60, 0.11, 0.0, $3, $4, $5)
                   RETURNING session_id""",
                date.today(), args.students, HALLS,
                None if args.no_start else datetime.now(timezone.utc),
                not args.no_start)

            students = [(f"CHM/2024/{i:04d}", sid, "CHM102") for i in range(1, args.students + 1)]
            await conn.executemany(
                "INSERT INTO students (matric_number, session_id, course_code) VALUES ($1,$2,$3)",
                students)

            await conn.execute(
                "INSERT INTO session_events (session_id, level, message) VALUES ($1,'INFO',$2)",
                sid, f"Seed: {len(stations)} stations, CHM102 session with {len(students)} students"
                     + ("" if args.no_start else " (started)"))
        print(f"Seeded {len(stations)} stations, session #{sid} with {len(students)} students"
              f" ({'not started' if args.no_start else 'active'}).")
    finally:
        await conn.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--no-start", action="store_true", help="create the session but leave it inactive")
    p.add_argument("--stations", type=int, default=250)
    p.add_argument("--students", type=int, default=500)
    asyncio.run(main(p.parse_args()))
