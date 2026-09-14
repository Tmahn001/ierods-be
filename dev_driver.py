"""Dev/demo driver: plays the examination server and the ping service against
a running IERODS backend so the dashboard moves on its own.

Every cycle it: follows the current batch recommendation (records the
admission and logs the ranked students in on free stations), submits students
whose (accelerated) exam time is up, and occasionally silences an occupied
station so a failure is detected. Recovery plans are NOT approved here: that is
the administrator's decision, made on the dashboard.

Usage:  python dev_driver.py [--api http://localhost:8000] [--speed 20] [--fail-every 6]
        --speed 20  => a 60-minute exam takes 3 minutes of wall-clock time
"""
import argparse
import json
import random
import time
import sys
import urllib.request

sys.stdout.reconfigure(line_buffering=True)


def call(api, method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(api + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"error": e.code, "detail": e.read().decode()[:200]}


def main(a):
    rng = random.Random(a.seed)
    session = call(a.api, "GET", "/api/sessions/active")
    if not session:
        raise SystemExit("No active session. Run seed.py or start one on the pre-session page.")
    duration_wall = session["examDurationMin"] * 60 / a.speed
    print(f"[driver] {session['courseCode']}: {session['examDurationMin']} min exam "
          f"≈ {duration_wall:.0f}s wall-clock at {a.speed}x")
    seated: dict[str, float] = {}   # matric -> wall-clock login time
    cycle = 0
    while True:
        cycle += 1
        rec = call(a.api, "GET", "/api/batches/recommendation")
        if "error" in rec:
            print("[driver] no recommendation:", rec); time.sleep(a.interval); continue

        # 1. Follow the recommendation: record the batch, then seat the ranked list.
        if rec["readyToAdmit"]:
            call(a.api, "POST", "/api/batches/admit", {"size": rec["recommendedSize"], "admittedBy": "dev_driver"})
            stations = call(a.api, "GET", "/api/workstations")
            free = [w["stationId"] for w in stations if w["status"] == "AVAILABLE"]
            rng.shuffle(free)   # students sit wherever they like (Section 0)
            n = 0
            for s, st in zip(rec["admissionOrder"], free):
                r = call(a.api, "POST", "/api/students/session-event",
                         {"matricNumber": s["matricNumber"], "stationId": st, "event": "LOGIN"})
                if r.get("ok"):
                    seated[s["matricNumber"]] = time.time(); n += 1
            print(f"[driver] cycle {cycle}: called {rec['recommendedSize']}, seated {n}")

        # 2. Submissions: everyone whose accelerated exam time has elapsed (±15%).
        now = time.time()
        due = [m for m, t0 in seated.items() if now - t0 >= duration_wall * rng.uniform(0.7, 1.0)]
        for m in due:
            r = call(a.api, "POST", "/api/students/session-event", {"matricNumber": m, "event": "SUBMIT"})
            seated.pop(m, None)
        if due:
            print(f"[driver] cycle {cycle}: {len(due)} submitted")

        # 3. Occasionally a station goes silent.
        if a.fail_every and cycle % a.fail_every == 0:
            stations = call(a.api, "GET", "/api/workstations")
            occupied = [w for w in stations if w["status"] == "OCCUPIED"]
            if occupied:
                w = rng.choice(occupied)
                call(a.api, "POST", "/api/workstations/heartbeat", {"stationId": w["stationId"], "statusCode": "OK"})
                call(a.api, "POST", f"/api/workstations/{w['stationId']}/simulate-silence")
                seated.pop(w["currentMatric"], None)   # disrupted; resumes after reseat login
                print(f"[driver] cycle {cycle}: {w['stationId']} went silent ({w['currentMatric']})")

        # 4. Students relocated by an approved plan sit down again on a free seat.
        disrupted_back = call(a.api, "GET", "/api/students?status=IN_SESSION")
        if isinstance(disrupted_back, list):
            stations = None
            for s in disrupted_back:
                if s["matricNumber"] in seated:
                    continue
                st = s.get("currentStation")
                if stations is None:
                    stations = {w["stationId"]: w for w in call(a.api, "GET", "/api/workstations")}
                if st and stations.get(st, {}).get("status") == "FAILED":
                    free = [k for k, w in stations.items() if w["status"] == "AVAILABLE"]
                    if free:
                        target = rng.choice(free)
                        r = call(a.api, "POST", "/api/students/session-event",
                                 {"matricNumber": s["matricNumber"], "stationId": target, "event": "LOGIN"})
                        if r.get("ok"):
                            stations[target]["status"] = "OCCUPIED"
                            seated[s["matricNumber"]] = time.time() - duration_wall * 0.5
                            print(f"[driver] {s['matricNumber']} reseated on {target}")
                else:
                    seated[s["matricNumber"]] = time.time()

        if rec["queueDepth"] == 0 and not seated:
            print("[driver] everyone processed."); break
        time.sleep(a.interval)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--speed", type=float, default=20.0)
    p.add_argument("--interval", type=float, default=5.0, help="seconds between driver cycles")
    p.add_argument("--fail-every", type=int, default=6, help="silence a station every N cycles (0 = never)")
    p.add_argument("--seed", type=int, default=1)
    main(p.parse_args())
