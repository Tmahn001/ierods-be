"""Heartbeat ping service (Section 1: "Workstation alive / dead / slow").

Pings every workstation every POLL_INTERVAL_SEC seconds and posts the result
to POST /api/workstations/heartbeat. Runs as a separate process next to the API.

Input: a CSV mapping station_id -> host (IP or hostname), e.g.
    station_id,host
    ICT-A-R01-S01,10.10.1.11

Usage:  python pinger.py stations.csv [--api http://localhost:8000] [--slow-ms 500]
"""
import argparse
import asyncio
import csv
import json
import sys
import time
import urllib.request

from config import POLL_INTERVAL_SEC


async def ping(host: str, timeout_s: float) -> tuple[str, int | None]:
    """Returns (OK|SLOW|TIMEOUT, response_ms). Uses the system ping binary so
    no raw-socket privileges are needed."""
    flag = "-W" if sys.platform != "darwin" else "-t"   # timeout flag differs
    t0 = time.perf_counter()
    proc = await asyncio.create_subprocess_exec(
        "ping", "-c", "1", flag, str(int(max(1, timeout_s))), host,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    try:
        rc = await asyncio.wait_for(proc.wait(), timeout=timeout_s + 1)
    except asyncio.TimeoutError:
        proc.kill()
        return "TIMEOUT", None
    ms = int((time.perf_counter() - t0) * 1000)
    if rc != 0:
        return "TIMEOUT", None
    return "OK", ms


def post(api: str, station_id: str, status: str, ms: int | None):
    body = json.dumps({"stationId": station_id, "statusCode": status, "responseMs": ms}).encode()
    req = urllib.request.Request(f"{api}/api/workstations/heartbeat", data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        r.read()


async def cycle(stations: list[tuple[str, str]], api: str, slow_ms: int, timeout_s: float):
    results = await asyncio.gather(*(ping(host, timeout_s) for _, host in stations))
    counts = {"OK": 0, "SLOW": 0, "TIMEOUT": 0}
    for (station_id, _), (status, ms) in zip(stations, results):
        if status == "OK" and ms is not None and ms > slow_ms:
            status = "SLOW"
        counts[status] += 1
        try:
            await asyncio.to_thread(post, api, station_id, status, ms)
        except Exception as e:  # API down: keep pinging, report next cycle
            print(f"[pinger] post failed for {station_id}: {e}")
    print(f"[pinger] {time.strftime('%H:%M:%S')} ok={counts['OK']} slow={counts['SLOW']} timeout={counts['TIMEOUT']}")


async def main(args):
    with open(args.csv, newline="") as f:
        stations = [(r["station_id"].strip(), r["host"].strip()) for r in csv.DictReader(f)]
    print(f"[pinger] {len(stations)} stations, every {POLL_INTERVAL_SEC}s -> {args.api}")
    while True:
        started = time.monotonic()
        await cycle(stations, args.api, args.slow_ms, args.timeout)
        await asyncio.sleep(max(0, POLL_INTERVAL_SEC - (time.monotonic() - started)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("csv")
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--slow-ms", type=int, default=500)
    p.add_argument("--timeout", type=float, default=2.0, help="per-ping timeout, seconds")
    asyncio.run(main(p.parse_args()))
