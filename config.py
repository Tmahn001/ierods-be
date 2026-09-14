"""Central configuration. Every service imports settings from here.
Never hardcode a connection string inside a service file."""
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/ierods")
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", 30))
HEARTBEAT_TIMEOUT_SEC = int(os.getenv("HEARTBEAT_TIMEOUT_SEC", 90))
MANUAL_RECOVERY_BASELINE_MIN = float(os.getenv("MANUAL_RECOVERY_BASELINE_MIN", 40))
BATCH_READY_THRESHOLD = int(os.getenv("BATCH_READY_THRESHOLD", 10))
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "http://localhost:3000")

# Dev only. When true, the heartbeat monitor keeps every station that is
# currently within the heartbeat window "alive" each cycle, standing in for the
# ping service (pinger.py). Stations backdated past the window (simulate-silence)
# are NOT refreshed, so failure detection still runs through the normal path.
HEARTBEAT_SIMULATE = os.getenv("HEARTBEAT_SIMULATE", "false").lower() in ("1", "true", "yes")
