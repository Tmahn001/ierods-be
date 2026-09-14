from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio

from config import FRONTEND_ORIGIN
from database import init_pool, close_pool
from websocket.manager import ws_manager
from routers import workstations, students, sessions, batches, failures, recovery, metrics, events
from services.heartbeat_monitor import heartbeat_loop
from services.session_log_reader import session_log_loop
from services.metrics_engine import metrics_loop

background_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool()
    background_tasks.append(asyncio.create_task(heartbeat_loop()))
    background_tasks.append(asyncio.create_task(session_log_loop()))
    background_tasks.append(asyncio.create_task(metrics_loop()))
    yield
    for task in background_tasks:
        task.cancel()
    await close_pool()


app = FastAPI(title="IERODS API", version="1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(workstations.router, prefix="/api/workstations", tags=["workstations"])
app.include_router(students.router,     prefix="/api/students",     tags=["students"])
app.include_router(sessions.router,     prefix="/api/sessions",     tags=["sessions"])
app.include_router(batches.router,      prefix="/api/batches",      tags=["batches"])
app.include_router(failures.router,     prefix="/api/failures",     tags=["failures"])
app.include_router(recovery.router,     prefix="/api/recovery",     tags=["recovery"])
app.include_router(metrics.router,      prefix="/api/metrics",      tags=["metrics"])
app.include_router(events.router,       prefix="/api/events",       tags=["events"])


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()   # keepalive pings from client
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


@app.get("/health")
async def health():
    return {"status": "ok"}
