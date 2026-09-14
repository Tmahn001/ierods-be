from fastapi import WebSocket
import json
import asyncio
from typing import Any


class WebSocketManager:
    def __init__(self):
        self.connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self.connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.connections:
            self.connections.remove(ws)

    async def broadcast(self, message_type: str, data: Any):
        payload = json.dumps({"type": message_type, "data": data}, default=str)
        dead = []
        for conn in list(self.connections):
            try:
                await conn.send_text(payload)
            except Exception:
                dead.append(conn)
        for conn in dead:
            self.disconnect(conn)


ws_manager = WebSocketManager()
