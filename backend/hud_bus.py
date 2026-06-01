from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import WebSocket


class HUDConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, event_type: str, data: Dict[str, Any]):
        payload = json.dumps(
            {
                "type": event_type,
                "data": data,
                "ts": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
        )
        dead: List[WebSocket] = []
        for ws in list(self.active):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


hud_manager = HUDConnectionManager()
_hud_loop: Optional[asyncio.AbstractEventLoop] = None


def set_hud_loop(loop: asyncio.AbstractEventLoop):
    global _hud_loop
    _hud_loop = loop


def emit_hud_event(event_type: str, data: Dict[str, Any]) -> bool:
    if not _hud_loop or not _hud_loop.is_running():
        return False
    try:
        asyncio.run_coroutine_threadsafe(hud_manager.broadcast(event_type, data), _hud_loop)
        return True
    except Exception:
        return False
