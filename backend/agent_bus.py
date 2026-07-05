"""
agent_bus.py — agent events over the EXISTING hud_bus
======================================================
REPLACES hud_events.py (discard that file — its /ws/hud route collides
with the one hud_bus.py already mounts). This module defines NO routes.

One bus, one envelope. Agent events ride hud_bus's {type, data} contract:

    {"type": "agent_event",
     "data": {"event": "dispatch|complete|fail|talk|learn|ticker",
              "agent": "...", "to_agent": null, "msg": "...",
              "origin": "local|railway", "ts": "..."}}

- Old HUD (marvis.js): ignores unknown type "agent_event" — unaffected.
- New HUD (marvis-3.html): parses it — already updated to this contract.

Location: MARVIS_CRM/backend/agent_bus.py

Wiring (main.py, local backend):
    from agent_bus import start_railway_replayer
    # in the startup event:
    start_railway_replayer()      # no-op on Railway

Agents (local or Railway) call:
    from agent_bus import hud_event
    hud_event("dispatch", agent="analytics", msg="autopsy started")

ADAPT-BEFORE-WIRING (Claude Code):
    _broadcast() below must call hud_bus's real broadcast function.
    Read hud_bus.py first; it exposes a manager used by main.py:1876's
    /ws/hud handler. Wire the ONE line marked ADAPT to whatever its
    thread-safe broadcast entry point is (or add a small
    broadcast_threadsafe() to hud_bus.py mirroring how it already pushes
    crm_snapshot events — additive only, do not restructure hud_bus).

DDL for Railway-origin events (run on local AND Railway Postgres):
    CREATE TABLE IF NOT EXISTS agent_events (
        id          SERIAL PRIMARY KEY,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        event_type  TEXT NOT NULL,
        agent       TEXT NOT NULL,
        to_agent    TEXT,
        msg         TEXT,
        replayed    BOOLEAN NOT NULL DEFAULT FALSE
    );
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger("agent_bus")

IS_RAILWAY = bool(os.environ.get("RAILWAY_ENVIRONMENT"))


# --------------------------------------------------------------------------
# Broadcast shim — the single ADAPT point
# --------------------------------------------------------------------------

def _broadcast(payload: dict) -> None:
    """Push one envelope to all connected HUD sockets via hud_bus."""
    import hud_bus
    # ADAPT resolved (2026-07-05): hud_bus already exposes a thread-safe sync
    # entry point — emit_hud_event(event_type, data) — which pushes to all HUD
    # sockets via asyncio.run_coroutine_threadsafe on the captured HUD loop.
    # Reuse it verbatim; zero changes to hud_bus.py. payload["type"]=="agent_event".
    hud_bus.emit_hud_event(payload["type"], payload["data"])


def _envelope(event: str, agent: str, msg: str, to_agent: str | None) -> dict:
    return {
        "type": "agent_event",
        "data": {
            "event": event,
            "agent": agent,
            "to_agent": to_agent,
            "msg": msg,
            "origin": "railway" if IS_RAILWAY else "local",
            "ts": datetime.now(timezone.utc).isoformat(),
        },
    }


# --------------------------------------------------------------------------
# Public API — identical signature to the old hud_events.hud_event
# --------------------------------------------------------------------------

def hud_event(event_type: str, agent: str, msg: str = "",
              to_agent: str | None = None) -> None:
    """Fire an agent event to the HUD. Never raises — agents must not die
    because the HUD is closed or the bus hiccups."""
    try:
        if IS_RAILWAY:
            _insert_agent_event(event_type, agent, to_agent, msg)
        else:
            _broadcast(_envelope(event_type, agent, msg, to_agent))
    except Exception as e:                          # pragma: no cover
        log.warning("hud_event dropped (ignored): %s", e)


def _insert_agent_event(etype: str, agent: str,
                        to_agent: str | None, msg: str) -> None:
    from db import get_db
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO agent_events (event_type, agent, to_agent, msg) "
            "VALUES (%s, %s, %s, %s)",
            (etype, agent, to_agent, msg),
        )
        conn.commit()
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Railway replayer — local backend only
# --------------------------------------------------------------------------

_REPLAY_INTERVAL = 10   # sync itself is 30s; this adds ≤10s on top


async def _replay_loop() -> None:
    from db import get_db
    while True:
        try:
            conn = get_db()
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id, event_type, agent, to_agent, msg, created_at "
                    "FROM agent_events WHERE replayed = FALSE "
                    "ORDER BY id ASC LIMIT 50"
                )
                rows = cur.fetchall()
                for (rid, etype, agent, to_agent, msg, created) in rows:
                    env = _envelope(etype, agent, msg, to_agent)
                    env["data"]["origin"] = "railway"
                    env["data"]["ts"] = created.isoformat() if created else None
                    _broadcast(env)
                if rows:
                    cur.execute(
                        "UPDATE agent_events SET replayed = TRUE "
                        "WHERE id = ANY(%s)", ([r[0] for r in rows],)
                    )
                    conn.commit()
            finally:
                conn.close()
        except Exception as e:      # table may not exist yet; stay quiet
            log.debug("replayer idle: %s", e)
        await asyncio.sleep(_REPLAY_INTERVAL)


def start_railway_replayer() -> None:
    """Call from FastAPI startup on the LOCAL backend only."""
    if IS_RAILWAY:
        return
    try:
        asyncio.get_event_loop().create_task(_replay_loop())
    except RuntimeError:
        log.warning("no running loop at startup — call from FastAPI startup event")
