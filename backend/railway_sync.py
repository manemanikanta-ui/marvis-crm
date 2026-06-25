"""
Railway → local PostgreSQL sync service for MARVIS CRM.

Reply-driven updates (Gmail/WhatsApp inbound) only ever land on the PUBLIC
Railway backend — Google/Meta cannot push to 127.0.0.1. This background service
pulls those updates down into the local PostgreSQL DB so the desktop CRM/HUD
sees replies in near real-time.

Runs ONLY when BOTH are true:
  1. DATABASE_URL points at a local Postgres (contains "localhost" / "127.0.0.1")
  2. RAILWAY_SYNC_URL env var is set
Otherwise it logs a single info line and stays dormant.

It never crashes the CRM: any per-cycle error is logged as a warning and the
loop simply retries on the next cycle.
"""

from __future__ import annotations

import os
import time
import threading
import logging
from datetime import datetime, timedelta

logger = logging.getLogger("marvis.railway_sync")

SYNC_INTERVAL_SECONDS = 30
SETTINGS_KEY = "railway_sync_last_run"
FIRST_RUN_LOOKBACK_DAYS = 7

_started = False
_start_lock = threading.Lock()


# ── connection helpers ───────────────────────────────────────────────────────

def _is_local_database() -> bool:
    """True when DATABASE_URL targets a local Postgres (sync destination)."""
    url = os.getenv("DATABASE_URL", "") or ""
    return "localhost" in url or "127.0.0.1" in url


def _connect(url: str):
    """Open a fresh psycopg2 connection (lazy import so a dormant service can't
    fail to load if psycopg2 is unavailable)."""
    import psycopg2
    return psycopg2.connect(url, connect_timeout=15)


def _columns(conn, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table,),
        )
        return [r[0] for r in cur.fetchall()]


# ── last-sync cursor (stored in the local settings table) ────────────────────

def _get_last_sync(local_conn) -> str:
    with local_conn.cursor() as cur:
        cur.execute("SELECT value FROM settings WHERE key = %s", (SETTINGS_KEY,))
        row = cur.fetchone()
    if row and row[0]:
        return row[0]
    # First run: look back a fixed window so we don't miss recent replies.
    return (datetime.now() - timedelta(days=FIRST_RUN_LOOKBACK_DAYS)).isoformat()


def _set_last_sync(local_conn, ts: str) -> None:
    with local_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO settings (key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (SETTINGS_KEY, ts),
        )
    local_conn.commit()


# ── per-table sync ───────────────────────────────────────────────────────────

def _sync_leads(railway_conn, local_conn, since: str) -> int:
    """Pull status/responded_at changes from Railway onto existing local leads.
    Never inserts — leads originate locally."""
    with railway_conn.cursor() as cur:
        cur.execute(
            "SELECT id, status, responded_at, updated_at FROM leads WHERE updated_at > %s",
            (since,),
        )
        rows = cur.fetchall()

    updated = 0
    with local_conn.cursor() as cur:
        for lead_id, status, responded_at, updated_at in rows:
            cur.execute(
                "UPDATE leads SET status = %s, responded_at = %s, updated_at = %s WHERE id = %s",
                (status, responded_at, updated_at, lead_id),
            )
            updated += cur.rowcount
    local_conn.commit()
    return updated


def _sync_inserts(railway_conn, local_conn, table: str, time_col: str,
                  since: str, where_extra: str = "") -> int:
    """Generic 'pull new rows' sync: INSERT ... ON CONFLICT (id) DO NOTHING.
    Only columns present in BOTH databases are copied (schema-drift safe)."""
    common = [c for c in _columns(railway_conn, table) if c in set(_columns(local_conn, table))]
    if not common:
        return 0
    col_list = ", ".join(f'"{c}"' for c in common)

    sql_where = f"{time_col} > %s"
    if where_extra:
        sql_where = f"{where_extra} AND {sql_where}"

    with railway_conn.cursor() as cur:
        cur.execute(f'SELECT {col_list} FROM "{table}" WHERE {sql_where}', (since,))
        rows = cur.fetchall()
    if not rows:
        return 0

    placeholders = ", ".join(["%s"] * len(common))
    insert_sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING'

    added = 0
    with local_conn.cursor() as cur:
        for row in rows:
            cur.execute(insert_sql, row)
            added += cur.rowcount
    local_conn.commit()
    return added


def _realign_activities_seq(local_conn) -> None:
    """Push activities_id_seq past every synced Railway id so local SERIAL inserts
    (which use nextval) never collide with an explicitly-synced id."""
    with local_conn.cursor() as cur:
        cur.execute(
            """
            SELECT setval('activities_id_seq',
                GREATEST(nextval('activities_id_seq'),
                (SELECT MAX(id) FROM activities)))
            """
        )
    local_conn.commit()


# ── one sync cycle ───────────────────────────────────────────────────────────

def _run_once() -> None:
    railway_url = os.getenv("RAILWAY_SYNC_URL")
    local_url = os.getenv("DATABASE_URL")

    railway_conn = _connect(railway_url)
    local_conn = _connect(local_url)
    try:
        # Capture the cycle start time up front and use it as the next cursor, so
        # rows written to Railway *during* this cycle are caught next time.
        cycle_started = datetime.now().isoformat()
        since = _get_last_sync(local_conn)

        leads_updated = _sync_leads(railway_conn, local_conn, since)
        activities_added = _sync_inserts(
            railway_conn, local_conn, "activities", "created_at", since,
            where_extra="direction = 'inbound'",
        )
        # Keep the local sequence ahead of all synced Railway ids so future
        # local inserts (nextval-based) never hit a duplicate-key collision.
        if activities_added > 0:
            _realign_activities_seq(local_conn)

        _sync_inserts(railway_conn, local_conn, "unmatched_replies", "received_at", since)

        _set_last_sync(local_conn, cycle_started)

        # Only log when something actually changed (no idle-cycle spam).
        if leads_updated or activities_added:
            logger.info(
                f"🔄 Sync complete: {leads_updated} leads updated, {activities_added} activities added"
            )
    finally:
        for c in (railway_conn, local_conn):
            try:
                c.close()
            except Exception:
                pass


def _loop() -> None:
    while True:
        time.sleep(SYNC_INTERVAL_SECONDS)
        try:
            _run_once()
        except Exception as e:
            logger.warning(f"⚠️ Railway sync cycle failed (retry next cycle): {e}")


# ── public entry point ───────────────────────────────────────────────────────

def start_sync_service() -> None:
    """Start the background sync thread if (and only if) both conditions hold."""
    global _started
    with _start_lock:
        if _started:
            return
        if not _is_local_database():
            logger.info("Railway sync disabled: DATABASE_URL is not local (no localhost/127.0.0.1)")
            return
        if not os.getenv("RAILWAY_SYNC_URL"):
            logger.info("Railway sync disabled: RAILWAY_SYNC_URL not set")
            return

        t = threading.Thread(target=_loop, daemon=True, name="railway-sync")
        t.start()
        _started = True
        logger.info(f"Railway sync service started (every {SYNC_INTERVAL_SECONDS}s)")
