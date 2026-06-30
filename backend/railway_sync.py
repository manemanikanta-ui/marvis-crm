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
from datetime import datetime, timedelta, timezone

# Old cursors were written naive in India Standard Time. Interpret naive values
# as IST explicitly rather than relying on the host's local tz. IST has no DST,
# so the fixed +05:30 offset is an exact fallback if the IANA tzdata is missing.
try:
    from zoneinfo import ZoneInfo  # stdlib since Python 3.9
    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover - tzdata not installed
    IST = timezone(timedelta(hours=5, minutes=30))

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

def _get_last_sync(local_conn) -> datetime:
    """Return the sync cursor as a UTC-aware datetime, so every timestamp
    comparison is timezone-correct regardless of how the value was stored."""
    with local_conn.cursor() as cur:
        cur.execute("SELECT value FROM settings WHERE key = %s", (SETTINGS_KEY,))
        row = cur.fetchone()
    if row and row[0]:
        try:
            parsed = datetime.fromisoformat(row[0])
        except ValueError:
            parsed = None
        if parsed is not None:
            # Old cursors were written naive in IST. Treat a naive value as IST
            # explicitly (never rely on the host's local tz), then convert to UTC.
            # An already-aware value (new UTC format) is just normalised to UTC.
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=IST)
            return parsed.astimezone(timezone.utc)
    # First run (or an unparseable value): look back a fixed window.
    return datetime.now(timezone.utc) - timedelta(days=FIRST_RUN_LOOKBACK_DAYS)


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
            "SELECT id, status, responded_at, updated_at FROM leads "
            "WHERE NULLIF(updated_at, '')::timestamptz > %s",
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

    # Cast the text timestamp column to timestamptz so the comparison is by true
    # instant (handles +05:30 / +00:00 / naive uniformly). NULLIF guards a blank.
    sql_where = f"NULLIF({time_col}, '')::timestamptz > %s"
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


# Columns copied when pulling an inbound activity from Railway. `id` is
# deliberately omitted: both DBs mint activity ids from the same sequence, so an
# id-based INSERT ... ON CONFLICT (id) silently drops a Railway row whose id is
# already used by a *different* local row. Local assigns a fresh id instead, and
# we dedup on the natural key (lead_id, type, direction, created_at).
_ACTIVITY_SYNC_COLUMNS = [
    "lead_id", "type", "channel", "content", "status",
    "scheduled_at", "sent_at", "response", "direction",
    "metadata_json", "campaign_name", "classification", "created_at",
]
_ACTIVITY_NATURAL_KEY = ("lead_id", "type", "direction", "created_at")


def _sync_activities(railway_conn, local_conn, since) -> int:
    """Pull new inbound activities from Railway, deduped on the natural key
    (lead_id, type, direction, created_at) rather than id. since=None pulls ALL
    inbound rows (one-time backfill); otherwise only those newer than the cursor.
    Only columns present in BOTH databases are copied (schema-drift safe)."""
    common = set(_columns(railway_conn, "activities")) & set(_columns(local_conn, "activities"))
    cols = [c for c in _ACTIVITY_SYNC_COLUMNS if c in common]
    if not set(_ACTIVITY_NATURAL_KEY) <= set(cols):
        return 0
    col_list = ", ".join(f'"{c}"' for c in cols)

    where = "direction = 'inbound'"
    params: tuple = ()
    if since is not None:
        where += " AND NULLIF(created_at, '')::timestamptz > %s"
        params = (since,)

    with railway_conn.cursor() as cur:
        cur.execute(f"SELECT {col_list} FROM activities WHERE {where}", params)
        rows = cur.fetchall()
    if not rows:
        return 0

    ix = {c: cols.index(c) for c in _ACTIVITY_NATURAL_KEY}
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = f"INSERT INTO activities ({col_list}) VALUES ({placeholders})"

    added = 0
    with local_conn.cursor() as cur:
        for row in rows:
            # Natural-key existence check — skip rows already synced (any id).
            cur.execute(
                "SELECT 1 FROM activities "
                "WHERE lead_id = %s AND type = %s AND direction = %s "
                "AND NULLIF(created_at, '')::timestamptz = %s::timestamptz LIMIT 1",
                (row[ix["lead_id"]], row[ix["type"]], row[ix["direction"]], row[ix["created_at"]]),
            )
            if cur.fetchone():
                continue
            cur.execute(insert_sql, row)  # no id → local sequence assigns a fresh one
            added += 1
    local_conn.commit()
    return added


def _backfill_inbound_activities() -> None:
    """One-time startup catch-up: pull EVERY Railway inbound activity (no time
    filter) and insert any missing locally via the natural-key dedup. Recovers
    rows the old id-based sync silently dropped before this fix existed."""
    railway_conn = _connect(os.getenv("RAILWAY_SYNC_URL"))
    local_conn = _connect(os.getenv("DATABASE_URL"))
    try:
        added = _sync_activities(railway_conn, local_conn, since=None)
        logger.info(f"🔄 Backfill: added {added} missing inbound activities")
    finally:
        for c in (railway_conn, local_conn):
            try:
                c.close()
            except Exception:
                pass


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


# ── local → Railway lead push ────────────────────────────────────────────────

# Full column set pushed local → Railway so a locally-created lead exists on the
# public backend (where the Gmail/WhatsApp webhooks run and need to match it).
_LEAD_PUSH_COLUMNS = [
    "id", "name", "contact_name", "business_type", "phone", "email",
    "email_source", "website", "address", "city", "rating", "reviews",
    "score", "source", "status", "priority", "notes", "whatsapp_message",
    "email_subject", "email_body", "created_at", "updated_at",
    "contacted_at", "responded_at",
]
# Incremental (30s) cycle refreshes only the mutable fields on conflict — never
# clobber Railway-side content with stale local values mid-session.
_LEAD_UPDATE_COLUMNS = ["status", "responded_at", "updated_at", "contacted_at"]
# The one-time startup full sync also refreshes the contact identity fields so
# Railway holds the correct email/phone/name — the Gmail webhook matches replies
# by LOWER(email), so a wrong/missing email there means missed matches.
_LEAD_FULL_SYNC_UPDATE_COLUMNS = _LEAD_UPDATE_COLUMNS + ["name", "email", "phone"]


def _push_leads_to_railway(railway_conn, local_conn, since: str | None,
                           update_columns: list[str] | None = None) -> int:
    """Push locally-created/updated leads up to Railway.

    INSERT ... ON CONFLICT (id) DO UPDATE (refreshing `update_columns`). When
    `since` is None, pushes ALL local leads (one-time full sync); otherwise only
    rows whose created_at/updated_at is newer than the cursor. Only columns
    present in BOTH databases are copied (schema-drift safe)."""
    if update_columns is None:
        update_columns = _LEAD_UPDATE_COLUMNS
    common_set = set(_columns(local_conn, "leads")) & set(_columns(railway_conn, "leads"))
    cols = [c for c in _LEAD_PUSH_COLUMNS if c in common_set]
    if "id" not in cols:
        return 0
    col_list = ", ".join(f'"{c}"' for c in cols)

    select_sql = f"SELECT {col_list} FROM leads"
    params: tuple = ()
    if since is not None:
        select_sql += (
            " WHERE NULLIF(updated_at, '')::timestamptz > %s "
            "OR NULLIF(created_at, '')::timestamptz > %s"
        )
        params = (since, since)

    with local_conn.cursor() as cur:
        cur.execute(select_sql, params)
        rows = cur.fetchall()
    if not rows:
        return 0

    update_cols = [c for c in update_columns if c in cols]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = (
        f"INSERT INTO leads ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {set_clause}"
    )

    pushed = 0
    with railway_conn.cursor() as cur:
        for row in rows:
            cur.execute(insert_sql, row)
            pushed += 1
    railway_conn.commit()
    return pushed


# ── local → Railway proof-pack push ──────────────────────────────────────────

# proof_packs are deduped on `code` (UNIQUE), not id — both DBs mint ids from the
# same sequence, and Railway owns the live view_count/last_viewed_at (the public
# portal increments them). So we INSERT WITHOUT id (Railway assigns its own) and
# ON CONFLICT (code) DO NOTHING — never overwrite Railway's view counts.
_PROOF_PACK_COLUMNS = [
    "code", "lead_id", "business_name", "category", "city", "phone", "website",
    "assets_json", "regen_count", "view_count", "created_at", "expires_at",
    "last_viewed_at", "status", "converted",
]


def _push_proof_packs_to_railway(railway_conn, local_conn, since: str | None) -> int:
    """Push locally-created proof packs up to Railway so the public /proof/{code}
    URL can always resolve them. since=None pushes ALL local packs (one-time full
    sync); otherwise only those created after the cursor. Schema-drift safe."""
    common = set(_columns(local_conn, "proof_packs")) & set(_columns(railway_conn, "proof_packs"))
    cols = [c for c in _PROOF_PACK_COLUMNS if c in common]
    if "code" not in cols:
        return 0
    col_list = ", ".join(f'"{c}"' for c in cols)

    select_sql = f"SELECT {col_list} FROM proof_packs"
    params: tuple = ()
    if since is not None:
        select_sql += " WHERE NULLIF(created_at, '')::timestamptz > %s"
        params = (since,)

    with local_conn.cursor() as cur:
        cur.execute(select_sql, params)
        rows = cur.fetchall()
    if not rows:
        return 0

    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = (
        f"INSERT INTO proof_packs ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT (code) DO NOTHING"
    )

    pushed = 0
    for row in rows:
        # Per-row commit so one bad row (e.g. a lead_id FK not yet on Railway)
        # can't abort the whole batch — that pack simply retries next cycle.
        try:
            with railway_conn.cursor() as cur:
                cur.execute(insert_sql, row)
                pushed += cur.rowcount
            railway_conn.commit()
        except Exception as exc:
            railway_conn.rollback()
            logger.warning(f"⚠️ proof pack push skipped one row: {exc}")
    return pushed


def _realign_railway_leads_seq(railway_conn) -> None:
    """Keep Railway's leads_id_seq in the reserved range so Railway-minted leads
    (e.g. the website contact form) never collide with local-origin ids.
    Idempotent — self-heals the sequence on every startup (e.g. after a DB
    restore that would otherwise reset it below the reserved floor)."""
    with railway_conn.cursor() as cur:
        cur.execute(
            "SELECT setval('leads_id_seq', GREATEST(MAX(id), 500000), true) FROM leads"
        )
    railway_conn.commit()


def _initial_full_sync() -> None:
    """One-time push of ALL local leads to Railway (no time filter), so existing
    leads are immediately matchable by the public webhook."""
    railway_conn = _connect(os.getenv("RAILWAY_SYNC_URL"))
    local_conn = _connect(os.getenv("DATABASE_URL"))
    try:
        pushed = _push_leads_to_railway(
            railway_conn, local_conn, since=None,
            update_columns=_LEAD_FULL_SYNC_UPDATE_COLUMNS,
        )
        logger.info(f"🔄 Initial full sync: pushed {pushed} leads to Railway")
        # Realign Railway leads_id_seq so it never collides with local ids
        # (local uses 1–499999, Railway starts at 500000).
        _realign_railway_leads_seq(railway_conn)
        # Proof packs after leads (FK lead_id → leads must already be on Railway).
        packs = _push_proof_packs_to_railway(railway_conn, local_conn, since=None)
        logger.info(f"🔄 Initial full sync: pushed {packs} proof packs to Railway")
    finally:
        for c in (railway_conn, local_conn):
            try:
                c.close()
            except Exception:
                pass


# ── one sync cycle ───────────────────────────────────────────────────────────

def _run_once() -> None:
    railway_url = os.getenv("RAILWAY_SYNC_URL")
    local_url = os.getenv("DATABASE_URL")

    railway_conn = _connect(railway_url)
    local_conn = _connect(local_url)
    try:
        # Capture the cycle start time up front and use it as the next cursor, so
        # rows written to Railway *during* this cycle are caught next time. Stored
        # as UTC-aware ISO so the next read compares by true instant.
        cycle_started = datetime.now(timezone.utc).isoformat()
        since = _get_last_sync(local_conn)
        logger.info(f"Sync cursor: UTC={since.isoformat()} IST={since.astimezone(IST).isoformat()}")

        leads_updated = _sync_leads(railway_conn, local_conn, since)
        # Activities use natural-key dedup (not id): both DBs mint activity ids
        # from the same sequence, so id-based conflict detection silently drops
        # Railway's inbound rows. No explicit id is inserted, so no seq realign.
        activities_added = _sync_activities(railway_conn, local_conn, since)

        unmatched_added = _sync_inserts(
            railway_conn, local_conn, "unmatched_replies", "received_at", since
        )

        # LOCAL → RAILWAY: push locally new/changed leads up so the public
        # webhook can match any lead that exists locally.
        leads_pushed = _push_leads_to_railway(railway_conn, local_conn, since)
        # Proof packs after leads (FK lead_id) so the public /proof/{code} resolves.
        packs_pushed = _push_proof_packs_to_railway(railway_conn, local_conn, since)

        # Confirm each table sync actually executed this cycle (debug visibility).
        logger.info(
            f"  leads checked, activities checked, unmatched checked, leads/packs pushed "
            f"(updated={leads_updated} acts={activities_added} "
            f"unmatched={unmatched_added} pushed={leads_pushed} packs={packs_pushed})"
        )

        _set_last_sync(local_conn, cycle_started)

        # ALWAYS log cycle completion, every cycle (not gated on counts).
        logger.info(f"🔄 Cycle done: {leads_updated} leads, {activities_added} activities")

        # Extra detail only when something actually changed (no idle-cycle spam).
        if leads_updated or activities_added or leads_pushed or packs_pushed:
            logger.info(
                f"🔄 Sync complete: {leads_updated} leads updated, "
                f"{activities_added} activities added, {leads_pushed} leads pushed, "
                f"{packs_pushed} proof packs pushed"
            )
    finally:
        for c in (railway_conn, local_conn):
            try:
                c.close()
            except Exception:
                pass


def _loop() -> None:
    # Run the one-time full lead push first (off the request path, so it never
    # blocks FastAPI startup), then settle into the 30s incremental cycle.
    try:
        _initial_full_sync()
    except Exception as e:
        logger.warning(f"⚠️ Initial full sync failed: {e}")

    # One-time catch-up for inbound activities the old id-based sync dropped.
    try:
        _backfill_inbound_activities()
    except Exception as e:
        logger.warning(f"⚠️ Inbound activity backfill failed: {e}")

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
