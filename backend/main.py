"""
MARVIS CRM Backend
FastAPI + SQLite — Lead Management + Auto Outreach
"""

import sys
# Windows consoles default to cp1252, which raises UnicodeEncodeError on emoji
# in print() (e.g. the auto-send engine's robot glyph) and aborts startup when
# launched directly via `uvicorn ...`. Force UTF-8 so no print can crash boot.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

import asyncio
import logging
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List
import sqlite3
import pandas as pd
import json
import os
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone
import threading
import time
import random
import string
from dotenv import load_dotenv

from db import get_db as shared_get_db
from hud_bus import emit_hud_event, hud_manager, set_hud_loop

load_dotenv()

logger = logging.getLogger("marvis.main")
# uvicorn does not configure the root logger, so app-level INFO logs (e.g. the
# Gmail webhook step logging) would otherwise never reach Railway stdout.
logging.basicConfig(level=logging.INFO)

from crm_core import (
    classify_reply,
    ensure_crm_schema,
    is_schema_verified,
    mark_schema_ready,
    get_campaign_stats,
    get_lead_timeline,
    get_pending_approvals,
    log_activity_event,
    random_delay_seconds,
    update_lead_status,
    load_safety_settings,
)

app = FastAPI(title="MARVIS CRM", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_csp_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data: https:; "
        "frame-src 'self' https://www.openstreetmap.org https://maps.googleapis.com https://maps.google.com https://www.google.com; "
        "connect-src 'self' https://maps.googleapis.com https://nominatim.openstreetmap.org https://maps.google.com;"
    )
    return response


from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
import pathlib

@app.get("/")
@app.get("/dashboard")
async def dashboard():
    # Railway deploys from the repo root (root dir ""), so frontend/ sits at
    # ../frontend relative to backend/main.py (i.e. /app/frontend). Check the
    # known locations and serve whichever exists.
    candidates = [
        "/app/frontend/index.html",
        os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html"),
        os.path.join(os.path.dirname(__file__), "frontend", "index.html"),
    ]
    for path in candidates:
        if os.path.exists(path):
            return FileResponse(path)
    return JSONResponse(
        {"error": "Frontend index.html not found", "checked": candidates},
        status_code=404,
    )


# ─────────────────────────────────────────────
# PROOF ASSET PORTAL (registered early — before any other routes match)
# ─────────────────────────────────────────────

@app.post("/api/proof/generate")
async def proof_generate(request: Request):
    """Generate proof asset pack for a business. Returns portal URL."""
    try:
        body = await request.json()
        business_name = body.get("business_name", "").strip()
        category      = body.get("category", "").strip()
        city          = (body.get("city") or "Hyderabad").strip()
        phone         = body.get("phone", "").strip()
        website       = body.get("website", "").strip()
        lead_id       = body.get("lead_id")

        if not business_name:
            return JSONResponse({"error": "business_name is required"}, status_code=400)

        # Generate assets via Claude
        from enrichment import generate_proof_assets
        assets = await generate_proof_assets({
            "name": business_name, "category": category,
            "city": city, "phone": phone, "website": website
        })
        if "error" in assets:
            return JSONResponse(assets, status_code=500)

        # Create pack record
        code = generate_proof_code()
        expires_at = (datetime.utcnow() + timedelta(days=7)).isoformat()

        conn = get_db()
        conn.execute("""
            INSERT INTO proof_packs
            (code, lead_id, business_name, category, city, phone, website,
             assets_json, expires_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (code, lead_id, business_name, category, city, phone, website,
              json.dumps(assets), expires_at))
        conn.commit()
        conn.close()

        # Build the portal URL from the request host (works on whatever port the
        # CRM is actually running — currently 8002, not the old 8001).
        base = str(request.base_url).rstrip("/")
        portal_url = f"{base}/proof/{code}"
        return JSONResponse({
            "success": True,
            "code": code,
            "portal_url": portal_url,
            "expires_at": expires_at,
            "assets": assets
        })
    except Exception as e:
        import traceback
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()},
                            status_code=500)


@app.get("/proof/{code}")
async def proof_portal(code: str, request: Request):
    """Serve the proof asset portal page for a given code."""
    conn = get_db()
    pack = conn.execute(
        "SELECT * FROM proof_packs WHERE code = ?", (code.upper(),)
    ).fetchone()
    conn.close()

    if not pack:
        return HTMLResponse("<h2>Portal not found.</h2>", status_code=404)

    pack = dict(pack)

    # Check expiry
    if pack["status"] == "expired" or (
        pack["expires_at"] and
        datetime.utcnow() > datetime.fromisoformat(pack["expires_at"])
    ):
        conn = get_db()
        conn.execute("UPDATE proof_packs SET status='expired' WHERE code = ?",
                     (pack["code"],))
        conn.commit()
        conn.close()
        return HTMLResponse(_expired_page(pack), status_code=410)

    # Increment view count
    conn = get_db()
    conn.execute("""
        UPDATE proof_packs SET
        view_count = view_count + 1,
        last_viewed_at = datetime('now')
        WHERE code = ?
    """, (pack["code"],))
    conn.commit()
    conn.close()

    # Telegram notification on first view (pack still holds the pre-increment count)
    if pack["view_count"] == 0:
        try:
            from telegram_notify import notify
            notify(f"👁 <b>Proof pack opened</b>\n{pack['business_name']}\nCode: {pack['code']}")
        except Exception:
            pass

    assets = json.loads(pack["assets_json"] or "{}")

    # Serve the portal HTML with the pack data injected
    proof_html_path = os.path.join(
        os.path.dirname(__file__), "..", "frontend", "proof.html"
    )
    with open(proof_html_path, encoding="utf-8") as f:
        html = f.read()

    pack_data = {**pack, "assets": assets}
    pack_data.pop("assets_json", None)
    html = html.replace(
        "/* __PACK_DATA__ */",
        f"window.PROOF_PACK = {json.dumps(pack_data)};"
    )
    return HTMLResponse(html)


@app.post("/api/proof/{code}/regenerate")
async def proof_regenerate(code: str):
    """Regenerate assets for a proof pack. Max 2 regenerations."""
    conn = get_db()
    pack = conn.execute(
        "SELECT * FROM proof_packs WHERE code = ?", (code.upper(),)
    ).fetchone()
    conn.close()

    if not pack:
        return JSONResponse({"error": "Pack not found"}, status_code=404)

    pack = dict(pack)

    if pack["regen_count"] >= 2:
        return JSONResponse({
            "error": "Regeneration limit reached",
            "limit_reached": True
        }, status_code=403)

    # Generate new assets
    from enrichment import generate_proof_assets
    assets = await generate_proof_assets({
        "name": pack["business_name"],
        "category": pack["category"],
        "city": pack["city"],
        "phone": pack["phone"],
        "website": pack["website"]
    })

    if "error" in assets:
        return JSONResponse(assets, status_code=500)

    new_regen_count = pack["regen_count"] + 1

    conn = get_db()
    conn.execute("""
        UPDATE proof_packs SET
        assets_json = ?, regen_count = ?
        WHERE code = ?
    """, (json.dumps(assets), new_regen_count, pack["code"]))
    conn.commit()
    conn.close()

    # Telegram hot signal on 2nd regeneration
    if new_regen_count >= 2:
        try:
            from telegram_notify import notify
            notify(
                f"🔥 <b>HOT SIGNAL — Regen limit hit</b>\n"
                f"{pack['business_name']}\nCode: {pack['code']}\n"
                f"They regenerated twice — call them now."
            )
        except Exception:
            pass

    return JSONResponse({
        "success": True,
        "assets": assets,
        "regen_count": new_regen_count,
        "regen_remaining": 2 - new_regen_count,
        "limit_reached": new_regen_count >= 2
    })


@app.get("/api/proof/stats")
async def proof_stats():
    """Return all proof packs for admin view in MARVIS CRM."""
    conn = get_db()
    packs = conn.execute("""
        SELECT id, code, business_name, category, city,
               regen_count, view_count, status,
               created_at, expires_at, last_viewed_at, converted
        FROM proof_packs
        ORDER BY created_at DESC
    """).fetchall()
    conn.close()
    return JSONResponse([dict(p) for p in packs])


def _expired_page(pack: dict) -> str:
    return f"""<!DOCTYPE html><html><head><title>Expired</title>
    <style>body{{font-family:sans-serif;text-align:center;padding:60px;
    background:#f9f9f9;color:#333}}</style></head><body>
    <h2>This proof pack has expired</h2>
    <p>The assets for <strong>{pack['business_name']}</strong>
    were available for 7 days.</p>
    <p>Contact <a href="mailto:manikanta@talktivai.com">
    manikanta@talktivai.com</a> to get a fresh pack.</p>
    </body></html>"""

DB_PATH = "data/crm.db"

# ─────────────────────────────────────────────
# DATABASE SETUP
# ─────────────────────────────────────────────

def get_db():
    return shared_get_db()


def outbound_allowed(status: str) -> bool:
    return str(status or "").lower() in {"approved", "contacted", "responded", "interested", "booked"}


def google_api_key() -> str:
    """Google Places key — accept either env var name (CLAUDE.md uses GOOGLE_PLACES_API_KEY)."""
    return os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY") or ""


def compute_lead_score(reviews=0, website="", phone="", email="") -> int:
    """
    Outbound-fit score (0-100). Reachability weighted highest, then established
    business signal from review volume. A genuinely "hot" lead (>=75) needs a
    verified contact method plus either strong reach or an established presence.
    """
    score = 0
    if email:   score += 35
    if phone:   score += 25
    if website: score += 15
    try:
        r = int(reviews or 0)
    except (TypeError, ValueError):
        r = 0
    if r >= 100:  score += 25
    elif r >= 50: score += 20
    elif r >= 20: score += 15
    elif r >= 5:  score += 8
    return min(score, 100)


def recompute_all_scores() -> dict:
    """Re-score every lead from its stored fields using the current model. Idempotent."""
    conn = get_db()
    rows = [dict(r) for r in conn.execute(
        "SELECT id, reviews, website, phone, email FROM leads"
    ).fetchall()]
    updated = 0
    for row in rows:
        new_score = compute_lead_score(
            reviews=row.get("reviews", 0),
            website=row.get("website", "") or "",
            phone=row.get("phone", "") or "",
            email=row.get("email", "") or "",
        )
        conn.execute("UPDATE leads SET score = ? WHERE id = ?", (new_score, row["id"]))
        updated += 1
    conn.commit()
    conn.close()
    return {"updated": updated}


def init_db():
    os.makedirs("data", exist_ok=True)
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            contact_name TEXT DEFAULT '',
            business_type TEXT,
            phone TEXT,
            email TEXT,
            email_source TEXT DEFAULT '',
            website TEXT,
            address TEXT,
            city TEXT DEFAULT '',
            rating REAL,
            reviews INTEGER DEFAULT 0,
            score INTEGER DEFAULT 0,
            source TEXT DEFAULT 'google_maps',
            status TEXT DEFAULT 'new',
            priority TEXT DEFAULT 'medium',
            notes TEXT,
            whatsapp_message TEXT,
            email_subject TEXT,
            email_body TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS activities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            channel TEXT,
            content TEXT,
            status TEXT DEFAULT 'pending',
            scheduled_at TEXT,
            sent_at TEXT,
            response TEXT,
            direction TEXT DEFAULT 'outbound',
            metadata_json TEXT DEFAULT '{}',
            campaign_name TEXT DEFAULT '',
            classification TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );

        CREATE TABLE IF NOT EXISTS follow_ups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            message TEXT,
            channel TEXT DEFAULT 'whatsapp',
            scheduled_at TEXT,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );

        CREATE TABLE IF NOT EXISTS proof_packs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            code          TEXT UNIQUE NOT NULL,
            lead_id       INTEGER REFERENCES leads(id),
            business_name TEXT NOT NULL,
            category      TEXT,
            city          TEXT,
            phone         TEXT,
            website       TEXT,
            assets_json   TEXT,
            regen_count   INTEGER DEFAULT 0,
            view_count    INTEGER DEFAULT 0,
            created_at    TEXT DEFAULT (datetime('now')),
            expires_at    TEXT,
            last_viewed_at TEXT,
            status        TEXT DEFAULT 'active',
            converted     INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS unmatched_replies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_email TEXT,
            subject TEXT,
            snippet TEXT,
            received_at TEXT
        );

        CREATE TABLE IF NOT EXISTS gmail_watch_state (
            id INTEGER PRIMARY KEY,
            history_id TEXT,
            expiration TEXT,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        INSERT OR IGNORE INTO settings VALUES ('email_host', 'smtp.gmail.com');
        INSERT OR IGNORE INTO settings VALUES ('email_port', '587');
        INSERT OR IGNORE INTO settings VALUES ('email_user', '');
        INSERT OR IGNORE INTO settings VALUES ('email_pass', '');
        INSERT OR IGNORE INTO settings VALUES ('sender_name', 'Manikanta | Talktiv AI');
        INSERT OR IGNORE INTO settings VALUES ('auto_followup_days', '3');
        INSERT OR IGNORE INTO settings VALUES ('whatsapp_enabled', 'false');
        INSERT OR IGNORE INTO settings VALUES ('auto_sequence_enabled', 'true');
        INSERT OR IGNORE INTO settings VALUES ('auto_send_enabled', 'false');
        INSERT OR IGNORE INTO settings VALUES ('wa_phone_id', '');
        INSERT OR IGNORE INTO settings VALUES ('wa_token', '');
        INSERT OR IGNORE INTO settings VALUES ('wa_verify_token', 'marvis_verify_2024');
        INSERT OR IGNORE INTO settings VALUES ('wa_auto_reply', 'true');
        INSERT OR IGNORE INTO settings VALUES ('broadcast_status', 'idle');
        INSERT OR IGNORE INTO settings VALUES ('broadcast_result', '');
        INSERT OR IGNORE INTO settings VALUES ('scheduler_enabled', 'true');
        INSERT OR IGNORE INTO settings VALUES ('scheduler_run_time', '09:00');
        INSERT OR IGNORE INTO settings VALUES ('scheduler_queries', '["dental clinics in Hyderabad","real estate agents in Hyderabad","property dealers in Hyderabad","salons in Hyderabad","gyms in Hyderabad","coaching centres in Hyderabad","interior designers in Hyderabad","CA firms in Hyderabad"]');
        INSERT OR IGNORE INTO settings VALUES ('scheduler_jobs', '[{"location":"Hyderabad","category":"Real Estate","leads":20,"time":"09:00","enabled":true}]');
        INSERT OR IGNORE INTO settings VALUES ('scheduler_job_last_run', '{}');
        INSERT OR IGNORE INTO settings VALUES ('scheduler_enrich_limit', '20');
        INSERT OR IGNORE INTO settings VALUES ('scheduler_last_run', '');
        INSERT OR IGNORE INTO settings VALUES ('scheduler_next_run', '');
        INSERT OR IGNORE INTO settings VALUES ('scheduler_last_status', 'idle');
        INSERT OR IGNORE INTO settings VALUES ('scheduler_last_result', '');
        INSERT OR IGNORE INTO settings VALUES ('max_emails_per_day', '50');
        INSERT OR IGNORE INTO settings VALUES ('max_whatsapp_per_day', '20');
        INSERT OR IGNORE INTO settings VALUES ('random_delay_min', '0');
        INSERT OR IGNORE INTO settings VALUES ('random_delay_max', '0');
        INSERT OR IGNORE INTO settings VALUES ('office_hours_only', 'true');
        INSERT OR IGNORE INTO settings VALUES ('pause_on_failures', 'true');
    """)
    # Commit the table creation BEFORE any ALTER runs. On PostgreSQL a redundant
    # ALTER (column already present from the CREATE above) aborts the transaction;
    # committing first guarantees the schema persists regardless.
    conn.commit()
    conn.close()

    # Additive migrations for existing DBs (CREATE IF NOT EXISTS won't add new
    # columns). Each runs on its own connection and is skipped when the column
    # already exists, so it can never abort/roll back the schema transaction.
    from db import table_columns
    for _col in ("contact_name", "city"):
        mconn = get_db()
        try:
            if _col not in table_columns(mconn, "leads"):
                mconn.execute(f"ALTER TABLE leads ADD COLUMN {_col} TEXT DEFAULT ''")
                mconn.commit()
        except Exception:
            try:
                mconn.rollback()
            except Exception:
                pass
        finally:
            mconn.close()


def generate_proof_code() -> str:
    """Generate a unique 6-char alphanumeric code, e.g. SMI924."""
    while True:
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        conn = get_db()
        exists = conn.execute(
            "SELECT id FROM proof_packs WHERE code = ?", (code,)
        ).fetchone()
        conn.close()
        if not exists:
            return code

# ─────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────

class LeadCreate(BaseModel):
    name: str
    contact_name: Optional[str] = ""
    city: Optional[str] = ""
    business_type: Optional[str] = ""
    phone: Optional[str] = ""
    email: Optional[str] = ""
    website: Optional[str] = ""
    address: Optional[str] = ""
    rating: Optional[float] = 0
    reviews: Optional[int] = 0
    score: Optional[int] = 0
    whatsapp_message: Optional[str] = ""
    email_subject: Optional[str] = ""
    email_body: Optional[str] = ""
    notes: Optional[str] = ""
    status: Optional[str] = "new"
    source: Optional[str] = "manual"

class LeadUpdate(BaseModel):
    status: Optional[str] = None
    priority: Optional[str] = None
    notes: Optional[str] = None
    email: Optional[str] = None
    email_source: Optional[str] = None
    phone: Optional[str] = None
    email_subject: Optional[str] = None
    email_body: Optional[str] = None
    whatsapp_message: Optional[str] = None

class ActivityCreate(BaseModel):
    lead_id: int
    type: str
    channel: str
    content: str
    status: Optional[str] = "logged"
    direction: Optional[str] = "outbound"
    metadata_json: Optional[str] = ""
    campaign_name: Optional[str] = ""
    scheduled_at: Optional[str] = None

class EmailSend(BaseModel):
    lead_id: int
    to_email: str
    subject: str
    body: str

class SettingsUpdate(BaseModel):
    key: str
    value: str

class SchedulerConfigUpdate(BaseModel):
    enabled: Optional[bool] = None
    auto_send_enabled: Optional[bool] = None
    run_time: Optional[str] = None
    enrich_limit: Optional[int] = None
    jobs: Optional[List[dict]] = None
    queries: Optional[List[str]] = None

class LeadScrapeRequest(BaseModel):
    query: str
    city: Optional[str] = ""
    state: Optional[str] = ""
    country: Optional[str] = "India"
    max_results: int = 20

class BulkIds(BaseModel):
    lead_ids: List[int]

class FollowUpCreate(BaseModel):
    lead_id: int
    scheduled_at: str
    note: str

# Google Places region bias per country (no hardcoded fallbacks beyond a sane default)
COUNTRY_REGION_MAP = {
    "India": "in",
    "United States": "us",
    "USA": "us",
    "US": "us",
    "United Kingdom": "gb",
    "UK": "gb",
    "Australia": "au",
    "Canada": "ca",
    "UAE": "ae",
    "United Arab Emirates": "ae",
    "Singapore": "sg",
    "New Zealand": "nz",
}

# ─────────────────────────────────────────────
# IMPORT FROM EXCEL
# ─────────────────────────────────────────────

@app.post("/api/import-leads")
async def import_leads(file_path: str = "../leads_enriched.xlsx"):
    """Import leads from the scraper Excel output"""
    try:
        df = pd.read_excel(file_path, sheet_name="Enriched Leads")
        conn = get_db()
        imported = 0
        skipped = 0

        for _, row in df.iterrows():
            # Check for duplicate by name + phone
            existing = conn.execute(
                "SELECT id FROM leads WHERE name = ? OR phone = ?",
                (str(row.get('name', '')), str(row.get('phone', '')))
            ).fetchone()

            if existing:
                skipped += 1
                continue

            cursor = conn.execute("""
                INSERT INTO leads 
                (name, business_type, phone, email, email_source, website, address, rating, reviews, 
                 score, whatsapp_message, email_subject, email_body)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(row.get('name', '')),
                str(row.get('query', '')).replace('in Hyderabad', '').replace('in Secunderabad', '').strip(),
                str(row.get('phone', '')) if pd.notna(row.get('phone')) else '',
                str(row.get('email', '')) if pd.notna(row.get('email')) else '',
                str(row.get('email_source', '')) if pd.notna(row.get('email_source')) else '',
                str(row.get('website', '')) if pd.notna(row.get('website')) else '',
                str(row.get('address', '')) if pd.notna(row.get('address')) else '',
                float(row.get('rating', 0)) if pd.notna(row.get('rating')) else 0,
                int(row.get('reviews', 0)) if pd.notna(row.get('reviews')) else 0,
                int(row.get('score', 0)) if pd.notna(row.get('score')) else 0,
                str(row.get('whatsapp_message', '')) if pd.notna(row.get('whatsapp_message')) else '',
                str(row.get('email_subject', '')) if pd.notna(row.get('email_subject')) else '',
                str(row.get('email_body', '')) if pd.notna(row.get('email_body')) else '',
            ))
            lead_id = cursor.lastrowid
            emit_hud_event("new_lead", {
                "name": str(row.get('name', '')),
                "category": str(row.get('query', '')).replace('in Hyderabad', '').replace('in Secunderabad', '').strip(),
                "lead_id": lead_id,
            })
            imported += 1

        conn.commit()
        conn.close()

        # Auto-enrich imported leads
        if imported > 0:
            try:
                from enrichment import enrich_batch
                import threading
                t = threading.Thread(
                    target=enrich_batch,
                    kwargs={"limit": imported, "only_missing": True},
                    daemon=True
                )
                t.start()
                print(f"✨ Auto-enrichment started for {imported} leads")
            except Exception as e:
                print(f"Auto-enrich start error: {e}")

        return {"success": True, "imported": imported, "skipped": skipped}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ─────────────────────────────────────────────
# LEADS CRUD
# ─────────────────────────────────────────────

@app.get("/api/leads")
async def get_leads(
    status: Optional[str] = None,
    priority: Optional[str] = None,
    category: Optional[str] = None,
    search: Optional[str] = None,
    sort: str = "created_desc",
    limit: int = 100,
    offset: int = 0,
    min_score: Optional[int] = None,
    created_today: bool = False,
    contacted_today: bool = False,
):
    order_map = {
        "score_desc":   "score DESC",
        "score_asc":    "score ASC",
        "reviews_desc": "reviews DESC",
        "reviews_asc":  "reviews ASC",
        "name_asc":     "name ASC",
        "name_desc":    "name DESC",
        "created_desc": "created_at DESC",
        "created_asc":  "created_at ASC",
    }
    order_clause = order_map.get(sort, "created_at DESC")

    conn = get_db()
    query = "SELECT * FROM leads WHERE 1=1"
    params = []

    if status and status != "all":
        query += " AND status = ?"
        params.append(status)
    if priority:
        query += " AND priority = ?"
        params.append(priority)
    if category and category != "all":
        query += " AND business_type = ?"
        params.append(category)
    if search:
        query += " AND (name LIKE ? OR business_type LIKE ? OR address LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
    if min_score is not None:
        query += " AND score >= ?"
        params.append(min_score)
    if created_today:
        query += " AND DATE(created_at) = DATE('now')"
    if contacted_today:
        # No last_contacted column on leads — derive from activities sent today.
        query += " AND id IN (SELECT lead_id FROM activities WHERE DATE(sent_at) = DATE('now'))"

    query += f" ORDER BY {order_clause} LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    leads = [dict(row) for row in conn.execute(query, params).fetchall()]
    total = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    conn.close()
    return {"leads": leads, "total": total}

@app.get("/api/leads/{lead_id}")
async def get_lead(lead_id: int):
    conn = get_db()
    lead = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    activities = conn.execute(
        "SELECT * FROM activities WHERE lead_id = ? ORDER BY created_at DESC",
        (lead_id,)
    ).fetchall()
    conn.close()
    return {**dict(lead), "activities": [dict(a) for a in activities]}

@app.patch("/api/leads/{lead_id}")
async def update_lead(lead_id: int, update: LeadUpdate):
    conn = get_db()
    fields = {k: v for k, v in update.dict().items() if v is not None}
    if not fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    status_value = fields.pop("status", None)
    fields['updated_at'] = datetime.now().isoformat()
    set_clause = ", ".join([f"{k} = ?" for k in fields])
    conn.execute(
        f"UPDATE leads SET {set_clause} WHERE id = ?",
        list(fields.values()) + [lead_id]
    )
    conn.commit()
    conn.close()

    if status_value:
        update_lead_status(
            lead_id,
            status_value,
            source="api",
            note=fields.get("notes", "") or "",
        )
    return {"success": True}

@app.post("/api/leads")
async def create_lead(lead: LeadCreate):
    # Score from the provided contact fields (so manual leads rank like scraped ones).
    score = lead.score or compute_lead_score(
        reviews=lead.reviews or 0,
        website=lead.website or "",
        phone=lead.phone or "",
        email=lead.email or "",
    )
    conn = get_db()
    cursor = conn.execute("""
        INSERT INTO leads (name, contact_name, business_type, phone, email, website, address, city,
            rating, reviews, score, status, source,
            whatsapp_message, email_subject, email_body, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (lead.name, lead.contact_name, lead.business_type, lead.phone, lead.email, lead.website,
          lead.address, lead.city, lead.rating, lead.reviews, score,
          lead.status or "new", lead.source or "manual",
          lead.whatsapp_message, lead.email_subject, lead.email_body, lead.notes))
    conn.commit()
    lead_id = cursor.lastrowid
    row = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    emit_hud_event("new_lead", {
        "name": lead.name,
        "category": lead.business_type or "",
        "lead_id": lead_id,
    })
    return {"success": True, "id": lead_id, "lead": dict(row) if row else None}

@app.delete("/api/leads/{lead_id}")
async def delete_lead(lead_id: int):
    conn = get_db()
    conn.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
    conn.execute("DELETE FROM activities WHERE lead_id = ?", (lead_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@app.post("/api/leads/scrape")
def scrape_leads_sync(req: LeadScrapeRequest):
    """
    Synchronous Google Places scrape — scrape + email + score + import,
    then return the found leads immediately.
    Returns: {success, total_found, new_leads, query, leads:[...]}
    Defined as a sync endpoint so FastAPI runs it in a threadpool (blocking HTTP is fine).
    """
    import re
    from bs4 import BeautifulSoup

    GKEY = google_api_key()
    if not GKEY:
        raise HTTPException(status_code=400, detail="Google Places API key not configured (set GOOGLE_API_KEY or GOOGLE_PLACES_API_KEY)")

    location_str = ", ".join([p for p in [req.city, req.state, req.country] if p])
    search_query = f"{req.query} in {location_str}" if location_str else req.query
    region = COUNTRY_REGION_MAP.get((req.country or "").strip(), "in")
    max_results = max(1, int(req.max_results or 20))

    EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
    BLACKLIST = {'example.com', 'sentry.io', 'wixpress.com', 'google.com', 'facebook.com'}

    leads = []
    seen = set()
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": search_query, "key": GKEY, "region": region, "language": "en"},
            timeout=15,
        )
        places = r.json().get("results", [])
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Google Places error: {e}")

    for place in places[:max_results]:
        pid = place.get("place_id")
        if not pid or pid in seen:
            continue
        seen.add(pid)

        try:
            det = requests.get(
                "https://maps.googleapis.com/maps/api/place/details/json",
                params={
                    "place_id": pid,
                    "fields": "name,formatted_phone_number,website,rating,user_ratings_total,formatted_address,geometry",
                    "key": GKEY,
                },
                timeout=10,
            ).json().get("result", {})
        except Exception:
            det = {}

        name = det.get("name", place.get("name", ""))
        website = det.get("website", "")
        phone = det.get("formatted_phone_number", "")
        reviews = det.get("user_ratings_total", 0) or 0
        lat = det.get("geometry", {}).get("location", {}).get("lat", 0)
        lng = det.get("geometry", {}).get("location", {}).get("lng", 0)

        email, email_source = "", ""
        if website:
            try:
                w = website if website.startswith("http") else "https://" + website
                rr = requests.get(w, headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
                soup = BeautifulSoup(rr.text, "html.parser")
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    if href.startswith("mailto:"):
                        e = href.replace("mailto:", "").split("?")[0].strip().lower()
                        if "@" in e and not any(b in e for b in BLACKLIST):
                            email, email_source = e, "mailto"
                            break
                if not email:
                    for m in EMAIL_REGEX.findall(rr.text):
                        if "@" in m and "." in m.split("@")[1] and not any(b in m for b in BLACKLIST):
                            email, email_source = m.lower(), "website"
                            break
            except Exception:
                pass

        score = compute_lead_score(reviews=reviews, website=website, phone=phone, email=email)

        leads.append({
            "name": name,
            "business_type": req.query.strip(),
            "phone": phone,
            "email": email,
            "email_source": email_source,
            "website": website,
            "rating": det.get("rating", 0) or 0,
            "reviews": reviews,
            "address": det.get("formatted_address", ""),
            "score": score,
            "lat": lat,
            "lng": lng,
            "place_id": pid,
        })

    # Import into CRM with de-dup by name/phone
    imported = 0
    conn = get_db()
    for lead in leads:
        existing = conn.execute(
            "SELECT id FROM leads WHERE name = ? OR (phone = ? AND phone != '')",
            (lead["name"], lead["phone"]),
        ).fetchone()
        if existing:
            lead["id"] = existing["id"]
            continue
        cursor = conn.execute(
            """
            INSERT INTO leads (name, business_type, phone, email, email_source,
                website, address, rating, reviews, score, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'crm_scrape')
            """,
            (
                lead["name"], lead["business_type"], lead["phone"], lead["email"],
                lead["email_source"], lead["website"], lead["address"],
                float(lead["rating"] or 0), int(lead["reviews"] or 0), int(lead["score"] or 0),
            ),
        )
        lead["id"] = cursor.lastrowid
        imported += 1
        emit_hud_event("new_lead", {
            "name": lead["name"], "category": lead["business_type"], "lead_id": lead["id"],
        })
    conn.commit()
    conn.close()

    return {
        "success": True,
        "total_found": len(leads),
        "new_leads": imported,
        "query": search_query,
        "leads": leads,
    }


@app.post("/api/leads/bulk-delete")
async def bulk_delete_leads(req: BulkIds):
    """Delete multiple leads (and their activities) by id array."""
    if not req.lead_ids:
        raise HTTPException(status_code=400, detail="No lead_ids provided")
    conn = get_db()
    placeholders = ",".join("?" for _ in req.lead_ids)
    conn.execute(f"DELETE FROM activities WHERE lead_id IN ({placeholders})", list(req.lead_ids))
    result = conn.execute(f"DELETE FROM leads WHERE id IN ({placeholders})", list(req.lead_ids))
    deleted = result.rowcount
    conn.commit()
    conn.close()
    return {"success": True, "deleted": deleted}


@app.post("/api/leads/bulk-enrich")
async def bulk_enrich_leads(req: BulkIds, background_tasks: BackgroundTasks):
    """Trigger Claude enrichment for multiple leads by id array (runs in background)."""
    if not req.lead_ids:
        raise HTTPException(status_code=400, detail="No lead_ids provided")
    ids = list(req.lead_ids)

    def run():
        from enrichment import enrich_lead_in_db
        for lid in ids:
            try:
                enrich_lead_in_db(lid)
            except Exception as e:
                print(f"bulk-enrich error for lead {lid}: {e}")

    background_tasks.add_task(run)
    return {"success": True, "message": f"Enriching {len(ids)} leads in background", "count": len(ids)}

# ─────────────────────────────────────────────
# ACTIVITIES & OUTREACH
# ─────────────────────────────────────────────

@app.post("/api/activities")
async def log_activity(activity: ActivityCreate):
    meta = {}
    if activity.metadata_json:
        try:
            meta = json.loads(activity.metadata_json)
        except Exception:
            meta = {"raw": activity.metadata_json}
    log_activity_event(
        activity.lead_id,
        activity.type,
        activity.channel,
        activity.content,
        status=activity.status or "logged",
        direction=activity.direction or "outbound",
        campaign_name=activity.campaign_name or "",
        metadata=meta,
        scheduled_at=activity.scheduled_at,
    )
    if activity.status and activity.status.lower() == "logged":
        conn = get_db()
        _now = datetime.now().isoformat()
        conn.execute(
            "UPDATE leads SET status = 'contacted', updated_at = ?, contacted_at = ? WHERE id = ?",
            (_now, _now, activity.lead_id)
        )
        conn.commit()
        conn.close()
    return {"success": True}

@app.post("/api/send-email")
async def send_email(data: EmailSend, background_tasks: BackgroundTasks):
    from email_engine import send_email_now, schedule_sequence
    conn = get_db()
    lead = conn.execute("SELECT status FROM leads WHERE id = ?", (data.lead_id,)).fetchone()
    conn.close()
    if lead and not outbound_allowed(lead["status"]):
        raise HTTPException(status_code=403, detail="Lead must be approved before sending")
    def send():
        result = send_email_now(data.to_email, data.subject, data.body, data.lead_id, "manual")
        if result["success"]:
            conn = get_db()
            s = {r["key"]: r["value"] for r in conn.execute("SELECT * FROM settings").fetchall()}
            conn.close()
            if s.get("auto_sequence_enabled", "true") == "true":
                schedule_sequence(data.lead_id, data.to_email)
    background_tasks.add_task(send)
    return {"success": True, "message": "Email sending in background"}

@app.post("/api/test-email")
async def test_email(background_tasks: BackgroundTasks):
    from email_engine import send_email_now
    conn = get_db()
    settings = {row["key"]: row["value"] for row in conn.execute("SELECT * FROM settings").fetchall()}
    conn.close()
    email_user = settings.get("email_user", "")
    if not email_user:
        raise HTTPException(status_code=400, detail="Email not configured")
    def send():
        send_email_now(email_user, "MARVIS CRM — Email is working!", "Your email automation is configured correctly.\n\n— MARVIS", 0, "test")
    background_tasks.add_task(send)
    return {"success": True, "message": f"Test email sent to {email_user}"}

@app.post("/api/send-sequence/{lead_id}")
async def send_full_sequence(lead_id: int, to_email: str, background_tasks: BackgroundTasks):
    from email_engine import send_email_now, schedule_sequence, TEMPLATES
    conn = get_db()
    lead = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    lead = dict(lead)
    if not outbound_allowed(lead.get("status", "")):
        raise HTTPException(status_code=403, detail="Lead must be approved before sending")
    def run():
        content = TEMPLATES["initial"]["build"](lead)
        result = send_email_now(to_email, content["subject"], content["body"], lead_id, "initial")
        if result["success"]:
            schedule_sequence(lead_id, to_email)
            print(f"Sequence launched for {lead['name']}")
    background_tasks.add_task(run)
    return {"success": True, "message": "Sequence launched — initial + Day 3 + Day 7 follow-ups scheduled"}

@app.get("/api/email-queue")
async def get_email_queue():
    conn = get_db()
    queue = [dict(r) for r in conn.execute("""
        SELECT f.*, l.name as lead_name, l.business_type
        FROM follow_ups f JOIN leads l ON f.lead_id = l.id
        WHERE f.channel = 'email' ORDER BY f.scheduled_at ASC LIMIT 100
    """).fetchall()]
    conn.close()
    return {"queue": queue}

@app.post("/api/bulk-sequence")
async def bulk_sequence(background_tasks: BackgroundTasks):
    from email_engine import send_email_now, schedule_sequence, TEMPLATES
    conn = get_db()
    leads = [dict(r) for r in conn.execute(
        "SELECT * FROM leads WHERE email IS NOT NULL AND email != '' AND status = 'approved' LIMIT 50"
    ).fetchall()]
    conn.close()
    def run_bulk():
        sent = 0
        for lead in leads:
            content = TEMPLATES["initial"]["build"](lead)
            result = send_email_now(lead["email"], content["subject"], content["body"], lead["id"], "initial")
            if result["success"]:
                schedule_sequence(lead["id"], lead["email"])
                sent += 1
                time.sleep(2)
        print(f"Bulk done: {sent}/{len(leads)}")
    background_tasks.add_task(run_bulk)
    return {"success": True, "message": f"Bulk sequence started for {len(leads)} leads"}

@app.post("/api/schedule-followup/{lead_id}")
async def schedule_followup(lead_id: int, days: int = 3):
    """Schedule automatic follow-up"""
    conn = get_db()
    lead = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    if not lead:
        conn.close()
        raise HTTPException(status_code=404, detail="Lead not found")
    if not outbound_allowed(lead["status"]):
        conn.close()
        raise HTTPException(status_code=403, detail="Lead must be approved before scheduling follow-ups")

    lead = dict(lead)
    scheduled = (datetime.now() + timedelta(days=days)).isoformat()
    followup_msg = f"""Hi {lead['name'].split()[0] if lead['name'] else 'there'},

Just following up on my message from {days} days ago about improving your online presence.

I'd love to send you the free content sample — Google profile + email sequence + social captions. Takes 2 minutes to review and there's absolutely no commitment.

Would that be helpful? — Manikanta | Talktiv AI"""

    conn.execute("""
        INSERT INTO follow_ups (lead_id, message, channel, scheduled_at, status)
        VALUES (?, ?, 'whatsapp', ?, 'pending')
    """, (lead_id, followup_msg, scheduled))
    log_activity_event(
        lead_id,
        "followup_scheduled",
        "whatsapp",
        followup_msg,
        status="pending",
        direction="outbound",
        metadata={"days": days, "source": "api"},
    )
    conn.commit()
    conn.close()
    return {"success": True, "scheduled_at": scheduled, "message": followup_msg}


@app.post("/api/follow-ups")
async def create_follow_up(req: FollowUpCreate):
    """Create a follow-up from the Reply Detail drawer: stores the user's note as
    the follow-up message at the chosen date, status 'pending'."""
    conn = get_db()
    cursor = conn.execute(
        "INSERT INTO follow_ups (lead_id, message, scheduled_at, status) VALUES (?, ?, ?, 'pending')",
        (req.lead_id, req.note, req.scheduled_at),
    )
    new_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {"success": True, "id": new_id}

# ─────────────────────────────────────────────
# DASHBOARD STATS
# ─────────────────────────────────────────────

@app.get("/api/health")
def health():
    # CRM / DB reachability
    crm_ok = False
    try:
        conn = get_db()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        crm_ok = True
    except Exception as e:
        print(f"health: DB check failed: {e}")

    claude_ok = bool(os.getenv("ANTHROPIC_API_KEY"))
    google_ok = bool(google_api_key())

    scheduler_ok = False
    next_run = ""
    try:
        from scheduler import get_scheduler_status
        sched = get_scheduler_status()
        scheduler_ok = bool(sched.get("enabled"))
        run_time = sched.get("run_time", "")
        next_run = f"{run_time} IST" if run_time else ""
    except Exception as e:
        print(f"health: scheduler check failed: {e}")

    return {
        "status": "ok" if crm_ok else "degraded",
        "service": "marvis-crm",
        "crm": "ok" if crm_ok else "down",
        "claude_api": "configured" if claude_ok else "not_configured",
        "google_api": "configured" if google_ok else "not_configured",
        "scheduler": "running" if scheduler_ok else "stopped",
        "next_run": next_run,
    }


@app.post("/api/bulk-approve")
async def bulk_approve():
    """Approve all leads currently in pending_review status."""
    conn = get_db()
    result = conn.execute(
        "UPDATE leads SET status='approved' WHERE status='pending_review'"
    )
    conn.commit()
    count = result.rowcount
    conn.close()
    log_activity_event(None, "bulk_approve", "system", f"Bulk approved {count} leads", status="logged", direction="system", metadata={"approved": count})
    emit_hud_event("bulk_approve", {"approved": count})
    return {"approved": count, "message": f"{count} leads approved"}


@app.post("/api/recompute-scores")
async def recompute_scores():
    """Re-score all existing leads with the current scoring model (fixes legacy inflated scores)."""
    result = recompute_all_scores()
    conn = get_db()
    hot = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE score >= 75 AND (COALESCE(email,'') != '' OR COALESCE(phone,'') != '')"
    ).fetchone()[0]
    conn.close()
    return {"success": True, "updated": result["updated"], "hot_leads": hot}


@app.get("/api/test/anthropic")
def test_anthropic_key():
    """Verify the configured Anthropic API key with a tiny live call."""
    key = os.getenv("ANTHROPIC_API_KEY", "")
    if not key:
        return {"ok": False, "configured": False, "message": "ANTHROPIC_API_KEY not set"}
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=key)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=5,
            messages=[{"role": "user", "content": "Reply with OK"}],
        )
        text = (resp.content[0].text if resp.content else "").strip()
        return {"ok": True, "configured": True, "message": f"Claude reachable ({text[:20]})"}
    except Exception as e:
        return {"ok": False, "configured": True, "message": f"Claude error: {str(e)[:160]}"}


@app.get("/api/test/google")
def test_google_key():
    """Verify the configured Google Places key with a tiny live call."""
    key = google_api_key()
    if not key:
        return {"ok": False, "configured": False, "message": "Google Places key not set (GOOGLE_API_KEY / GOOGLE_PLACES_API_KEY)"}
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": "cafe in Hyderabad", "key": key, "region": "in"},
            timeout=10,
        )
        data = r.json()
        status = data.get("status", "")
        if status in ("OK", "ZERO_RESULTS"):
            return {"ok": True, "configured": True, "message": f"Google Places reachable ({status})"}
        return {"ok": False, "configured": True, "message": f"Google error: {status} {data.get('error_message', '')}".strip()[:160]}
    except Exception as e:
        return {"ok": False, "configured": True, "message": f"Google error: {str(e)[:160]}"}


@app.post("/api/test/email")
async def test_email_smtp(request: Request):
    """Synchronous Gmail SMTP test — sends a real message and returns the actual result."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    conn = get_db()
    settings = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM settings").fetchall()}
    conn.close()
    email_user = settings.get("email_user", "")
    email_pass = settings.get("email_pass", "")
    sender_name = settings.get("sender_name", "Manikanta | Talktiv AI")
    to_email = (body.get("to") or "").strip() or email_user

    if not email_user or not email_pass:
        return JSONResponse({"error": "Gmail credentials not configured in Settings"}, status_code=400)
    if not to_email:
        return JSONResponse({"error": "No recipient email provided"}, status_code=400)

    try:
        msg = MIMEMultipart()
        msg["From"] = f"{sender_name} <{email_user}>"
        msg["To"] = to_email
        msg["Subject"] = "MARVIS CRM — Test Email"
        msg.attach(MIMEText(
            "This is a test email from MARVIS CRM. If you received this, Gmail SMTP is configured correctly.",
            "plain",
        ))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=20) as server:
            server.login(email_user, email_pass)
            server.sendmail(email_user, to_email, msg.as_string())
        return JSONResponse({"success": True, "message": f"Test email sent to {to_email}"})
    except smtplib.SMTPAuthenticationError:
        return JSONResponse({"error": "Gmail authentication failed. Check your App Password."}, status_code=401)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/stats")
async def get_stats():
    conn = get_db()

    total = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    by_status = dict(conn.execute(
        "SELECT status, COUNT(*) FROM leads GROUP BY status"
    ).fetchall())
    hot_leads = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE score >= 75 AND (COALESCE(email,'') != '' OR COALESCE(phone,'') != '')"
    ).fetchone()[0]
    contacted = by_status.get('contacted', 0)
    converted = by_status.get('converted', 0)
    pending_followups = conn.execute(
        "SELECT COUNT(*) FROM follow_ups WHERE status = 'pending'"
    ).fetchone()[0]
    recent_leads = [dict(r) for r in conn.execute(
        "SELECT name, business_type, score, status, created_at FROM leads ORDER BY created_at DESC LIMIT 5"
    ).fetchall()]
    by_type = dict(conn.execute(
        "SELECT business_type, COUNT(*) FROM leads GROUP BY business_type ORDER BY COUNT(*) DESC LIMIT 6"
    ).fetchall())
    pending_approvals = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE status IN ('pending_review', 'new') AND (COALESCE(whatsapp_message,'') != '' OR COALESCE(email_subject,'') != '' OR COALESCE(email_body,'') != '')"
    ).fetchone()[0]
    new_leads = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE date(created_at) = date('now')"
    ).fetchone()[0]
    emails_sent_today = conn.execute(
        "SELECT COUNT(*) FROM activities WHERE channel = 'email' AND status = 'sent' AND date(created_at) = date('now')"
    ).fetchone()[0]
    replies_today = conn.execute(
        "SELECT COUNT(*) FROM activities WHERE type IN ('reply', 'inbound', 'email_reply', 'wa_reply', 'whatsapp_reply') AND date(created_at) = date('now')"
    ).fetchone()[0]
    # Replies received today that have not yet had any outbound response after them
    new_replies = conn.execute(
        """
        SELECT COUNT(*) FROM activities a
        WHERE a.type IN ('reply', 'inbound', 'email_reply', 'wa_reply', 'whatsapp_reply')
          AND date(a.created_at) = date('now')
          AND NOT EXISTS (
              SELECT 1 FROM activities b
              WHERE b.lead_id = a.lead_id
                AND b.direction = 'outbound'
                AND b.created_at > a.created_at
          )
        """
    ).fetchone()[0]

    conn.close()

    # Real scheduler state (no hardcoded values)
    scheduler_running = False
    next_run = ""
    try:
        from scheduler import get_scheduler_status
        sched = get_scheduler_status()
        scheduler_running = bool(sched.get("enabled"))
        run_time = sched.get("run_time", "")
        next_run = f"{run_time} IST" if run_time else ""
    except Exception as e:
        print(f"scheduler status error in /api/stats: {e}")

    return {
        "total_leads": total,
        "new_leads": new_leads,
        "hot_leads": hot_leads,
        "contacted": contacted,
        "converted": converted,
        "pending_followups": pending_followups,
        "pending_approvals": pending_approvals,
        "emails_sent_today": emails_sent_today,
        "replies_today": replies_today,
        "new_replies": new_replies,
        "scheduler_running": scheduler_running,
        "next_run": next_run,
        "conversion_rate": round((converted / contacted * 100) if contacted > 0 else 0, 1),
        "by_status": by_status,
        "by_type": by_type,
        "recent_leads": recent_leads
    }


@app.get("/api/hot-leads")
async def hot_leads(limit: int = 10):
    conn = get_db()
    leads = [
        dict(row)
        for row in conn.execute(
            """
            SELECT id, name, business_type, phone, email, score, status, created_at, updated_at
            FROM leads
            WHERE score >= 75 AND (COALESCE(email,'') != '' OR COALESCE(phone,'') != '')
            ORDER BY score DESC, updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    ]
    conn.close()
    return {"leads": leads, "count": len(leads)}


@app.get("/api/recent-replies")
async def recent_replies(limit: int = 10):
    conn = get_db()
    replies = [
        dict(row)
        for row in conn.execute(
            """
            SELECT a.*, l.name AS lead_name, l.business_type, l.phone
            FROM activities a
            LEFT JOIN leads l ON l.id = a.lead_id
            WHERE a.type IN ('reply', 'inbound', 'ai_reply', 'email_reply', 'wa_reply', 'whatsapp_reply')
            ORDER BY a.created_at DESC, a.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    ]
    conn.close()
    return {"replies": replies, "count": len(replies)}


@app.get("/api/lead-timeline/{lead_id}")
async def lead_timeline(lead_id: int):
    result = get_lead_timeline(lead_id)
    if not result.get("lead"):
        raise HTTPException(status_code=404, detail="Lead not found")
    return result


@app.get("/api/pending-approvals")
async def pending_approvals(limit: int = 100):
    return {"leads": get_pending_approvals(limit=limit)}


@app.post("/api/approve-outreach/{lead_id}")
async def approve_outreach(lead_id: int):
    return update_lead_status(
        lead_id,
        "approved",
        source="approval",
        note="Outreach approved",
    )


@app.post("/api/pause-lead/{lead_id}")
async def pause_lead(lead_id: int):
    return update_lead_status(
        lead_id,
        "paused",
        source="approval",
        note="Paused by user",
    )


@app.get("/api/campaign-stats")
async def campaign_stats():
    return get_campaign_stats()


# ─────────────────────────────────────────────
# ANALYTICS — activity logs
# Discriminator in this DB is `channel` ('email'/'whatsapp') + `status`,
# with replies as direction='inbound'. type fallbacks cover older rows.
# ─────────────────────────────────────────────

@app.get("/api/analytics/emails")
async def analytics_emails(limit: int = 100):
    """Return OUTBOUND email activity only (sent emails — never inbound replies).
    direction defaults to 'outbound', so this keeps every sent row and excludes
    inbound email_reply rows that share channel='email'."""
    conn = get_db()
    rows = conn.execute("""
        SELECT a.*, l.name AS lead_name, l.email AS lead_email
        FROM activities a
        LEFT JOIN leads l ON a.lead_id = l.id
        WHERE (a.channel = 'email'
               OR a.type IN ('email', 'email_sent', 'sequence_email', 'followup_scheduled'))
          AND a.direction = 'outbound'
        ORDER BY a.created_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/analytics/whatsapp")
async def analytics_whatsapp(limit: int = 100):
    """Return WhatsApp activity log."""
    conn = get_db()
    rows = conn.execute("""
        SELECT a.*, l.name AS lead_name, l.phone AS lead_phone
        FROM activities a
        LEFT JOIN leads l ON a.lead_id = l.id
        WHERE a.channel = 'whatsapp'
           OR a.type IN ('whatsapp', 'whatsapp_sent', 'wa_sent')
        ORDER BY a.created_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/analytics/replies")
async def analytics_replies(limit: int = 100):
    """Return INBOUND replies only. All real replies (Gmail + WhatsApp) are logged
    with direction='inbound', so the strict filter guarantees no outbound row leaks
    in (the old type-OR could match an outbound row sharing a reply-ish type)."""
    conn = get_db()
    rows = conn.execute("""
        SELECT a.*, l.name AS lead_name, l.phone AS lead_phone,
               l.email AS lead_email
        FROM activities a
        LEFT JOIN leads l ON a.lead_id = l.id
        WHERE a.direction = 'inbound'
        ORDER BY a.created_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/analytics/summary")
async def analytics_summary():
    """All-time campaign totals + cumulative rates (denominator = all outreach sent)."""
    conn = get_db()
    # 'sent'-like statuses observed in this DB: sent / delivered / opened.
    total_emails = conn.execute("""
        SELECT COUNT(*) FROM activities
        WHERE channel = 'email' AND status IN ('sent', 'delivered', 'opened')
    """).fetchone()[0]
    total_whatsapp = conn.execute("""
        SELECT COUNT(*) FROM activities
        WHERE channel = 'whatsapp' AND status IN ('sent', 'delivered', 'opened')
    """).fetchone()[0]
    total_replies = conn.execute("""
        SELECT COUNT(*) FROM activities
        WHERE direction = 'inbound'
           OR type IN ('reply', 'inbound', 'wa_reply', 'whatsapp_reply', 'email_reply')
    """).fetchone()[0]
    total_interested = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE status = 'interested'"
    ).fetchone()[0]
    total_converted = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE status = 'converted'"
    ).fetchone()[0]
    conn.close()

    total_outreach = total_emails + total_whatsapp

    def rate(n: int) -> float:
        return round((n / total_outreach) * 100, 1) if total_outreach else 0.0

    return {
        "total_emails": total_emails,
        "total_whatsapp": total_whatsapp,
        "total_outreach": total_outreach,
        "total_replies": total_replies,
        "total_interested": total_interested,
        "total_converted": total_converted,
        "reply_rate": rate(total_replies),
        "interest_rate": rate(total_interested),
        "conversion_rate": rate(total_converted),
    }

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────

@app.websocket("/ws/hud")
async def hud_websocket(websocket: WebSocket):
    await hud_manager.connect(websocket)
    try:
        # Push current CRM snapshot so HUD populates immediately on connect.
        # The DB work is synchronous (psycopg2) — run it in a worker thread so a
        # slow/locked query can never freeze the event loop and stall every other
        # HTTP request (same off-loop pattern as ensure_crm_schema in startup).
        try:
            def _snapshot():
                conn = get_db()
                total = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
                hot = conn.execute("SELECT COUNT(*) FROM leads WHERE score >= 60").fetchone()[0]
                approvals = conn.execute(
                    "SELECT COUNT(*) FROM leads WHERE status IN ('pending_review','new')"
                    " AND (COALESCE(whatsapp_message,'') != '' OR COALESCE(email_subject,'') != ''"
                    " OR COALESCE(email_body,'') != '')"
                ).fetchone()[0]
                conn.close()
                return total, hot, approvals

            total, hot, approvals = await asyncio.to_thread(_snapshot)
            await websocket.send_text(json.dumps({
                "type": "crm_snapshot",
                "data": {"total_leads": total, "hot_leads": hot, "pending_approvals": approvals},
                "ts": datetime.now(timezone.utc).isoformat(),
            }, ensure_ascii=False))
        except Exception:
            pass
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        hud_manager.disconnect(websocket)


@app.get("/api/settings")
async def get_settings():
    def _load():
        conn = get_db()
        s = {row['key']: row['value'] for row in conn.execute("SELECT * FROM settings").fetchall()}
        conn.close()
        return s
    settings = await asyncio.to_thread(_load)
    # Mask password
    if settings.get('email_pass'):
        settings['email_pass'] = '••••••••'
    return settings

@app.post("/api/settings")
async def update_settings(update: SettingsUpdate):
    def _save():
        conn = get_db()
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (update.key, update.value)
        )
        conn.commit()
        conn.close()
    await asyncio.to_thread(_save)
    return {"success": True}


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/scheduler/status")
async def scheduler_status():
    from scheduler import get_scheduler_status
    return await asyncio.to_thread(get_scheduler_status)


@app.post("/api/scheduler/run-now")
async def scheduler_run_now():
    from scheduler import run_scheduler_now
    return run_scheduler_now()


@app.post("/api/scheduler/config")
async def scheduler_config(update: SchedulerConfigUpdate):
    from scheduler import update_scheduler_config
    return update_scheduler_config({k: v for k, v in update.dict().items() if v is not None})


# ─────────────────────────────────────────────
# WHATSAPP ENDPOINTS
# ─────────────────────────────────────────────

class WASendRequest(BaseModel):
    lead_id: int
    phone: str
    message: str

class WATemplateRequest(BaseModel):
    lead_id: int
    phone: str
    template_name: str
    language: str = "en"

@app.get("/api/whatsapp/verify")
async def wa_verify():
    """Verify WhatsApp credentials"""
    from whatsapp_engine import verify_whatsapp_credentials
    return verify_whatsapp_credentials()

@app.post("/api/whatsapp/send")
async def wa_send(req: WASendRequest, background_tasks: BackgroundTasks):
    """Send WhatsApp message to a lead"""
    from whatsapp_engine import send_whatsapp_text
    conn = get_db()
    lead = conn.execute("SELECT status FROM leads WHERE id = ?", (req.lead_id,)).fetchone()
    conn.close()
    if lead and not outbound_allowed(lead["status"]):
        raise HTTPException(status_code=403, detail="Lead must be approved before sending")
    def send():
        result = send_whatsapp_text(req.phone, req.message, req.lead_id)
        print(f"WhatsApp send result: {result}")
    background_tasks.add_task(send)
    return {"success": True, "message": "WhatsApp message queued"}

@app.post("/api/whatsapp/send-now")
async def wa_send_now(req: WASendRequest):
    """Send WhatsApp message synchronously — returns result immediately"""
    from whatsapp_engine import send_whatsapp_text
    conn = get_db()
    lead = conn.execute("SELECT status FROM leads WHERE id = ?", (req.lead_id,)).fetchone()
    conn.close()
    if lead and not outbound_allowed(lead["status"]):
        return {"success": False, "error": "Lead must be approved before sending"}
    result = send_whatsapp_text(req.phone, req.message, req.lead_id)
    return result

@app.post("/api/whatsapp/schedule-followup/{lead_id}")
async def wa_schedule_followup(lead_id: int, phone: str, days: int = 3):
    """Schedule WhatsApp follow-up"""
    from whatsapp_engine import schedule_wa_followup
    conn = get_db()
    lead = conn.execute("SELECT status FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    if lead and not outbound_allowed(lead["status"]):
        raise HTTPException(status_code=403, detail="Lead must be approved before scheduling follow-ups")
    return schedule_wa_followup(lead_id, phone, days)

@app.get("/api/whatsapp/status")
async def wa_status():
    """Get WhatsApp config status"""
    conn = get_db()
    settings = {r['key']: r['value'] for r in conn.execute("SELECT * FROM settings").fetchall()}
    conn.close()
    has_phone_id = bool(settings.get('wa_phone_id') or os.getenv('WHATSAPP_PHONE_ID'))
    has_token    = bool(settings.get('wa_token') or os.getenv('WHATSAPP_TOKEN'))
    return {
        "configured": has_phone_id and has_token,
        "has_phone_id": has_phone_id,
        "has_token": has_token
    }


# ─────────────────────────────────────────────
# EMAIL FINDER ENDPOINTS
# ─────────────────────────────────────────────

@app.post("/api/find-email/{lead_id}")
async def find_email_for_lead(lead_id: int, background_tasks: BackgroundTasks):
    """Find email for a single lead"""
    conn = get_db()
    lead = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    def run():
        from email_finder import find_email
        result = find_email(dict(lead))
        if result["email"]:
            conn = get_db()
            conn.execute(
                "UPDATE leads SET email = ?, email_source = ?, updated_at = ? WHERE id = ?",
                (result["email"], result.get("source","scraped"), datetime.now().isoformat(), lead_id)
            )
            conn.commit()
            conn.close()
            print(f"📧 Email found for {lead['name']}: {result['email']}")

    background_tasks.add_task(run)
    return {"success": True, "message": "Email search started in background"}

@app.post("/api/find-emails-batch")
async def find_emails_batch(background_tasks: BackgroundTasks, limit: int = 50):
    """Find emails for all leads missing one"""
    def run():
        from email_finder import run_batch_finder
        result = run_batch_finder(limit=limit)
        print(f"📧 Batch complete: {result}")

    background_tasks.add_task(run)
    return {"success": True, "message": f"Batch email finder started for up to {limit} leads"}

@app.get("/api/email-stats")
async def get_email_stats():
    """Stats on email coverage"""
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    with_email = conn.execute("SELECT COUNT(*) FROM leads WHERE email IS NOT NULL AND email != ''").fetchone()[0]
    with_website = conn.execute("SELECT COUNT(*) FROM leads WHERE website IS NOT NULL AND website != ''").fetchone()[0]
    conn.close()
    return {
        "total_leads": total,
        "with_email": with_email,
        "without_email": total - with_email,
        "with_website": with_website,
        "coverage_pct": round(with_email / total * 100, 1) if total > 0 else 0
    }

# ─────────────────────────────────────────────
# AUTO FOLLOWUP SCHEDULER
# ─────────────────────────────────────────────

def followup_scheduler():
    """Background thread — checks for due follow-ups every 30 min"""
    while True:
        try:
            conn = get_db()
            now = datetime.now().isoformat()
            due = conn.execute(
                "SELECT * FROM follow_ups WHERE status = 'pending' AND scheduled_at <= ?",
                (now,)
            ).fetchall()

            for fu in due:
                fu = dict(fu)
                print(f"📱 Follow-up due for lead {fu['lead_id']}: {fu['channel']}")
                conn.execute(
                    "UPDATE follow_ups SET status = 'ready' WHERE id = ?",
                    (fu['id'],)
                )
                log_activity_event(
                    fu['lead_id'],
                    "followup_scheduled",
                    fu['channel'],
                    fu['message'],
                    status="ready_to_send",
                    direction="outbound",
                    metadata={"followup_id": fu['id'], "source": "followup_scheduler"},
                )

            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Scheduler error: {e}")

        time.sleep(1800)  # Check every 30 minutes


@app.get("/api/autocomplete-location")
async def autocomplete_location(query: str, country: str = "India", type: str = "suburb"):
    """Google Places autocomplete for location fields"""
    GKEY = google_api_key()
    if not GKEY:
        return {"suggestions": []}
    try:
        types_map = {"country": "country", "state": "administrative_area_level_1", "suburb": "locality|sublocality"}
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/autocomplete/json",
            params={"input": query, "key": GKEY, "types": types_map.get(type, "locality"), "language": "en"},
            timeout=5
        )
        data = r.json()
        suggestions = [p["description"] for p in data.get("predictions", [])[:6]]
        return {"suggestions": suggestions}
    except Exception as e:
        return {"suggestions": []}


# ─────────────────────────────────────────────
# FIND LEADS — IN-CRM SCRAPER
# ─────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    country: str
    state: str
    suburb: str
    categories: List[str]
    max_per_query: int = 20

@app.post("/api/scrape-leads")
async def scrape_leads_endpoint(req: ScrapeRequest, background_tasks: BackgroundTasks):
    """Run scraper from CRM UI — scrape + email + score + import in one shot"""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../MARVIS_LEAD_MACHINE'))

    location_str = f"{req.suburb}, {req.state}, {req.country}"
    queries = [f"{cat} in {req.suburb} {req.state}" for cat in req.categories]

    def run():
        try:
            # Try importing from MARVIS_LEAD_MACHINE
            try:
                from scraper import scrape_leads, score_lead, find_email
                use_full = True
            except ImportError:
                use_full = False

            if use_full:
                leads = scrape_leads(queries, max_per_query=req.max_per_query)
            else:
                # Fallback inline scraper using Google Places API
                import requests as req_lib
                GKEY = google_api_key()
                leads = []
                seen = set()

                for query in queries:
                    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
                    params = {
                        "query": query,
                        "key": GKEY,
                        "region": "in",
                        "language": "en",
                    }
                    r = req_lib.get(url, params=params, timeout=10)
                    data = r.json()
                    places = data.get("results", [])

                    for place in places[:req.max_per_query]:
                        pid = place.get("place_id")
                        if pid in seen: continue
                        seen.add(pid)

                        # Get details
                        det_url = "https://maps.googleapis.com/maps/api/place/details/json"
                        det = req_lib.get(det_url, params={
                            "place_id": pid,
                            "fields": "name,formatted_phone_number,website,rating,user_ratings_total,formatted_address,geometry",
                            "key": GKEY
                        }, timeout=10).json().get("result", {})

                        website = det.get("website", "")
                        name = det.get("name", place.get("name", ""))
                        lat = det.get("geometry", {}).get("location", {}).get("lat", 0)
                        lng = det.get("geometry", {}).get("location", {}).get("lng", 0)

                        # Simple email finder
                        email, email_source = "", ""
                        if website:
                            import re
                            from bs4 import BeautifulSoup
                            EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
                            BLACKLIST = {'example.com','sentry.io','wixpress.com','google.com','facebook.com'}
                            try:
                                headers = {'User-Agent': 'Mozilla/5.0'}
                                w = 'https://' + website if not website.startswith('http') else website
                                rr = req_lib.get(w, headers=headers, timeout=6)
                                soup = BeautifulSoup(rr.text, 'html.parser')
                                for link in soup.find_all('a', href=True):
                                    href = link['href']
                                    if href.startswith('mailto:'):
                                        e = href.replace('mailto:','').split('?')[0].strip().lower()
                                        if '@' in e and not any(b in e for b in BLACKLIST):
                                            email, email_source = e, 'mailto'
                                            break
                                if not email:
                                    for m in EMAIL_REGEX.findall(rr.text):
                                        if '@' in m and '.' in m.split('@')[1] and not any(b in m for b in BLACKLIST):
                                            email, email_source = m.lower(), 'website'
                                            break
                            except: pass

                        reviews = det.get("user_ratings_total", 0)
                        score = compute_lead_score(
                            reviews=reviews,
                            website=website,
                            phone=det.get("formatted_phone_number", ""),
                            email=email,
                        )

                        leads.append({
                            "name": name,
                            "phone": det.get("formatted_phone_number", ""),
                            "email": email,
                            "email_source": email_source,
                            "website": website,
                            "rating": det.get("rating", 0),
                            "reviews": reviews,
                            "address": det.get("formatted_address", ""),
                            "place_id": pid,
                            "query": query,
                            "score": score,
                            "lat": lat,
                            "lng": lng
                        })
                        import time; time.sleep(0.1)

            # Import into CRM
            imported = 0
            skipped = 0
            conn = get_db()
            for lead in leads:
                existing = conn.execute(
                    "SELECT id FROM leads WHERE name = ? OR (phone = ? AND phone != '')",
                    (lead.get("name",""), lead.get("phone",""))
                ).fetchone()
                if existing:
                    skipped += 1
                    continue

                btype = lead.get("query","").replace(f"in {req.suburb} {req.state}","").strip()
                conn.execute("""
                    INSERT INTO leads (name, business_type, phone, email, email_source,
                        website, address, rating, reviews, score, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'crm_scrape')
                """, (
                    lead.get("name",""),
                    btype,
                    lead.get("phone",""),
                    lead.get("email",""),
                    lead.get("email_source",""),
                    lead.get("website",""),
                    lead.get("address",""),
                    float(lead.get("rating") or 0),
                    int(lead.get("reviews") or 0),
                    int(lead.get("score") or 0),
                ))
                imported += 1
            conn.commit()

            # Store map data
            map_leads = [l for l in leads if l.get("lat") and l.get("lng")]
            conn.execute("DELETE FROM settings WHERE key = 'last_scrape_map'")
            conn.execute("INSERT INTO settings (key, value) VALUES ('last_scrape_map', ?)",
                (json.dumps(map_leads[:100]),))
            # Store all found leads for display in Found Leads panel
            conn.execute("DELETE FROM settings WHERE key = 'last_scrape_leads'")
            conn.execute("INSERT INTO settings (key, value) VALUES ('last_scrape_leads', ?)",
                (json.dumps(leads),))
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_scrape_result', ?)",
                (json.dumps({"imported": imported, "skipped": skipped, "total": len(leads)}),))
            conn.commit()
            conn.close()
            print(f"✅ Scrape done: {imported} imported, {skipped} skipped")

            # Auto-enrich new leads in background
            if imported > 0:
                print(f"✨ Auto-enriching {imported} new leads...")
                try:
                    from enrichment import enrich_batch
                    enrich_batch(limit=imported, only_missing=True)
                    print("✅ Auto-enrichment complete")
                except Exception as e:
                    print(f"Auto-enrich error: {e}")

        except Exception as e:
            import traceback
            print(f"Scrape error: {traceback.format_exc()}")

    background_tasks.add_task(run)
    return {"success": True, "message": f"Scraping {len(queries)} categories in {req.suburb}..."}

@app.get("/api/scrape-status")
async def scrape_status():
    conn = get_db()
    result   = conn.execute("SELECT value FROM settings WHERE key = 'last_scrape_result'").fetchone()
    map_data = conn.execute("SELECT value FROM settings WHERE key = 'last_scrape_map'").fetchone()
    leads_data = conn.execute("SELECT value FROM settings WHERE key = 'last_scrape_leads'").fetchone()
    conn.close()

    def safe_parse(row):
        try:
            val = row[0] if row else ""
            if not val or not val.strip():
                return None
            return json.loads(val)
        except Exception:
            return None

    parsed_result = safe_parse(result)
    parsed_map    = safe_parse(map_data)
    parsed_leads  = safe_parse(leads_data)
    return {
        "status": "done" if parsed_result else "running",
        "result": parsed_result,
        "map_leads": parsed_map or [],
        "leads": parsed_leads or []
    }

@app.get("/api/leads-map")
async def leads_map():
    """Get all leads with coordinates for map display"""
    conn = get_db()
    # We'll geocode from address on the fly for existing leads
    leads = [dict(r) for r in conn.execute(
        "SELECT id, name, business_type, phone, email, address, score, status FROM leads ORDER BY score DESC LIMIT 200"
    ).fetchall()]
    conn.close()
    return {"leads": leads}


# ─────────────────────────────────────────────
# ENRICHMENT ENDPOINTS
# ─────────────────────────────────────────────

@app.post("/api/enrich/{lead_id}")
async def enrich_single(lead_id: int, background_tasks: BackgroundTasks):
    """Generate WhatsApp + email for a single lead"""
    from enrichment import enrich_lead_in_db

    def run():
        result = enrich_lead_in_db(lead_id)
        print(f"Enrichment result: {result.get('success')} — {result.get('name','')}")

    background_tasks.add_task(run)
    return {"success": True, "message": "Generating message — refresh in 5 seconds"}

@app.post("/api/enrich-now/{lead_id}")
async def enrich_single_now(lead_id: int, request: Request):
    """Generate WhatsApp + email synchronously — returns result immediately.
    Optional body {"context": "..."} personalises the prompt (manual leads)."""
    from enrichment import enrich_lead_in_db
    context = None
    force = False
    try:
        body = await request.json()
        context = (body or {}).get("context")
        force = bool((body or {}).get("force", False))
    except Exception:
        context, force = None, False  # no/!json body — plain enrich, as before
    return enrich_lead_in_db(lead_id, context=context, force=force)

@app.post("/api/enrich-batch")
async def enrich_batch_endpoint(background_tasks: BackgroundTasks, limit: int = 50):
    """Enrich all leads missing messages"""
    from enrichment import enrich_batch

    conn = get_db()
    pending = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE whatsapp_message IS NULL OR whatsapp_message = ''"
    ).fetchone()[0]
    conn.close()

    def run():
        from enrichment import enrich_batch
        result = enrich_batch(limit=limit)
        print(f"Batch enrichment done: {result}")

    background_tasks.add_task(run)
    return {
        "success": True,
        "message": f"Enriching {min(pending, limit)} leads in background",
        "pending": pending
    }

@app.get("/api/enrich-stats")
async def enrich_stats():
    """Stats on enrichment coverage"""
    conn = get_db()
    total      = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    enriched   = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE whatsapp_message IS NOT NULL AND whatsapp_message != ''"
    ).fetchone()[0]
    has_email_body = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE email_body IS NOT NULL AND email_body != ''"
    ).fetchone()[0]
    conn.close()
    return {
        "total": total,
        "enriched": enriched,
        "not_enriched": total - enriched,
        "has_email": has_email_body,
        "coverage_pct": round(enriched / total * 100, 1) if total else 0
    }


# ─────────────────────────────────────────────
# BROADCAST ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/api/broadcast/stats")
async def broadcast_stats():
    from broadcast import get_broadcast_stats
    return get_broadcast_stats()

@app.post("/api/broadcast/send")
async def broadcast_send(
    background_tasks: BackgroundTasks,
    limit: int = 50,
    business_type: Optional[str] = None,
    dry_run: bool = False
):
    """Launch email broadcast"""
    from broadcast import run_broadcast

    # Store progress in settings
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO settings VALUES ('broadcast_status', 'running')")
    conn.execute("INSERT OR REPLACE INTO settings VALUES ('broadcast_result', '')")
    conn.commit()
    conn.close()

    def run():
        import json
        result = run_broadcast(limit=limit, business_type=business_type,
                               dry_run=dry_run, delay_seconds=3.0)
        conn = get_db()
        conn.execute("INSERT OR REPLACE INTO settings VALUES ('broadcast_status', 'done')")
        conn.execute("INSERT OR REPLACE INTO settings VALUES ('broadcast_result', ?)",
                     (json.dumps(result),))
        conn.commit()
        conn.close()

    background_tasks.add_task(run)
    return {"success": True, "message": f"Broadcast started for up to {limit} leads"}

@app.get("/api/broadcast/status")
async def broadcast_status():
    import json
    conn = get_db()
    status = conn.execute("SELECT value FROM settings WHERE key='broadcast_status'").fetchone()
    result = conn.execute("SELECT value FROM settings WHERE key='broadcast_result'").fetchone()
    conn.close()
    return {
        "status": status[0] if status else "idle",
        "result": json.loads(result[0]) if result and result[0] else None
    }

# ─────────────────────────────────────────────
# WHATSAPP WEBHOOK MOUNT
# ─────────────────────────────────────────────

@app.get("/webhook/whatsapp")
async def wa_webhook_verify(request: Request):
    """Mount WhatsApp webhook verification on main server"""
    from whatsapp_engine import get_wa_settings
    settings = get_wa_settings()
    verify_token = settings.get('wa_verify_token',
                    os.getenv('WHATSAPP_VERIFY_TOKEN', 'marvis_verify_2024'))
    mode      = request.query_params.get("hub.mode")
    token     = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")
    if mode == "subscribe" and token == verify_token:
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse(challenge)
    from fastapi.responses import JSONResponse
    return JSONResponse({"error": "Forbidden"}, status_code=403)

@app.post("/webhook/whatsapp")
async def wa_webhook_receive(request: Request, background_tasks: BackgroundTasks):
    """Receive inbound WhatsApp messages"""
    try:
        data = await request.json()
        entry   = data.get("entry", [{}])[0]
        changes = entry.get("changes", [{}])[0]
        value   = changes.get("value", {})
        messages = value.get("messages", [])

        for msg in messages:
            msg_type = msg.get("type")
            phone    = msg.get("from", "")
            if msg_type == "text":
                text = msg.get("text", {}).get("body", "")
            else:
                text = f"[{msg_type}]"

            def handle(phone=phone, text=text, msg_type=msg_type):
                from whatsapp_webhook import (
                    find_lead_by_phone,
                    generate_ai_reply,
                    send_wa_reply,
                    get_conversation_history,
                )
                lead = find_lead_by_phone(phone)
                if not lead:
                    return
                classification = classify_reply(text, lead)
                mapped_status = {
                    "interested": "interested",
                    "booked": "booked",
                    "not_interested": "declined",
                    "spam": "declined",
                    "pricing": "contacted",
                    "callback": "contacted",
                    "followup_later": "contacted",
                }.get(classification["label"], "contacted")
                log_activity_event(
                    lead["id"],
                    "reply",
                    "whatsapp",
                    text[:1000],
                    status="received",
                    direction="inbound",
                    metadata={
                        "phone": phone,
                        "classification": classification["label"],
                        "confidence": classification.get("confidence", 0),
                    },
                    classification=classification["label"],
                )
                update_lead_status(
                    lead["id"],
                    mapped_status,
                    source="webhook",
                    note=f"Inbound reply classified as {classification['label']}",
                    metadata={"classification": classification["label"], "confidence": classification.get("confidence", 0)},
                )
                print(f"📱 Reply from {lead['name']}: {text[:60]}")
                conn = get_db()
                settings = {r['key']: r['value'] for r in conn.execute("SELECT * FROM settings").fetchall()}
                conn.close()
                if settings.get('wa_auto_reply', 'true') == 'true' and msg_type == 'text':
                    history = get_conversation_history(lead["id"])
                    reply   = generate_ai_reply(lead, text, history)
                    send_wa_reply(phone, reply, settings)
                    log_activity_event(
                        lead["id"],
                        "ai_reply",
                        "whatsapp",
                        reply[:1000],
                        status="sent",
                        direction="outbound",
                        metadata={"source": "fastapi_webhook"},
                    )

            background_tasks.add_task(handle)

    except Exception as e:
        print(f"Webhook error: {e}")

    from fastapi.responses import JSONResponse
    return JSONResponse({"status": "ok"})

# ─────────────────────────────────────────────
# GMAIL WEBHOOK (Pub/Sub push) + WATCH
# ─────────────────────────────────────────────

def _gmail_process_push(body):
    try:
        from gmail_service import process_gmail_push
        process_gmail_push(body)
    except Exception as e:
        logger.exception(f"❌ Gmail webhook background error: {e}")


@app.post("/api/gmail/webhook")
async def gmail_webhook(request: Request, background_tasks: BackgroundTasks):
    """Google Pub/Sub push endpoint for talktiv.ai@gmail.com inbound mail.
    Acknowledges immediately (200) and processes the reply in the background."""
    from fastapi.responses import JSONResponse
    # Shared-secret auth: Pub/Sub push subscription is configured to send
    # "Authorization: Bearer <GMAIL_WEBHOOK_SECRET>". If the secret isn't set yet,
    # warn and allow through so unconfigured environments don't break.
    secret = os.getenv("GMAIL_WEBHOOK_SECRET")
    if secret:
        if request.headers.get("authorization", "") != f"Bearer {secret}":
            return JSONResponse({"error": "unauthorized"}, status_code=401)
    else:
        print("⚠️ GMAIL_WEBHOOK_SECRET not set — /api/gmail/webhook is unauthenticated")
    try:
        body = await request.json()
    except Exception:
        body = {}
    background_tasks.add_task(_gmail_process_push, body)
    return JSONResponse({"status": "ok"}, status_code=200)


@app.get("/api/gmail/watch")
async def gmail_watch_endpoint():
    """Register / renew the Gmail INBOX watch. Returns the expiration timestamp."""
    try:
        from gmail_service import gmail_watch
        result = gmail_watch()
    except Exception as e:
        return {"success": False, "error": str(e)}
    if result.get("error"):
        return {"success": False, "error": result["error"]}
    return {
        "success": True,
        "history_id": result.get("history_id"),
        "expiration": result.get("expiration"),
    }


@app.on_event("startup")
async def startup():
    logger.info("startup: starting set_hud_loop")
    set_hud_loop(asyncio.get_running_loop())
    logger.info("startup: completed set_hud_loop")

    logger.info("startup: starting init_db")
    init_db()
    logger.info("startup: completed init_db")

    logger.info("startup: starting ensure_crm_schema")
    try:
        # Fast 5s pre-check: if a previous boot already wrote the schema_verified
        # marker, skip the schema DDL (and its potential 15s lock-wait) entirely.
        verified = await asyncio.wait_for(asyncio.to_thread(is_schema_verified), timeout=5.0)
        if verified:
            mark_schema_ready()
            logger.info("startup: schema already verified, skipping")
        else:
            await asyncio.wait_for(asyncio.to_thread(ensure_crm_schema), timeout=15.0)
            logger.info("startup: completed ensure_crm_schema")
    except asyncio.TimeoutError:
        # The check or the schema run blew its budget — skip rather than wedge
        # startup, and mark ready so lazy callers don't re-trigger the hang.
        mark_schema_ready()
        logger.warning("startup: schema check/ensure timed out — skipping ensure_crm_schema")

    # Migration: add email_source column if missing.
    # Guard with a column-exists check so a no-op ALTER never tries to take a
    # table lock on Postgres (which can block behind another connection).
    logger.info("startup: starting migration email_source column")
    try:
        from db import table_columns
        conn = get_db()
        existing_cols = table_columns(conn, "leads")
        conn.close()
        if "email_source" not in existing_cols:
            conn = get_db()
            conn.execute("ALTER TABLE leads ADD COLUMN email_source TEXT DEFAULT ''")
            conn.commit()
            conn.close()
            print("✅ Migration: email_source column added")
        else:
            logger.info("startup: email_source column already exists, skipping")
    except Exception:
        pass
    logger.info("startup: completed migration email_source column")

    # Migration: add responded_at column if missing (THING 3 — set when a lead replies)
    logger.info("startup: starting migration responded_at column")
    try:
        from db import table_columns
        conn = get_db()
        existing_cols = table_columns(conn, "leads")
        conn.close()
        if "responded_at" not in existing_cols:
            conn = get_db()
            conn.execute("ALTER TABLE leads ADD COLUMN responded_at TEXT")
            conn.commit()
            conn.close()
            print("✅ Migration: responded_at column added")
        else:
            logger.info("startup: responded_at column already exists, skipping")
    except Exception:
        pass
    logger.info("startup: completed migration responded_at column")

    # One-time migration: re-score legacy leads with the current scoring model
    logger.info("startup: starting migration recompute_all_scores")
    try:
        conn = get_db()
        row = conn.execute("SELECT value FROM settings WHERE key = 'score_model_version'").fetchone()
        version = row[0] if row else None
        conn.close()
        if version != "2":
            logger.info("startup: recompute_all_scores running (version != '2')")
            result = recompute_all_scores()
            conn = get_db()
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('score_model_version', '2')"
            )
            conn.commit()
            conn.close()
            print(f"✅ Migration: re-scored {result['updated']} leads (scoring model v2)")
        else:
            logger.info("startup: recompute_all_scores skipped (already v2)")
    except Exception as e:
        print(f"Score migration error: {e}")
    logger.info("startup: completed migration recompute_all_scores")

    logger.info("startup: starting followup_scheduler thread")
    t = threading.Thread(target=followup_scheduler, daemon=True)
    t.start()
    logger.info("startup: completed followup_scheduler thread")

    logger.info("startup: starting start_auto_engine")
    from email_engine import start_auto_engine
    start_auto_engine()
    logger.info("startup: completed start_auto_engine")

    logger.info("startup: starting start_scheduler_service")
    from scheduler import start_scheduler_service
    start_scheduler_service()
    logger.info("startup: completed start_scheduler_service")

    from railway_sync import start_sync_service
    start_sync_service()
    logger.info("startup: completed start_sync_service")


@app.on_event("shutdown")
async def shutdown():
    # Release pooled Postgres connections on graceful uvicorn shutdown so we
    # don't leave idle backends held open on Railway.
    try:
        from db import close_pg_pool
        close_pg_pool()
        logger.info("shutdown: closed Postgres connection pool")
    except Exception as e:
        logger.warning(f"shutdown: pool close failed: {e}")

    print("✅ MARVIS CRM started — http://localhost:8000")
    print("📧 Email automation engine running")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
