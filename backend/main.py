"""
MARVIS CRM Backend
FastAPI + SQLite — Lead Management + Auto Outreach
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List
import sqlite3
import pandas as pd
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import threading
import time
from dotenv import load_dotenv

load_dotenv()

from crm_core import (
    classify_reply,
    ensure_crm_schema,
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


from fastapi.responses import FileResponse
import pathlib

@app.get("/")
@app.get("/dashboard")
async def dashboard():
    return FileResponse(pathlib.Path(__file__).parent.parent / "frontend" / "index.html")

DB_PATH = "data/crm.db"

# ─────────────────────────────────────────────
# DATABASE SETUP
# ─────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def outbound_allowed(status: str) -> bool:
    return str(status or "").lower() in {"approved", "contacted", "interested", "booked"}

def init_db():
    os.makedirs("data", exist_ok=True)
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            business_type TEXT,
            phone TEXT,
            email TEXT,
            email_source TEXT DEFAULT '',
            website TEXT,
            address TEXT,
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
    conn.commit()
    conn.close()

# ─────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────

class LeadCreate(BaseModel):
    name: str
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

class LeadUpdate(BaseModel):
    status: Optional[str] = None
    priority: Optional[str] = None
    notes: Optional[str] = None
    email: Optional[str] = None
    email_source: Optional[str] = None
    phone: Optional[str] = None

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

            conn.execute("""
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
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    conn = get_db()
    query = "SELECT * FROM leads WHERE 1=1"
    params = []

    if status and status != "all":
        query += " AND status = ?"
        params.append(status)
    if priority:
        query += " AND priority = ?"
        params.append(priority)
    if search:
        query += " AND (name LIKE ? OR business_type LIKE ? OR address LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    query += " ORDER BY score DESC, created_at DESC LIMIT ? OFFSET ?"
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
    conn = get_db()
    cursor = conn.execute("""
        INSERT INTO leads (name, business_type, phone, email, website, address,
            rating, reviews, score, whatsapp_message, email_subject, email_body, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (lead.name, lead.business_type, lead.phone, lead.email, lead.website,
          lead.address, lead.rating, lead.reviews, lead.score,
          lead.whatsapp_message, lead.email_subject, lead.email_body, lead.notes))
    conn.commit()
    lead_id = cursor.lastrowid
    conn.close()
    return {"success": True, "id": lead_id}

@app.delete("/api/leads/{lead_id}")
async def delete_lead(lead_id: int):
    conn = get_db()
    conn.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
    conn.execute("DELETE FROM activities WHERE lead_id = ?", (lead_id,))
    conn.commit()
    conn.close()
    return {"success": True}

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
        conn.execute(
            "UPDATE leads SET status = 'contacted', updated_at = ? WHERE id = ?",
            (datetime.now().isoformat(), activity.lead_id)
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

# ─────────────────────────────────────────────
# DASHBOARD STATS
# ─────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats():
    conn = get_db()

    total = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    by_status = dict(conn.execute(
        "SELECT status, COUNT(*) FROM leads GROUP BY status"
    ).fetchall())
    hot_leads = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE score >= 60"
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

    conn.close()
    return {
        "total_leads": total,
        "hot_leads": hot_leads,
        "contacted": contacted,
        "converted": converted,
        "pending_followups": pending_followups,
        "pending_approvals": pending_approvals,
        "conversion_rate": round((converted / contacted * 100) if contacted > 0 else 0, 1),
        "by_status": by_status,
        "by_type": by_type,
        "recent_leads": recent_leads
    }


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
# SETTINGS
# ─────────────────────────────────────────────

@app.get("/api/settings")
async def get_settings():
    conn = get_db()
    settings = {row['key']: row['value'] for row in conn.execute("SELECT * FROM settings").fetchall()}
    conn.close()
    # Mask password
    if settings.get('email_pass'):
        settings['email_pass'] = '••••••••'
    return settings

@app.post("/api/settings")
async def update_settings(update: SettingsUpdate):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (update.key, update.value)
    )
    conn.commit()
    conn.close()
    return {"success": True}


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/scheduler/status")
async def scheduler_status():
    from scheduler import get_scheduler_status
    return get_scheduler_status()


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
    import os
    from dotenv import load_dotenv
    load_dotenv()
    GKEY = os.getenv("GOOGLE_API_KEY", "")
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
                from dotenv import load_dotenv
                load_dotenv()
                GKEY = os.getenv("GOOGLE_API_KEY", "")
                leads = []
                seen = set()

                for query in queries:
                    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
                    params = {"query": query, "key": GKEY}
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
                        score = 0
                        if reviews < 20: score += 40
                        elif reviews < 50: score += 30
                        elif reviews < 100: score += 20
                        else: score += 5
                        if website: score += 15
                        if det.get("formatted_phone_number"): score += 20
                        if email: score += 25
                        score = min(score, 100)

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
            import json
            conn.execute("INSERT INTO settings (key, value) VALUES ('last_scrape_map', ?)",
                (json.dumps(map_leads[:100]),))
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
    return {
        "status": "done" if parsed_result else "running",
        "result": parsed_result,
        "map_leads": parsed_map or []
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
async def enrich_single_now(lead_id: int):
    """Generate WhatsApp + email synchronously — returns result immediately"""
    from enrichment import enrich_lead_in_db
    return enrich_lead_in_db(lead_id)

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

@app.on_event("startup")
async def startup():
    init_db()
    ensure_crm_schema()
    # Migration: add email_source column if missing
    try:
        conn = get_db()
        conn.execute("ALTER TABLE leads ADD COLUMN email_source TEXT DEFAULT ''")
        conn.commit()
        conn.close()
        print("✅ Migration: email_source column added")
    except Exception:
        pass  # Column already exists
    t = threading.Thread(target=followup_scheduler, daemon=True)
    t.start()
    from email_engine import start_auto_engine
    start_auto_engine()
    from scheduler import start_scheduler_service
    start_scheduler_service()
    print("✅ MARVIS CRM started — http://localhost:8000")
    print("📧 Email automation engine running")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
