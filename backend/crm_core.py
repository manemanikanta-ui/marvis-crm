"""
Shared CRM helpers for activity logging, timeline building, approvals,
reply classification, and safety settings.
"""

from __future__ import annotations

import json
import os
import random
import sqlite3
import threading
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from db import get_db, now_sql, table_columns

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "crm.db"
IST = ZoneInfo("Asia/Kolkata")

ALLOWED_REPLY_LABELS = [
    "interested",
    "not_interested",
    "pricing",
    "callback",
    "followup_later",
    "spam",
    "booked",
]

SKIP_SCHEDULER_STATUSES = {
    "paused",
    "replied",
    "responded",
    "interested",
    "booked",
    "declined",
    "converted",
    "no_response",
}

DEFAULT_SAFETY_SETTINGS = {
    "max_emails_per_day": "50",
    "max_whatsapp_per_day": "20",
    "random_delay_min": "0",
    "random_delay_max": "0",
    "office_hours_only": "true",
    "pause_on_failures": "true",
}

_schema_lock = threading.Lock()
_schema_ready = False

TIMELINE_ICON_MAP = {
    "email": ("📧", "Email Sent"),
    "whatsapp": ("📱", "WhatsApp Sent"),
    "reply": ("📩", "Reply Received"),
    "inbound": ("📩", "Reply Received"),
    "email_reply": ("📧", "Email Reply"),
    "wa_reply": ("💬", "WhatsApp Reply"),
    "whatsapp_reply": ("💬", "WhatsApp Reply"),
    "ai_reply": ("🤖", "AI Reply"),
    "followup_scheduled": ("📅", "Follow-up Scheduled"),
    "followup_sent": ("📬", "Follow-up Sent"),
    "status_change": ("🔁", "Status Change"),
    "scheduler_job": ("⚙️", "Scheduler Action"),
    "note": ("📝", "Note"),
    "outreach": ("🚀", "Outreach"),
}


def _ensure_column(conn, table: str, column: str, ddl: str):
    columns = table_columns(conn, table)
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _settings_dict(conn: Optional[sqlite3.Connection] = None) -> Dict[str, str]:
    created = False
    if conn is None:
        conn = get_db()
        created = True
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    settings = {row["key"]: row["value"] for row in rows}
    if created:
        conn.close()
    return settings


def _now_iso() -> str:
    return datetime.now().astimezone(IST).isoformat()


def ensure_crm_schema():
    global _schema_ready
    if _schema_ready:
        return

    with _schema_lock:
        if _schema_ready:
            return

        conn = get_db()

        def _step(fn):
            # Run one DDL/migration statement in isolation: commit on success,
            # roll back on failure so a single error can't poison the connection
            # (critical on PostgreSQL, where an aborted txn blocks all later SQL).
            try:
                fn()
                conn.commit()
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"  ensure_crm_schema: step skipped ({exc})")

        # 1. CREATE TABLEs first — they must exist before any ALTER/SELECT runs.
        _step(lambda: conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lead_status_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                old_status TEXT,
                new_status TEXT NOT NULL,
                source TEXT DEFAULT 'system',
                note TEXT,
                metadata_json TEXT DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            )
            """
        ))

        _step(lambda: conn.execute(
            """
            CREATE TABLE IF NOT EXISTS system_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                channel TEXT DEFAULT 'system',
                content TEXT,
                status TEXT DEFAULT 'logged',
                direction TEXT DEFAULT 'system',
                metadata_json TEXT DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        ))

        # 2. Additive ALTERs — each isolated so one failure doesn't block the rest.
        _step(lambda: _ensure_column(conn, "activities", "direction", "TEXT DEFAULT 'outbound'"))
        _step(lambda: _ensure_column(conn, "activities", "metadata_json", "TEXT DEFAULT '{}'"))
        _step(lambda: _ensure_column(conn, "activities", "campaign_name", "TEXT DEFAULT ''"))
        _step(lambda: _ensure_column(conn, "activities", "classification", "TEXT DEFAULT ''"))

        # Status-pipeline timestamps on leads.
        _step(lambda: _ensure_column(conn, "leads", "contacted_at", "TEXT"))
        _step(lambda: _ensure_column(conn, "leads", "responded_at", "TEXT"))

        # 3. Default safety settings.
        for key, value in DEFAULT_SAFETY_SETTINGS.items():
            _step(lambda k=key, v=value: conn.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (k, v),
            ))

        conn.close()
        _schema_ready = True


def ensure_logs_dir() -> Path:
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    return log_dir


def log_activity_event(
    lead_id: Optional[int],
    activity_type: str,
    channel: str,
    content: str,
    *,
    status: str = "logged",
    direction: str = "outbound",
    campaign_name: str = "",
    metadata: Optional[Dict[str, Any]] = None,
    classification: str = "",
    scheduled_at: Optional[str] = None,
    sent_at: Optional[str] = None,
    response: str = "",
    created_at: Optional[str] = None,
) -> int:
    ensure_crm_schema()
    payload = metadata or {}
    created_at = created_at or _now_iso()
    conn = get_db()
    if lead_id is None:
        cursor = conn.execute(
            """
            INSERT INTO system_events (
                event_type, channel, content, status, direction, metadata_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                activity_type,
                channel,
                content[:2000],
                status,
                direction,
                json.dumps(payload, ensure_ascii=False),
                created_at,
            ),
        )
        conn.commit()
        event_id = cursor.lastrowid
        conn.close()
        return int(event_id or 0)
    cursor = conn.execute(
        """
        INSERT INTO activities (
            lead_id, type, channel, content, status, scheduled_at, sent_at,
            response, created_at, direction, metadata_json, campaign_name, classification
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lead_id,
            activity_type,
            channel,
            content[:2000],
            status,
            scheduled_at,
            sent_at or (created_at if status == "sent" else None),
            response,
            created_at,
            direction,
            json.dumps(payload, ensure_ascii=False),
            campaign_name,
            classification,
        ),
    )
    conn.commit()
    activity_id = cursor.lastrowid
    conn.close()
    return int(activity_id)


def log_status_change(
    lead_id: int,
    old_status: str,
    new_status: str,
    *,
    source: str = "system",
    note: str = "",
    metadata: Optional[Dict[str, Any]] = None,
) -> int:
    ensure_crm_schema()
    payload = metadata or {}
    conn = get_db()
    conn.execute(
        """
        INSERT INTO lead_status_history (
            lead_id, old_status, new_status, source, note, metadata_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lead_id,
            old_status,
            new_status,
            source,
            note,
            json.dumps(payload, ensure_ascii=False),
            _now_iso(),
        ),
    )
    conn.commit()
    conn.close()
    return log_activity_event(
        lead_id,
        "status_change",
        "system",
        f"{old_status or 'unknown'} -> {new_status}",
        status="logged",
        direction="system",
        metadata={"source": source, "note": note, **payload},
    )


def update_lead_status(
    lead_id: int,
    new_status: str,
    *,
    source: str = "system",
    note: str = "",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ensure_crm_schema()
    conn = get_db()
    lead = conn.execute("SELECT id, status, name FROM leads WHERE id = ?", (lead_id,)).fetchone()
    if not lead:
        conn.close()
        return {"success": False, "error": "Lead not found"}

    old_status = str(lead["status"] or "")
    if old_status == new_status:
        conn.close()
        return {"success": True, "lead_id": lead_id, "status": new_status, "unchanged": True}

    now_iso = _now_iso()
    if new_status == "contacted":
        # Anchor for the no_response auto-tag job (Decision 2).
        conn.execute(
            "UPDATE leads SET status = ?, updated_at = ?, contacted_at = ? WHERE id = ?",
            (new_status, now_iso, now_iso, lead_id),
        )
    else:
        conn.execute(
            "UPDATE leads SET status = ?, updated_at = ? WHERE id = ?",
            (new_status, now_iso, lead_id),
        )
    conn.commit()
    conn.close()

    log_status_change(
        lead_id,
        old_status,
        new_status,
        source=source,
        note=note,
        metadata=metadata,
    )

    return {
        "success": True,
        "lead_id": lead_id,
        "lead_name": lead["name"],
        "old_status": old_status,
        "new_status": new_status,
    }


def _item_title(item_type: str, channel: str, status: str) -> str:
    key = (item_type or "").lower()
    channel = (channel or "").lower()
    if key in TIMELINE_ICON_MAP:
        return TIMELINE_ICON_MAP[key][1]
    if channel in TIMELINE_ICON_MAP:
        return TIMELINE_ICON_MAP[channel][1]
    if status == "failed":
        return "Delivery Failed"
    if status == "sent":
        return "Message Sent"
    return item_type.replace("_", " ").title() if item_type else "Activity"


def _item_icon(item_type: str, channel: str) -> str:
    key = (item_type or "").lower()
    channel = (channel or "").lower()
    if key in TIMELINE_ICON_MAP:
        return TIMELINE_ICON_MAP[key][0]
    if channel in TIMELINE_ICON_MAP:
        return TIMELINE_ICON_MAP[channel][0]
    return "•"


def format_timeline_item(item: Dict[str, Any]) -> Dict[str, Any]:
    item_type = str(item.get("type") or item.get("activity_type") or "").lower()
    channel = str(item.get("channel") or "").lower()
    status = str(item.get("status") or "")
    direction = str(item.get("direction") or ("inbound" if item_type in {"reply", "inbound"} else "outbound"))
    content = str(item.get("content") or item.get("message") or item.get("response") or "").strip()
    title = _item_title(item_type, channel, status)
    icon = _item_icon(item_type, channel)
    preview = content.replace("\n", " ").strip()
    if len(preview) > 150:
        preview = preview[:147].rstrip() + "..."

    return {
        "type": item_type or item.get("type", ""),
        "channel": channel or item.get("channel", ""),
        "direction": direction,
        "content": content,
        "status": status,
        "created_at": item.get("created_at", ""),
        "title": title,
        "icon": icon,
        "preview": preview,
        "campaign_name": item.get("campaign_name", "") or "",
        "metadata": item.get("metadata_json", "") or item.get("metadata", "") or "",
    }


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    return dict(row)


def get_lead_timeline(lead_id: int) -> Dict[str, Any]:
    ensure_crm_schema()
    conn = get_db()
    lead = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    if not lead:
        conn.close()
        return {"lead": None, "timeline": []}

    activities = [
        _row_to_dict(row)
        for row in conn.execute(
            "SELECT * FROM activities WHERE lead_id = ? ORDER BY created_at ASC, id ASC",
            (lead_id,),
        ).fetchall()
    ]
    followups = [
        _row_to_dict(row)
        for row in conn.execute(
            "SELECT * FROM follow_ups WHERE lead_id = ? ORDER BY created_at ASC, id ASC",
            (lead_id,),
        ).fetchall()
    ]
    status_history = [
        _row_to_dict(row)
        for row in conn.execute(
            "SELECT * FROM lead_status_history WHERE lead_id = ? ORDER BY created_at ASC, id ASC",
            (lead_id,),
        ).fetchall()
    ]
    conn.close()

    timeline: List[Dict[str, Any]] = []
    for row in activities:
        meta = {}
        try:
            meta = json.loads(row.get("metadata_json") or "{}")
        except Exception:
            meta = {}
        row["metadata"] = meta
        if not row.get("direction"):
            row["direction"] = "inbound" if row.get("type") in {"reply", "inbound", "email_reply", "wa_reply", "whatsapp_reply"} else "outbound"
        timeline.append(format_timeline_item(row))

    for row in followups:
        message = row.get("message") or ""
        channel = row.get("channel") or "whatsapp"
        timeline.append(
            format_timeline_item(
                {
                    "type": "followup_scheduled",
                    "channel": channel,
                    "direction": "outbound",
                    "content": message,
                    "status": row.get("status") or "pending",
                    "created_at": row.get("created_at") or row.get("scheduled_at") or "",
                    "campaign_name": row.get("campaign_name", ""),
                    "metadata_json": row.get("metadata_json", "{}"),
                }
            )
        )

    for row in status_history:
        timeline.append(
            format_timeline_item(
                {
                    "type": "status_change",
                    "channel": "system",
                    "direction": "system",
                    "content": f"{row.get('old_status') or 'unknown'} -> {row.get('new_status') or ''}",
                    "status": "logged",
                    "created_at": row.get("created_at") or "",
                    "campaign_name": "",
                    "metadata_json": row.get("metadata_json", "{}"),
                }
            )
        )

    if not status_history:
        timeline.append(
            format_timeline_item(
                {
                    "type": "status_change",
                    "channel": "system",
                    "direction": "system",
                    "content": f"Lead created with status {lead['status'] or 'new'}",
                    "status": "logged",
                    "created_at": lead["created_at"] or "",
                    "campaign_name": "",
                    "metadata_json": "{}",
                }
            )
        )

    timeline = sorted(
        timeline,
        key=lambda item: (str(item.get("created_at") or ""), str(item.get("title") or "")),
    )

    return {"lead": dict(lead), "timeline": timeline}


def get_pending_approvals(limit: int = 100) -> List[Dict[str, Any]]:
    ensure_crm_schema()
    conn = get_db()
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT *
            FROM leads
            WHERE status IN ('pending_review', 'new')
              AND (
                COALESCE(whatsapp_message, '') != ''
                OR COALESCE(email_subject, '') != ''
                OR COALESCE(email_body, '') != ''
              )
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    ]
    conn.close()
    return rows


def classify_reply(message: str, lead: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Classify an inbound reply using Claude, with keyword fallback.
    Returns a dict with label + confidence.
    """
    text = (message or "").strip()
    lowered = text.lower()

    fallback_rules = [
        ("booked", ["booked", "appointment confirmed", "demo fixed", "meeting fixed"]),
        ("spam", ["spam", "remove me", "unsubscribe", "stop", "wrong number"]),
        ("not_interested", ["not interested", "no thanks", "not now", "not now please"]),
        ("pricing", ["price", "pricing", "cost", "how much", "rate", "quote"]),
        ("callback", ["call me", "ring me", "tomorrow", "later", "next week", "call back"]),
        ("followup_later", ["follow up later", "later", "after diwali", "next month", "next quarter"]),
        ("interested", ["interested", "demo", "yes", "sure", "let's talk", "send details"]),
    ]
    for label, phrases in fallback_rules:
        if any(phrase in lowered for phrase in phrases):
            return {"label": label, "confidence": 0.72, "source": "keyword"}

    try:
        import anthropic  # type: ignore

        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
        lead_context = ""
        if lead:
            lead_context = (
                f"Lead: {lead.get('name', '')} | Type: {lead.get('business_type', '')} | "
                f"Location: {lead.get('address', '')}"
            )

        prompt = f"""
Classify the reply into exactly one label from this list:
interested, not_interested, pricing, callback, followup_later, spam, booked

Reply to classify:
{text}

{lead_context}

Return raw JSON only in this shape:
{{"label":"one_label_here","confidence":0.0,"reason":"short reason"}}
"""
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=120,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(raw)
        label = str(parsed.get("label", "")).strip().lower()
        if label not in ALLOWED_REPLY_LABELS:
            raise ValueError(f"invalid label: {label}")
        confidence = float(parsed.get("confidence", 0.5) or 0.5)
        return {
            "label": label,
            "confidence": max(0.0, min(confidence, 1.0)),
            "reason": str(parsed.get("reason", "")).strip(),
            "source": "claude",
        }
    except Exception:
        return {"label": "followup_later", "confidence": 0.35, "source": "fallback"}


def load_safety_settings() -> Dict[str, Any]:
    ensure_crm_schema()
    conn = get_db()
    settings = _settings_dict(conn)
    conn.close()
    return {
        "max_emails_per_day": int(settings.get("max_emails_per_day", "50") or 50),
        "max_whatsapp_per_day": int(settings.get("max_whatsapp_per_day", "20") or 20),
        "random_delay_min": int(settings.get("random_delay_min", "0") or 0),
        "random_delay_max": int(settings.get("random_delay_max", "0") or 0),
        "office_hours_only": _parse_bool(settings.get("office_hours_only", "true"), True),
        "pause_on_failures": _parse_bool(settings.get("pause_on_failures", "true"), True),
    }


def is_office_hours(now: Optional[datetime] = None) -> bool:
    current = (now or datetime.now(IST)).astimezone(IST)
    start = dt_time(hour=9, minute=0)
    end = dt_time(hour=18, minute=30)
    return start <= current.time() <= end


def count_today_activity(channel: str, *, status: str = "sent") -> int:
    ensure_crm_schema()
    conn = get_db()
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM activities
        WHERE channel = ?
          AND status = ?
          AND date(created_at) = date('now')
        """,
        (channel, status),
    ).fetchone()
    conn.close()
    return int(row[0] if row else 0)


def get_campaign_stats() -> Dict[str, Any]:
    ensure_crm_schema()
    conn = get_db()
    emails_sent = count_today_activity("email", status="sent")
    whatsapp_sent = count_today_activity("whatsapp", status="sent")
    replies_row = conn.execute(
        """
        SELECT COUNT(*)
        FROM activities
        WHERE type IN ('reply', 'inbound', 'email_reply', 'wa_reply', 'whatsapp_reply')
          AND date(created_at) = date('now')
        """
    ).fetchone()
    replies = replies_row[0] if replies_row else 0
    interested_row = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE status = 'interested'"
    ).fetchone()
    interested = interested_row[0] if interested_row else 0
    booked_row = conn.execute(
        "SELECT COUNT(*) FROM leads WHERE status = 'booked'"
    ).fetchone()
    booked = booked_row[0] if booked_row else 0
    followups_row = conn.execute(
        "SELECT COUNT(*) FROM follow_ups WHERE status = 'pending'"
    ).fetchone()
    followups = followups_row[0] if followups_row else 0
    failures_row = conn.execute(
        """
        SELECT COUNT(*)
        FROM activities
        WHERE status LIKE 'failed%'
          AND date(created_at) = date('now')
        """
    ).fetchone()
    failures = failures_row[0] if failures_row else 0

    by_campaign = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                COALESCE(campaign_name, '') AS campaign_name,
                COUNT(*) AS total,
                SUM(CASE WHEN channel = 'email' AND status = 'sent' THEN 1 ELSE 0 END) AS emails_sent,
                SUM(CASE WHEN channel = 'whatsapp' AND status = 'sent' THEN 1 ELSE 0 END) AS whatsapp_sent,
                SUM(CASE WHEN type IN ('reply', 'inbound', 'email_reply', 'wa_reply', 'whatsapp_reply') THEN 1 ELSE 0 END) AS replies
            FROM activities
            GROUP BY COALESCE(campaign_name, '')
            ORDER BY total DESC
            LIMIT 10
            """
        ).fetchall()
    ]
    by_category = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                COALESCE(l.business_type, '') AS business_type,
                COUNT(*) AS total,
                SUM(CASE WHEN a.channel = 'email' AND a.status = 'sent' THEN 1 ELSE 0 END) AS emails_sent,
                SUM(CASE WHEN a.channel = 'whatsapp' AND a.status = 'sent' THEN 1 ELSE 0 END) AS whatsapp_sent,
                SUM(CASE WHEN a.type IN ('reply', 'inbound', 'email_reply', 'wa_reply', 'whatsapp_reply') THEN 1 ELSE 0 END) AS replies
            FROM activities a
            JOIN leads l ON l.id = a.lead_id
            GROUP BY COALESCE(l.business_type, '')
            ORDER BY total DESC
            LIMIT 10
            """
        ).fetchall()
    ]
    by_location = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                COALESCE(l.address, '') AS location,
                COUNT(*) AS total,
                SUM(CASE WHEN a.channel = 'email' AND a.status = 'sent' THEN 1 ELSE 0 END) AS emails_sent,
                SUM(CASE WHEN a.channel = 'whatsapp' AND a.status = 'sent' THEN 1 ELSE 0 END) AS whatsapp_sent,
                SUM(CASE WHEN a.type IN ('reply', 'inbound', 'email_reply', 'wa_reply', 'whatsapp_reply') THEN 1 ELSE 0 END) AS replies
            FROM activities a
            JOIN leads l ON l.id = a.lead_id
            GROUP BY COALESCE(l.address, '')
            ORDER BY total DESC
            LIMIT 10
            """
        ).fetchall()
    ]
    conn.close()

    return {
        "emails_sent": emails_sent,
        "whatsapp_sent": whatsapp_sent,
        "replies": replies,
        "interested": interested,
        "booked": booked,
        "followups": followups,
        "failure_rate": round((failures / max(1, emails_sent + whatsapp_sent)) * 100, 1),
        "by_campaign": by_campaign,
        "by_category": by_category,
        "by_location": by_location,
    }


def random_delay_seconds() -> float:
    safety = load_safety_settings()
    min_delay = max(0, int(safety["random_delay_min"]))
    max_delay = max(min_delay, int(safety["random_delay_max"]))
    if max_delay <= 0:
        return 0.0
    return random.uniform(min_delay, max_delay)
