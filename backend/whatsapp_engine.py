"""
MARVIS WhatsApp Engine
Standalone WhatsApp Business API for CRM lead outreach
Uses Meta Cloud API (same credentials as Talktiv)
"""

import requests
import sqlite3
import os
import json
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

from db import get_db as shared_get_db
from crm_core import ensure_crm_schema, log_activity_event, update_lead_status

load_dotenv()

DB_PATH = "data/crm.db"

# ─────────────────────────────────────────────
# META WHATSAPP CLOUD API
# ─────────────────────────────────────────────

def get_wa_settings():
    """Get WhatsApp settings from DB"""
    conn = shared_get_db()
    settings = {r['key']: r['value'] for r in conn.execute("SELECT * FROM settings").fetchall()}
    conn.close()
    return settings

def send_whatsapp_text(to_phone: str, message: str, lead_id: int = 0) -> dict:
    """
    Send a free-form text message via WhatsApp Cloud API.
    Works within 24hr customer service window.
    For cold outreach, use send_whatsapp_template() instead.
    """
    s = get_wa_settings()
    phone_id = s.get('wa_phone_id', os.getenv('WHATSAPP_PHONE_ID', ''))
    token    = s.get('wa_token', os.getenv('WHATSAPP_TOKEN', ''))

    if not phone_id or not token:
        return {"success": False, "error": "WhatsApp not configured — add WA_PHONE_ID and WA_TOKEN in settings"}

    # Clean phone number
    phone = clean_phone(to_phone)
    if not phone:
        return {"success": False, "error": f"Invalid phone number: {to_phone}"}

    url = f"https://graph.facebook.com/v18.0/{phone_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": phone,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": message
        }
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=15)
        data = r.json()

        if r.status_code == 200 and data.get("messages"):
            msg_id = data["messages"][0].get("id", "")
            _log_wa_activity(lead_id, phone, message, "sent", msg_id)
            return {"success": True, "message_id": msg_id, "to": phone}
        else:
            error = data.get("error", {}).get("message", str(data))
            _log_wa_activity(lead_id, phone, message, f"failed: {error}")
            return {"success": False, "error": error, "raw": data}

    except Exception as e:
        _log_wa_activity(lead_id, phone, message, f"error: {str(e)}")
        return {"success": False, "error": str(e)}


def send_whatsapp_template(to_phone: str, template_name: str,
                            language: str = "en", components: list = None,
                            lead_id: int = 0) -> dict:
    """
    Send an approved WhatsApp template message.
    Required for initiating conversations (cold outreach).
    Template must be pre-approved in Meta Business Manager.
    """
    s = get_wa_settings()
    phone_id = s.get('wa_phone_id', os.getenv('WHATSAPP_PHONE_ID', ''))
    token    = s.get('wa_token', os.getenv('WHATSAPP_TOKEN', ''))

    if not phone_id or not token:
        return {"success": False, "error": "WhatsApp not configured"}

    phone = clean_phone(to_phone)
    if not phone:
        return {"success": False, "error": f"Invalid phone: {to_phone}"}

    url = f"https://graph.facebook.com/v18.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    template_payload = {
        "name": template_name,
        "language": {"code": language}
    }
    if components:
        template_payload["components"] = components

    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "template",
        "template": template_payload
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=15)
        data = r.json()

        if r.status_code == 200 and data.get("messages"):
            msg_id = data["messages"][0].get("id", "")
            _log_wa_activity(lead_id, phone, f"[TEMPLATE: {template_name}]", "sent", msg_id)
            return {"success": True, "message_id": msg_id}
        else:
            error = data.get("error", {}).get("message", str(data))
            _log_wa_activity(lead_id, phone, f"[TEMPLATE: {template_name}]", f"failed: {error}")
            return {"success": False, "error": error}

    except Exception as e:
        return {"success": False, "error": str(e)}


def clean_phone(phone: str) -> str:
    """
    Normalize Indian phone numbers to E.164 format.
    Examples:
      09123456789  → 919123456789
      +91 91234 56789 → 919123456789
      91 9123456789 → 919123456789
    """
    if not phone:
        return ""

    # Remove all non-digits
    digits = ''.join(c for c in phone if c.isdigit())

    if not digits:
        return ""

    # Indian numbers: 10 digits → add 91
    if len(digits) == 10:
        return "91" + digits

    # Already has country code
    if len(digits) == 12 and digits.startswith("91"):
        return digits

    # Has leading 0: 09123456789 → 919123456789
    if len(digits) == 11 and digits.startswith("0"):
        return "91" + digits[1:]

    # International: keep as is if reasonable length
    if 10 <= len(digits) <= 15:
        return digits

    return ""


def _log_wa_activity(lead_id: int, phone: str, message: str, status: str, msg_id: str = ""):
    """Log WhatsApp send to CRM activities"""
    try:
        ensure_crm_schema()
        if lead_id and lead_id > 0:
            log_activity_event(
                lead_id,
                "whatsapp",
                "whatsapp",
                message[:500],
                status=status,
                direction="outbound",
                metadata={"phone": phone, "msg_id": msg_id},
            )
            if status == "sent":
                conn = shared_get_db()
                lead = conn.execute("SELECT status FROM leads WHERE id = ?", (lead_id,)).fetchone()
                conn.close()
                if lead and str(lead["status"] or "") in {"new", "pending_review", "approved"}:
                    update_lead_status(
                        lead_id,
                        "contacted",
                        source="whatsapp",
                        note="WhatsApp sent",
                        metadata={"phone": phone},
                    )
    except Exception as e:
        print(f"WA log error: {e}")


# ─────────────────────────────────────────────
# VERIFY CREDENTIALS
# ─────────────────────────────────────────────

def verify_whatsapp_credentials() -> dict:
    """Test if WhatsApp API credentials are valid"""
    s = get_wa_settings()
    phone_id = s.get('wa_phone_id', os.getenv('WHATSAPP_PHONE_ID', ''))
    token    = s.get('wa_token', os.getenv('WHATSAPP_TOKEN', ''))

    if not phone_id or not token:
        return {"valid": False, "error": "Missing WA_PHONE_ID or WA_TOKEN"}

    try:
        url = f"https://graph.facebook.com/v18.0/{phone_id}"
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        data = r.json()

        if r.status_code == 200:
            return {
                "valid": True,
                "phone_id": phone_id,
                "display_name": data.get("display_phone_number", ""),
                "verified_name": data.get("verified_name", ""),
                "quality": data.get("quality_rating", "")
            }
        else:
            return {"valid": False, "error": data.get("error", {}).get("message", "Unknown error")}
    except Exception as e:
        return {"valid": False, "error": str(e)}


# ─────────────────────────────────────────────
# FOLLOW-UP SCHEDULER
# ─────────────────────────────────────────────

def schedule_wa_followup(lead_id: int, phone: str, days: int = 3) -> dict:
    """Schedule a WhatsApp follow-up message"""
    conn = shared_get_db()
    lead = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()

    if not lead:
        return {"success": False, "error": "Lead not found"}

    lead = dict(lead)
    name = lead.get('name', '').split()[0] if lead.get('name') else 'there'
    scheduled_at = (datetime.now() + timedelta(days=days)).isoformat()

    followup_msg = f"""Hi! Just following up on my message from {days} days ago.

I'd love to show you what Talktiv AI can do for {lead.get('name', 'your business')} — AI voice agents that handle customer calls in Telugu, Hindi, English and more, 24/7.

Happy to share a free sample. Would you have 10 minutes this week?

— Manikanta | Talktiv AI"""

    conn = shared_get_db()
    conn.execute("""
        INSERT INTO follow_ups (lead_id, message, channel, scheduled_at, status)
        VALUES (?, ?, 'whatsapp', ?, 'pending')
    """, (lead_id, f"WA|{phone}|{followup_msg}", scheduled_at))
    conn.commit()
    conn.close()
    log_activity_event(
        lead_id,
        "followup_scheduled",
        "whatsapp",
        followup_msg,
        status="pending",
        direction="outbound",
        metadata={"phone": phone, "days": days, "source": "whatsapp_engine"},
    )

    return {"success": True, "scheduled_at": scheduled_at, "message": followup_msg}
