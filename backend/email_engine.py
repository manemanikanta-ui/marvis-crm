"""
MARVIS Email Automation Engine
Handles: auto-send on import, follow-up sequences, scheduling, logging
"""

import smtplib
import sqlite3
import threading
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from typing import Optional
import pathlib

from db import get_db as shared_get_db
from crm_core import log_activity_event, update_lead_status, ensure_crm_schema

DB_PATH = "data/crm.db"

# ─────────────────────────────────────────────
# EMAIL TEMPLATES
# ─────────────────────────────────────────────

TEMPLATES = {
    "initial": {
        "delay_days": 0,
        "label": "Initial Outreach",
        "build": lambda lead: {
            "subject": lead.get("email_subject") or f"Quick question about {lead['name']}'s online presence",
            "body": lead.get("email_body") or f"""Hi,

I noticed {lead['name']} online and wanted to reach out.

I help Hyderabad businesses like yours get more leads through better online presence — Google profile optimization, email follow-up sequences, and social content — all done for you in 48 hours.

I'd love to send you a free sample pack tailored to your business. No cost, no commitment.

Would that be helpful?

— Manikanta
Talktiv AI | talktivai.com"""
        }
    },
    "followup_3": {
        "delay_days": 3,
        "label": "Follow-up Day 3",
        # If schedule_sequence attached a proof pack (lead['_proof_url'], only for
        # score >= 60), the Day-3 email hands over the actual sample pack link;
        # otherwise it keeps the original "want me to send it over?" wording.
        "build": lambda lead: {
            "subject": f"Re: {lead['name']} — still happy to help",
            "body": (
                "Hi,\n\n"
                "Just following up on my note from a few days ago.\n\n"
                + (
                    f"I put together a free sample pack for {lead['name']} — a Google profile "
                    "write-up, a 5-email sequence, and 30 social captions. Here it is, no commitment:\n"
                    f"{lead['_proof_url']}\n\n"
                    "Worth a 2-minute look?\n\n"
                    if lead.get('_proof_url') else
                    f"I put together a quick sample — Google profile optimization + 5 follow-up "
                    f"emails + 30 social captions — specifically for businesses like {lead['name']}.\n\n"
                    "Takes 2 minutes to review and there's zero commitment. Want me to send it over?\n\n"
                )
                + "— Manikanta\n"
                "Talktiv AI | talktivai.com"
            )
        }
    },
    "followup_7": {
        "delay_days": 7,
        "label": "Follow-up Day 7",
        "build": lambda lead: {
            "subject": f"One last thing for {lead['name']}",
            "body": f"""Hi,

I'll keep this short — I've reached out a couple of times about helping {lead['name']} get more leads online.

If the timing isn't right, no worries at all. But if you ever want to explore how AI voice agents and better online content can help your business, I'm here.

Reply anytime — even months from now.

— Manikanta
Founder, Talktiv AI
talktivai.com"""
        }
    },
    "followup_21": {
        "delay_days": 21,
        "label": "Follow-up Day 21",
        "build": lambda lead: {
            "subject": f"Keeping the door open — {lead['name']}",
            "body": f"""Hi,

Closing my notes on {lead['name']} for now — I won't keep emailing.

If down the line you want an AI assistant handling missed calls, enquiries, and follow-ups for you, my door stays open. Just reply to this whenever the timing is right.

Wishing you a strong quarter.

— Manikanta
Founder, Talktiv AI
talktivai.com"""
        }
    }
}

# ─────────────────────────────────────────────
# CORE EMAIL SENDER
# ─────────────────────────────────────────────

def get_db():
    return shared_get_db()

def get_settings():
    conn = get_db()
    settings = {row['key']: row['value'] for row in conn.execute("SELECT * FROM settings").fetchall()}
    conn.close()
    return settings

def send_email_now(
    to_email: str,
    subject: str,
    body: str,
    lead_id: int,
    template_key: str = "manual",
    settings: dict = None,
    campaign_name: str = "",
    metadata: dict = None
) -> dict:
    """Send a single email and log it"""
    if not settings:
        settings = get_settings()

    email_user = settings.get("email_user", "")
    email_pass = settings.get("email_pass", "")
    sender_name = settings.get("sender_name", "Manikanta | Talktiv AI")

    if not email_user or not email_pass:
        return {"success": False, "error": "Email not configured"}

    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = f"{sender_name} <{email_user}>"
        msg["To"] = to_email
        msg["Subject"] = subject
        msg["Reply-To"] = email_user

        # Plain text version
        msg.attach(MIMEText(body, "plain"))

        # HTML version — simple but clean
        html_body = f"""
        <html><body style="font-family:Georgia,serif;max-width:560px;margin:40px auto;color:#1a1a2e;line-height:1.7;">
        <div style="border-left:3px solid #7c6af7;padding-left:20px;margin-bottom:24px;">
            <p style="color:#7c6af7;font-size:12px;letter-spacing:1px;text-transform:uppercase;margin:0">Talktiv AI</p>
        </div>
        {''.join(f'<p style="margin:0 0 14px">{line}</p>' for line in body.split('\n') if line.strip())}
        <div style="margin-top:32px;padding-top:20px;border-top:1px solid #eee;font-size:11px;color:#999;">
            Talktiv AI · Hyderabad · talktivai.com<br>
            <a href="mailto:{email_user}" style="color:#7c6af7">Reply to unsubscribe</a>
        </div>
        </body></html>
        """
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.login(email_user, email_pass)
            server.send_message(msg)

        # Log success
        _log_activity(lead_id, template_key, to_email, subject, "sent", campaign_name=campaign_name, metadata=metadata)
        conn = get_db()
        lead = conn.execute("SELECT status FROM leads WHERE id = ?", (lead_id,)).fetchone()
        conn.close()
        if lead and str(lead["status"] or "") in {"new", "pending_review", "approved"}:
            update_lead_status(
                lead_id,
                "contacted",
                source="email",
                note=f"Email sent via {template_key}",
                metadata={"to_email": to_email},
            )
        return {"success": True, "to": to_email}

    except smtplib.SMTPAuthenticationError:
        _log_activity(lead_id, template_key, to_email, subject, "failed_auth", campaign_name=campaign_name, metadata=metadata)
        return {"success": False, "error": "Gmail authentication failed. Check App Password."}
    except smtplib.SMTPRecipientsRefused:
        _log_activity(lead_id, template_key, to_email, subject, "failed_invalid_email", campaign_name=campaign_name, metadata=metadata)
        return {"success": False, "error": "Invalid email address"}
    except Exception as e:
        _log_activity(lead_id, template_key, to_email, subject, f"failed: {str(e)}", campaign_name=campaign_name, metadata=metadata)
        return {"success": False, "error": str(e)}

def _log_activity(lead_id, template_key, to_email, subject, status, campaign_name: str = "", metadata: dict = None):
    """Log email activity to DB"""
    try:
        ensure_crm_schema()
        log_activity_event(
            lead_id,
            "email",
            "email",
            f"{template_key}: {subject}",
            status=status,
            direction="outbound",
            campaign_name=campaign_name,
            metadata={"to_email": to_email, **(metadata or {})},
        )
    except Exception as e:
        print(f"Log error: {e}")

# ─────────────────────────────────────────────
# SEQUENCE SCHEDULER
# ─────────────────────────────────────────────

def schedule_sequence(lead_id: int, to_email: str):
    """Schedule full 3-email follow-up sequence for a lead"""
    conn = get_db()
    lead = dict(conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone() or {})
    conn.close()

    if not lead:
        return

    # Proof pack for the Day-3 follow-up: only for warm+ leads (score >= 60), and
    # only here — after the lead has been approved into the sequence, NEVER at
    # enrichment. Idempotent (reuses an existing pack). The portal link is woven
    # into the Day-3 email by the followup_3 template via lead['_proof_url'].
    # Token cost is accepted here by design.
    try:
        if int(lead.get("score") or 0) >= 60:
            from enrichment import get_lead_proof_url, create_proof_pack_for_lead
            portal = get_lead_proof_url(lead_id) or create_proof_pack_for_lead(lead)
            if portal:
                lead["_proof_url"] = portal
    except Exception as e:
        print(f"  Day-3 proof pack skipped for lead {lead_id}: {e}")

    # Schedule follow-up emails (Day 3, Day 7 and Day 21) — all queued up front.
    for key in ["followup_3", "followup_7", "followup_21"]:
        template = TEMPLATES[key]
        scheduled_at = (datetime.now() + timedelta(days=template["delay_days"])).isoformat()
        content = template["build"](lead)

        conn = get_db()
        conn.execute("""
            INSERT INTO follow_ups (lead_id, message, channel, scheduled_at, status)
            VALUES (?, ?, 'email', ?, 'pending')
        """, (lead_id, f"EMAIL|{to_email}|{content['subject']}|{content['body']}", scheduled_at))
        conn.commit()
        conn.close()
        log_activity_event(
            lead_id,
            "followup_scheduled",
            "email",
            content["subject"],
            status="pending",
            direction="outbound",
            metadata={"to_email": to_email, "template": key, "scheduled_at": scheduled_at},
        )

    print(f"📅 Sequence scheduled for lead {lead_id} ({to_email})")

# ─────────────────────────────────────────────
# AUTO-SEND ENGINE (runs in background thread)
# ─────────────────────────────────────────────

class AutoSendEngine:
    def __init__(self):
        self.running = False
        self.thread = None
        self.check_interval = 300  # Check every 5 minutes

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        print("🤖 Auto-send engine started")

    def stop(self):
        self.running = False

    def _run(self):
        while self.running:
            try:
                self._process_due_followups()
            except Exception as e:
                print(f"Auto-send error: {e}")
            time.sleep(self.check_interval)

    def _process_due_followups(self):
        """Find and send all due follow-up emails"""
        settings = get_settings()
        if not settings.get("email_user") or not settings.get("email_pass"):
            return  # Not configured yet

        auto_enabled = settings.get("auto_send_enabled", "false")
        if auto_enabled != "true":
            return  # Auto-send not enabled

        conn = get_db()
        now = datetime.now().isoformat()
        due = conn.execute(
            "SELECT * FROM follow_ups WHERE status IN ('pending', 'ready') AND scheduled_at <= ? AND channel = 'email'",
            (now,)
        ).fetchall()
        conn.close()

        for fu in due:
            fu = dict(fu)
            try:
                # Parse the stored message format: EMAIL|to|subject|body
                parts = fu["message"].split("|", 3)
                if len(parts) != 4 or parts[0] != "EMAIL":
                    continue

                _, to_email, subject, body = parts
                result = send_email_now(
                    to_email=to_email,
                    subject=subject,
                    body=body,
                    lead_id=fu["lead_id"],
                    template_key="auto_followup",
                    settings=settings
                )

                # Update follow-up status
                new_status = "sent" if result["success"] else f"failed: {result.get('error','')}"
                conn = get_db()
                conn.execute(
                    "UPDATE follow_ups SET status = ? WHERE id = ?",
                    (new_status, fu["id"])
                )
                conn.commit()
                conn.close()

                if result["success"]:
                    print(f"✉️ Auto-sent follow-up to {to_email} for lead {fu['lead_id']}")
                else:
                    print(f"❌ Failed follow-up to {to_email}: {result.get('error')}")

            except Exception as e:
                print(f"Follow-up processing error: {e}")
                conn = get_db()
                conn.execute(
                    "UPDATE follow_ups SET status = ? WHERE id = ?",
                    (f"error: {str(e)}", fu["id"])
                )
                conn.commit()
                conn.close()

# Global engine instance
auto_engine = AutoSendEngine()

def start_auto_engine():
    auto_engine.start()
