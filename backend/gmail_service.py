"""
Gmail webhook + watch for MARVIS CRM (THING 4).

Receives Google Cloud Pub/Sub push notifications when talktiv.ai@gmail.com gets
mail, resolves the new message(s) via the Gmail History API, matches the sender
to a lead, logs the reply, advances the lead to "responded", and triggers the
Tier-2 warm-lead proof pack. Unmatched senders are recorded in unmatched_replies.

Google client libraries are imported lazily so the app still boots if they are
not yet installed (they ship via requirements.txt on the Railway deploy).

Env (already set in Railway): GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET,
GMAIL_REFRESH_TOKEN, PUBSUB_TOPIC.
"""

from __future__ import annotations

import os
import re
import json
import base64
import logging
from datetime import datetime

from db import get_db

logger = logging.getLogger("marvis.gmail")

# Gmail API user alias for the authenticated account (talktiv.ai@gmail.com).
GMAIL_USER = "me"
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")


def _gmail_service():
    """Build an authenticated Gmail API client from the env refresh token."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request as GoogleRequest
    from googleapiclient.discovery import build

    creds = Credentials(
        token=None,
        refresh_token=os.getenv("GMAIL_REFRESH_TOKEN"),
        client_id=os.getenv("GMAIL_CLIENT_ID"),
        client_secret=os.getenv("GMAIL_CLIENT_SECRET"),
        token_uri="https://oauth2.googleapis.com/token",
        scopes=GMAIL_SCOPES,
    )
    creds.refresh(GoogleRequest())
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


# ── watch-state helpers (single row, id=1; cross-DB safe, no INSERT OR REPLACE) ──

def _get_last_history_id():
    conn = get_db()
    row = conn.execute("SELECT history_id FROM gmail_watch_state WHERE id = 1").fetchone()
    conn.close()
    return row["history_id"] if row else None


def _upsert_watch_state(history_id: str, expiration: str = None):
    conn = get_db()
    now = datetime.now().isoformat()
    existing = conn.execute("SELECT id FROM gmail_watch_state WHERE id = 1").fetchone()
    if existing:
        if expiration is not None:
            conn.execute(
                "UPDATE gmail_watch_state SET history_id = ?, expiration = ?, created_at = ? WHERE id = 1",
                (history_id, expiration, now),
            )
        else:
            conn.execute(
                "UPDATE gmail_watch_state SET history_id = ? WHERE id = 1",
                (history_id,),
            )
    else:
        conn.execute(
            "INSERT INTO gmail_watch_state (id, history_id, expiration, created_at) VALUES (1, ?, ?, ?)",
            (history_id, expiration or "", now),
        )
    conn.commit()
    conn.close()


def _extract_email(message):
    """Pull the plain sender address out of the Gmail message's From header,
    handling both "Display Name <email@domain.com>" and bare "email@domain.com"."""
    headers = message.get('payload', {}).get('headers', [])
    for h in headers:
        if h.get('name', '').lower() == 'from':
            value = h.get('value', '')
            # Extract email from "Name <email>" or plain "email"
            match = re.search(r'<([^>]+)>', value)
            if match:
                return match.group(1).strip().lower()
            return value.strip().lower()
    return None


# ── push processing ─────────────────────────────────────────────────────────

def gmail_watch() -> dict:
    """Call users.watch() on the INBOX, store the returned historyId/expiration,
    and return them. This also (re)establishes the history cursor for pushes."""
    topic = os.getenv("PUBSUB_TOPIC")
    if not topic:
        return {"error": "PUBSUB_TOPIC not configured"}
    service = _gmail_service()
    resp = (
        service.users()
        .watch(userId=GMAIL_USER, body={"topicName": topic, "labelIds": ["INBOX"]})
        .execute()
    )
    history_id = str(resp.get("historyId") or "")
    expiration = str(resp.get("expiration") or "")
    _upsert_watch_state(history_id, expiration)
    return {"history_id": history_id, "expiration": expiration}


def _list_added_message_ids(service, start_history_id: str):
    ids = []
    page_token = None
    try:
        while True:
            kwargs = dict(
                userId=GMAIL_USER,
                startHistoryId=start_history_id,
                historyTypes=["messageAdded"],
                labelId="INBOX",
            )
            if page_token:
                kwargs["pageToken"] = page_token
            resp = service.users().history().list(**kwargs).execute()
            for h in resp.get("history", []):
                for m in h.get("messagesAdded", []):
                    mid = (m.get("message") or {}).get("id")
                    if mid:
                        ids.append(mid)
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
    except Exception as e:
        logger.exception(f"❌ Gmail webhook error: {e}")
    # de-dupe, preserve order
    return list(dict.fromkeys(ids))


def _process_message(service, message_id: str):
    try:
        msg = (
            service.users()
            .messages()
            .get(
                userId=GMAIL_USER,
                id=message_id,
                format="metadata",
                metadataHeaders=["From", "Subject"],
            )
            .execute()
        )
    except Exception as e:
        logger.exception(f"❌ Gmail webhook error: {e}")
        return

    headers = {
        (h.get("name") or "").lower(): h.get("value", "")
        for h in (msg.get("payload", {}) or {}).get("headers", [])
    }
    sender_email = _extract_email(msg)
    logger.info(f"📧 Extracted sender email: {sender_email}")
    subject = headers.get("subject", "")
    snippet = (msg.get("snippet", "") or "")[:200]
    if not sender_email:
        return
    _handle_reply(sender_email, subject, snippet)


def _handle_reply(sender_email: str, subject: str, snippet: str):
    now = datetime.now().isoformat()
    logger.info(f"🔍 Searching leads for email: {sender_email}")
    conn = get_db()
    lead = conn.execute(
        "SELECT id, status, score, name FROM leads WHERE LOWER(email) = LOWER(?) LIMIT 1", (sender_email,)
    ).fetchone()
    conn.close()
    logger.info(f"🔍 DB result: {dict(lead) if lead else None}")

    if not lead:
        conn = get_db()
        conn.execute(
            "INSERT INTO unmatched_replies (sender_email, subject, snippet, received_at) VALUES (?, ?, ?, ?)",
            (sender_email, subject, snippet, now),
        )
        conn.commit()
        conn.close()
        print(f"  📥 Unmatched email reply from {sender_email}")
        return

    lead = dict(lead)
    lead_id = lead["id"]

    # Log the inbound reply.
    try:
        from crm_core import log_activity_event
        log_activity_event(
            lead_id, "email_reply", "email", snippet,
            status="received", direction="inbound",
            metadata={"subject": subject, "sender": sender_email, "source": "gmail_webhook"},
        )
    except Exception as e:
        logger.exception(f"❌ Gmail webhook error: {e}")

    # Only a "contacted" lead advances to "responded" (do not override other states).
    if str(lead.get("status") or "") == "contacted":
        logger.info(f"📝 Updating lead {lead_id} status")
        conn = get_db()
        conn.execute(
            "UPDATE leads SET status = 'responded', responded_at = ?, updated_at = ? WHERE id = ?",
            (now, now, lead_id),
        )
        conn.commit()
        conn.close()
        print(f"  💬 Lead {lead_id} → responded")

        # Telegram alert for a matched email reply.
        try:
            from telegram_notify import notify
            notify(
                f"📧 <b>Email Reply</b>\n"
                f"{lead.get('name') or sender_email} → responded\n"
                f"{snippet[:100]}"
            )
            logger.info("📱 Telegram notification sent for email reply")
        except Exception:
            logger.exception("❌ Telegram notify failed")

    # Tier-2: warm lead with no proof pack yet → generate + send the assets email.
    try:
        from enrichment import lead_has_proof_pack, handle_warm_lead_responded
        if int(lead.get("score") or 0) < 75 and not lead_has_proof_pack(lead_id):
            handle_warm_lead_responded(lead_id)
    except Exception as e:
        logger.exception(f"❌ Gmail webhook error: {e}")


def process_gmail_push(body: dict):
    """Handle one Pub/Sub push. Runs in a background task (after the 200 ack)."""
    try:
        message = (body or {}).get("message", {}) or {}
        data_b64 = message.get("data", "")
        if not data_b64:
            return
        decoded = base64.b64decode(data_b64).decode("utf-8")
        notif = json.loads(decoded)  # {"emailAddress": ..., "historyId": ...}
        new_history_id = str(notif.get("historyId") or "")

        last_history_id = _get_last_history_id()
        if not last_history_id:
            # No baseline yet (watch not initialised) — set cursor and wait for next push.
            if new_history_id:
                _upsert_watch_state(new_history_id)
            return

        service = _gmail_service()
        for mid in _list_added_message_ids(service, last_history_id):
            _process_message(service, mid)

        # Advance the cursor so the next push only sees newer history.
        if new_history_id:
            _upsert_watch_state(new_history_id)
    except Exception as e:
        logger.exception(f"❌ Gmail webhook error: {e}")
