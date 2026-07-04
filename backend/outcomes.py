"""
MARVIS Reply-Outcomes reconciler (Phase 0 — the capture layer).

Turns every inbound reply into a structured `reply_outcomes` row — the raw
material the Campaign Autopsy (Phase 1) reads to learn what works.

⛔ gmail_service.py is OFF-LIMITS and is NOT touched. Instead this reads the
`activities` table — into which gmail_service, the WhatsApp webhook, AND the
Railway→local sync all write inbound replies — on a trigger (the local
scheduler). Each new inbound reply activity is materialised into a
reply_outcomes row. Idempotent: dedupes on activity_id, safe to run every cycle.

Cross-DB safe: uses db.get_db() (SQLite local / Postgres Railway); db.py
translates `?`→`%s` and `datetime('now')`→`NOW()`.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

from db import get_db

logger = logging.getLogger("marvis.outcomes")

# Inbound reply activity types written by gmail_service / whatsapp / sync.
REPLY_TYPES = ("email_reply", "wa_reply", "whatsapp_reply", "reply", "inbound")
_POSITIVE = {"interested", "booked", "pricing", "callback"}
_NEGATIVE = {"not_interested", "spam"}

_schema_ready = False


def ensure_outcomes_schema() -> None:
    """Create reply_outcomes if missing. Isolated + rollback-safe (an aborted
    txn on Postgres would otherwise poison later SQL on the connection)."""
    global _schema_ready
    if _schema_ready:
        return
    conn = get_db()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reply_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER,
                activity_id INTEGER UNIQUE,
                campaign_name TEXT DEFAULT '',
                channel TEXT,
                label TEXT,
                confidence REAL,
                sentiment TEXT,
                reply_text TEXT,
                subject TEXT DEFAULT '',
                sender TEXT DEFAULT '',
                email_template TEXT DEFAULT '',
                email_subject TEXT DEFAULT '',
                days_to_reply INTEGER,
                lead_score INTEGER,
                business_type TEXT DEFAULT '',
                city TEXT DEFAULT '',
                responded_at TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.commit()
        _schema_ready = True
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("ensure_outcomes_schema skipped (non-fatal): %s", exc)
    finally:
        conn.close()


def _sentiment(label: str) -> str:
    if label in _POSITIVE:
        return "positive"
    if label in _NEGATIVE:
        return "negative"
    return "neutral"


def _days_between(start: str, end: str):
    try:
        a = datetime.fromisoformat(str(start)[:19])
        b = datetime.fromisoformat(str(end)[:19])
        return max(0, (b - a).days)
    except Exception:
        return None


def _last_outbound_email(conn, lead_id, before_created_at):
    """Best-effort lineage: the outbound email this reply is most likely answering.
    activities.content for sends is stored as 'template_key: subject'."""
    try:
        row = conn.execute(
            """
            SELECT content FROM activities
            WHERE lead_id = ? AND channel = 'email' AND direction = 'outbound'
              AND status = 'sent' AND created_at <= ?
            ORDER BY created_at DESC LIMIT 1
            """,
            (lead_id, before_created_at),
        ).fetchone()
    except Exception:
        return "", ""
    if not row:
        return "", ""
    content = str(row["content"] or "")
    if ":" in content:
        template, subject = content.split(":", 1)
        return template.strip(), subject.strip()
    return "", content.strip()


def record_new_outcomes(limit: int = 200) -> dict:
    """Scan `activities` for inbound replies with no reply_outcomes row yet;
    classify + insert. Idempotent, per-row committed (one bad row can't abort the
    batch on Postgres). Never raises to the caller. Returns {scanned, added}."""
    ensure_outcomes_schema()
    conn = get_db()
    scanned = added = 0
    try:
        placeholders = ",".join("?" for _ in REPLY_TYPES)
        rows = conn.execute(
            f"""
            SELECT a.id AS activity_id, a.lead_id, a.type, a.channel, a.content,
                   a.metadata_json, a.campaign_name, a.classification, a.created_at,
                   l.name AS lead_name, l.score AS lead_score,
                   l.business_type, l.city, l.contacted_at
            FROM activities a
            LEFT JOIN leads l ON l.id = a.lead_id
            WHERE a.direction = 'inbound' AND a.type IN ({placeholders})
              AND NOT EXISTS (
                  SELECT 1 FROM reply_outcomes r WHERE r.activity_id = a.id
              )
            ORDER BY a.created_at ASC
            LIMIT ?
            """,
            (*REPLY_TYPES, limit),
        ).fetchall()
    except Exception as exc:
        conn.close()
        logger.warning("record_new_outcomes scan failed (non-fatal): %s", exc)
        return {"scanned": 0, "added": 0, "error": str(exc)}

    for row in rows:
        scanned += 1
        r = dict(row)
        try:
            meta = json.loads(r.get("metadata_json") or "{}")
        except Exception:
            meta = {}
        reply_text = str(r.get("content") or "")

        # Classification: prefer the one already stored (WhatsApp path sets it);
        # otherwise classify now (email replies arrive without a label).
        label = str(r.get("classification") or "").strip()
        confidence = None
        if not label:
            try:
                from crm_core import classify_reply
                res = classify_reply(
                    reply_text,
                    {"name": r.get("lead_name"), "business_type": r.get("business_type"), "address": r.get("city")},
                )
                label = str(res.get("label", "")).strip()
                confidence = res.get("confidence")
            except Exception:
                label = ""

        template, esubject = ("", "")
        if r.get("lead_id"):
            template, esubject = _last_outbound_email(conn, r["lead_id"], r["created_at"])
        days = _days_between(r.get("contacted_at"), r.get("created_at")) if r.get("contacted_at") else None

        try:
            conn.execute(
                """
                INSERT INTO reply_outcomes
                    (lead_id, activity_id, campaign_name, channel, label, confidence,
                     sentiment, reply_text, subject, sender, email_template, email_subject,
                     days_to_reply, lead_score, business_type, city, responded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r.get("lead_id"), r["activity_id"], r.get("campaign_name") or "",
                    r.get("channel") or "", label, confidence, _sentiment(label),
                    reply_text[:2000], meta.get("subject", ""),
                    meta.get("sender") or meta.get("phone", ""),
                    template, esubject, days, r.get("lead_score"),
                    r.get("business_type") or "", r.get("city") or "", r.get("created_at"),
                ),
            )
            conn.commit()  # per-row commit: a bad row can't abort the batch (Postgres)
            added += 1
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.warning("outcome insert skipped for activity %s: %s", r["activity_id"], exc)

    conn.close()
    if added:
        logger.info("reply_outcomes: +%d (scanned %d)", added, scanned)
    return {"scanned": scanned, "added": added}
