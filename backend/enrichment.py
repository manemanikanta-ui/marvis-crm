"""
MARVIS AI Enrichment Engine
Generates personalized WhatsApp messages + email subject/body
for any lead using Claude API.
Runs:
  - Automatically after scrape/import
  - On-demand per lead via API
"""

import os
import sqlite3
import json
import time
from datetime import datetime
from dotenv import load_dotenv

from db import get_db as shared_get_db
from hud_bus import emit_hud_event
from crm_core import log_activity_event, update_lead_status

load_dotenv()

DB_PATH = "data/crm.db"


def get_db():
    return shared_get_db()


def generate_for_lead(lead: dict) -> dict:
    """
    Generate WhatsApp message + email subject + email body
    for a single lead using Claude.
    Returns dict with keys: whatsapp_message, email_subject, email_body
    """
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

    name    = lead.get("name", "")
    btype   = lead.get("business_type", "")
    reviews = lead.get("reviews", 0)
    address = lead.get("address", "")
    website = lead.get("website", "")
    score   = lead.get("score", 0)
    email   = lead.get("email", "")

    # ── WhatsApp Message ──
    wa_prompt = f"""Write a short WhatsApp outreach message on behalf of Manikanta, founder of Talktiv AI.

Talktiv AI helps Indian businesses handle customer calls 24/7 using AI voice agents in Telugu, Hindi, Tamil, English and 18 more Indian languages.

Target business:
- Name: {name}
- Type: {btype}
- Reviews: {reviews}
- Location: {address}
- Has website: {"Yes" if website else "No"}

Requirements:
- Warm, conversational, NOT salesy
- Reference their business type naturally
- Mention one specific pain point (missed calls, language barriers, or after-hours inquiries)
- Offer a FREE demo
- Under 80 words
- End with a soft question
- Sign off: Manikanta | Talktiv AI
- Sound human, not like a template

Return ONLY the message, nothing else."""

    # ── Email ──
    email_prompt = f"""Write a cold outreach email on behalf of Manikanta, founder of Talktiv AI.

Talktiv AI helps Indian businesses handle customer calls 24/7 using AI voice agents in Telugu, Hindi, Tamil, English and 18 more Indian languages.

Target business:
- Name: {name}
- Type: {btype}
- Reviews: {reviews}
- Location: {address}
- Website: {website or "None"}

Requirements:
- Subject line: specific to their business type + one clear benefit
- Body: under 120 words
- Reference their online presence or low reviews as opportunity
- Offer a FREE content/demo pack
- One clear CTA
- Sign off: Manikanta | Founder, Talktiv AI | talktivai.com
- Warm professional tone

Return ONLY valid JSON with keys: subject, body
No markdown, no backticks, just raw JSON."""

    result = {"whatsapp_message": "", "email_subject": "", "email_body": ""}

    try:
        # Generate WhatsApp
        wa_resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=200,
            messages=[{"role": "user", "content": wa_prompt}]
        )
        result["whatsapp_message"] = wa_resp.content[0].text.strip()

        # Generate Email
        email_resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=400,
            messages=[{"role": "user", "content": email_prompt}]
        )
        raw = email_resp.content[0].text.strip()
        # Clean JSON
        raw = raw.replace("```json", "").replace("```", "").strip()
        email_data = json.loads(raw)
        result["email_subject"] = email_data.get("subject", "")
        result["email_body"]    = email_data.get("body", "")

    except json.JSONDecodeError:
        # Email JSON failed — extract manually
        result["email_subject"] = f"Quick question about {name}'s online presence"
        result["email_body"]    = raw  # Use raw text as body

    except Exception as e:
        print(f"  Enrichment error for {name}: {e}")

    return result


def enrich_lead_in_db(lead_id: int) -> dict:
    """Generate and save enrichment for a single lead"""
    conn = get_db()
    lead = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()

    if not lead:
        return {"success": False, "error": "Lead not found"}

    lead = dict(lead)

    # Skip if already enriched
    if lead.get("whatsapp_message") and lead.get("email_subject"):
        return {"success": True, "skipped": True, "message": "Already enriched"}

    print(f"  ✨ Enriching: {lead['name'][:40]}")
    result = generate_for_lead(lead)

    if result["whatsapp_message"] or result["email_subject"]:
        conn = get_db()
        conn.execute("""
            UPDATE leads SET
                whatsapp_message = ?,
                email_subject = ?,
                email_body = ?,
                updated_at = ?
            WHERE id = ?
        """, (
            result["whatsapp_message"],
            result["email_subject"],
            result["email_body"],
            datetime.now().isoformat(),
            lead_id
        ))
        conn.commit()
        conn.close()
        if lead.get("status") not in {"paused", "interested", "booked", "converted", "declined"}:
            update_lead_status(
                lead_id,
                "pending_review",
                source="enrichment",
                note="Outreach generated and waiting for approval",
            )
            emit_hud_event("approval_needed", {"lead_id": lead_id, "name": lead["name"]})
        log_activity_event(
            lead_id,
            "enrichment",
            "system",
            "Generated WhatsApp and email outreach",
            status="logged",
            direction="system",
            metadata={"generated": True},
        )
        return {"success": True, "lead_id": lead_id, "name": lead["name"], **result}

    return {"success": False, "error": "Generation failed"}


def enrich_batch(limit: int = 50, only_missing: bool = True) -> dict:
    """
    Enrich multiple leads in batch.
    only_missing=True: only enrich leads without messages yet
    """
    conn = get_db()
    query = "SELECT id, name FROM leads WHERE 1=1"
    if only_missing:
        query += " AND (whatsapp_message IS NULL OR whatsapp_message = '')"
    query += f" ORDER BY score DESC LIMIT {limit}"

    leads = [dict(r) for r in conn.execute(query).fetchall()]
    conn.close()

    if not leads:
        return {"enriched": 0, "skipped": 0, "failed": 0,
                "message": "No leads need enrichment"}

    print(f"\n✨ MARVIS Enrichment — Processing {len(leads)} leads")
    print("=" * 50)

    enriched = 0
    failed   = 0

    for i, lead in enumerate(leads):
        print(f"[{i+1}/{len(leads)}] {lead['name'][:40]}")
        result = enrich_lead_in_db(lead["id"])

        if result.get("success") and not result.get("skipped"):
            enriched += 1
        elif not result.get("success"):
            failed += 1

        time.sleep(0.3)  # gentle rate limit

    print(f"\n✅ Done — Enriched: {enriched} | Failed: {failed}")
    return {"enriched": enriched, "failed": failed,
            "total": len(leads), "skipped": 0}


if __name__ == "__main__":
    result = enrich_batch(limit=20)
    print(result)
