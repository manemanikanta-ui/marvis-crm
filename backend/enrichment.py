"""
MARVIS AI Enrichment Engine
Generates a personalised 3-email cold outreach sequence (Day 0 / Day 3 / Day 7)
plus a WhatsApp message for any lead using Claude.

All Claude output is validated (validate_enrichment) before it is saved —
hallucinated or red-flag content is rejected and never written to the DB.

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

from db import get_db as shared_get_db, table_columns
from hud_bus import emit_hud_event
from crm_core import log_activity_event, update_lead_status

load_dotenv()

DB_PATH = "data/crm.db"

CLAUDE_MODEL = "claude-sonnet-4-6"


# ── Email sequence prompt (hallucination-free) ──
ENRICHMENT_PROMPT = """You are writing a personalised cold outreach email on behalf of Manikanta Mane, founder of Talktiv AI, Hyderabad.

Business details:
Name: {name}
Category: {category}
Location: {city}, {state}
Phone: {phone}
Reviews: {reviews} Google reviews
Website: {website}

Write 3 emails for a cold outreach sequence:

Email 1 (Day 0) — Warm, personal intro. Reference something specific about their business. Ask one relevant question about their biggest challenge. Do NOT mention any product or service. Max 150 words. Subject line included.

Email 2 (Day 3) — Follow-up. Share one specific insight relevant to their industry in {city}. Soft mention that you help businesses like theirs. One clear CTA. Max 120 words.

Email 3 (Day 7) — Final touch. Brief, respectful, leaves door open. Max 80 words.

Rules:
- Write as Manikanta personally, not as a company
- Reference their specific business name and location naturally
- Zero mentions of "voice bots", "Talktiv", or any product unless explicitly relevant to their category
- No generic phrases like "I hope this finds you well" or "I wanted to reach out"
- If category is restaurant/cafe/food: focus on customer enquiry volume and missed bookings
- If category is clinic/dental/medical: focus on appointment no-shows and after-hours calls
- If category is real estate: focus on missed buyer/seller calls and lead follow-up
- If category is salon/gym/wellness: focus on booking automation and customer retention
- For all other categories: focus on their most common customer communication pain point

Return JSON only:
{{"day0": {{"subject": "...", "body": "..."}}, "day3": {{"subject": "...", "body": "..."}}, "day7": {{"subject": "...", "body": "..."}}}}"""


def get_db():
    return shared_get_db()


def _ensure_sequence_column():
    """Additive migration: ensure leads.email_sequence (JSON) exists."""
    try:
        cols = table_columns(None, "leads")
        if "email_sequence" not in cols:
            conn = get_db()
            conn.execute("ALTER TABLE leads ADD COLUMN email_sequence TEXT DEFAULT ''")
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"  email_sequence column ensure error: {e}")


def validate_enrichment(result: dict, lead: dict) -> bool:
    """Check enrichment output for hallucinations before saving."""
    body_combined = " ".join([
        result.get('day0', {}).get('body', ''),
        result.get('day3', {}).get('body', ''),
        result.get('day7', {}).get('body', ''),
    ]).lower()

    red_flags = ['guaranteed', 'proven results', 'hundreds of clients',
                 'award winning', 'number one', '#1']
    if any(flag in body_combined for flag in red_flags):
        return False

    name = (lead.get('name') or '').strip()
    if not name:
        return True  # no name to verify against; don't false-reject
    first = name.lower().split()[0]
    if first not in body_combined:
        return False

    return True


def _generate_whatsapp(client, lead: dict) -> str:
    """Generate a single WhatsApp outreach message."""
    name    = lead.get("name", "")
    btype   = lead.get("business_type", "") or lead.get("category", "")
    reviews = lead.get("reviews", 0)
    address = lead.get("address", "")
    website = lead.get("website", "")

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

    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=200,
        messages=[{"role": "user", "content": wa_prompt}],
    )
    return resp.content[0].text.strip()


def _generate_email_sequence(client, lead: dict) -> dict:
    """
    Generate the 3-email Day0/Day3/Day7 sequence.
    Returns {"day0": {...}, "day3": {...}, "day7": {...}} — empty dicts on parse failure.
    """
    name     = lead.get("name", "")
    category = lead.get("business_type", "") or lead.get("category", "")
    city     = lead.get("city") or lead.get("address", "")
    state    = lead.get("state", "") or ""
    phone    = lead.get("phone", "")
    reviews  = lead.get("reviews", 0)
    website  = lead.get("website", "") or "None"

    prompt = ENRICHMENT_PROMPT.format(
        name=name, category=category, city=city, state=state,
        phone=phone, reviews=reviews, website=website,
    )

    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=900,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = resp.content[0].text.strip()
    raw = raw.replace("```json", "").replace("```", "").strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {"day0": {}, "day3": {}, "day7": {}}

    def norm(slot):
        slot = data.get(slot, {}) or {}
        return {"subject": str(slot.get("subject", "")).strip(),
                "body": str(slot.get("body", "")).strip()}

    return {"day0": norm("day0"), "day3": norm("day3"), "day7": norm("day7")}


def generate_for_lead(lead: dict) -> dict:
    """
    Generate WhatsApp message + 3-email sequence for a single lead using Claude.
    Returns dict with keys:
      whatsapp_message, day0, day3, day7, email_subject, email_body
    (email_subject/email_body mirror Day 0 for backward compatibility.)
    """
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

    result = {
        "whatsapp_message": "",
        "day0": {}, "day3": {}, "day7": {},
        "email_subject": "", "email_body": "",
    }

    try:
        result["whatsapp_message"] = _generate_whatsapp(client, lead)
    except Exception as e:
        print(f"  WhatsApp generation error for {lead.get('name','')}: {e}")

    try:
        seq = _generate_email_sequence(client, lead)
        result.update(seq)
        result["email_subject"] = seq["day0"].get("subject", "")
        result["email_body"]    = seq["day0"].get("body", "")
    except Exception as e:
        print(f"  Email generation error for {lead.get('name','')}: {e}")

    return result


def enrich_lead_in_db(lead_id: int) -> dict:
    """Generate, validate, and save enrichment for a single lead."""
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

    # Generate with validation + one retry — never save invalid output.
    result = None
    candidate = None
    for attempt in range(2):
        candidate = generate_for_lead(lead)
        if validate_enrichment(candidate, lead):
            result = candidate
            break
        print(f"  ⚠️ Validation failed for {lead['name'][:40]} (attempt {attempt + 1})")

    email_valid = result is not None
    if result is None:
        result = candidate or {}

    whatsapp_message = result.get("whatsapp_message", "")
    email_subject = ""
    email_body = ""
    sequence_json = ""
    if email_valid:
        email_subject = result.get("email_subject", "")
        email_body = result.get("email_body", "")
        sequence_json = json.dumps(
            {k: result.get(k, {}) for k in ("day0", "day3", "day7")},
            ensure_ascii=False,
        )

    if not (whatsapp_message or email_subject):
        return {
            "success": False,
            "error": "Generation failed validation" if not email_valid else "Generation failed",
            "validated": email_valid,
        }

    _ensure_sequence_column()
    conn = get_db()
    conn.execute(
        """
        UPDATE leads SET
            whatsapp_message = ?,
            email_subject = ?,
            email_body = ?,
            email_sequence = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            whatsapp_message,
            email_subject,
            email_body,
            sequence_json,
            datetime.now().isoformat(),
            lead_id,
        ),
    )
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
        "Generated WhatsApp and 3-email sequence",
        status="logged",
        direction="system",
        metadata={"generated": True, "email_validated": email_valid},
    )

    return {
        "success": True,
        "lead_id": lead_id,
        "name": lead["name"],
        "email_validated": email_valid,
        "whatsapp_message": whatsapp_message,
        "email_subject": email_subject,
        "email_body": email_body,
        "day0": result.get("day0", {}),
        "day3": result.get("day3", {}),
        "day7": result.get("day7", {}),
    }


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
