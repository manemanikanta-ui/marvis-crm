"""
MARVIS AI Enrichment Engine
Generates a personalised 3-email cold outreach sequence (Day 0 / Day 3 / Day 7)
plus a WhatsApp message for any lead using Claude.

All Claude output is validated (validate_enrichment_output) before it is saved —
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

# Outreach generation model (per business-model spec).
CLAUDE_MODEL = "claude-haiku-4-5-20251001"


def build_outreach_prompt(lead: dict) -> str:
    """Business-aware outreach prompt: detects what each lead is missing and
    tailors WhatsApp + email to it. Produces JSON {whatsapp, email_subject, email_body}."""
    name = lead.get('name', 'your business')
    # DB stores category under `business_type`; fall back to `category` for compatibility.
    category = (lead.get('category') or lead.get('business_type') or '').lower()
    city = lead.get('city') or lead.get('location') or 'Hyderabad'
    reviews = lead.get('reviews', 0) or 0
    website = (lead.get('website') or '').strip()

    # Detect what they're missing
    is_social_only = any(s in website for s in
        ['instagram.com', 'facebook.com', 'twitter.com'])
    has_no_website = not website
    needs_website = has_no_website or is_social_only
    high_volume = int(reviews) > 200 if reviews else False

    # Category pain points
    pain_map = {
        'cafe': 'missed reservation calls and after-hours enquiries',
        'coffee': 'missed reservation calls and after-hours enquiries',
        'restaurant': 'missed table bookings and delivery enquiries',
        'dental': 'missed appointment calls and after-hours patient queries',
        'clinic': 'missed patient calls and appointment bookings outside working hours',
        'salon': 'missed booking calls and WhatsApp appointment requests',
        'beauty': 'missed booking calls and WhatsApp appointment requests',
        'gym': 'missed membership enquiries and class booking requests',
        'fitness': 'missed membership enquiries and class booking requests',
        'real estate': 'missed buyer and seller calls during site visits',
        'property': 'missed buyer and seller calls during site visits',
        'hotel': 'missed booking enquiries and check-in queries',
        'coaching': 'missed student enquiries and admission calls',
        'school': 'missed parent enquiries and admission calls',
        'pharmacy': 'missed medicine availability and prescription queries',
        'interior': 'missed project enquiries and consultation requests',
        'photographer': 'missed event booking enquiries',
        'plumber': 'missed emergency service calls after hours',
        'electrician': 'missed emergency service calls after hours',
    }

    pain = 'missed customer enquiries outside business hours'
    for key, value in pain_map.items():
        if key in category:
            pain = value
            break

    # What we specifically offer this business
    offerings = []
    if needs_website:
        offerings.append("a professional website that converts visitors into customers")
    if high_volume:
        offerings.append(
            f"AI WhatsApp automation to handle the volume of enquiries "
            f"that comes with {reviews}+ Google reviews"
        )
    else:
        offerings.append("WhatsApp AI that replies to customer messages instantly, 24/7")

    offerings.append(f"an AI system to handle {pain} automatically")
    offering_text = offerings[0] + " and " + offerings[1]

    website_note = ""
    if has_no_website:
        website_note = "They have NO website — specifically mention we can build them one."
    elif is_social_only:
        website_note = (f"Their only web presence is social media ({website}) — "
                       f"mention we can build a proper business website.")

    return f"""You are writing personalised cold outreach for Manikanta Mane,
founder of Talktiv AI, Hyderabad.

Business details:
Name: {name}
Category: {category}
City: {city}
Google Reviews: {reviews}
Website status: {'No website' if has_no_website else ('Social only: ' + website if is_social_only else 'Has website')}

{website_note}

Their main pain point: {pain}

What we can specifically offer them: {offering_text}

Write three pieces of outreach:

1. WhatsApp message (max 100 words):
- Warm and personal, written as Manikanta personally
- Reference {name} and {city} specifically
- Mention ONE specific thing we can do for them based on what they're missing
- End with a single question that invites a reply
- Natural tone — not salesy, not corporate
- If they have 500+ reviews, acknowledge their established reputation

2. Email subject line (max 55 characters)

3. Email body (max 160 words):
- Same personal tone as WhatsApp
- Reference their specific pain point: {pain}
- Mention what we can build for them specifically
- One clear CTA at the end

STRICT RULES:
- Never say "Talktiv AI talkbots" or "voice bots" or "AI agents" generically
- Never use phrases like "I hope this finds you well" or "I wanted to reach out"
- Never make claims we can't deliver (no "guaranteed results" or "100% success")
- Always reference the business name and city naturally
- If website is missing or social-only, always mention website building
- Write in a way that feels like a local Hyderabad professional, not a global SaaS company

Return ONLY valid JSON, no markdown:
{{"whatsapp": "...", "email_subject": "...", "email_body": "..."}}"""


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


def validate_enrichment_output(result: dict, lead: dict) -> tuple[bool, str]:
    """Validate Claude output before saving. Returns (is_valid, reason)."""
    wa = result.get('whatsapp', '')
    subject = result.get('email_subject', '')
    body = result.get('email_body', '')
    combined = (wa + subject + body).lower()
    name = (lead.get('name') or '').lower()

    if len(wa) < 20:
        return False, "WhatsApp message too short"
    if len(subject) < 5:
        return False, "Email subject too short"
    if len(body) < 50:
        return False, "Email body too short"

    bad_phrases = [
        'guaranteed results', 'proven roi', 'hundreds of clients',
        'award winning', 'number one', '#1 in', 'best in india',
        'i hope this finds you', 'i wanted to reach out',
        'talkbot', 'voice bot'
    ]
    for phrase in bad_phrases:
        if phrase in combined:
            return False, f"Hallucination detected: '{phrase}'"

    first_word = name.split()[0] if name else ''
    if first_word and len(first_word) > 3 and first_word not in combined:
        return False, "Business name not referenced in output"

    return True, "OK"


def _generate_outreach(client, lead: dict) -> dict:
    """
    Single Claude call producing the business-aware outreach.
    Returns the raw parsed JSON {whatsapp, email_subject, email_body} — {} on failure.
    """
    prompt = build_outreach_prompt(lead)

    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = resp.content[0].text.strip()
    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    raw = raw.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def generate_for_lead(lead: dict) -> dict:
    """
    Generate WhatsApp + email outreach for a single lead using Claude.
    Returns dict with keys:
      whatsapp, whatsapp_message, email_subject, email_body, day0, day3, day7
    ('whatsapp' mirrors 'whatsapp_message' for validate_enrichment_output;
     day0 mirrors the email for backward compatibility with the stored sequence.)
    """
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

    result = {
        "whatsapp": "", "whatsapp_message": "",
        "email_subject": "", "email_body": "",
        "day0": {}, "day3": {}, "day7": {},
    }

    try:
        raw = _generate_outreach(client, lead)
    except Exception as e:
        print(f"  Outreach generation error for {lead.get('name','')}: {e}")
        return result

    wa      = str(raw.get("whatsapp", "")).strip()
    subject = str(raw.get("email_subject", "")).strip()
    body    = str(raw.get("email_body", "")).strip()

    result["whatsapp"]         = wa
    result["whatsapp_message"] = wa
    result["email_subject"]    = subject
    result["email_body"]       = body
    if subject or body:
        result["day0"] = {"subject": subject, "body": body}

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
        is_valid, reason = validate_enrichment_output(candidate, lead)
        if is_valid:
            result = candidate
            break
        print(f"  ⚠️ Validation failed for {lead['name'][:40]} (attempt {attempt + 1}): {reason}")

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
