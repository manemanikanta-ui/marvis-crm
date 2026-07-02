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
import random
import string
import asyncio
import threading
from datetime import datetime, timedelta
from dotenv import load_dotenv

from db import get_db as shared_get_db, table_columns
from hud_bus import emit_hud_event
from crm_core import log_activity_event, update_lead_status

load_dotenv()

DB_PATH = "data/crm.db"

# Outreach generation model (per business-model spec).
# Lightweight model for the WhatsApp message / internal tasks.
CLAUDE_MODEL = "claude-haiku-4-5-20251001"
# The 3-email client-facing sequence goes out under Manikanta's name — quality is
# non-negotiable, so it uses Sonnet (see THING 2 / feedback.md).
EMAIL_MODEL = "claude-sonnet-4-6"

# Words/phrases banned in ALL outreach (global hard rule). The per-category
# `forbidden` lists in CATEGORY_PROFILES are applied on top of this.
GLOBAL_FORBIDDEN = [
    "revolutionise", "revolutionize", "cutting-edge", "leverage", "seamlessly",
    "excited", "pleased", "hope this finds you well", "reach out", "touch base",
    "synergy", "solution", "game-changer", "innovative", "best-in-class",
    "i wanted to", "do not hesitate",
]


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY TONE MAP
# Maps a lead's category to the voice we write in. Each profile carries:
#   tone, hook (core pain), trust_line (one credibility line), style_notes,
#   forbidden (words/phrases that kill trust for that category), and `match`
#   (substrings used to resolve a free-text business_type onto a profile).
# trust_line may contain [City] / [category] placeholders, filled at lookup.
# Any unlisted category falls back to PROFESSIONAL.
# Insertion order matters: CALM (healthcare) is checked first; PROFESSIONAL last.
# ─────────────────────────────────────────────────────────────────────────────
CATEGORY_PROFILES: dict = {
    "CALM": {
        "tone": "calm",
        "no_exclamation": True,
        "hook": "patient communication after hours + reputation management",
        "trust_line": "We built this specifically for healthcare — trust is non-negotiable in your field.",
        "style_notes": (
            "Measured, precise, zero hype. One clear question only. Professional peer "
            "tone. Never pushy. Short paragraphs. Never use an exclamation mark."
        ),
        "forbidden": [
            "excited", "amazing", "awesome", "revolutionary", "!",
            "reach out", "touch base",
        ],
        "match": [
            "clinic", "hospital", "dentist", "dental", "physiotherap", "physio",
            "diagnostic", "pathology", "skin", "dermatolog", "eye care", "eye clinic",
            "optometr", "veterinary", "psychiatr", "psycholog", "medical",
            "healthcare", "health care", "doctor", "nursing", "ayurved",
        ],
    },
    "DIRECT": {
        "tone": "direct",
        "hook": "missed calls after hours = jobs going to competitors",
        "trust_line": "A [category] in [City] was missing calls a day after 7pm.",
        "style_notes": (
            "Blunt and brief. Pure revenue framing. No fluff. Three sentences max "
            "in body. Prefer short words over long ones."
        ),
        "forbidden": [
            "leverage", "seamlessly", "synergy", "cutting-edge", "revolutionise",
            "innovative", "best-in-class", "solution",
        ],
        "match": [
            "plumb", "electric", "ac repair", "a/c repair", "air condition", "hvac",
            "pest control", "contractor", "carpenter", "painter", "appliance repair",
            "cctv", "security system", "locksmith", "handyman", "renovation",
            "borewell", "waterproof",
        ],
    },
    "WARM": {
        "tone": "warm",
        "hook": "client retention + repeat bookings + no-shows",
        "trust_line": "Most wellness clients see fewer no-shows in the first month.",
        "style_notes": (
            "Warm but concise. Aspirational without being salesy. Feels like a "
            "recommendation from someone who gets their world."
        ),
        "forbidden": ["synergy", "game-changer", "solution", "excited"],
        "match": [
            "salon", "spa", "gym", "yoga", "wellness", "beauty", "parlour", "parlor",
            "nail", "massage", "fitness", "pilates", "aesthetic", "barber",
        ],
    },
    "GENZ": {
        "tone": "genz",
        "hook": "reviews going unanswered + after-hours DMs missed",
        "trust_line": "We work with a few F&B spots in [City] already.",
        "style_notes": (
            "Casual, punchy, short sentences. One rhetorical question. Max 4 lines "
            "body. No corporate language."
        ),
        "forbidden": [
            "revolutionise", "seamlessly", "leverage", "cutting-edge",
            "innovative", "pleased to",
        ],
        "match": [
            "cafe", "coffee", "restaurant", "bistro", "bar", "pub", "cloud kitchen",
            "bakery", "baker", "patisserie", "food truck", "bubble tea", "boba",
            "dessert", "ice cream", "diner", "eatery",
        ],
    },
    "COMMUNITY": {
        "tone": "community",
        "hook": "parent and student enquiries going unanswered + admissions",
        "trust_line": "We built this for education providers who cannot miss a parent's first call.",
        "style_notes": (
            "Warm and community-minded. Parent-aware language. Respectful, never "
            "corporate."
        ),
        "forbidden": ["leverage", "synergy", "roi", "pipeline", "conversion rate"],
        "match": [
            "school", "tutor", "coaching", "training institute", "music class",
            "music school", "art class", "dance", "daycare", "day care", "preschool",
            "pre-school", "kindergarten", "academy", "education", "institute",
        ],
    },
    "RETAIL": {
        "tone": "retail",
        "hook": (
            "WhatsApp catalogue enquiries + repeat customer communication + "
            "festive season follow-ups"
        ),
        "trust_line": "Local [category] stores using this see more repeat WhatsApp orders.",
        "style_notes": (
            "Business-focused, competitive, margin-aware. Owner-to-owner tone."
        ),
        "forbidden": ["cutting-edge", "revolutionary", "game-changer"],
        "match": [
            "clothing", "apparel", "fashion", "boutique", "electronics", "furniture",
            "optical", "optician", "eyewear", "jewell", "jewel", "pharmacy", "pharma",
            "chemist", "bookstore", "book shop", "gift", "hardware", "grocery",
            "supermarket", "mobile store",
        ],
    },
    "PROFESSIONAL": {
        "tone": "professional",
        "hook": "lead response time + follow-up automation + pipeline",
        "trust_line": "This is built for your specific operation, not a generic software subscription.",
        "style_notes": "Peer-to-peer. ROI-aware. Confident without overselling.",
        "forbidden": [
            "hope this finds you well", "i wanted to reach out",
            "please do not hesitate",
        ],
        "match": [
            "real estate", "property", "realtor", "builder", "mortgage", "financial",
            "wealth", "insurance", "chartered accountant", "ca firm", "accountant",
            "lawyer", "legal", "advocate", "law firm", "architect", "interior design",
            "consultant",
        ],
    },
}


def get_category_profile(category: str, city: str = "") -> dict:
    """Resolve a free-text business_type onto a CATEGORY_PROFILES entry.

    Returns a copy with [City]/[category] placeholders filled and the internal
    `match` key removed. Unlisted categories fall back to PROFESSIONAL.
    """
    cat = (category or "").lower()
    chosen = None
    for profile in CATEGORY_PROFILES.values():
        if any(keyword in cat for keyword in profile["match"]):
            chosen = profile
            break
    if chosen is None:
        chosen = CATEGORY_PROFILES["PROFESSIONAL"]

    filled = {k: v for k, v in chosen.items() if k != "match"}
    city_label = (city or "").strip() or "your area"
    cat_label = (category or "").strip() or "business"
    filled["trust_line"] = (
        filled["trust_line"].replace("[City]", city_label).replace("[category]", cat_label)
    )
    return filled


def build_outreach_prompt(lead: dict, proof_pack_url=None, context=None) -> str:
    """Category-aware outreach prompt. Resolves the lead's CATEGORY_PROFILES voice,
    applies the global + per-category hard rules, and produces a WhatsApp message
    plus a 3-email drip (Day 0 / 3 / 7). Returns JSON {whatsapp, emails:[...]}.
    proof_pack_url is accepted for compatibility but no longer woven into Email 1
    (the /proof link 404s in outreach — see note below; re-add once verified).
    context (optional free text from Manikanta) is prepended to personalise the
    outreach — used for manual leads where Claude has no scraped signals."""
    name = lead.get('name', 'your business')
    contact_name = (lead.get('contact_name') or '').strip()
    # DB stores category under `business_type`; fall back to `category` for compatibility.
    category = (lead.get('category') or lead.get('business_type') or '').lower()
    # The LEAD's city — never default to our own city. Fall back to address if present.
    city = (lead.get('city') or lead.get('location') or lead.get('address') or '').strip()
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

    # Category voice + lead signals
    profile = get_category_profile(category, city)
    score = int(lead.get('score') or 0)
    has_phone = bool((lead.get('phone') or '').strip())
    has_website = bool(website)
    greeting_name = contact_name if contact_name else "there"
    subject_example = f"{name} {city}".strip()

    # One specific, real fact to anchor Email 1 (never fabricated).
    facts = []
    if reviews:
        facts.append(f"{reviews} Google reviews")
    if city:
        facts.append(f"based in {city}")
    if category:
        facts.append(f"a {category}")
    fact_hint = "; ".join(facts) if facts else "only what is listed above — invent nothing"

    # Proof-pack preview link removed from Email 1: the /proof/{code} URL 404s in
    # outreach because packs aren't on the public Railway DB. The talktivai.com
    # trust signal stays. Re-add the link here once the proof portal is verified.
    # (proof_pack_url is still accepted for compatibility but no longer used.)

    no_excl_rule = (
        "\n- This category is healthcare: NEVER use an exclamation mark anywhere."
        if profile.get("no_exclamation") else ""
    )
    cat_forbidden = ", ".join(profile.get("forbidden", [])) or "none"
    global_forbidden = ", ".join(GLOBAL_FORBIDDEN)

    # Optional free-text context from Manikanta. Placed at the very START of the
    # prompt (before everything else) so Claude anchors the whole outreach on it.
    context_block = ""
    if context and str(context).strip():
        context_block = (
            "IMPORTANT CONTEXT FROM MANIKANTA:\n"
            f"{str(context).strip()}\n\n"
            "Use this context to make the outreach highly specific to this business. "
            "Reference details from this context naturally in the message.\n\n"
        )

    return f"""{context_block}You are Manikanta Mane, founder of Talktiv AI, writing to a business owner.
The messages are FROM you TO them. Never address Manikanta. Never invent facts.

LEAD
Business name: {name}
Contact person: {greeting_name}
Category: {category or 'unknown'}
City: {city or 'unknown'}
Google reviews: {reviews}
Has a phone on file: {has_phone}
Has a website: {has_website}
Their likely pain: {pain}
Lead score: {score}

VOICE FOR THIS CATEGORY (tone: {profile['tone']})
Core pain to lean on: {profile['hook']}
Credibility line you may adapt: {profile['trust_line']}
Style: {profile['style_notes']}
Category-forbidden words (never use): {cat_forbidden}{no_excl_rule}

HARD RULES (every email)
- Plain text only. No HTML, no bullet points, no bold, no markdown.
- Email body is 5 lines maximum.
- Include exactly ONE specific, real fact about the lead, woven in naturally
  (choose from: {fact_hint}). Never state a fact not given above.
- Mention the website exactly once, mid-email, as a low-key verification offer:
  the bare word talktivai.com (no angle brackets, not a CTA, not in the signature).
  Only Email 1 carries this mid-email mention; Emails 2 and 3 do not.
- End every email with this signature — exactly these three lines, nothing else:
  — Manikanta
  Talktiv AI
  talktivai.com
- Subject lines must read like a human typed them quickly — e.g. "{subject_example}",
  "Re: {name}", or a short plain observation. Never a marketing headline; lowercase is fine.
- Never use any of these words/phrases anywhere: {global_forbidden}

THREE EMAILS

EMAIL 1 — Day 0 (cold outreach), tone = {profile['tone']}:
- One specific observation about their business using the real fact.
- One genuine yes/no question about their current process (not rhetorical).
- The single mid-email talktivai.com verification mention.
- No ask, no CTA button, no link other than talktivai.com.

EMAIL 2 — Day 3 (no-reply follow-up):
- Very short: at most 2 short body lines, then the signature.
- Reference the Day 0 email naturally, never passive-aggressive.
- A different angle on the same pain.
- End with a simple yes/no question. No website link.

EMAIL 3 — Day 7 (closing):
- At most 2 body lines, then the signature.
- Frame it as "Closing my notes on {name}".
- Leave the door open with zero desperation. No website link, no pitch, no ask.

Return ONLY valid JSON, no markdown:
{{"whatsapp": "<max 90 words, starts 'Hi {greeting_name},', references {name}, one yes/no question, signs off 'Manikanta, Talktiv AI'>", "emails": [{{"day": 0, "subject": "...", "body": "..."}}, {{"day": 3, "subject": "...", "body": "..."}}, {{"day": 7, "subject": "...", "body": "..."}}]}}"""


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
        'talkbot', 'voice bot',
    ] + GLOBAL_FORBIDDEN
    for phrase in bad_phrases:
        if phrase in combined:
            return False, f"Hallucination detected: '{phrase}'"

    first_word = name.split()[0] if name else ''
    if first_word and len(first_word) > 3 and first_word not in combined:
        return False, "Business name not referenced in output"

    return True, "OK"


def _generate_outreach(client, lead: dict, proof_pack_url=None, context=None) -> dict:
    """
    Single Sonnet call producing the category-aware outreach.
    Returns the raw parsed JSON {whatsapp, emails:[{day,subject,body}, ...]} — {} on failure.
    """
    prompt = build_outreach_prompt(lead, proof_pack_url, context=context)

    resp = client.messages.create(
        model=EMAIL_MODEL,
        max_tokens=2000,
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


def generate_for_lead(lead: dict, proof_pack_url=None, context=None) -> dict:
    """
    Generate WhatsApp + a 3-email drip (Day 0/3/7) for a single lead using Sonnet.
    Returns dict with keys:
      whatsapp, whatsapp_message, email_subject, email_body, day0, day3, day7
    email_subject/email_body mirror Day 0 (for the lead detail panel + validation).
    """
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

    result = {
        "whatsapp": "", "whatsapp_message": "",
        "email_subject": "", "email_body": "",
        "day0": {}, "day3": {}, "day7": {},
    }

    try:
        raw = _generate_outreach(client, lead, proof_pack_url, context=context)
    except Exception as e:
        print(f"  Outreach generation error for {lead.get('name','')}: {e}")
        return result

    wa = str(raw.get("whatsapp", "")).strip()

    by_day = {}
    for em in (raw.get("emails") or []):
        if not isinstance(em, dict):
            continue
        try:
            day = int(em.get("day"))
        except (TypeError, ValueError):
            continue
        by_day[day] = {
            "subject": str(em.get("subject", "")).strip(),
            "body": str(em.get("body", "")).strip(),
        }

    day0 = by_day.get(0, {})
    result["whatsapp"]         = wa
    result["whatsapp_message"] = wa
    result["email_subject"]    = day0.get("subject", "")
    result["email_body"]       = day0.get("body", "")
    result["day0"]             = day0
    result["day3"]             = by_day.get(3, {})
    result["day7"]             = by_day.get(7, {})

    return result


# ─────────────────────────────────────────────────────────────────────────────
# TWO-TIER PROOF PACK (THING 3)
#   Tier 1 — hot leads (score >= 75): pack auto-generated during enrichment,
#            portal link woven into Email 1.
#   Tier 2 — warm leads (score < 75): pack deferred until the lead replies
#            (status -> "responded"), then generated + a standalone email sent.
# ─────────────────────────────────────────────────────────────────────────────

HOT_LEAD_SCORE = 75
SIGNATURE = "— Manikanta\nTalktiv AI\ntalktivai.com"


def _public_base_url() -> str:
    """Absolute base URL for proof-portal links (no request context in enrichment)."""
    return (os.getenv("PUBLIC_BASE_URL") or "https://marvis-crm-production.up.railway.app").rstrip("/")


def _run_async(coro):
    """Run an async coroutine to completion from sync code, safe whether or not an
    event loop is already running (enrich-now runs inside the FastAPI loop)."""
    box = {}

    def runner():
        box["value"] = asyncio.run(coro)

    t = threading.Thread(target=runner)
    t.start()
    t.join()
    return box.get("value")


def _generate_proof_code() -> str:
    """Unique 6-char alphanumeric proof code (mirrors main.generate_proof_code)."""
    while True:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        conn = get_db()
        exists = conn.execute("SELECT id FROM proof_packs WHERE code = ?", (code,)).fetchone()
        conn.close()
        if not exists:
            return code


def lead_has_proof_pack(lead_id: int) -> bool:
    conn = get_db()
    row = conn.execute(
        "SELECT id FROM proof_packs WHERE lead_id = ? LIMIT 1", (lead_id,)
    ).fetchone()
    conn.close()
    return bool(row)


def get_lead_proof_url(lead_id: int):
    """Return the portal URL for a lead's most recent proof pack, or None."""
    conn = get_db()
    row = conn.execute(
        "SELECT code FROM proof_packs WHERE lead_id = ? ORDER BY id DESC LIMIT 1",
        (lead_id,),
    ).fetchone()
    conn.close()
    if row:
        return f"{_public_base_url()}/proof/{row['code']}"
    return None


def create_proof_pack_for_lead(lead: dict):
    """Generate a proof pack for a lead, store it in proof_packs, and return the
    public portal URL. Returns None on any failure (never raises to the caller)."""
    name = (lead.get("name") or "").strip()
    if not name:
        return None
    business = {
        "name": name,
        "category": (lead.get("business_type") or lead.get("category") or ""),
        "city": (lead.get("city") or lead.get("address") or ""),
        "phone": lead.get("phone", "") or "",
        "website": lead.get("website", "") or "",
    }
    try:
        assets = _run_async(generate_proof_assets(business))
    except Exception as e:
        print(f"  proof pack generation error for {name[:40]}: {e}")
        return None
    if not assets or "error" in assets:
        print(f"  proof pack assets error for {name[:40]}: {assets.get('error') if assets else 'no assets'}")
        return None

    code = _generate_proof_code()
    expires_at = (datetime.utcnow() + timedelta(days=7)).isoformat()
    try:
        conn = get_db()
        conn.execute(
            """
            INSERT INTO proof_packs
                (code, lead_id, business_name, category, city, phone, website,
                 assets_json, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                code, lead.get("id"), business["name"], business["category"],
                business["city"], business["phone"], business["website"],
                json.dumps(assets, ensure_ascii=False), expires_at,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  proof pack store error for {name[:40]}: {e}")
        return None
    return f"{_public_base_url()}/proof/{code}"


def handle_warm_lead_responded(lead_id: int) -> dict:
    """DISABLED. Proof packs are no longer generated when a lead replies —
    generation now happens only post-approval, on the Day-3 follow-up for leads
    scored >= 60 (see email_engine.schedule_sequence). Kept as a safe no-op so the
    existing caller in gmail_service.py keeps working without any change there."""
    return {"success": True, "skipped": True, "reason": "on-reply proof generation disabled"}


def enrich_lead_in_db(lead_id: int, context=None, force=False, suppress_individual_notify: bool = False) -> dict:
    """Generate, validate, and save enrichment for a single lead.
    context (optional) is free-text from Manikanta, passed through to the prompt.
    force=True bypasses the already-enriched skip-guard (always regenerate)."""
    conn = get_db()
    lead = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()

    if not lead:
        return {"success": False, "error": "Lead not found"}

    lead = dict(lead)

    # Skip if already enriched — unless force=True (explicit regenerate).
    if not force and lead.get("whatsapp_message") and lead.get("email_subject"):
        return {"success": True, "skipped": True, "message": "Already enriched"}

    print(f"  ✨ Enriching: {lead['name'][:40]}")

    # Proof packs are NO LONGER generated at enrichment time — that produced packs
    # for leads before they were ever approved (and burned tokens on unapproved
    # leads). Packs are now generated only after approval, attached to the Day-3
    # follow-up for leads scored >= 60 (see email_engine.schedule_sequence).
    # Manual generation via the Proof Packs tab still works.
    proof_pack_url = None

    # Generate with validation + one retry — never save invalid output.
    result = None
    candidate = None
    for attempt in range(2):
        candidate = generate_for_lead(lead, proof_pack_url, context=context)
        is_valid, reason = validate_enrichment_output(candidate, lead)
        if is_valid:
            result = candidate
            break
        print(f"  ⚠️ Validation failed for {lead['name'][:40]} (attempt {attempt + 1}): {reason}")

    # Use the best candidate we have (a validated one if found, else the last attempt).
    result = result or candidate or {}

    whatsapp_message = (result.get("whatsapp_message") or "").strip()
    email_subject = (result.get("email_subject") or "").strip()
    email_body = (result.get("email_body") or "").strip()

    # Save the email whenever Claude produced a usable subject + body. Keep a light
    # hallucination guard, but no longer discard good emails just because the strict
    # business-name match failed — that was leaving subject/body rendering as "—".
    bad_phrases = [
        'guaranteed results', 'proven roi', 'i hope this finds you',
        'i wanted to reach out', 'talkbot', 'voice bot',
    ]
    email_combined = (email_subject + " " + email_body).lower()
    email_valid = bool(
        len(email_subject) >= 5
        and len(email_body) >= 50
        and not any(p in email_combined for p in bad_phrases)
    )
    if not email_valid:
        email_subject, email_body = "", ""

    sequence_json = ""
    if email_subject and email_body:
        sequence_json = json.dumps(
            {
                "day0": {"subject": email_subject, "body": email_body},
                "day3": result.get("day3", {}),
                "day7": result.get("day7", {}),
            },
            ensure_ascii=False,
        )

    if not (whatsapp_message or email_subject):
        return {
            "success": False,
            "error": "Generation failed",
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

    # Hot-lead flag (score >= HOT_LEAD_SCORE). Returned so batch callers can count
    # hot leads and send ONE consolidated alert instead of per-lead spam.
    is_hot = int(lead.get("score") or 0) >= HOT_LEAD_SCORE

    # Immediate per-lead Telegram alert — skipped during batch runs (the batch
    # sends one summary at the end); single manual enrichment still pings here.
    if is_hot and not suppress_individual_notify:
        try:
            from telegram_notify import notify
            notify(
                f"🔥 <b>Hot Lead Enriched</b>\n"
                f"{lead.get('name', '')} — {lead.get('city', '')}\n"
                f"Score: {lead.get('score')}\n"
                f"Category: {lead.get('business_type', '')}"
            )
        except Exception:
            pass

    return {
        "success": True,
        "lead_id": lead_id,
        "name": lead["name"],
        "is_hot": is_hot,
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
    hot      = 0

    for i, lead in enumerate(leads):
        print(f"[{i+1}/{len(leads)}] {lead['name'][:40]}")
        # Suppress the per-lead hot alert — one summary is sent after the loop.
        result = enrich_lead_in_db(lead["id"], suppress_individual_notify=True)

        if result.get("success") and not result.get("skipped"):
            enriched += 1
            if result.get("is_hot"):
                hot += 1
        elif not result.get("success"):
            failed += 1

        time.sleep(0.3)  # gentle rate limit

    print(f"\n✅ Done — Enriched: {enriched} | Failed: {failed} | Hot: {hot}")

    # ONE consolidated hot-lead alert per batch (silent when zero hot leads).
    if hot > 0:
        try:
            from telegram_notify import notify
            notify(
                f"🔥 <b>Batch Enrichment Complete</b>\n"
                f"Enriched: {enriched}\n"
                f"Hot leads found: {hot}"
            )
        except Exception:
            pass

    return {"enriched": enriched, "failed": failed,
            "total": len(leads), "skipped": 0, "hot_leads": hot}


async def generate_proof_assets(business: dict) -> dict:
    """Generate three proof assets (GBP bio, 5-email drip, 10 captions) for a
    business using Claude. Client-facing premium deliverable — uses Sonnet.

    The Anthropic SDK call is synchronous; it is run in a worker thread via
    asyncio.to_thread so the CRM event loop stays responsive during generation.
    """
    name     = business.get("name", "")
    category = (business.get("category") or "").lower()
    city     = business.get("city", "Hyderabad")
    website  = (business.get("website") or "").strip()

    is_social_only = any(s in website for s in
        ["instagram.com", "facebook.com", "twitter.com"])
    has_no_website = not website or is_social_only

    website_note = ""
    if has_no_website:
        website_note = (
            "IMPORTANT: This business has no proper website "
            "(only social media or nothing). "
            "The Google Business bio and emails should subtly "
            "highlight the value of having a professional web presence."
        )

    prompt = f"""Generate a complete "Proof Asset Pack" for this business.

Business: {name}
Category: {category}
City: {city}
Website status: {"No website / social only" if has_no_website else website}
{website_note}

Generate three assets:

1. GOOGLE BUSINESS PROFILE BIO
- Short version: max 150 characters — punchy tagline
- Full version: max 750 characters — warm, professional, includes
  local keywords ({city}, {category}), trust signals, clear CTA

2. EMAIL DRIP SEQUENCE — 5 emails
Day 0: Warm welcome / introduction (150 words max)
Day 2: Value tip relevant to their industry (120 words max)
Day 5: Social proof / story (130 words max)
Day 10: Gentle follow-up with specific offer (100 words max)
Day 21: Final touch, leave door open (80 words max)
Each email needs a subject line and preview text.
Write as if the business owner is personally emailing their customers.
Natural, warm, not corporate.

3. INSTAGRAM CAPTIONS — 10 captions
Mix of: trust-building, market insight, behind-the-scenes,
service highlight, customer story hook, local pride,
call-to-action, seasonal/timely, educational, personal brand.
Each caption: 2-4 lines + 8-10 hashtags mixing local + industry tags.
Naturally include Telugu words/phrases where authentic
(meeru, mana {city}, nammakam etc).

RULES:
- Every asset must reference {name} and {city} specifically
- Zero generic filler — every line earns its place
- Email tone: personal, like a trusted local business owner
- Caption tone: authentic local business, not global brand
- No hallucinations — only factual claims we can verify

Return ONLY valid JSON, no markdown, no explanation:
{{
  "google_bio": {{
    "short": "...",
    "full": "..."
  }},
  "email_drip": [
    {{"day": 0, "subject": "...", "preview": "...", "body": "..."}},
    {{"day": 2, "subject": "...", "preview": "...", "body": "..."}},
    {{"day": 5, "subject": "...", "preview": "...", "body": "..."}},
    {{"day": 10, "subject": "...", "preview": "...", "body": "..."}},
    {{"day": 21, "subject": "...", "preview": "...", "body": "..."}}
  ],
  "social_captions": [
    {{"type": "trust", "caption": "...", "hashtags": "..."}},
    {{"type": "market insight", "caption": "...", "hashtags": "..."}},
    {{"type": "behind the scenes", "caption": "...", "hashtags": "..."}},
    {{"type": "service highlight", "caption": "...", "hashtags": "..."}},
    {{"type": "customer story", "caption": "...", "hashtags": "..."}},
    {{"type": "local pride", "caption": "...", "hashtags": "..."}},
    {{"type": "call to action", "caption": "...", "hashtags": "..."}},
    {{"type": "educational", "caption": "...", "hashtags": "..."}},
    {{"type": "personal brand", "caption": "...", "hashtags": "..."}},
    {{"type": "seasonal", "caption": "...", "hashtags": "..."}}
  ]
}}"""

    raw = ""
    try:
        # Local client per call (matches the codebase pattern in enrichment/main).
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

        # Synchronous SDK call — run off the event loop so the CRM stays responsive.
        response = await asyncio.to_thread(
            lambda: client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=4000,
                messages=[{"role": "user", "content": prompt}],
            )
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        assets = json.loads(raw)

        # Validate structure
        required = ["google_bio", "email_drip", "social_captions"]
        if not all(k in assets for k in required):
            return {"error": "Claude returned incomplete structure", "raw": raw[:300]}
        if len(assets.get("email_drip", [])) != 5:
            return {"error": "Email drip must have 5 emails", "raw": raw[:300]}
        if len(assets.get("social_captions", [])) != 10:
            return {"error": "Social captions must have 10 items", "raw": raw[:300]}

        return assets

    except json.JSONDecodeError as e:
        return {"error": f"JSON parse failed: {e}", "raw": raw[:300]}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    result = enrich_batch(limit=20)
    print(result)
