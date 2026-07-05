"""
campaign_autopsy.py — Analytics Agent, Phase 1
===============================================
7 days after an approval batch, analyse what actually happened and write
it where it compounds: one note per campaign in the vault, plus proposed
pattern updates a human reviews in Obsidian.

Illusion-free guarantees:
  - Refuses to run on campaigns with < MIN_SENDS sends (no autopsy on noise).
  - All numbers computed from reply_outcomes / leads tables — Claude only
    NARRATES the numbers it is given; it never invents metrics. The prompt
    forbids introducing figures not present in the input.
  - If the Claude call fails, the metrics note is still written (data first,
    narrative optional).

Scheduling (add to existing scheduler at 19:00 IST daily):
    from campaign_autopsy import run_due_autopsies
    run_due_autopsies()          # inside the scheduler's safe-wrapper pattern

Manual trigger endpoint (main.py):
    @app.post("/api/autopsy/{batch_id}")
    def trigger_autopsy(batch_id: str):
        return run_autopsy(batch_id, force=True)

VERIFY-BEFORE-WIRING (Claude Code):
  Column names below marked VERIFY must be checked against the real
  schema (reply_outcomes was built in outcomes.py; leads table is long-
  standing). Do not guess — read the CREATE TABLE / model definitions.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

log = logging.getLogger("autopsy")

MIN_SENDS = 20           # below this an autopsy is statistically meaningless
AUTOPSY_AGE_DAYS = 7     # batch must be at least this old
MODEL = "claude-sonnet-4-6"   # project standard — do not change


# --------------------------------------------------------------------------
# Metric computation (pure SQL — the source of truth)
# --------------------------------------------------------------------------

def compute_metrics(batch_id: str) -> dict | None:
    """All autopsy numbers, straight from the DB. None if batch too small."""
    from db import get_db
    conn = get_db()
    try:
        cur = conn.cursor()

        # VERIFY: leads table columns — approval_batch_id, sent_at, subject,
        # score, category. Adjust to real names.
        cur.execute(
            """SELECT COUNT(*) FROM leads
               WHERE approval_batch_id = %s AND sent_at IS NOT NULL""",
            (batch_id,),
        )
        sent = cur.fetchone()[0]
        if sent < MIN_SENDS:
            return None

        # VERIFY: reply_outcomes columns — lead_id, sentiment, reply_type,
        # hours_to_reply, subject_line, lead_score, category, tone_profile
        cur.execute(
            """SELECT ro.sentiment, ro.reply_type, ro.hours_to_reply,
                      ro.subject_line, ro.lead_score, ro.tone_profile
               FROM reply_outcomes ro
               JOIN leads l ON l.id = ro.lead_id
               WHERE l.approval_batch_id = %s""",
            (batch_id,),
        )
        rows = cur.fetchall()

        replies = len(rows)
        positive = sum(1 for r in rows if (r[0] or "").lower() == "positive")

        # best subject: subject with most replies
        subj_counts: dict[str, int] = {}
        for r in rows:
            if r[3]:
                subj_counts[r[3]] = subj_counts.get(r[3], 0) + 1
        best_subject = max(subj_counts, key=subj_counts.get) if subj_counts else None

        # score bands
        bands = {"0-40": [0, 0], "40-60": [0, 0], "60-70": [0, 0],
                 "70-85": [0, 0], "85-100": [0, 0]}

        def band(s):
            s = s or 0
            return ("0-40" if s < 40 else "40-60" if s < 60 else
                    "60-70" if s < 70 else "70-85" if s < 85 else "85-100")

        cur.execute(
            """SELECT score FROM leads
               WHERE approval_batch_id = %s AND sent_at IS NOT NULL""",
            (batch_id,),
        )
        for (s,) in cur.fetchall():
            bands[band(s)][0] += 1
        for r in rows:
            bands[band(r[4])][1] += 1

        band_rates = {
            b: (n_rep / n_sent if n_sent else 0.0, n_sent)
            for b, (n_sent, n_rep) in bands.items() if n_sent > 0
        }
        best_band = max(band_rates, key=lambda b: band_rates[b][0]) if band_rates else None

        # tone performance
        tone_counts: dict[str, int] = {}
        for r in rows:
            if r[5]:
                tone_counts[r[5]] = tone_counts.get(r[5], 0) + 1

        # avg hours to reply
        hrs = [r[2] for r in rows if r[2] is not None]
        avg_hours = round(sum(hrs) / len(hrs), 1) if hrs else None

        # VERIFY: category source — leads.category for the batch
        cur.execute(
            """SELECT category, COUNT(*) FROM leads
               WHERE approval_batch_id = %s GROUP BY category
               ORDER BY COUNT(*) DESC LIMIT 1""",
            (batch_id,),
        )
        row = cur.fetchone()
        category = row[0] if row else "unknown"

        return {
            "batch_id": batch_id,
            "category": category,
            "sent": sent,
            "replies": replies,
            "reply_rate": round(replies / sent * 100, 1),
            "positive": positive,
            "positive_rate": round(positive / sent * 100, 1),
            "best_subject": best_subject,
            "best_score_band": best_band,
            "band_rates": {b: round(r * 100, 1) for b, (r, _n) in band_rates.items()},
            "tone_replies": tone_counts,
            "avg_hours_to_reply": avg_hours,
        }
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Narrative + proposals (single Claude call; numbers are read-only input)
# --------------------------------------------------------------------------

_NARRATIVE_PROMPT = """You are the Analytics agent of MARVIS, analysing a completed
cold-outreach campaign. Below are the ONLY facts. Do not invent, extrapolate,
or introduce any number not present in the JSON. If the data is too thin to
support a lesson, say so plainly.

CAMPAIGN DATA (source of truth):
{data}

Produce exactly this JSON (no markdown fences, no commentary):
{{
  "summary": "<3 sentences: what happened, what stood out, honest caveats>",
  "proposals": [
    {{"category": "<category or 'all'>", "rule": "<one actionable pattern>",
      "evidence": "<the specific numbers above that support it>",
      "confidence": "<confirmed|tentative>"}}
  ]
}}
Max 3 proposals. A proposal with weak evidence must be marked tentative.
If nothing is supportable, return an empty proposals list."""


def generate_narrative(metrics: dict) -> dict:
    """One Claude call. Fail-soft: on any error return metrics-only stub."""
    try:
        import anthropic
        client = anthropic.Anthropic()  # ANTHROPIC_API_KEY from env
        resp = client.messages.create(
            model=MODEL,
            max_tokens=800,
            messages=[{"role": "user",
                       "content": _NARRATIVE_PROMPT.format(
                           data=json.dumps(metrics, indent=2))}],
        )
        text = resp.content[0].text.strip()
        text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        return json.loads(text)
    except Exception as e:
        log.warning("narrative generation failed, writing metrics-only: %s", e)
        return {"summary": "(narrative unavailable — metrics recorded)",
                "proposals": []}


# --------------------------------------------------------------------------
# Vault + notification output
# --------------------------------------------------------------------------

def write_outputs(metrics: dict, narrative: dict) -> str:
    import vault

    n = _next_campaign_number()
    rel = f"campaigns/campaign-{n:03d}-{metrics['category']}.md"

    body = [
        f"# Campaign Autopsy · {metrics['batch_id']}",
        f"**Category:** {metrics['category']} · **Autopsied:** {datetime.now():%Y-%m-%d %H:%M}",
        "",
        "## Numbers",
        f"- Sent: {metrics['sent']}",
        f"- Replies: {metrics['replies']} ({metrics['reply_rate']}%)",
        f"- Positive: {metrics['positive']} ({metrics['positive_rate']}%)",
        f"- Best subject: {metrics['best_subject'] or '—'}",
        f"- Best score band: {metrics['best_score_band'] or '—'} "
        f"(rates: {metrics['band_rates']})",
        f"- Tone → replies: {metrics['tone_replies'] or '—'}",
        f"- Avg hours to reply: {metrics['avg_hours_to_reply'] or '—'}",
        "",
        "## Read",
        narrative.get("summary", ""),
        "",
        "## Proposals",
    ]
    for p in narrative.get("proposals", []):
        body.append(f"- [{p.get('confidence','tentative')}] ({p.get('category','all')}) "
                    f"{p.get('rule','')} — evidence: {p.get('evidence','')}")
    if not narrative.get("proposals"):
        body.append("- none supportable from this data")
    vault.write_note(rel, "\n".join(body) + "\n")

    # append proposals to brain/Patterns.md ## Proposed (human promotes later)
    for p in narrative.get("proposals", []):
        vault.append_note(
            "brain/Patterns.md",
            f"- ({p.get('category','all')}) {p.get('rule','')} "
            f"[{metrics['batch_id']} · {p.get('confidence')}]",
        )

    vault.daily_note(
        f"Autopsy {metrics['batch_id']}: {metrics['reply_rate']}% reply, "
        f"{len(narrative.get('proposals', []))} proposals → Patterns.md"
    )
    return rel


def _next_campaign_number() -> int:
    import vault
    notes = vault.latest_notes("campaigns", 1)
    if not notes:
        return 1
    name = notes[0]["name"]
    try:
        return int(name.split("-")[1]) + 1
    except (IndexError, ValueError):
        return len(vault.latest_notes("campaigns", 999)) + 1


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def run_autopsy(batch_id: str, force: bool = False) -> dict:
    from hud_events import hud_event
    hud_event("dispatch", "analytics", f"autopsy started · {batch_id}")

    metrics = compute_metrics(batch_id)
    if metrics is None:
        hud_event("fail", "analytics",
                  f"{batch_id}: < {MIN_SENDS} sends — autopsy skipped (too small)")
        return {"status": "skipped", "reason": f"fewer than {MIN_SENDS} sends"}

    narrative = generate_narrative(metrics)
    rel = write_outputs(metrics, narrative)
    _mark_autopsied(batch_id)

    hud_event("complete", "analytics",
              f"{batch_id}: {metrics['reply_rate']}% reply · note {rel}")

    # Telegram — use the existing notifier, do not modify telegram_notify.py
    try:
        from telegram_notify import send_telegram_message  # VERIFY exact fn name
        send_telegram_message(
            f"📊 Campaign Autopsy · {batch_id}\n"
            f"Sent {metrics['sent']} · Replies {metrics['replies']} "
            f"({metrics['reply_rate']}%) · Positive {metrics['positive']} "
            f"({metrics['positive_rate']}%)\n"
            f"Best subject: {metrics['best_subject'] or '—'}\n"
            f"Best band: {metrics['best_score_band'] or '—'}\n"
            f"📝 {len(narrative.get('proposals', []))} proposals → Patterns.md"
        )
    except Exception as e:
        log.warning("telegram notify failed: %s", e)

    return {"status": "ok", "note": rel, "metrics": metrics}


def run_due_autopsies() -> list[dict]:
    """Find approval batches ≥7 days old without an autopsy, run each."""
    from db import get_db
    cutoff = datetime.now() - timedelta(days=AUTOPSY_AGE_DAYS)
    conn = get_db()
    try:
        cur = conn.cursor()
        # VERIFY: where approval batches live — assumes leads.approval_batch_id
        # + leads.approved_at, and an autopsied marker (add column or a small
        # autopsies table; column shown here):
        cur.execute(
            """SELECT DISTINCT approval_batch_id FROM leads
               WHERE approval_batch_id IS NOT NULL
                 AND approved_at < %s
                 AND (autopsied IS NULL OR autopsied = FALSE)""",
            (cutoff,),
        )
        batches = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    return [run_autopsy(b) for b in batches]


def _mark_autopsied(batch_id: str) -> None:
    from db import get_db
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE leads SET autopsied = TRUE WHERE approval_batch_id = %s",
            (batch_id,),
        )
        conn.commit()
    finally:
        conn.close()
