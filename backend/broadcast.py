"""
MARVIS Email Broadcast Engine
Send bulk outreach emails to all leads with email addresses
With rate limiting, deduplication, and tracking
"""

import sqlite3
import time
import os
from datetime import datetime
from email_engine import send_email_now, TEMPLATES
from dotenv import load_dotenv

from db import get_db as shared_get_db

load_dotenv()

DB_PATH = "data/crm.db"

def get_db():
    return shared_get_db()

def get_broadcast_leads(
    status_filter: str = "new",
    has_email: bool = True,
    limit: int = 50,
    business_type: str = None
) -> list:
    """Get leads eligible for broadcast"""
    conn = get_db()
    query = "SELECT * FROM leads WHERE 1=1"
    params = []

    if has_email:
        query += " AND email IS NOT NULL AND email != ''"
    if status_filter:
        query += " AND status = ?"
        params.append(status_filter)
    if business_type:
        query += " AND business_type LIKE ?"
        params.append(f"%{business_type}%")

    # Exclude leads already in broadcast queue
    query += """ AND id NOT IN (
        SELECT DISTINCT lead_id FROM activities
        WHERE channel = 'email' AND type = 'outreach'
        AND status = 'sent'
    )"""

    query += " ORDER BY score DESC LIMIT ?"
    params.append(limit)

    leads = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()
    return leads

def run_broadcast(
    limit: int = 50,
    delay_seconds: float = 2.0,
    business_type: str = None,
    dry_run: bool = False,
    progress_callback=None
) -> dict:
    """
    Run email broadcast to all eligible leads.
    delay_seconds: wait between sends (avoid Gmail spam limits)
    dry_run: if True, shows what would be sent without sending
    progress_callback: optional function(current, total, lead_name, result)
    """

    leads = get_broadcast_leads(limit=limit, business_type=business_type)

    if not leads:
        return {"sent": 0, "failed": 0, "skipped": 0, "message": "No eligible leads found"}

    print(f"\n📧 MARVIS Email Broadcast")
    print(f"{'DRY RUN — ' if dry_run else ''}Sending to {len(leads)} leads")
    print("=" * 55)

    sent = 0
    failed = 0
    skipped = 0
    results = []

    for i, lead in enumerate(leads):
        name    = lead.get("name", "")
        email   = lead.get("email", "")
        lead_id = lead.get("id")

        print(f"\n[{i+1}/{len(leads)}] {name[:40]}")
        print(f"  📧 {email}")

        if not email or "@" not in email:
            print(f"  ⚠️  Invalid email — skipping")
            skipped += 1
            continue

        # Build email content
        template = TEMPLATES["initial"]
        content = template["build"](lead)

        if dry_run:
            print(f"  🔍 DRY RUN — would send: {content['subject']}")
            results.append({"lead": name, "email": email, "status": "dry_run"})
            continue

        # Send
        result = send_email_now(
            to_email=email,
            subject=content["subject"],
            body=content["body"],
            lead_id=lead_id,
            template_key="broadcast"
        )

        if result.get("success"):
            sent += 1
            print(f"  ✅ Sent!")
            results.append({"lead": name, "email": email, "status": "sent"})

            # Schedule follow-up sequence
            from email_engine import schedule_sequence
            schedule_sequence(lead_id, email)

        else:
            failed += 1
            error = result.get("error", "Unknown error")
            print(f"  ❌ Failed: {error}")
            results.append({"lead": name, "email": email, "status": "failed", "error": error})

        if progress_callback:
            progress_callback(i + 1, len(leads), name, result)

        # Rate limiting — be gentle with Gmail
        if i < len(leads) - 1:
            time.sleep(delay_seconds)

    print(f"\n{'='*55}")
    print(f"✅ Broadcast complete")
    print(f"   Sent:    {sent}")
    print(f"   Failed:  {failed}")
    print(f"   Skipped: {skipped}")
    print(f"   Total:   {len(leads)}")

    return {
        "sent": sent,
        "failed": failed,
        "skipped": skipped,
        "total": len(leads),
        "results": results
    }

def get_broadcast_stats() -> dict:
    """Get email broadcast statistics"""
    conn = get_db()

    total_leads = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    with_email  = conn.execute("SELECT COUNT(*) FROM leads WHERE email != '' AND email IS NOT NULL").fetchone()[0]
    already_sent = conn.execute("""
        SELECT COUNT(DISTINCT lead_id) FROM activities
        WHERE channel = 'email' AND type IN ('outreach','broadcast') AND status = 'sent'
    """).fetchone()[0]
    eligible = conn.execute("""
        SELECT COUNT(*) FROM leads
        WHERE email != '' AND email IS NOT NULL
        AND status = 'new'
        AND id NOT IN (
            SELECT DISTINCT lead_id FROM activities
            WHERE channel = 'email' AND status = 'sent'
        )
    """).fetchone()[0]

    # By business type
    # Positional index, not dict(fetchall()): Postgres RowProxy iterates as column
    # NAMES, so dict([row, ...]) would collapse to {'business_type': 'count'}.
    by_type = {row[0]: row[1] for row in conn.execute("""
        SELECT business_type, COUNT(*) FROM leads
        WHERE email != '' AND email IS NOT NULL AND status = 'new'
        GROUP BY business_type ORDER BY COUNT(*) DESC
    """).fetchall()}

    conn.close()
    return {
        "total_leads": total_leads,
        "with_email": with_email,
        "already_emailed": already_sent,
        "eligible_for_broadcast": eligible,
        "coverage_pct": round(with_email / total_leads * 100, 1) if total_leads else 0,
        "by_type": by_type
    }

if __name__ == "__main__":
    # Show stats first
    stats = get_broadcast_stats()
    print("\n📊 Broadcast Stats:")
    print(f"  Total leads:     {stats['total_leads']}")
    print(f"  With email:      {stats['with_email']} ({stats['coverage_pct']}%)")
    print(f"  Already emailed: {stats['already_emailed']}")
    print(f"  Ready to send:   {stats['eligible_for_broadcast']}")
    print(f"\n  By type:")
    for t, c in stats['by_type'].items():
        print(f"    {t}: {c}")

    # Dry run first
    print("\n🔍 Running dry run first...")
    result = run_broadcast(limit=5, dry_run=True)

    confirm = input(f"\n✅ Send to {stats['eligible_for_broadcast']} leads? (yes/no): ")
    if confirm.lower() == 'yes':
        run_broadcast(limit=stats['eligible_for_broadcast'], delay_seconds=3.0)
    else:
        print("Cancelled.")
