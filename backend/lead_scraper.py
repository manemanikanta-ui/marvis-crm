"""
Inline Google Places lead scraper — Railway-safe, no MARVIS_LEAD_MACHINE dependency.

Shared engine for the hourly scraping session (scheduler.py). Mirrors the logic the
/api/scrape-leads endpoint uses, extracted so it can be called directly in-process
(not over HTTP). Dedupes by place_id (and name) against existing leads and inserts
new businesses as 'pending_review'. Never raises to the caller.
"""
from __future__ import annotations

import os
import re
import logging

import requests

from db import get_db
from hud_bus import emit_hud_event

logger = logging.getLogger("marvis.scraper")

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_BLACKLIST = {"example.com", "sentry.io", "wixpress.com", "google.com", "facebook.com"}
_REGION = "in"


def _google_key() -> str:
    return os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY") or ""


def _score(reviews=0, website="", phone="", email="") -> int:
    """Outbound-fit score (mirrors main.compute_lead_score) — kept local to avoid
    importing main.py (circular)."""
    score = 0
    if email:
        score += 35
    if phone:
        score += 25
    if website:
        score += 15
    try:
        r = int(reviews or 0)
    except (TypeError, ValueError):
        r = 0
    if r >= 100:
        score += 25
    elif r >= 50:
        score += 20
    elif r >= 20:
        score += 15
    elif r >= 5:
        score += 8
    return min(score, 100)


def _find_email(website: str):
    if not website:
        return "", ""
    try:
        from bs4 import BeautifulSoup
        w = website if website.startswith("http") else "https://" + website
        rr = requests.get(w, headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
        soup = BeautifulSoup(rr.text, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.startswith("mailto:"):
                e = href.replace("mailto:", "").split("?")[0].strip().lower()
                if "@" in e and not any(b in e for b in _BLACKLIST):
                    return e, "mailto"
        for m in _EMAIL_RE.findall(rr.text):
            if "@" in m and "." in m.split("@")[1] and not any(b in m for b in _BLACKLIST):
                return m.lower(), "website"
    except Exception:
        pass
    return "", ""


def scrape_and_import_city(category: str, city: str, max_results: int = 20,
                           campaign_name: str = "", source: str = "hourly_scrape") -> dict:
    """Scrape `category` in `city` via Google Places, dedupe by place_id/name, and
    insert new businesses as pending_review. Returns
    {found, new_leads, skipped, place_ids}. Never raises."""
    gkey = _google_key()
    if not gkey:
        return {"found": 0, "new_leads": 0, "skipped": 0, "place_ids": [], "error": "no_google_key"}

    query = f"{category} in {city}".strip()
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": query, "key": gkey, "region": _REGION, "language": "en"},
            timeout=15,
        )
        places = r.json().get("results", [])
    except Exception as exc:
        logger.warning("Places textsearch failed for %s: %s", query, exc)
        return {"found": 0, "new_leads": 0, "skipped": 0, "place_ids": [], "error": str(exc)}

    found = new_leads = skipped = 0
    seen = []
    conn = get_db()
    try:
        for place in places[: max(1, int(max_results or 20))]:
            pid = place.get("place_id")
            if not pid or pid in seen:
                continue
            seen.append(pid)
            found += 1
            try:
                det = requests.get(
                    "https://maps.googleapis.com/maps/api/place/details/json",
                    params={
                        "place_id": pid,
                        "fields": "name,formatted_phone_number,website,rating,user_ratings_total,formatted_address,geometry",
                        "key": gkey,
                    },
                    timeout=10,
                ).json().get("result", {})
            except Exception:
                det = {}

            name = (det.get("name") or place.get("name") or "").strip()
            if not name:
                continue

            # Dedup: skip businesses already known by place_id OR name.
            existing = conn.execute(
                "SELECT id FROM leads WHERE place_id = ? OR name = ?", (pid, name)
            ).fetchone()
            if existing:
                skipped += 1
                continue

            website = det.get("website", "") or ""
            phone = det.get("formatted_phone_number", "") or ""
            reviews = det.get("user_ratings_total", 0) or 0
            email, email_source = _find_email(website)
            score = _score(reviews, website, phone, email)

            cursor = conn.execute(
                """
                INSERT INTO leads (name, business_type, phone, email, email_source, website,
                    address, city, rating, reviews, score, source, place_id, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending_review')
                """,
                (
                    name, category, phone, email, email_source, website,
                    det.get("formatted_address", ""), city,
                    float(det.get("rating", 0) or 0), int(reviews or 0), int(score),
                    source, pid,
                ),
            )
            new_leads += 1
            try:
                emit_hud_event("new_lead", {"name": name, "category": category, "lead_id": cursor.lastrowid})
            except Exception:
                pass
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("scrape import error for %s/%s: %s", city, category, exc)
    finally:
        conn.close()

    return {"found": found, "new_leads": new_leads, "skipped": skipped, "place_ids": seen}
