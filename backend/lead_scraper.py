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
from math import cos, radians

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


# ─────────────────────────────────────────────────────────────────────────────
# GRID-BASED AREA COVERAGE (Nearby Search + lat/lng grid overlay) — full city
# coverage beyond Text Search's ~20/60-result cap.
# ─────────────────────────────────────────────────────────────────────────────

CITY_CENTERS = {
    "hyderabad": (17.3850, 78.4867),
    "mumbai": (19.0760, 72.8777),
    "delhi": (28.6139, 77.2090),
    "visakhapatnam": (17.6868, 83.2185),
    "bangalore": (12.9716, 77.5946),
    "chennai": (13.0827, 80.2707),
    "pune": (18.5204, 73.8567),
}


def _geocode_city(city: str):
    """Resolve a city name → (lat, lng) via Google Geocoding. None on failure."""
    gkey = _google_key()
    if not gkey:
        return None
    try:
        data = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": city, "key": gkey, "language": "en"},
            timeout=10,
        ).json()
        if data.get("status") == "REQUEST_DENIED":
            logger.error("Geocoding REQUEST_DENIED — enable 'Geocoding API' on this key: %s",
                         data.get("error_message", ""))
            return None
        results = data.get("results", [])
        if results:
            loc = results[0]["geometry"]["location"]
            return (loc["lat"], loc["lng"])
    except Exception as exc:
        logger.warning("Geocode failed for %s: %s", city, exc)
    return None


def generate_grid(city: str, grid_size: int = 4, spacing_km: float = 4.0):
    """lat/lng grid points covering a city. Empty list if the centre can't be resolved."""
    center = CITY_CENTERS.get(city.strip().lower()) or _geocode_city(city)
    if not center:
        return []
    lat, lng = center
    lat_step = spacing_km / 111.0
    lng_step = spacing_km / (111.0 * max(0.01, abs(cos(radians(lat)))))
    offset = (grid_size - 1) / 2.0
    points = []
    for i in range(grid_size):
        for j in range(grid_size):
            points.append((round(lat + (i - offset) * lat_step, 6),
                           round(lng + (j - offset) * lng_step, 6)))
    return points


def _nearby_search(lat: float, lng: float, keyword: str, radius: int = 3000):
    """Google Places Nearby Search for one grid point. Logs REQUEST_DENIED clearly."""
    gkey = _google_key()
    if not gkey:
        return []
    try:
        data = requests.get(
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
            params={"location": f"{lat},{lng}", "radius": int(radius), "keyword": keyword,
                    "type": "establishment", "key": gkey, "language": "en"},
            timeout=15,
        ).json()
        status = data.get("status", "")
        if status == "REQUEST_DENIED":
            logger.error("Nearby Search REQUEST_DENIED — enable 'Places API' on this key: %s",
                         data.get("error_message", ""))
            return []
        if status not in ("OK", "ZERO_RESULTS"):
            logger.warning("Nearby Search status %s: %s", status, data.get("error_message", ""))
        return data.get("results", [])
    except Exception as exc:
        logger.warning("Nearby Search failed at %s,%s: %s", lat, lng, exc)
        return []


def _place_details(pid: str, gkey: str) -> dict:
    try:
        return requests.get(
            "https://maps.googleapis.com/maps/api/place/details/json",
            params={"place_id": pid,
                    "fields": "name,formatted_phone_number,website,rating,user_ratings_total,formatted_address",
                    "key": gkey},
            timeout=10,
        ).json().get("result", {})
    except Exception:
        return {}


def scrape_grid_cell(category: str, city: str, lat: float, lng: float,
                     radius: int = 3000, max_results: int = 20, source: str = "grid_scrape") -> dict:
    """Scrape ONE grid cell via Nearby Search, dedupe by place_id/name, insert new
    businesses as pending_review. Returns {found, new_leads, skipped}. Never raises."""
    gkey = _google_key()
    if not gkey:
        return {"found": 0, "new_leads": 0, "skipped": 0, "error": "no_google_key"}
    places = _nearby_search(lat, lng, category, radius)
    if not places:
        return {"found": 0, "new_leads": 0, "skipped": 0}

    found = new_leads = skipped = 0
    conn = get_db()
    try:
        for place in places:
            if new_leads >= max_results:
                break
            pid = place.get("place_id")
            if not pid:
                continue
            found += 1
            name = (place.get("name") or "").strip()
            existing = conn.execute(
                "SELECT id FROM leads WHERE place_id = ? OR name = ?", (pid, name)
            ).fetchone()
            if existing:
                skipped += 1
                continue
            det = _place_details(pid, gkey)
            name = (det.get("name") or name).strip()
            if not name:
                continue
            website = det.get("website", "") or ""
            phone = det.get("formatted_phone_number", "") or ""
            reviews = det.get("user_ratings_total", place.get("user_ratings_total", 0)) or 0
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
                    float(det.get("rating", place.get("rating", 0)) or 0), int(reviews or 0), int(score),
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
        logger.warning("grid cell import error for %s (%s,%s): %s", city, lat, lng, exc)
    finally:
        conn.close()
    return {"found": found, "new_leads": new_leads, "skipped": skipped}
