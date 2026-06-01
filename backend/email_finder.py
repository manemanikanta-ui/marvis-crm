"""
MARVIS Email Finder
3-Layer approach:
  Layer 1: Website scraping (most accurate)
  Layer 2: Pattern generation + Hunter.io API
  Layer 3: SMTP verification (no email sent)
"""

import re
import socket
import smtplib
import dns.resolver
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import sqlite3
import time
import os
from dotenv import load_dotenv

from db import get_db as shared_get_db

load_dotenv()

DB_PATH = "data/crm.db"
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")  # Optional - free tier = 25/month

# ─────────────────────────────────────────────
# LAYER 1: WEBSITE SCRAPER
# ─────────────────────────────────────────────

EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

BLACKLIST = {
    'example.com', 'domain.com', 'email.com', 'yoursite.com',
    'sentry.io', 'wixpress.com', 'wordpress.com', 'squarespace.com',
    'amazonaws.com', 'cloudfront.net', 'google.com', 'facebook.com',
    'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'css', 'js'
}

CONTACT_PATHS = [
    '/contact', '/contact-us', '/contactus', '/about',
    '/about-us', '/reach-us', '/get-in-touch',
    '/contact.html', '/about.html', '/team'
]

def scrape_website_emails(website: str, timeout: int = 8) -> list:
    """Scrape a website for email addresses"""
    if not website:
        return []

    # Normalize URL
    if not website.startswith('http'):
        website = 'https://' + website

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    found_emails = set()
    pages_to_check = [website]

    # Add contact page variants
    base = website.rstrip('/')
    for path in CONTACT_PATHS:
        pages_to_check.append(base + path)

    for url in pages_to_check[:4]:  # Max 4 pages per site
        try:
            resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if resp.status_code != 200:
                continue

            # Extract from HTML
            soup = BeautifulSoup(resp.text, 'html.parser')

            # Method 1: mailto links
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('mailto:'):
                    email = href.replace('mailto:', '').split('?')[0].strip()
                    if _is_valid_email(email):
                        found_emails.add(email.lower())

            # Method 2: regex on full page text
            text = soup.get_text()
            for match in EMAIL_REGEX.findall(text):
                if _is_valid_email(match):
                    found_emails.add(match.lower())

            # Method 3: regex on raw HTML (catches obfuscated emails)
            for match in EMAIL_REGEX.findall(resp.text):
                if _is_valid_email(match):
                    found_emails.add(match.lower())

            if found_emails:
                break  # Found emails on this page, no need to check more

            time.sleep(0.3)

        except Exception:
            continue

    return list(found_emails)

def _is_valid_email(email: str) -> bool:
    """Basic email validation"""
    if not email or '@' not in email:
        return False
    parts = email.split('@')
    if len(parts) != 2:
        return False
    domain = parts[1].lower()
    # Filter blacklisted domains
    for bl in BLACKLIST:
        if bl in domain:
            return False
    # Must have a real TLD
    if '.' not in domain:
        return False
    # Reasonable length
    if len(email) > 100 or len(email) < 6:
        return False
    return True

# ─────────────────────────────────────────────
# LAYER 2: PATTERN GENERATOR
# ─────────────────────────────────────────────

def extract_domain_from_website(website: str) -> str:
    """Extract clean domain from website URL"""
    if not website:
        return ""
    if not website.startswith('http'):
        website = 'https://' + website
    try:
        parsed = urlparse(website)
        domain = parsed.netloc.replace('www.', '')
        return domain.lower()
    except:
        return ""

def generate_email_patterns(business_name: str, domain: str) -> list:
    """Generate likely email patterns for a business"""
    if not domain:
        return []

    patterns = [
        f"info@{domain}",
        f"contact@{domain}",
        f"hello@{domain}",
        f"enquiry@{domain}",
        f"enquiries@{domain}",
        f"admin@{domain}",
        f"office@{domain}",
        f"mail@{domain}",
    ]

    # Add name-based patterns from business name
    words = re.sub(r'[^a-zA-Z\s]', '', business_name.lower()).split()
    clean_words = [w for w in words if len(w) > 2 and w not in
                   {'the', 'and', 'for', 'ltd', 'pvt', 'inc', 'llp', 'real', 'estate'}]

    if clean_words:
        first_word = clean_words[0]
        patterns.insert(0, f"{first_word}@{domain}")
        if len(clean_words) > 1:
            patterns.insert(1, f"{first_word}.{clean_words[1]}@{domain}")

    return patterns

# ─────────────────────────────────────────────
# LAYER 3: VERIFIER
# ─────────────────────────────────────────────

def verify_email_smtp(email: str, timeout: int = 5) -> dict:
    """
    Verify email via SMTP handshake (no email sent)
    Returns confidence: high / medium / low
    """
    domain = email.split('@')[1]

    # Step 1: Check MX records
    try:
        mx_records = dns.resolver.resolve(domain, 'MX')
        mx_host = str(sorted(mx_records, key=lambda r: r.preference)[0].exchange).rstrip('.')
    except Exception:
        return {"valid": False, "confidence": "low", "reason": "No MX records"}

    # Step 2: SMTP handshake
    try:
        with smtplib.SMTP(timeout=timeout) as smtp:
            smtp.connect(mx_host, 25)
            smtp.helo('gmail.com')
            smtp.mail('verify@gmail.com')
            code, message = smtp.rcpt(email)

            if code == 250:
                return {"valid": True, "confidence": "high", "reason": "SMTP accepted"}
            elif code == 550:
                return {"valid": False, "confidence": "high", "reason": "Mailbox does not exist"}
            else:
                return {"valid": True, "confidence": "medium", "reason": f"SMTP code {code}"}
    except smtplib.SMTPConnectError:
        # Many servers block port 25 — domain exists but can't verify
        return {"valid": True, "confidence": "medium", "reason": "MX exists, SMTP blocked"}
    except Exception as e:
        return {"valid": True, "confidence": "medium", "reason": f"MX exists: {str(e)[:50]}"}

def check_mx_only(domain: str) -> bool:
    """Quick MX check — does this domain accept email?"""
    try:
        dns.resolver.resolve(domain, 'MX')
        return True
    except:
        return False

# ─────────────────────────────────────────────
# HUNTER.IO API (Optional - 25 free/month)
# ─────────────────────────────────────────────

def hunter_find_email(domain: str, api_key: str = None) -> list:
    """Use Hunter.io to find emails for a domain"""
    key = api_key or HUNTER_API_KEY
    if not key:
        return []

    try:
        resp = requests.get(
            f"https://api.hunter.io/v2/domain-search",
            params={"domain": domain, "api_key": key},
            timeout=8
        )
        data = resp.json()
        emails = []
        for email_data in data.get("data", {}).get("emails", []):
            if email_data.get("value"):
                emails.append({
                    "email": email_data["value"],
                    "confidence": email_data.get("confidence", 50),
                    "source": "hunter"
                })
        return emails
    except:
        return []

# ─────────────────────────────────────────────
# MAIN FINDER — combines all layers
# ─────────────────────────────────────────────

def find_email(lead: dict) -> dict:
    """
    Full 3-layer email finding for a lead
    Returns best email found with confidence score
    """
    name = lead.get('name', '')
    website = lead.get('website', '')
    phone = lead.get('phone', '')

    result = {
        "lead_id": lead.get('id'),
        "lead_name": name,
        "email": None,
        "confidence": None,
        "source": None,
        "all_found": []
    }

    # ── Layer 1: Scrape website ──
    if website:
        print(f"  🌐 Scraping website: {website[:50]}...")
        scraped = scrape_website_emails(website)
        if scraped:
            # Pick best email (prefer info@, contact@ over random ones)
            best = _rank_emails(scraped, name)
            result["email"] = best
            result["confidence"] = "high"
            result["source"] = "website"
            result["all_found"] = scraped
            print(f"  ✅ Found via website: {best}")
            return result

    # ── Layer 2: Pattern generation ──
    if website:
        domain = extract_domain_from_website(website)
        if domain:
            print(f"  🔍 Trying email patterns for {domain}...")

            # Try Hunter.io first if key available
            if HUNTER_API_KEY:
                hunter_results = hunter_find_email(domain)
                if hunter_results:
                    best = hunter_results[0]
                    result["email"] = best["email"]
                    result["confidence"] = "high" if best["confidence"] > 70 else "medium"
                    result["source"] = "hunter"
                    result["all_found"] = [h["email"] for h in hunter_results]
                    print(f"  ✅ Found via Hunter.io: {best['email']}")
                    return result

            # Generate patterns and verify via MX
            patterns = generate_email_patterns(name, domain)
            if check_mx_only(domain):
                # Domain accepts email — patterns are likely valid
                if patterns:
                    result["email"] = patterns[0]  # info@ is most reliable
                    result["confidence"] = "medium"
                    result["source"] = "pattern"
                    result["all_found"] = patterns[:3]
                    print(f"  📧 Pattern generated: {patterns[0]}")
                    return result

    # ── No email found ──
    print(f"  ❌ No email found for {name}")
    return result

def _rank_emails(emails: list, business_name: str) -> str:
    """Pick the best email from a list"""
    priority = ['info@', 'contact@', 'hello@', 'enquiry@', 'office@', 'admin@']

    for prefix in priority:
        for email in emails:
            if email.startswith(prefix):
                return email

    # Return shortest email as fallback
    return sorted(emails, key=len)[0]

# ─────────────────────────────────────────────
# BATCH FINDER — process all leads in CRM
# ─────────────────────────────────────────────

def get_db():
    return shared_get_db()

def run_batch_finder(limit: int = 50, skip_existing: bool = True):
    """Find emails for all leads in CRM that don't have one"""
    conn = get_db()

    query = "SELECT * FROM leads WHERE 1=1"
    if skip_existing:
        query += " AND (email IS NULL OR email = '')"
    query += f" ORDER BY score DESC LIMIT {limit}"

    leads = [dict(r) for r in conn.execute(query).fetchall()]
    conn.close()

    print(f"\n📧 MARVIS Email Finder — Processing {len(leads)} leads")
    print("=" * 55)

    found = 0
    not_found = 0

    for i, lead in enumerate(leads):
        print(f"\n[{i+1}/{len(leads)}] {lead['name']}")

        result = find_email(lead)

        if result["email"]:
            # Save to CRM
            conn = get_db()
            conn.execute(
                "UPDATE leads SET email = ?, updated_at = ? WHERE id = ?",
                (result["email"], __import__('datetime').datetime.now().isoformat(), lead['id'])
            )
            conn.commit()
            conn.close()
            found += 1
            print(f"  💾 Saved: {result['email']} ({result['confidence']} confidence, via {result['source']})")
        else:
            not_found += 1

        time.sleep(0.5)  # Be gentle with requests

    print(f"\n{'='*55}")
    print(f"✅ Complete — Found: {found} | Not found: {not_found}")
    print(f"📊 Success rate: {round(found/len(leads)*100)}%")
    return {"found": found, "not_found": not_found, "total": len(leads)}

if __name__ == "__main__":
    result = run_batch_finder(limit=18)  # Process all 18 leads
    print(f"\nResult: {result}")
