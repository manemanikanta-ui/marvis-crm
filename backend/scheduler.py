"""
MARVIS CRM Daily Scheduler

Runs the daily lead machine flow:
scrape -> enrich -> import -> email new leads -> process due follow-ups -> log summary
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv

from db import get_db as shared_get_db
from hud_bus import emit_hud_event
from crm_core import (
    SKIP_SCHEDULER_STATUSES,
    count_today_activity,
    ensure_crm_schema,
    is_office_hours,
    log_activity_event,
    load_safety_settings,
    random_delay_seconds,
    update_lead_status,
)

load_dotenv()

try:
    import schedule as schedule_lib  # type: ignore
except Exception:
    schedule_lib = None


BASE_DIR = Path(__file__).resolve().parent
CRM_DB = BASE_DIR / "data" / "crm.db"
LEAD_MACHINE_DIR = BASE_DIR.parent.parent / "MARVIS_LEAD_MACHINE"
LEADS_XLSX = LEAD_MACHINE_DIR / "leads.xlsx"
LEADS_ENRICHED_XLSX = LEAD_MACHINE_DIR / "leads_enriched.xlsx"
IST = ZoneInfo("Asia/Kolkata")

DEFAULT_JOBS = [
    {
        "location": "Hyderabad",
        "category": "Real Estate",
        "leads": 20,
        "time": "09:00",
        "enabled": True,
    }
]

DEFAULT_SETTINGS = {
    "scheduler_enabled": "true",
    "scheduler_run_time": "09:00",
    "scheduler_queries": json.dumps(["real estate agents in Hyderabad"]),
    "scheduler_jobs": json.dumps(DEFAULT_JOBS),
    "scheduler_enrich_limit": "20",
    "scheduler_job_last_run": "{}",
    "scheduler_last_run": "",
    "scheduler_next_run": "",
    "scheduler_last_status": "idle",
    "scheduler_last_result": "",
}

_scheduler_lock = threading.Lock()
_scheduler_thread: Optional[threading.Thread] = None
_scheduler_stop = threading.Event()
_job_running = threading.Event()
_logger = logging.getLogger("marvis.scheduler")


def get_db():
    return shared_get_db()


def _ensure_logging():
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(exist_ok=True)

    if any(getattr(h, "_marvis_scheduler_handler", False) for h in _logger.handlers):
        return

    handler = logging.FileHandler(log_dir / "scheduler.log", encoding="utf-8")
    handler._marvis_scheduler_handler = True  # type: ignore[attr-defined]
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    _logger.setLevel(logging.INFO)
    _logger.addHandler(handler)
    _logger.propagate = False


def ensure_schema():
    ensure_crm_schema()
    conn = get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduler_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trigger_type TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            summary_json TEXT,
            details_json TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    for key, value in DEFAULT_SETTINGS.items():
        conn.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )
    conn.commit()
    conn.close()
    _ensure_logging()


def _settings_dict() -> Dict[str, str]:
    ensure_schema()
    conn = get_db()
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    conn.close()
    return {row["key"]: row["value"] for row in rows}


def _set_setting(key: str, value: Any):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (key, "" if value is None else str(value)),
    )
    conn.commit()
    conn.close()


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_queries(raw: str) -> List[str]:
    if not raw:
        return [f"{job['category']} in {job['location']}" for job in DEFAULT_JOBS]
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except Exception:
        pass

    cleaned = []
    for chunk in raw.replace("\r", "\n").split("\n"):
        for piece in chunk.split(","):
            item = piece.strip()
            if item:
                cleaned.append(item)
    return cleaned or [f"{job['category']} in {job['location']}" for job in DEFAULT_JOBS]


def _normalize_job(job: Dict[str, Any]) -> Dict[str, Any]:
    location = str(job.get("location", "")).strip()
    category = str(job.get("category", "")).strip()
    time_value = str(job.get("time", "09:00")).strip() or "09:00"
    enabled = _parse_bool(job.get("enabled", True), True)
    try:
        leads = int(job.get("leads", 20) or 20)
    except Exception:
        leads = 20
    leads = max(1, min(leads, 200))

    if not location and category:
        location = "Hyderabad"
    if not category and location:
        category = "Real Estate"

    return {
        "location": location or "Hyderabad",
        "category": category or "Real Estate",
        "leads": leads,
        "time": time_value if len(time_value.split(":")) == 2 else "09:00",
        "enabled": enabled,
    }


def _job_key(job: Dict[str, Any]) -> str:
    normalized = _normalize_job(job)
    return "|".join(
        [
            normalized["location"].lower(),
            normalized["category"].lower(),
            normalized["time"],
            str(normalized["leads"]),
        ]
    )


def _parse_jobs(raw: str) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict):
                        jobs.append(_normalize_job(item))
        except Exception:
            pass

    if jobs:
        return jobs

    legacy_queries = _parse_queries(_settings_dict().get("scheduler_queries", ""))
    run_time = _settings_dict().get("scheduler_run_time", "09:00") or "09:00"
    enrich_limit = int(_settings_dict().get("scheduler_enrich_limit", "20") or 20)

    for query in legacy_queries[:1]:
        category = query.split(" in ", 1)[0].strip() if " in " in query else query.strip()
        location = query.split(" in ", 1)[1].strip() if " in " in query else "Hyderabad"
        jobs.append(
            _normalize_job(
                {
                    "location": location,
                    "category": category,
                    "leads": enrich_limit,
                    "time": run_time,
                    "enabled": True,
                }
            )
        )
    return jobs or [dict(DEFAULT_JOBS[0])]


def _job_query(job: Dict[str, Any]) -> str:
    normalized = _normalize_job(job)
    return f"{normalized['category']} in {normalized['location']}"


def _load_job_run_map() -> Dict[str, str]:
    settings = _settings_dict()
    raw = settings.get("scheduler_job_last_run", "{}") or "{}"
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items()}
    except Exception:
        pass
    return {}


def _save_job_run_map(run_map: Dict[str, str]):
    _set_setting("scheduler_job_last_run", json.dumps(run_map, ensure_ascii=False))


def _compute_next_job_run(
    job: Dict[str, Any],
    reference: Optional[datetime] = None,
    run_map: Optional[Dict[str, str]] = None,
) -> datetime:
    normalized = _normalize_job(job)
    now = (reference or _now_ist()).astimezone(IST)
    target_time = _parse_run_time(normalized["time"])
    candidate = now.replace(
        hour=target_time.hour,
        minute=target_time.minute,
        second=0,
        microsecond=0,
    )
    job_key = _job_key(normalized)
    today = now.date().isoformat()
    if run_map and run_map.get(job_key) == today:
        candidate += timedelta(days=1)
        return candidate
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def _parse_run_time(value: str) -> dt_time:
    try:
        hour, minute = [int(part) for part in value.split(":", 1)]
        return dt_time(hour=hour, minute=minute)
    except Exception:
        return dt_time(hour=9, minute=0)


def _now_ist() -> datetime:
    return datetime.now(IST)


def _compute_next_run(run_time: str, reference: Optional[datetime] = None) -> datetime:
    now = (reference or _now_ist()).astimezone(IST)
    target_time = _parse_run_time(run_time)
    candidate = now.replace(
        hour=target_time.hour,
        minute=target_time.minute,
        second=0,
        microsecond=0,
    )
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def _load_lead_machine_modules():
    if str(LEAD_MACHINE_DIR) not in sys.path:
        sys.path.insert(0, str(LEAD_MACHINE_DIR))

    from scraper import filter_and_rank, save_results, scrape_leads  # type: ignore
    from enrich import enrich_leads  # type: ignore

    return scrape_leads, filter_and_rank, save_results, enrich_leads


def _import_enriched_leads(file_path: Path, run_started_at: datetime, campaign_name: str = "") -> Dict[str, Any]:
    if not file_path.exists():
        return {"imported": 0, "skipped": 0, "new_leads": []}

    df = pd.read_excel(file_path, sheet_name="Enriched Leads")
    if df.empty:
        return {"imported": 0, "skipped": 0, "new_leads": []}

    conn = get_db()
    imported = 0
    skipped = 0
    new_leads: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        name = str(row.get("name", "")).strip()
        phone = str(row.get("phone", "")).strip()
        email = str(row.get("email", "")).strip()

        existing = conn.execute(
            "SELECT id FROM leads WHERE name = ? OR (phone = ? AND phone != '')",
            (name, phone),
        ).fetchone()
        if existing:
            skipped += 1
            continue

        cursor = conn.execute(
            """
            INSERT INTO leads (
                name, business_type, phone, email, email_source, website, address,
                rating, reviews, score, source, whatsapp_message, email_subject, email_body,
                status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'scheduler', ?, ?, ?, 'pending_review', ?, ?)
            """,
            (
                name,
                str(row.get("query", "")).replace("in Hyderabad", "").replace("in Secunderabad", "").strip(),
                phone,
                email,
                str(row.get("email_source", "")).strip(),
                str(row.get("website", "")).strip(),
                str(row.get("address", "")).strip(),
                float(row.get("rating", 0) or 0),
                int(row.get("reviews", 0) or 0),
                int(row.get("score", 0) or 0),
                str(row.get("whatsapp_message", "")).strip(),
                str(row.get("email_subject", "")).strip(),
                str(row.get("email_body", "")).strip(),
                run_started_at.isoformat(),
                run_started_at.isoformat(),
            ),
        )
        imported += 1
        log_activity_event(
            cursor.lastrowid,
            "scheduler_job",
            "system",
            f"Imported by scheduler for {campaign_name or 'daily job'}",
            status="logged",
            direction="system",
            campaign_name=campaign_name,
            metadata={"source": "scheduler", "campaign_name": campaign_name, "query": str(row.get("query", "")).strip()},
        )
        new_leads.append(
            {
                "id": cursor.lastrowid,
                "name": name,
                "phone": phone,
                "email": email,
                "email_subject": str(row.get("email_subject", "")).strip(),
                "email_body": str(row.get("email_body", "")).strip(),
                "whatsapp_message": str(row.get("whatsapp_message", "")).strip(),
            }
        )
        emit_hud_event("new_lead", {"name": name, "category": campaign_name or str(row.get("query", "")).strip(), "lead_id": cursor.lastrowid})

    conn.commit()
    conn.close()
    return {"imported": imported, "skipped": skipped, "new_leads": new_leads}


def _approved_outreach_queue(limit: int = 100) -> List[Dict[str, Any]]:
    conn = get_db()
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT *
            FROM leads
            WHERE status = 'approved'
              AND COALESCE(email, '') != ''
              AND (
                COALESCE(email_subject, '') != ''
                OR COALESCE(email_body, '') != ''
                OR COALESCE(whatsapp_message, '') != ''
              )
              AND NOT EXISTS (
                SELECT 1
                FROM activities a
                WHERE a.lead_id = leads.id
                  AND a.channel = 'email'
                  AND a.type = 'email'
                  AND a.status = 'sent'
              )
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    ]
    conn.close()
    return rows


def _send_initial_emails(new_leads: List[Dict[str, Any]]) -> Dict[str, Any]:
    from email_engine import schedule_sequence, send_email_now  # type: ignore

    settings = _settings_dict()
    auto_send = _parse_bool(settings.get("auto_send_enabled", "false"), False)
    auto_sequence = _parse_bool(settings.get("auto_sequence_enabled", "true"), True)
    safety = load_safety_settings()

    sent = 0
    failed = 0
    results = []

    if not auto_send:
        return {"sent": 0, "failed": 0, "results": [{"status": "skipped", "reason": "auto_send_disabled"}]}

    if safety["office_hours_only"] and not is_office_hours():
        return {"sent": 0, "failed": 0, "results": [{"status": "skipped", "reason": "outside_office_hours"}]}

    if count_today_activity("email", status="sent") >= int(safety["max_emails_per_day"]):
        return {"sent": 0, "failed": 0, "results": [{"status": "skipped", "reason": "email_limit_reached"}]}

    for lead in new_leads:
        lead_id = int(lead["id"])
        lead_status = str(lead.get("status") or "")
        if lead_status != "approved":
            results.append({"lead_id": lead_id, "status": "skipped", "reason": f"status_{lead_status or 'unknown'}"})
            continue

        email = lead.get("email", "")
        if not email:
            results.append({"lead_id": lead_id, "status": "skipped", "reason": "missing_email"})
            continue

        subject = lead.get("email_subject") or f"Quick question about {lead.get('name', '')}"
        body = lead.get("email_body") or "Hi,\n\nI wanted to reach out about how Talktiv AI can help your business.\n\n— Manikanta"
        delay_seconds = random_delay_seconds()
        if delay_seconds > 0:
            time.sleep(delay_seconds)
        result = send_email_now(
            email,
            subject,
            body,
            lead_id,
            "scheduler",
            campaign_name=lead.get("campaign_name", ""),
            metadata={"source": "scheduler"},
        )
        if result.get("success"):
            sent += 1
            if auto_sequence:
                schedule_sequence(lead_id, email)
            results.append({"lead_id": lead_id, "status": "sent"})
        else:
            failed += 1
            results.append({"lead_id": lead_id, "status": "failed", "error": result.get("error", "unknown")})
            if safety["pause_on_failures"]:
                break

    if not safety["pause_on_failures"] or failed == 0:
        for lead in _approved_outreach_queue():
            if count_today_activity("email", status="sent") >= int(safety["max_emails_per_day"]):
                results.append({"lead_id": lead.get("id"), "status": "skipped", "reason": "email_limit_reached"})
                break

            lead_id = int(lead["id"])
            email = lead.get("email", "")
            if not email:
                results.append({"lead_id": lead_id, "status": "skipped", "reason": "missing_email"})
                continue

            subject = lead.get("email_subject") or f"Quick question about {lead.get('name', '')}"
            body = lead.get("email_body") or "Hi,\n\nI wanted to reach out about how Talktiv AI can help your business.\n\n— Manikanta"
            delay_seconds = random_delay_seconds()
            if delay_seconds > 0:
                time.sleep(delay_seconds)
            result = send_email_now(
                email,
                subject,
                body,
                lead_id,
                "scheduler",
                campaign_name=lead.get("business_type", ""),
                metadata={"source": "scheduler", "approved_queue": True},
            )
            if result.get("success"):
                sent += 1
                if auto_sequence:
                    schedule_sequence(lead_id, email)
                results.append({"lead_id": lead_id, "status": "sent", "queue": "approved"})
            else:
                failed += 1
                results.append({"lead_id": lead_id, "status": "failed", "error": result.get("error", "unknown"), "queue": "approved"})
                if safety["pause_on_failures"]:
                    break

    return {"sent": sent, "failed": failed, "results": results}


def _process_due_followups() -> Dict[str, Any]:
    conn = get_db()
    now = _now_ist().isoformat()
    rows = conn.execute(
        """
        SELECT f.*, l.status AS lead_status, l.business_type, l.address, l.name
        FROM follow_ups f
        JOIN leads l ON l.id = f.lead_id
        WHERE f.status IN ('pending', 'ready') AND f.scheduled_at <= ?
        ORDER BY scheduled_at ASC
        """,
        (now,),
    ).fetchall()
    conn.close()

    if not rows:
        return {"processed": 0, "sent": 0, "failed": 0, "skipped": 0}

    from email_engine import send_email_now  # type: ignore
    from whatsapp_engine import send_whatsapp_text  # type: ignore
    safety = load_safety_settings()

    if safety["office_hours_only"] and not is_office_hours():
        return {"processed": 0, "sent": 0, "failed": 0, "skipped": len(rows), "reason": "outside_office_hours"}

    processed = sent = failed = skipped = 0
    for row in rows:
        processed += 1
        followup = dict(row)
        lead_id = int(followup["lead_id"])
        channel = (followup.get("channel") or "").lower()
        message = followup.get("message") or ""
        lead_status = str(followup.get("lead_status") or "")
        status = "sent"
        response = ""

        if lead_status in {"new", "pending_review", "paused", "replied", "interested", "booked", "declined", "converted"}:
            skipped += 1
            status = "skipped"
            response = f"lead_status_{lead_status or 'unknown'}"
            conn = get_db()
            conn.execute("UPDATE follow_ups SET status = ? WHERE id = ?", (status, followup["id"]))
            conn.commit()
            conn.close()
            continue

        if channel == "email" and count_today_activity("email", status="sent") >= int(safety["max_emails_per_day"]):
            skipped += 1
            status = "skipped"
            response = "email_limit_reached"
        if channel == "whatsapp" and count_today_activity("whatsapp", status="sent") >= int(safety["max_whatsapp_per_day"]):
            skipped += 1
            status = "skipped"
            response = "whatsapp_limit_reached"
        if status == "skipped":
            conn = get_db()
            conn.execute("UPDATE follow_ups SET status = ? WHERE id = ?", (status, followup["id"]))
            conn.commit()
            conn.close()
            continue

        try:
            delay_seconds = random_delay_seconds()
            if delay_seconds > 0:
                time.sleep(delay_seconds)
            if channel == "email" and message.startswith("EMAIL|"):
                _, to_email, subject, body = message.split("|", 3)
                result = send_email_now(
                    to_email,
                    subject,
                    body,
                    lead_id,
                    "followup",
                    campaign_name=followup.get("campaign_name", ""),
                    metadata={"source": "scheduler_followup", "followup_id": followup["id"]},
                )
                if result.get("success"):
                    sent += 1
                else:
                    failed += 1
                    status = "failed"
                    response = result.get("error", "email_failed")
            elif channel == "whatsapp" and message.startswith("WA|"):
                _, phone, body = message.split("|", 2)
                result = send_whatsapp_text(phone, body, lead_id)
                if result.get("success"):
                    sent += 1
                else:
                    failed += 1
                    status = "failed"
                    response = result.get("error", "wa_failed")
            else:
                skipped += 1
                status = "skipped"
                response = "unsupported_format"
        except Exception as exc:
            failed += 1
            status = "failed"
            response = str(exc)
            if safety["pause_on_failures"]:
                pass

        conn = get_db()
        conn.execute(
            "UPDATE follow_ups SET status = ? WHERE id = ?",
            (status, followup["id"]),
        )
        conn.commit()
        conn.close()

        if response:
            _logger.info("Follow-up %s for lead %s: %s", followup["id"], lead_id, response)

    return {"processed": processed, "sent": sent, "failed": failed, "skipped": skipped}


def _write_run_log(payload: Dict[str, Any]):
    _logger.info(json.dumps(payload, ensure_ascii=False))


def _record_run(started_at: datetime, finished_at: datetime, status: str, summary: Dict[str, Any], details: Dict[str, Any], trigger_type: str):
    conn = get_db()
    conn.execute(
        """
        INSERT INTO scheduler_runs (
            trigger_type, started_at, finished_at, status, summary_json, details_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trigger_type,
            started_at.isoformat(),
            finished_at.isoformat(),
            status,
            json.dumps(summary, ensure_ascii=False),
            json.dumps(details, ensure_ascii=False),
            finished_at.isoformat(),
        ),
    )
    conn.commit()
    conn.close()


def _run_single_job(job: Dict[str, Any], trigger_type: str, started_at: datetime) -> Dict[str, Any]:
    scrape_leads, filter_and_rank, save_results, enrich_leads = _load_lead_machine_modules()
    normalized = _normalize_job(job)
    query = _job_query(normalized)
    query_label = f"{normalized['category']} in {normalized['location']}"

    leads = scrape_leads([query], max_per_query=normalized["leads"])
    df = filter_and_rank(leads)
    save_results(df, str(LEADS_XLSX))
    enrich_leads(input_file=str(LEADS_XLSX), output_file=str(LEADS_ENRICHED_XLSX), limit=normalized["leads"])

    import_result = _import_enriched_leads(LEADS_ENRICHED_XLSX, started_at, campaign_name=query_label)
    email_result = _send_initial_emails(import_result["new_leads"])

    job_summary = {
        "location": normalized["location"],
        "category": normalized["category"],
        "time": normalized["time"],
        "leads_target": normalized["leads"],
        "scraped": len(leads),
        "imported": import_result["imported"],
        "skipped": import_result["skipped"],
        "new_leads": len(import_result["new_leads"]),
        "emails_sent": email_result["sent"],
        "emails_failed": email_result["failed"],
        "query": query_label,
        "campaign_name": query_label,
    }
    job_details = {
        "query": query_label,
        "job": normalized,
        "email_result": email_result,
        "import_result": {"imported": import_result["imported"], "skipped": import_result["skipped"]},
    }
    return {"summary": job_summary, "details": job_details, "import_result": import_result}


def _execute_jobs(jobs: List[Dict[str, Any]], trigger_type: str, include_followups: bool) -> Dict[str, Any]:
    if not jobs:
        return {
            "success": True,
            "status": "idle",
            "summary": {},
            "details": {"jobs": [], "followup_result": {"processed": 0, "sent": 0, "failed": 0, "skipped": 0}},
        }

    started_at = _now_ist()
    overall = {
        "jobs_total": len(jobs),
        "jobs_completed": 0,
        "scraped": 0,
        "imported": 0,
        "skipped": 0,
        "new_leads": 0,
        "emails_sent": 0,
        "emails_failed": 0,
        "followups_processed": 0,
        "followups_sent": 0,
        "followups_failed": 0,
        "followups_skipped": 0,
    }
    details: Dict[str, Any] = {"trigger": trigger_type, "jobs": []}
    status = "success"
    run_map = _load_job_run_map()

    try:
        for job in jobs:
            normalized = _normalize_job(job)
            try:
                result = _run_single_job(normalized, trigger_type, started_at)
                job_summary = result["summary"]
                details["jobs"].append(result["details"])
                overall["jobs_completed"] += 1
                overall["scraped"] += job_summary["scraped"]
                overall["imported"] += job_summary["imported"]
                overall["skipped"] += job_summary["skipped"]
                overall["new_leads"] += job_summary["new_leads"]
                overall["emails_sent"] += job_summary["emails_sent"]
                overall["emails_failed"] += job_summary["emails_failed"]
                run_map[_job_key(normalized)] = started_at.astimezone(IST).date().isoformat()
            except Exception as job_exc:
                status = "partial_failure"
                job_error = {
                    "job": normalized,
                    "error": str(job_exc),
                    "traceback": traceback.format_exc(),
                }
                details.setdefault("job_errors", []).append(job_error)
                overall.setdefault("job_failures", 0)
                overall["job_failures"] += 1
                _logger.error("Scheduler job failed: %s", job_exc)

        followup_result = _process_due_followups() if include_followups else {"processed": 0, "sent": 0, "failed": 0, "skipped": 0}
        overall["followups_processed"] = followup_result["processed"]
        overall["followups_sent"] = followup_result["sent"]
        overall["followups_failed"] = followup_result["failed"]
        overall["followups_skipped"] = followup_result["skipped"]
        details["followup_result"] = followup_result
        _save_job_run_map(run_map)
        if details.get("job_errors"):
            status = "partial_failure"
    except Exception as exc:
        status = "failed"
        details["error"] = str(exc)
        details["traceback"] = traceback.format_exc()
        overall["error"] = str(exc)

    finished_at = _now_ist()
    next_runs = [_compute_next_job_run(job, finished_at, run_map) for job in jobs if _parse_bool(job.get("enabled", True), True)]
    next_run = min(next_runs).isoformat() if next_runs else _compute_next_run(_settings_dict().get("scheduler_run_time", "09:00"), finished_at).isoformat()

    _set_setting("scheduler_last_run", finished_at.isoformat())
    _set_setting("scheduler_last_status", status)
    _set_setting("scheduler_last_result", json.dumps({"summary": overall, "details": details}, ensure_ascii=False))
    _set_setting("scheduler_next_run", next_run)
    _record_run(started_at, finished_at, status, overall, details, trigger_type)
    _write_run_log({"status": status, "summary": overall, "details": details})
    if status == "success":
        emit_hud_event("scheduler_complete", {"jobs": len(jobs), "leads_found": overall.get("new_leads", 0)})
    elif status in {"failed", "partial_failure"}:
        emit_hud_event("scheduler_failed", {"error": details.get("error") or "Scheduler run failed"})

    return {"success": status == "success", "status": status, "summary": overall, "details": details}


def run_daily_job(trigger_type: str = "scheduled", jobs: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    ensure_schema()
    if _job_running.is_set():
        return {"success": False, "status": "busy", "message": "Scheduler job already running"}

    with _scheduler_lock:
        if _job_running.is_set():
            return {"success": False, "status": "busy", "message": "Scheduler job already running"}
        _job_running.set()

    try:
        settings = _settings_dict()
        enabled = _parse_bool(settings.get("scheduler_enabled", "true"), True)
        if trigger_type not in {"manual", "test"} and not enabled:
            return {"success": False, "status": "paused", "message": "Scheduler is disabled"}

        job_list = jobs if jobs is not None else _parse_jobs(settings.get("scheduler_jobs", ""))
        if not job_list:
            legacy_queries = _parse_queries(settings.get("scheduler_queries", ""))
            legacy_time = settings.get("scheduler_run_time", "09:00") or "09:00"
            legacy_limit = int(settings.get("scheduler_enrich_limit", "20") or 20)
            job_list = [
                {
                    "location": (q.split(" in ", 1)[1].strip() if " in " in q else "Hyderabad"),
                    "category": (q.split(" in ", 1)[0].strip() if " in " in q else q.strip()),
                    "leads": legacy_limit,
                    "time": legacy_time,
                    "enabled": True,
                }
                for q in legacy_queries
            ]

        return _execute_jobs([_normalize_job(job) for job in job_list], trigger_type, include_followups=True)
    finally:
        _job_running.clear()


def _scheduler_loop():
    ensure_schema()
    while not _scheduler_stop.is_set():
        try:
            settings = _settings_dict()
            if not _parse_bool(settings.get("scheduler_enabled", "true"), True):
                time.sleep(30)
                continue

            jobs = _parse_jobs(settings.get("scheduler_jobs", ""))
            if not jobs:
                time.sleep(30)
                continue

            now = _now_ist()
            run_map = _load_job_run_map()
            due_jobs = []
            next_runs = []

            for job in jobs:
                normalized = _normalize_job(job)
                if not normalized["enabled"]:
                    continue
                next_runs.append(_compute_next_job_run(normalized, now, run_map))
                today_candidate = now.replace(
                    hour=_parse_run_time(normalized["time"]).hour,
                    minute=_parse_run_time(normalized["time"]).minute,
                    second=0,
                    microsecond=0,
                )
                if now >= today_candidate and run_map.get(_job_key(normalized)) != now.date().isoformat():
                    due_jobs.append(normalized)

            if next_runs:
                _set_setting("scheduler_next_run", min(next_runs).isoformat())

            if due_jobs and not _job_running.is_set():
                run_daily_job("scheduled", due_jobs)

            time.sleep(60)
        except Exception as exc:
            _logger.error("Scheduler loop error: %s", exc)
            time.sleep(30)


def start_scheduler_service():
    global _scheduler_thread
    ensure_schema()
    with _scheduler_lock:
        if _scheduler_thread and _scheduler_thread.is_alive():
            return {"running": True}
        _scheduler_stop.clear()
        _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True, name="marvis-scheduler")
        _scheduler_thread.start()
        return {"running": True}


def get_scheduler_status() -> Dict[str, Any]:
    settings = _settings_dict()
    conn = get_db()
    runs = [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM scheduler_runs ORDER BY id DESC LIMIT 10"
        ).fetchall()
    ]
    conn.close()

    last_run = settings.get("scheduler_last_run", "")
    next_run = settings.get("scheduler_next_run", "")
    enabled = _parse_bool(settings.get("scheduler_enabled", "true"), True)
    auto_send = _parse_bool(settings.get("auto_send_enabled", "false"), False)
    jobs = _parse_jobs(settings.get("scheduler_jobs", ""))
    legacy_queries = _parse_queries(settings.get("scheduler_queries", ""))
    run_time = settings.get("scheduler_run_time", "09:00")
    enrich_limit = int(settings.get("scheduler_enrich_limit", "20") or 20)
    safety = load_safety_settings()

    run_map = _load_job_run_map()
    next_runs = [_compute_next_job_run(job, None, run_map) for job in jobs if _parse_bool(job.get("enabled", True), True)]
    computed_next_run = min(next_runs).isoformat() if next_runs else _compute_next_run(run_time).isoformat()

    return {
        "enabled": enabled,
        "auto_send_enabled": auto_send,
        "run_time": run_time,
        "enrich_limit": enrich_limit,
        "queries": legacy_queries,
        "jobs": jobs,
        "next_run": next_run or computed_next_run,
        "last_run": last_run,
        "last_status": settings.get("scheduler_last_status", "idle"),
        "last_result": settings.get("scheduler_last_result", ""),
        "recent_runs": runs,
        "running": _job_running.is_set(),
        "safety": safety,
    }


def update_scheduler_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    ensure_schema()

    if "enabled" in payload:
        _set_setting("scheduler_enabled", "true" if _parse_bool(payload.get("enabled"), True) else "false")
    if "auto_send_enabled" in payload:
        value = "true" if _parse_bool(payload.get("auto_send_enabled"), False) else "false"
        _set_setting("auto_send_enabled", value)
    if "run_time" in payload and str(payload.get("run_time", "")).strip():
        _set_setting("scheduler_run_time", str(payload["run_time"]).strip())
    if "enrich_limit" in payload and str(payload.get("enrich_limit", "")).strip():
        _set_setting("scheduler_enrich_limit", int(payload["enrich_limit"]))
    if "jobs" in payload:
        raw_jobs = payload.get("jobs", [])
        normalized_jobs = []
        if isinstance(raw_jobs, list):
            for job in raw_jobs:
                if isinstance(job, dict):
                    normalized_jobs.append(_normalize_job(job))
        _set_setting("scheduler_jobs", json.dumps(normalized_jobs, ensure_ascii=False))
        _set_setting("scheduler_queries", json.dumps([_job_query(job) for job in normalized_jobs], ensure_ascii=False))
        if normalized_jobs:
            _set_setting("scheduler_run_time", normalized_jobs[0]["time"])
            _set_setting("scheduler_enrich_limit", normalized_jobs[0]["leads"])
    elif "queries" in payload:
        queries = payload.get("queries", [])
        if isinstance(queries, str):
            normalized = _parse_queries(queries)
        elif isinstance(queries, list):
            normalized = [str(item).strip() for item in queries if str(item).strip()]
        else:
            normalized = [f"{job['category']} in {job['location']}" for job in DEFAULT_JOBS]
        _set_setting("scheduler_queries", json.dumps(normalized, ensure_ascii=False))

    current = get_scheduler_status()
    _set_setting("scheduler_next_run", current["next_run"])
    return get_scheduler_status()


def run_scheduler_now() -> Dict[str, Any]:
    if _job_running.is_set():
        return {"success": False, "status": "busy", "message": "Scheduler job already running"}

    result_holder: Dict[str, Any] = {}

    def _runner():
        result_holder["result"] = run_daily_job("manual")

    thread = threading.Thread(target=_runner, daemon=True, name="marvis-scheduler-manual")
    thread.start()
    thread.join()
    return result_holder.get("result", {"success": False, "status": "failed", "message": "No result returned"})


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="MARVIS CRM scheduler")
    parser.add_argument("--test", action="store_true", help="Run the scheduler job once and print the result")
    args = parser.parse_args()

    if args.test:
        print(json.dumps(run_daily_job("test"), indent=2, ensure_ascii=False))
    else:
        start_scheduler_service()
        print("MARVIS scheduler running. Next run:", get_scheduler_status()["next_run"])
        while True:
            time.sleep(5)
