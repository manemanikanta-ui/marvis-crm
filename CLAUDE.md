# MARVIS CRM — FastAPI backend + single-file dashboard for the Talktiv AI SDR pipeline (scrape→enrich→email/WhatsApp→CRM→analytics).

## Stack & run
- Python 3 / FastAPI / SQLAlchemy · SQLite local, Postgres on Railway (`db.py` bridges both).
- Run (Electron-owned): quit Electron fully → taskkill stale python → relaunch (main.js owns uvicorn).
- Run (manual): `python -m uvicorn main:app --host 127.0.0.1 --port 8003` (NO --reload; 127.0.0.1 only — rationale in root CLAUDE.md).
- Frontend: refresh `frontend/index.html` (no build step).

## Map
- `main.py` — all API routes + startup (large file).
- `crm_core.py` — leads / pipeline / contacts.
- `scheduler.py` — APScheduler jobs + LOCAL approved-queue drainer (the one Gmail sender).
- `email_engine.py` — SMTP send + 3-email drip · `email_finder.py` — email discovery.
- `enrichment.py` — Claude enrichment; holds the ONLY model-ID strings.
- `whatsapp_engine.py` · `broadcast.py` — outbound channels.
- `hud_bus.py` — `/ws/hud` (single owner) · `agent_bus.py` — `hud_event()` emitter.
- `db.py` — cross-DB engine + RowProxy · `auth_middleware.py` — API-key gate (P1).

## Conventions
- DB: `conn = get_db(); …; conn.close()` — NEVER `with get_db()` (cross-connection commit/leak behavior).
- Schema changes run on BOTH local + Railway (30s sync shares data). New tables get a tenant/owner column.
- `dict(cursor.fetchall())` over 2-col aggregates is safe now (RowProxy mirrors sqlite3.Row).

## Don'ts
- PROTECTED — STOP + report if a task needs them: `gmail_service.py`, `telegram_notify.py`, `railway_sync.py`.
- `auth_middleware.py` exemption lists are SECURITY decisions — never modify without explicit confirm.
- ANY new send path ships its `IS_RAILWAY` gate in the SAME commit (invariant #8 — a toggle is not a guard).

## Pointers
- `feedback.md` (HUD 3 contract + invariant #8) · `MARVIS_Vault\projects\MARVIS-CRM.md` · `…\projects\MARVIS_MASTER_PLAN.md` · `…\brain\briefs\SECURITY_BUILD_BRIEF.md`.
