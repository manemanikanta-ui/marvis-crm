# MARVIS CRM — Claude Code Context
## SESSION START CHECKLIST — RUN BEFORE ANYTHING ELSE

1. Verify Zap token-saving proxy is active:
   cat ~/.claude/settings.json | grep -A3 "PreToolUse"
   If not active: run `zap init -g` then verify again.

2. Confirm correct working directory:
   pwd
   (Must match this project — if wrong, cd to correct path before any command)

3. Check active Claude model:
   Never use claude-sonnet-4-20250514 (retired 2026-06-15)
   Use claude-sonnet-4-6 for standard tasks
   Use claude-haiku-4-5-20251001 for lightweight tasks

4. Read feedback.md before starting:
   cat C:\Users\HP\PycharmProjects\PythonProject\feedback.md
## What This Project Is
MARVIS CRM is the core backend + dashboard of the MARVIS AI SDR operating system
built by Manikanta Mane for Talktiv AI (Hyderabad-based AI automation startup).
It automates the full outbound sales pipeline: lead scraping → enrichment → email/WhatsApp
outreach → CRM tracking → analytics. Goal: Manikanta only touches qualified leads ready to close.

## Project Location
C:\Users\HP\PycharmProjects\PythonProject\MARVIS_CRM

## Owner / Developer
Manikanta Mane — Solo founder, Talktiv AI, Hyderabad, India

---

## File Structure

### Backend (FastAPI — Railway hosted)
```
backend/
├── main.py              # FastAPI entry point + all API routes (58KB — LARGE FILE)
├── crm_core.py          # Core CRM logic: leads, pipeline, contacts (22KB)
├── scheduler.py         # APScheduler jobs: drip emails, follow-ups, daily tasks (37KB)
├── email_engine.py      # Gmail SMTP email automation, 3-email drip sequences (12KB)
├── email_finder.py      # Email discovery/finder logic (14KB)
├── enrichment.py        # Claude AI lead enrichment (8KB)
├── whatsapp_engine.py   # WhatsApp automation (10KB)
├── broadcast.py         # Broadcast messaging (7KB)
├── hud_bus.py           # WebSocket/SSE event bus for MARVIS HUD (2KB)
├── db.py                # SQLAlchemy DB connection + models (8KB)
├── Procfile             # Railway process: web: uvicorn main:app
├── railway.toml         # Railway deployment config
├── RAILWAY_ENV.md       # Railway environment variable reference
├── requirements.txt     # Python dependencies
├── .env                 # Local env vars (never commit)
├── data/                # Local SQLite DB + data files
└── logs/                # Application logs
```

### Frontend (Single-file dashboard)
```
frontend/
└── index.html           # Complete CRM dashboard UI (129KB — massive single file)
                         # Contains: Kanban pipeline, lead table, follow-up tracker,
                         # email composer, analytics charts, WhatsApp panel
```

### Root
```
MARVIS_CRM/
├── start.bat            # Windows batch: starts backend + opens frontend
├── vercel.json          # Vercel deployment config for frontend
└── vite.config.ts       # Vite build config
```

---

## Tech Stack
- **Backend**: Python 3 / FastAPI / Uvicorn
- **Database**: SQLite (local dev) → PostgreSQL (Railway production)
- **ORM**: SQLAlchemy
- **Scheduler**: APScheduler (in scheduler.py)
- **Email**: Gmail SMTP with App Password (email_engine.py)
- **WhatsApp**: whatsapp_engine.py (Baileys or similar)
- **AI Enrichment**: Claude API / Anthropic SDK (enrichment.py)
- **Frontend**: Vanilla HTML/CSS/JS — single file (frontend/index.html)
- **Hosting**: Railway (backend + PostgreSQL), Vercel (frontend)
- **HUD Connection**: hud_bus.py provides real-time data to MARVIS HUD via WebSocket/SSE

---

## Critical Architecture Rules

### Database
- Local dev: SQLite (stored in data/ folder)
- Production: PostgreSQL on Railway
- NEVER hardcode DATABASE_URL — use Railway reference: ${{Postgres.DATABASE_URL}}
- Use DATABASE_PUBLIC_URL when connecting from outside Railway network
- db.py handles the SQLAlchemy engine + session factory

### Email (Gmail SMTP)
- Uses personal Gmail account with App Password (NOT Google Workspace)
- App Password stored in .env as GMAIL_APP_PASSWORD
- From address: configured in .env
- Drip sequence: 3 emails (Day 0, Day 3, Day 7) managed by scheduler.py

### API Key Storage
All secrets in .env (local) and Railway environment variables (production):
- ANTHROPIC_API_KEY — Claude API for enrichment
- GMAIL_APP_PASSWORD — Gmail SMTP
- GOOGLE_PLACES_API_KEY — Lead scraper
- DATABASE_URL — PostgreSQL (Railway reference)
- WHATSAPP_* — WhatsApp config

---

## Current Priorities

### PRIORITY 1 — Fix HUD "Waiting for CRM data"
- The MARVIS HUD (Electron app in separate MARVIS/ folder) shows "Waiting for CRM data"
- hud_bus.py is supposed to stream live CRM data to the HUD via WebSocket or SSE
- Check: is hud_bus.py running as a separate process or integrated into main.py?
- Check: CORS settings in main.py allow Electron app origin
- Check: HUD crmService.js is pointing to correct Railway URL (not localhost)
- The HUD connects to this backend — fix must be coordinated between both projects

### PRIORITY 2 — Complete WhatsApp Automation
- File: whatsapp_engine.py (10KB)
- First client: Ukusa Rhino cafe (Rex AI concierge, already deployed on Railway)
- WhatsApp integration for MARVIS CRM outreach pipeline not yet complete
- Need: send WhatsApp messages from CRM lead records

### PRIORITY 3 — PostgreSQL Migration
- Currently running SQLite locally
- Need full migration to PostgreSQL for Railway production
- db.py needs to handle both (SQLite local, PostgreSQL prod) via DATABASE_URL env var

### PRIORITY 4 — Approval Workflows
- High-value leads should require manual approval before email/WhatsApp is sent
- Add approval_status field to leads table
- Frontend needs approve/reject buttons in lead cards

### PRIORITY 5 — First Paying Client Deployment
- Target: deploy a white-labeled MARVIS instance for a Hyderabad SMB client
- Client pipeline: Expert Dental Care (reference), Ukusa Rhino (active)

---

## Key Business Context
- Talktiv AI four-tier service ladder: Starter → Growth → Pro → Enterprise
- MARVIS automates: scrape → enrich → email drip → WhatsApp follow-up → HUD briefing
- Manikanta's father is a Telugu-speaking real estate agent — warm client network
- Target market: Hyderabad SMBs, especially real estate, dental, F&B, motorsport
- First reference client deployed: Ukusa Rhino (motorsport cafe, Sandeep Nadimpalli)
- Rex (WhatsApp AI) already live on Railway for Ukusa Rhino

---

## Coding Conventions Observed in This Project
- FastAPI routes use async def
- SQLAlchemy sessions use context managers (with Session() as db)
- Pydantic models for request/response validation
- APScheduler jobs defined in scheduler.py, started on app startup
- Frontend fetches backend via /api/* routes (check CORS in main.py)
- Single-file frontend (index.html) — all JS/CSS inline, no build step needed

---

## Do Not Touch
- data/ folder contents (contains live lead data)
- logs/ folder (runtime logs)
- __pycache__/ (Python cache)
- .env file (never read aloud or commit)

## When Making Changes
1. Backend changes → test locally with: uvicorn main:app --reload
2. Frontend changes → just refresh index.html in browser (no build needed)
3. Scheduler changes → restart uvicorn (scheduler starts on app startup)
4. DB schema changes → update db.py models AND write migration or recreate tables
5. Railway deploy → git push (Railway auto-deploys from GitHub)
