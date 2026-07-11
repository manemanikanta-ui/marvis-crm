# MARVIS CRM — frontend (folder rules; adds to ..\CLAUDE.md)

## Rules
- Single-file `index.html`, no build step. (Browser-verify-visual rule lives at root CLAUDE.md.)
- Security P1 is LIVE (a731aed): the API key is OPTIONAL locally (loopback exemption) but
  REQUIRED for non-loopback / Railway. All fetches go through the shared wrapper carrying
  `X-API-Key: window.__MARVIS_KEY__` — never bypass it with a bare `fetch()`.

## Pointers
- `MARVIS_Vault\brain\briefs\SECURITY_BUILD_BRIEF.md` · `..\CLAUDE.md`.
