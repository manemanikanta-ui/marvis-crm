# MARVIS CRM — backend (folder rules; adds to ..\CLAUDE.md + root CLAUDE.md)

## Rules
- VERIFY markers: any `# VERIFY` in code (e.g. `campaign_autopsy.py`) marks an UNCONFIRMED
  column/table/fn. Resolve it against the REAL schema — read `outcomes.py` / models, check the
  `reply_outcomes` + `leads` columns — before wiring. Never guess a name.
- HUD emission: `agent_bus.hud_event(...)` is the ONLY HUD emitter. Never hand-roll the
  `{type:"agent_event", data:{…}}` envelope at call sites. Wrap the call so a bus failure can
  never break the caller.
- Sanitise untrusted text (scraped names, email bodies, chat input) before any prompt assembly —
  no raw external strings in Claude API calls.

## Pointers
- `feedback.md` (HUD 3 envelope §2, invariant #8) · `..\CLAUDE.md`.
