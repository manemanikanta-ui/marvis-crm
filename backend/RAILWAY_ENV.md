# Railway Environment Variables

Set these in the Railway dashboard for the `marvis-crm` service:

- `ANTHROPIC_API_KEY=<from .env>`
- `GOOGLE_API_KEY=<from .env>`
- `WHATSAPP_PHONE_ID=1095383390320082`
- `WHATSAPP_TOKEN=<from .env>`
- `WHATSAPP_VERIFY_TOKEN=marvis_verify_2024`
- `DATABASE_URL=<Railway auto-injects this when Postgres is linked>`

Gmail credentials stay in the SQLite/Postgres `settings` table, so no change is needed there.
