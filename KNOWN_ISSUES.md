# Known Issues

## WhatsApp — auth error on send / verify (config, not code)
- **Symptom:** WhatsApp test/verify and outbound sends fail with an authentication error.
- **Cause:** The temporary 24-hour WhatsApp Cloud API token in `.env` has expired. This is **not a code bug** — `whatsapp_engine.py` is working as intended.
- **Resolution (manual):** Generate a **permanent System User access token** in Meta Business Manager
  (Business Settings → Users → System Users → Generate Token, with `whatsapp_business_messaging`
  + `whatsapp_business_management` permissions) and set it as `WHATSAPP_TOKEN` / `wa_token`
  in `.env` (local) and Railway env (production).
- **Status:** Open — to be configured manually. Do **not** modify WhatsApp code for this.
