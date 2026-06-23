"""
Telegram notifications for MARVIS CRM.

Thin wrapper around the Telegram Bot sendMessage API. Call sites use:

    from telegram_notify import notify
    notify("👁 <b>Proof pack opened</b>\\n...")

Messages may contain HTML (<b>, <i>, ...), so parse_mode defaults to "HTML".

Config (env / Railway Variables):
    TELEGRAM_BOT_TOKEN   bot token from @BotFather
    TELEGRAM_CHAT_ID     chat/channel id to deliver to

If either is missing the call is a no-op (prints a warning, returns False) so
notifications never crash the request path. Callers also wrap notify() in
try/except, so this module never raises.

Run directly to send a test message:
    python telegram_notify.py
"""

from __future__ import annotations

import os

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

TELEGRAM_API = "https://api.telegram.org"


def notify(text: str, parse_mode: str = "HTML") -> bool:
    """Send a Telegram message. Returns True on success, False otherwise.

    Reads the token/chat id from the environment on every call so values set
    after import (or in tests) are picked up.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print("telegram_notify: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set — skipping")
        return False

    try:
        resp = requests.post(
            f"{TELEGRAM_API}/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"telegram_notify: send failed {resp.status_code}: {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"telegram_notify: error sending message: {e}")
        return False


if __name__ == "__main__":
    ok = notify("🚀 MARVIS Telegram notifications working!")
    print("test notification sent" if ok else "test notification FAILED")
