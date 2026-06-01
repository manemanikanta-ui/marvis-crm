"""
MARVIS WhatsApp Webhook
Receives inbound WhatsApp messages, logs to CRM, triggers AI reply
"""

import os
import json
import sqlite3
import threading
from datetime import datetime
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import anthropic
import requests as req_lib
from crm_core import classify_reply, log_activity_event, update_lead_status

load_dotenv()

webhook_app = Flask(__name__)
DB_PATH = "data/crm.db"

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_settings():
    conn = get_db()
    s = {r['key']: r['value'] for r in conn.execute("SELECT * FROM settings").fetchall()}
    conn.close()
    return s

def clean_phone(phone: str) -> str:
    digits = ''.join(c for c in str(phone) if c.isdigit())
    if len(digits) == 10: return "91" + digits
    if len(digits) == 12 and digits.startswith("91"): return digits
    if len(digits) == 11 and digits.startswith("0"): return "91" + digits[1:]
    return digits

def find_lead_by_phone(phone: str):
    """Find CRM lead by phone number"""
    conn = get_db()
    digits = ''.join(c for c in str(phone) if c.isdigit())

    # Try multiple formats
    for fmt in [digits, digits[-10:], "91" + digits[-10:]]:
        lead = conn.execute(
            "SELECT * FROM leads WHERE REPLACE(REPLACE(REPLACE(phone,' ',''),'-',''),'+','') LIKE ?",
            (f"%{fmt[-10:]}%",)
        ).fetchone()
        if lead:
            conn.close()
            return dict(lead)
    conn.close()
    return None

def get_conversation_history(lead_id: int, limit: int = 10) -> list:
    """Get recent WhatsApp messages for a lead"""
    conn = get_db()
    messages = [dict(r) for r in conn.execute("""
        SELECT * FROM activities
        WHERE lead_id = ? AND channel = 'whatsapp'
        ORDER BY created_at DESC LIMIT ?
    """, (lead_id, limit)).fetchall()]
    conn.close()
    return list(reversed(messages))

def log_inbound(lead_id: int, phone: str, message: str, wa_msg_id: str = ""):
    """Log inbound WhatsApp message"""
    conn = get_db()
    lead = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    lead_dict = dict(lead) if lead else {}
    classification = classify_reply(message, lead_dict)
    mapped_status = {
        "interested": "interested",
        "booked": "booked",
        "not_interested": "declined",
        "spam": "declined",
        "pricing": "contacted",
        "callback": "contacted",
        "followup_later": "contacted",
    }.get(classification["label"], "contacted")

    log_activity_event(
        lead_id,
        "reply",
        "whatsapp",
        message[:1000],
        status="received",
        direction="inbound",
        metadata={
            "wa_msg_id": wa_msg_id,
            "phone": phone,
            "classification": classification["label"],
            "confidence": classification.get("confidence", 0),
        },
        classification=classification["label"],
    )
    update_lead_status(
        lead_id,
        mapped_status,
        source="webhook",
        note=f"Inbound reply classified as {classification['label']}",
        metadata={"classification": classification["label"], "confidence": classification.get("confidence", 0)},
    )

def send_wa_reply(phone: str, message: str, settings: dict) -> dict:
    """Send WhatsApp reply"""
    phone_id = settings.get('wa_phone_id', os.getenv('WHATSAPP_PHONE_ID', ''))
    token    = settings.get('wa_token', os.getenv('WHATSAPP_TOKEN', ''))

    url = f"https://graph.facebook.com/v18.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "text",
        "text": {"body": message, "preview_url": False}
    }

    try:
        r = req_lib.post(url, headers=headers, json=payload, timeout=10)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

# ─────────────────────────────────────────────
# AI REPLY GENERATOR
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """You are an AI sales assistant for Talktiv AI — a platform that helps Indian businesses handle customer calls automatically using AI voice agents in 22 Indian languages.

You are chatting on WhatsApp with a business owner or manager who received an outreach message about Talktiv AI.

Your job:
- Respond warmly and professionally
- Answer questions about Talktiv AI clearly
- Handle objections naturally
- Try to book a demo call or get them interested
- Keep responses SHORT — max 3-4 lines on WhatsApp
- Use simple language, no jargon
- If they ask for pricing, say "starting from ₹2,999/month, happy to share details on a quick call"
- If they want a demo, say "Great! I'll set that up — what time works for you this week?"
- Never be pushy
- Sign off as: Manikanta | Talktiv AI

About Talktiv AI:
- AI voice agents that answer calls 24/7 in Telugu, Hindi, Tamil, English + 18 more Indian languages
- Handles customer inquiries, appointments, lead capture automatically
- Works for dental clinics, real estate, salons, hospitals, coaching centres
- Setup in 48 hours, no technical team needed
- Free demo available at talktivai.com

Format for WhatsApp — keep it conversational, short, no markdown headers."""

def generate_ai_reply(lead: dict, inbound_message: str, history: list) -> str:
    """Generate AI reply using Claude"""
    try:
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

        # Build conversation context
        messages = []

        # Add history
        for h in history[-6:]:  # Last 6 messages
            role = "assistant" if h.get("type") == "outreach" else "user"
            messages.append({"role": role, "content": h.get("content", "")[:500]})

        # Add current message
        messages.append({"role": "user", "content": inbound_message})

        # Add lead context to system prompt
        lead_context = f"\n\nLead context: {lead.get('name','')}, {lead.get('business_type','')}, {lead.get('address','')}"

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            system=SYSTEM_PROMPT + lead_context,
            messages=messages
        )
        return response.content[0].text.strip()

    except Exception as e:
        print(f"AI reply error: {e}")
        return "Thanks for your message! I'll get back to you shortly. — Manikanta | Talktiv AI"

# ─────────────────────────────────────────────
# WEBHOOK ROUTES
# ─────────────────────────────────────────────

@webhook_app.route("/webhook/whatsapp", methods=["GET"])
def wa_verify():
    """Meta webhook verification"""
    settings = get_settings()
    verify_token = settings.get('wa_verify_token',
                    os.getenv('WHATSAPP_VERIFY_TOKEN', 'marvis_verify_2024'))

    mode      = request.args.get("hub.mode")
    token     = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == verify_token:
        print("✅ WhatsApp webhook verified")
        return challenge, 200
    return "Forbidden", 403

@webhook_app.route("/webhook/whatsapp", methods=["POST"])
def wa_receive():
    """Receive inbound WhatsApp messages"""
    try:
        data = request.get_json()

        # Extract message from Meta payload
        entry = data.get("entry", [{}])[0]
        changes = entry.get("changes", [{}])[0]
        value = changes.get("value", {})
        messages = value.get("messages", [])

        if not messages:
            return jsonify({"status": "ok"}), 200  # Always return 200 to Meta

        for msg in messages:
            msg_type = msg.get("type")
            phone    = msg.get("from", "")
            wa_id    = msg.get("id", "")

            if msg_type == "text":
                text = msg.get("text", {}).get("body", "")
            elif msg_type == "button":
                text = msg.get("button", {}).get("text", "")
            else:
                # Non-text message
                text = f"[{msg_type} message]"

            print(f"📱 Inbound from {phone}: {text[:100]}")

            # Find lead in CRM
            lead = find_lead_by_phone(phone)
            if not lead:
                # Unknown number — still log it
                print(f"  Unknown number: {phone}")
                continue

            # Log inbound message
            log_inbound(lead["id"], phone, text, wa_id)
            print(f"  Matched lead: {lead['name']} (ID: {lead['id']})")

            # Generate and send AI reply in background
            settings = get_settings()
            auto_reply = settings.get('wa_auto_reply', 'true')

            if auto_reply == 'true' and msg_type in ['text', 'button']:
                def reply_async(lead=lead, text=text, phone=phone, settings=settings):
                    try:
                        history = get_conversation_history(lead["id"])
                        ai_reply = generate_ai_reply(lead, text, history)
                        result = send_wa_reply(phone, ai_reply, settings)
                        print(f"  AI reply sent: {ai_reply[:60]}...")
                        log_activity_event(
                            lead["id"],
                            "ai_reply",
                            "whatsapp",
                            ai_reply[:1000],
                            status="sent",
                            direction="outbound",
                            metadata={"response": result, "source": "webhook_ai_reply"},
                        )
                    except Exception as e:
                        print(f"  Reply error: {e}")

                threading.Thread(target=reply_async, daemon=True).start()

    except Exception as e:
        print(f"Webhook error: {e}")

    # Always return 200 to Meta
    return jsonify({"status": "ok"}), 200

@webhook_app.route("/webhook/status", methods=["GET"])
def webhook_status():
    return jsonify({"status": "running", "time": datetime.now().isoformat()})

if __name__ == "__main__":
    print("🌐 WhatsApp webhook server starting on port 8001...")
    webhook_app.run(host="0.0.0.0", port=8001, debug=False)
