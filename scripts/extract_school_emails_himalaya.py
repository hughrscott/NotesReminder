#!/usr/bin/env python3
"""Extract school emails from Himalaya IMAP (huscott@schoolofrock.com).

Filters out Pike13/HubSpot/Dialpad/system notifications.
Stores genuine customer communications in school_email_messages
via the existing upsert_school_email_message from lead_followup_schema.

Run: python scripts/extract_school_emails_himalaya.py [--limit 500]
"""
import argparse, json, re, sqlite3, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import (
    ensure_lead_followup_schema,
    normalize_email,
    upsert_school_email_message,
    utc_now_iso,
)
from school_email import external_email_for_message, json_email_list

# ── constants ──────────────────────────────────────────────────────────────
SOR_ACCOUNT = "sor"
OUR_EMAIL = "huscott@schoolofrock.com"
OUR_EMAIL_N = normalize_email(OUR_EMAIL)
DB_PATH = ROOT / "reminders.db"

# Domains/senders we skip (notifications picked up by other scrapers)
SKIP_FROM_DOMAINS = {
    "pike13.com", "hubspot.com", "dialpad.com",
    "instagram.com", "mail.instagram.com", "zapier.com", "markel.com",
    "linkedin.com", "facebookmail.com",
}
SKIP_SENDERS = {"no-reply@", "noreply@", "notifications@", "alert@"}
# Subject patterns that indicate notifications (not customer comms)
SKIP_SUBJECT_PATTERNS = [
    r"added a note about",         # Pike13 lesson notes
    r"booked you for",              # Pike13 booking confirmations
    r"New Trial/Tour Booking",       # Pike13 trial bookings
    r"New Lead - Contact -",         # HubSpot new lead
    r"^HubSpot",                     # HubSpot generic
    r"^Follow-Up for",               # HubSpot follow-up sequences
    r"New login for your",           # Dialpad login notification
    r"^Your verification code",      # 2FA codes
    r"^Re: Update:",                 # forwarded internal threads (via Pike13)
    r"^Re: Following Up on",         # HubSpot follow-up via forward
    r"stories-recap",                # Instagram
    r"recently added to their stories", # Instagram
    r"Payment Confirmation",         # Markel/billing
    r"held Tasks are still waiting", # Zapier alerts
    r"unread messages.*instagram",   # Instagram
]

def should_skip(actual_from, subject):
    """Return True if this email should be filtered out (notification/system)."""
    from_lower = (actual_from or "").lower()
    subj = (subject or "").lower()
    # Domain check
    for d in SKIP_FROM_DOMAINS:
        if d in from_lower:
            return True
    # Sender prefix check
    for s in SKIP_SENDERS:
        if from_lower.startswith(s):
            return True
    # Subject pattern check
    for pat in SKIP_SUBJECT_PATTERNS:
        if re.search(pat, subj, re.IGNORECASE):
            return True
    # Internal @schoolofrock.com: allow only if it looks like personal customer comm
    if from_lower.endswith("@schoolofrock.com"):
        # Keep only if from huscott@ or a known staff to/from a customer
        # Skip staff-to-staff internal threads (they usually have "Re:" + customer name)
        pass  # For now, keep all that pass the above filters
    return False

def parse_himalaya_envelope(line):
    """Parse a himalaya envelope list line.
    Format: | ID | flags | subject | sender | date |
    Returns dict or None.
    """
    # Match: | 158514 |       | Subject text here | Sender Name | date |
    m = re.match(
        r"\|\s*(\d+)\s*\|([^|]*)\|(.*?)\|(.*?)\|(.*?)\|",
        line.strip()
    )
    if not m:
        return None
    eid = m.group(1)
    subject = m.group(3).strip()
    sender = m.group(4).strip()
    date_str = m.group(5).strip()
    return {"id": eid, "subject": subject, "sender": sender, "date": date_str}

def extract_email_from_sender(sender_text):
    """Extract email address from Himalaya sender field.
    E.g., "'HubSpot' via School of Rock West U <hubspot@schoolofrock.com>"
    """
    m = re.search(r'[\w.+-]+@[\w.-]+', sender_text)
    return m.group(0) if m else sender_text

def classify_direction(from_email):
    """inbound = from external to us, outbound = from us to external."""
    fn = normalize_email(from_email or "")
    return "inbound" if fn != OUR_EMAIL_N else "outbound"

def parse_iso_datetime(dt_str):
    """Parse various datetime formats from Himalaya envelope."""
    for fmt in (
        "%Y-%m-%d %H:%M%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M%:z",
    ):
        try:
            dt = datetime.strptime(dt_str.replace("+00:00", "+0000"), fmt)
            return dt.isoformat()
        except ValueError:
            continue
    # Try simpler
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.isoformat()
    except:
        return utc_now_iso()

def read_message_body(msg_id):
    """Read message body via Himalaya CLI. Returns raw text."""
    try:
        r = subprocess.run(
            ["himalaya", "message", "read", "-a", SOR_ACCOUNT, msg_id],
            capture_output=True, text=True, timeout=30
        )
        return r.stdout.strip()
    except Exception as e:
        return f"(read error: {e})"

def strip_html(raw_text):
    """Very basic HTML tag stripping for plain-text extraction."""
    return re.sub(r'<[^>]+>', '', raw_text).strip()

def extract_phone_numbers(text):
    """Extract US phone numbers from text."""
    phones = re.findall(
        r'(?:\+?1[-\s.]?)?\(?\d{3}\)?[-\s.]?\d{3}[-\s.]?\d{4}',
        text
    )
    return list(set(phones))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=500, help="Max envelopes to scan")
    ap.add_argument("--db", default=str(DB_PATH), help="SQLite DB path")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    ensure_lead_followup_schema(conn)

    # Get envelope list
    print(f"Fetching up to {args.limit} envelopes from 'sor'...")
    r = subprocess.run(
        ["himalaya", "envelope", "list", "-a", SOR_ACCOUNT, "-s", str(args.limit)],
        capture_output=True, text=True, timeout=60
    )
    lines = r.stdout.strip().split("\n")
    envelopes = []
    for line in lines:
        env = parse_himalaya_envelope(line)
        if env:
            envelopes.append(env)

    print(f"Found {len(envelopes)} envelopes")

    kept = skipped = stored = 0
    for env in envelopes:
        # Extract sender email from the sender display field
        from_email = extract_email_from_sender(env["sender"])
        subject = env.get("subject", "")

        if should_skip(from_email, subject):
            skipped += 1
            continue

        # Read the full message
        body = read_message_body(env["id"])
        raw_text = body

        # Parse the actual From email from the raw headers (more reliable than
        # the envelope sender field which may be just a display name).
        from_header_match = re.search(r'(?i)^From:\s*(.+)$', raw_text, re.MULTILINE)
        actual_from = from_email  # fallback
        if from_header_match:
            from_header = from_header_match.group(1)
            actual_from = extract_email_from_sender(from_header)

        if should_skip(actual_from, subject):
            skipped += 1
            continue

        # Recompute direction from the actual sender
        direction = classify_direction(actual_from)

        if direction == "inbound":
            to_emails_list = [OUR_EMAIL]
        else:
            # Outbound: parse the To header to find the customer email
            to_header_match = re.search(r'(?i)^To:\s*(.+)$', raw_text, re.MULTILINE)
            if to_header_match:
                to_emails_list = [extract_email_from_sender(to_header_match.group(1))]
            else:
                to_emails_list = []

        external = (
            actual_from if direction == "inbound"
            else (to_emails_list[0] if to_emails_list else "")
        )

        # Build snippet (first 200 chars of plain text)
        plain = strip_html(raw_text)[:200]
        # Remove excessive whitespace
        snippet = re.sub(r'\s+', ' ', plain).strip()

        # Extract phone numbers
        phones = extract_phone_numbers(raw_text)

        row = {
            "message_id": f"sor-{env['id']}",
            "thread_id": None,
            "school_mailbox": OUR_EMAIL,
            "school": None,
            "direction": direction,
            "message_at": parse_iso_datetime(env.get("date", "")),
            "from_email": actual_from,
            "from_email_normalized": normalize_email(actual_from),
            "to_emails": json_email_list(to_emails_list),
            "to_emails_normalized": json_email_list(to_emails_list),
            "cc_emails": "[]",
            "cc_emails_normalized": "[]",
            "external_email_normalized": normalize_email(external),
            "subject": subject,
            "snippet": snippet,
            "body": plain,
            "source_url": None,
            "raw_text": raw_text[:10000],
            "raw_json": json.dumps({"himalaya_id": env["id"], "phones": phones}),
            "updated_at": utc_now_iso(),
        }

        upsert_school_email_message(conn, row)
        stored += 1
        kept += 1

        if kept % 20 == 0:
            print(f"  processed {kept} / {len(envelopes)} (stored {stored}, skipped {skipped})")

    conn.commit()
    conn.close()

    print(f"\nDone. {kept} kept, {stored} stored, {skipped} skipped (from {len(envelopes)} envelopes)")

if __name__ == "__main__":
    main()
