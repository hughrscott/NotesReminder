import argparse
import re
import sqlite3


VOCEMAIL_PATTERNS = [
    r"automated voice messaging system",
    r"call has been forwarded to voicemail",
    r"mailbox is full",
    r"please leave a message",
    r"not available",
]

VENDOR_PATTERNS = [
    r"seo",
    r"marketing",
    r"advertis",
    r"web design",
    r"google ads",
    r"insurance",
    r"payroll",
    r"credit card processing",
    r"merchant services",
    r"loan",
    r"funding",
    r"solar",
    r"saas",
    r"software",
    r"it services",
    r"recruit",
    r"staffing",
    r"telemarketing",
]

WRONG_NUMBER_PATTERNS = [
    r"wrong number",
    r"do not call",
    r"remove me",
    r"stop calling",
    r"not interested",
]


def normalize(text: str) -> str:
    return (text or "").strip().lower()


def matches_any(patterns, text):
    for pattern in patterns:
        if re.search(pattern, text):
            return True
    return False


def bucket_intent(intent, transcript, summary, is_internal):
    intent_l = normalize(intent)
    text = " ".join(filter(None, [transcript, summary])).lower()

    if is_internal:
        return "Staff/Internal"

    if not text or matches_any(VOCEMAIL_PATTERNS, text):
        return "Voicemail-only / No Content"

    if matches_any(VENDOR_PATTERNS, text):
        return "Vendor/Sales Call"

    if matches_any(WRONG_NUMBER_PATTERNS, text):
        return "Wrong Number / Spam / Robocall"

    # Active student buckets
    if any(k in intent_l for k in ["reschedule", "schedule_change", "schedule change", "make up"]):
        return "Active Student – Reschedule"
    if any(k in intent_l for k in ["cancel", "cancellation", "account", "billing", "refund", "hold"]):
        return "Active Student – Cancel/Refund/Account"
    if any(k in intent_l for k in ["check_in", "check in", "confirmation", "arrival", "attendance", "notification", "follow_up", "callback", "return_call"]):
        return "Follow-up / Callback"

    # New lead buckets
    if "trial" in intent_l:
        return "New Lead – Trial Request"
    if any(k in intent_l for k in ["schedule_lesson", "schedule lesson", "schedule_lessons", "appointment scheduling", "class scheduling", "lesson inquiry", "guitar lesson inquiry"]):
        return "New Lead – Schedule/Availability"
    if any(k in intent_l for k in ["inquiry", "account setup", "account setup assistance", "pricing", "info"]):
        return "New Lead – Pricing/Info"

    # Fallback for text hints
    if any(k in text for k in ["trial", "sign up", "enroll", "enrollment"]):
        return "New Lead – Trial Request"
    if any(k in text for k in ["schedule", "availability", "appointment"]):
        return "New Lead – Schedule/Availability"
    if any(k in text for k in ["price", "cost", "pricing", "how much"]):
        return "New Lead – Pricing/Info"

    return "Unknown / Unclear"


def ensure_column(conn):
    columns = {
        row[1] for row in conn.execute("PRAGMA table_info(recording_transcripts)").fetchall()
    }
    if "intent_bucket" not in columns:
        conn.execute("ALTER TABLE recording_transcripts ADD COLUMN intent_bucket TEXT")
        conn.commit()


def parse_args():
    parser = argparse.ArgumentParser(description="Map transcript intents to a finite set of buckets.")
    parser.add_argument("--db", default="reminders.db")
    return parser.parse_args()


def main():
    args = parse_args()
    conn = sqlite3.connect(args.db)
    try:
        ensure_column(conn)
        rows = conn.execute(
            """
            SELECT r.call_id, r.intent, r.transcript_text, r.summary, l.is_internal
            FROM recording_transcripts r
            LEFT JOIN call_logs l ON l.call_id = r.call_id
            WHERE r.transcript_status = 'completed'
            """
        ).fetchall()
        for call_id, intent, transcript, summary, is_internal in rows:
            bucket = bucket_intent(intent, transcript, summary, is_internal == 1)
            conn.execute(
                "UPDATE recording_transcripts SET intent_bucket = ? WHERE call_id = ?",
                (bucket, call_id),
            )
        conn.commit()
        print(f"Updated {len(rows)} rows with intent_bucket.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
