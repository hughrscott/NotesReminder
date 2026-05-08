import hashlib
import json
import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from lead_followup_schema import normalize_email


SCHOOL_MAILBOXES = {
    "westu@schoolofrock.com": "West University Place",
    "theheights@schoolofrock.com": "The Heights",
}
SCHOOL_DOMAIN = "schoolofrock.com"
DEFAULT_TIMEZONE = ZoneInfo("America/Chicago")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)


def normalize_email_list(values):
    if not values:
        return []
    if isinstance(values, str):
        candidates = EMAIL_RE.findall(values)
    else:
        candidates = []
        for value in values:
            candidates.extend(EMAIL_RE.findall(str(value or "")))
    normalized = []
    for email in candidates:
        item = normalize_email(email)
        if item and item not in normalized:
            normalized.append(item)
    return normalized


def json_email_list(values):
    return json.dumps(normalize_email_list(values), sort_keys=True)


def school_for_mailbox(mailbox):
    return SCHOOL_MAILBOXES.get(normalize_email(mailbox) or "", "")


def is_school_email(email):
    normalized = normalize_email(email) or ""
    return normalized.endswith(f"@{SCHOOL_DOMAIN}")


def external_email_for_message(from_email, to_emails, cc_emails=None):
    candidates = []
    candidates.extend(normalize_email_list(from_email))
    candidates.extend(normalize_email_list(to_emails))
    candidates.extend(normalize_email_list(cc_emails))
    for email in candidates:
        if not is_school_email(email):
            return email
    return None


def classify_direction(from_email, to_emails, school_mailbox):
    mailbox = normalize_email(school_mailbox)
    from_list = normalize_email_list(from_email)
    to_list = normalize_email_list(to_emails)
    if mailbox in from_list:
        return "outbound"
    if mailbox in to_list:
        return "inbound"
    return "unknown"


def stable_email_id(school_mailbox, direction, message_at, subject, external_email, source_url="", raw_text=""):
    key = "|".join(
        [
            normalize_email(school_mailbox) or "",
            direction or "",
            message_at or "",
            subject or "",
            external_email or "",
            source_url or "",
            hashlib.sha256((raw_text or "").encode("utf-8")).hexdigest()[:12],
        ]
    )
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return f"school_email_{digest[:24]}"


def parse_gmail_datetime(value, now_year=None):
    text = str(value or "").strip()
    if not text:
        return None
    text = re.sub(r"\s+", " ", text)
    formats = (
        "%b %d, %Y, %I:%M %p",
        "%b %d, %Y at %I:%M %p",
        "%B %d, %Y, %I:%M %p",
        "%B %d, %Y at %I:%M %p",
        "%m/%d/%y, %I:%M %p",
        "%m/%d/%Y, %I:%M %p",
    )
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=DEFAULT_TIMEZONE).isoformat()
        except ValueError:
            pass
    short_formats = ("%b %d, %I:%M %p", "%B %d, %I:%M %p")
    for fmt in short_formats:
        try:
            parsed = datetime.strptime(text, fmt)
            parsed = parsed.replace(year=now_year or datetime.now(DEFAULT_TIMEZONE).year)
            return parsed.replace(tzinfo=DEFAULT_TIMEZONE).isoformat()
        except ValueError:
            pass
    return None


def delay_bucket(hours):
    if hours is None:
        return "none"
    if hours < 0:
        return "invalid"
    if hours <= 24:
        return "same_day"
    if hours <= 48:
        return "1_day"
    if hours <= 72:
        return "2_3_days"
    return "4_plus_days"


def gmail_query(mailbox, direction, start_date, end_date):
    after = start_date.replace("-", "/")
    before = (date.fromisoformat(end_date) + timedelta(days=1)).isoformat().replace("-", "/")
    side = "to" if direction == "inbound" else "from"
    return f"{side}:({mailbox}) after:{after} before:{before}"


def communication_label(row):
    channel = row.get("channel") or "unknown"
    direction = row.get("direction") or "unknown"
    return f"{channel}:{direction}"
