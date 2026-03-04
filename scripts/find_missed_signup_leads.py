import argparse
import csv
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta


GOOD_INTENT_KEYWORDS = (
    "trial",
    "schedule_lesson",
    "schedule lesson",
    "lesson inquiry",
    "guitar lesson inquiry",
    "class scheduling",
    "appointment scheduling",
    "account setup",
    "inquiry",
)

BAD_INTENT_KEYWORDS = (
    "reschedule",
    "cancel",
    "schedule_change",
    "schedule change",
    "follow_up",
    "voicemail",
    "return_call",
    "lesson confirmation",
    "appointment rescheduling",
    "appointment cancellation",
    "callback",
    "account update",
)

TEXT_KEYWORDS = (
    "trial",
    "sign up",
    "sign-up",
    "enroll",
    "enrollment",
    "schedule a lesson",
    "schedule lessons",
    "lesson inquiry",
    "class inquiry",
    "class schedule",
    "how much",
    "price",
    "pricing",
    "cost",
)


def parse_dt(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def is_signup_intent(intent, transcript):
    intent_l = (intent or "").lower()
    if any(bad in intent_l for bad in BAD_INTENT_KEYWORDS):
        return False
    if any(good in intent_l for good in GOOD_INTENT_KEYWORDS):
        return True
    text_l = (transcript or "").lower()
    return any(word in text_l for word in TEXT_KEYWORDS)


def normalize_digits(value):
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    return digits or None


def load_outbound_calls(conn):
    rows = conn.execute(
        """
        SELECT external_number_digits, date_started
        FROM dialpad_calls
        WHERE direction = 'outbound'
          AND external_number_digits IS NOT NULL
          AND trim(external_number_digits) != ''
        """
    ).fetchall()
    outbound = defaultdict(list)
    for digits, dt in rows:
        parsed = parse_dt(dt)
        if parsed:
            outbound[digits].append(parsed)
    return outbound


def load_client_phone_map(conn):
    rows = conn.execute(
        """
        SELECT "Client ID", "First Name", "Last Name", "Client Home Location",
               phone_digits, mobile_digits, account_manager_phone_digits
        FROM pike13_clients
        """
    ).fetchall()
    phone_map = defaultdict(list)
    for client_id, first, last, location, phone, mobile, account_phone in rows:
        name = " ".join(part for part in [first, last] if part).strip()
        if not name:
            continue
        for digits in (phone, mobile, account_phone):
            normalized = normalize_digits(digits)
            if normalized:
                phone_map[normalized].append((client_id, name, location or ""))
    return phone_map


def load_trial_dates(conn):
    rows = conn.execute(
        """
        SELECT lower(trim(students)) AS student_name, school, date(lesson_date) AS lesson_date
        FROM reminders
        WHERE students IS NOT NULL
          AND students NOT LIKE '%,%'
          AND lesson_type LIKE '%Trial%'
        """
    ).fetchall()
    trials = defaultdict(list)
    for student_name, school, lesson_date in rows:
        if not student_name:
            continue
        trials[(school, student_name)].append(lesson_date)
    return trials


def load_call_client_matches(conn):
    rows = conn.execute(
        "SELECT call_id, client_id FROM call_client_matches"
    ).fetchall()
    return {call_id: client_id for call_id, client_id in rows}


def load_client_lookup(conn):
    rows = conn.execute(
        """
        SELECT "Client ID", "First Name", "Last Name", "Client Home Location"
        FROM pike13_clients
        """
    ).fetchall()
    lookup = {}
    for client_id, first, last, location in rows:
        name = " ".join(part for part in [first, last] if part).strip()
        lookup[str(client_id)] = (name, location or "")
    return lookup


def school_matches_location(school_code, location):
    if school_code == "westu-sor":
        return "west u" in location.lower()
    if school_code == "theheights-sor":
        return "heights" in location.lower()
    return False


def parse_args():
    parser = argparse.ArgumentParser(
        description="Find inbound sign-up calls with no callback and no trial lesson."
    )
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--out", default="outputs/missed_signup_leads.csv")
    parser.add_argument(
        "--callback-days",
        type=int,
        default=7,
        help="Days to look for an outbound callback (default: 7)",
    )
    parser.add_argument(
        "--trial-days",
        type=int,
        default=30,
        help="Days to look for a trial lesson after the call (default: 30)",
    )
    parser.add_argument(
        "--assumed-monthly-rate",
        type=float,
        default=300.0,
        help="Assumed monthly revenue per converted lead (default: 300)",
    )
    parser.add_argument(
        "--expected-months",
        type=float,
        default=6.0,
        help="Expected retained months for revenue estimate (default: 6)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    conn = sqlite3.connect(args.db)
    try:
        outbound_calls = load_outbound_calls(conn)
        phone_map = load_client_phone_map(conn)
        trials = load_trial_dates(conn)
        call_client_matches = load_call_client_matches(conn)
        client_lookup = load_client_lookup(conn)

        rows = conn.execute(
            """
            SELECT r.call_id,
                   r.intent,
                   r.intent_bucket,
                   r.outcome,
                   r.sentiment,
                   r.topic,
                   r.summary,
                   r.transcript_text,
                   l.school_code,
                   l.date_started,
                   l.voicemail_transcript,
                   d.external_number_digits,
                   d.name
            FROM recording_transcripts r
            JOIN dialpad_calls d ON d.call_id = r.call_id
            JOIN call_logs l ON l.call_id = r.call_id
            WHERE r.transcript_status = 'completed'
              AND d.direction = 'inbound'
            """
        ).fetchall()

        cutoff_delta = timedelta(days=args.callback_days)
        trial_window = timedelta(days=args.trial_days)
        output_rows = []

        for (
            call_id,
            intent,
            intent_bucket,
            outcome,
            sentiment,
            topic,
            summary,
            transcript_text,
            school_code,
            date_started,
            voicemail_transcript,
            external_number_digits,
            caller_name,
        ) in rows:
            if intent_bucket not in (
                "New Lead – Trial Request",
                "New Lead – Pricing/Info",
                "New Lead – Schedule/Availability",
            ):
                continue
            if not is_signup_intent(intent, transcript_text):
                continue

            call_dt = parse_dt(date_started)
            if not call_dt:
                continue

            digits = normalize_digits(external_number_digits)
            if not digits:
                continue

            callbacks = outbound_calls.get(digits, [])
            had_callback = any(
                call_dt <= outbound_dt <= call_dt + cutoff_delta for outbound_dt in callbacks
            )
            if had_callback:
                continue

            matched_client_id = call_client_matches.get(call_id)
            matched_client_name = None
            matched_location = ""
            if matched_client_id:
                matched_client_name, matched_location = client_lookup.get(
                    str(matched_client_id), (None, "")
                )

            trial_found = False
            trial_date = None

            candidate_clients = []
            if matched_client_id and matched_client_name:
                candidate_clients = [(matched_client_id, matched_client_name, matched_location)]
            else:
                candidate_clients = [
                    item for item in phone_map.get(digits, [])
                    if school_matches_location(school_code, item[2])
                ]

            for _, client_name, _ in candidate_clients:
                key = (school_code, client_name.lower())
                for lesson_date in trials.get(key, []):
                    lesson_dt = datetime.strptime(lesson_date, "%Y-%m-%d")
                    if call_dt < lesson_dt <= call_dt + trial_window:
                        trial_found = True
                        trial_date = lesson_date
                        break
                if trial_found:
                    break

            if trial_found:
                continue

            est_revenue = args.assumed_monthly_rate * args.expected_months
            transcript_preview = (transcript_text or "").strip().replace("\n", " ")
            if len(transcript_preview) > 240:
                transcript_preview = transcript_preview[:240] + "..."

            output_rows.append(
                {
                    "call_id": call_id,
                    "school_code": school_code,
                    "call_datetime": date_started,
                    "caller_name": caller_name,
                    "external_number_digits": digits,
                    "intent": intent,
                    "intent_bucket": intent_bucket,
                    "outcome": outcome,
                    "sentiment": sentiment,
                    "topic": topic,
                    "summary": summary,
                    "voicemail_present": "yes" if voicemail_transcript else "no",
                    "matched_client_id": matched_client_id,
                    "matched_client_name": matched_client_name,
                    "trial_found_within_window": "no",
                    "transcript_preview": transcript_preview,
                    "estimated_revenue": round(est_revenue, 2),
                }
            )

        with open(args.out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=output_rows[0].keys() if output_rows else [])
            if output_rows:
                writer.writeheader()
                writer.writerows(output_rows)

        print(f"Wrote {len(output_rows)} leads to {args.out}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
