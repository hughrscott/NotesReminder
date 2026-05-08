import sqlite3
import unittest

from lead_followup_schema import ensure_lead_followup_schema, upsert_school_email_message, utc_now_iso
from school_email import (
    classify_direction,
    external_email_for_message,
    gmail_query,
    normalize_email_list,
    parse_gmail_datetime,
)
from source_completeness import refresh_identity_matches
from trial_followup_intelligence import build_trial_followup_report, render_trial_followup_markdown


def open_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE reminders (
            lesson_id TEXT,
            pike13_lesson_id TEXT,
            school TEXT,
            lesson_date TEXT,
            lesson_time TEXT,
            lesson_type TEXT,
            students TEXT,
            location TEXT,
            note_completed INTEGER,
            attendance_status TEXT,
            notes_text TEXT,
            note_timestamp TEXT,
            note_score REAL,
            last_checked TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE call_logs (
            call_id TEXT PRIMARY KEY,
            external_number TEXT,
            date_started TEXT,
            direction TEXT,
            category TEXT,
            name TEXT,
            school_code TEXT,
            school_name TEXT,
            voicemail_transcript TEXT,
            voicemail_recording_url TEXT,
            recording_url TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE recording_transcripts (
            call_id TEXT PRIMARY KEY,
            recording_url TEXT,
            transcript_text TEXT,
            outcome TEXT,
            summary TEXT
        )
        """
    )
    ensure_lead_followup_schema(conn)
    return conn


def insert_email(conn, message_id, direction, message_at, external_email):
    mailbox = "westu@schoolofrock.com"
    upsert_school_email_message(
        conn,
        {
            "message_id": message_id,
            "thread_id": "thread-" + message_id,
            "school_mailbox": mailbox,
            "school": "West University Place",
            "direction": direction,
            "message_at": message_at,
            "from_email": external_email if direction == "inbound" else mailbox,
            "from_email_normalized": external_email if direction == "inbound" else mailbox,
            "to_emails": f'["{mailbox if direction == "inbound" else external_email}"]',
            "to_emails_normalized": f'["{mailbox if direction == "inbound" else external_email}"]',
            "cc_emails": "[]",
            "cc_emails_normalized": "[]",
            "external_email_normalized": external_email,
            "subject": "Private subject",
            "snippet": "Private snippet",
            "body": "Private body",
            "source_url": "https://mail.google.com/private",
            "raw_text": "Private raw text",
            "raw_json": "{}",
            "updated_at": utc_now_iso(),
        },
    )


class SchoolEmailTests(unittest.TestCase):
    def test_email_direction_and_external_email(self):
        self.assertEqual(
            normalize_email_list("Calvin <Calvin@SchoolOfRock.com>, Lead <lead@example.com>"),
            ["calvin@schoolofrock.com", "lead@example.com"],
        )
        self.assertEqual(
            classify_direction("westu@schoolofrock.com", ["lead@example.com"], "westu@schoolofrock.com"),
            "outbound",
        )
        self.assertEqual(
            classify_direction("lead@example.com", ["westu@schoolofrock.com"], "westu@schoolofrock.com"),
            "inbound",
        )
        self.assertEqual(
            external_email_for_message("westu@schoolofrock.com", ["lead@example.com"]),
            "lead@example.com",
        )
        self.assertTrue(parse_gmail_datetime("Apr 22, 2026, 11:05 AM").startswith("2026-04-22T11:05:00"))
        self.assertIn("before:2026/05/01", gmail_query("westu@schoolofrock.com", "inbound", "2026-04-22", "2026-04-30"))
        self.assertTrue(gmail_query("westu@schoolofrock.com", "inbound", "2026-04-22", "2026-04-30", "maira").startswith("maira "))

    def test_email_identity_match_and_sanitized_trial_timeline(self):
        conn = open_db()
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, full_name, email, email_normalized, phone, phone_normalized,
                school, associated_deal_ids, raw_json, updated_at
            )
            VALUES ('contact-1', 'Private Name', 'lead@example.com', 'lead@example.com',
                    '7135551212', '7135551212', 'West University Place', 'deal-1',
                    '{"trusted": 1}', '2026-05-01T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, email, email_normalized, phone, phone_normalized, school, updated_at
            )
            VALUES ('person-1', 'Private Name', 'lead@example.com', 'lead@example.com',
                    '7135551212', '7135551212', 'West U', '2026-05-01T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO pike13_visits (
                visit_id, person_id, service, starts_at, status, no_show_flag, school, updated_at
            )
            VALUES ('visit-1', 'person-1', 'Adult Band Trial', '2026-04-25T13:30:00',
                    'No Show', 1, 'West U', '2026-05-01T00:00:00+00:00')
            """
        )
        insert_email(conn, "email-before", "outbound", "2026-04-22T11:05:00", "lead@example.com")
        insert_email(conn, "email-after", "outbound", "2026-04-26T10:00:00", "lead@example.com")

        refresh_identity_matches(conn)
        report = build_trial_followup_report(conn, "2026-04-22", "2026-04-30", "West U")
        markdown = render_trial_followup_markdown(report)

        self.assertEqual(report["summary"]["trial_rows"], 1)
        self.assertEqual(report["rows"][0]["outcome"], "no_show")
        self.assertTrue(report["rows"][0]["pre_trial_outreach_found"])
        self.assertTrue(report["rows"][0]["post_trial_outreach_found"])
        self.assertIn("post_no_show_followup_found", markdown)
        self.assertNotIn("Private Name", markdown)
        self.assertNotIn("lead@example.com", markdown)
        self.assertNotIn("Private body", markdown)


if __name__ == "__main__":
    unittest.main()
