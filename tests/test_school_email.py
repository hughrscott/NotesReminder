import sqlite3
import unittest

from lead_followup_schema import ensure_lead_followup_schema, upsert_school_email_message, utc_now_iso
from scripts.extract_hubspot_timeline_emails import hubspot_email_events
from scripts.extract_school_emails import is_okta_login_url, okta_credentials_available, parse_gmail_sync_response
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

    def test_okta_login_helpers_are_safe_without_credentials(self):
        self.assertTrue(is_okta_login_url("https://sor.okta.com/login/login.htm?fromURI=abc"))
        self.assertFalse(is_okta_login_url("https://mail.google.com/mail/u/0/#inbox"))
        self.assertIsInstance(okta_credentials_available(), bool)

    def test_gmail_sync_parser_keeps_true_school_customer_email_metadata_only(self):
        payload = [
            0,
            [
                [
                    "thread-f:1|msg-f:1",
                    None,
                    [
                        [
                            "msg-f:1",
                            [
                                [[[1, "westu@schoolofrock.com", "West U"]]],
                                None,
                                None,
                                [[1, "lead@example.com", "Lead"]],
                                "Private customer subject",
                                [None, [], 0, "https://mail.google.com/message/1"],
                                "Private body text",
                                1782261577245,
                            ],
                        ]
                    ],
                ]
            ],
        ]

        rows = parse_gmail_sync_response(
            __import__("json").dumps(payload),
            "westu@schoolofrock.com",
            "2026-06-01",
            "2026-06-24",
            "https://mail.google.com/search",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["message_id"], "msg-f:1")
        self.assertEqual(rows[0]["direction"], "outbound")
        self.assertEqual(rows[0]["external_email_normalized"], "lead@example.com")
        self.assertIn("2026-06-23", rows[0]["message_at"])
        self.assertEqual(rows[0]["subject"], "[redacted Gmail sync subject]")
        self.assertEqual(rows[0]["body"], "[redacted Gmail sync body]")
        self.assertEqual(rows[0]["raw_text"], "")

    def test_gmail_sync_parser_skips_system_notification_rows(self):
        payload = [
            0,
            [
                [
                    "thread-f:2|msg-f:2",
                    None,
                    [
                        [
                            "msg-f:2",
                            [
                                [[[1, "westu@schoolofrock.com", "West U"]]],
                                None,
                                None,
                                [[1, "voicemail@dialpad.com", "Dialpad"]],
                                "West U has a new voicemail",
                                [None, [], 0, "https://mail.google.com/message/2"],
                                "Private voicemail body",
                                1782261577245,
                            ],
                        ]
                    ],
                ]
            ],
        ]

        rows = parse_gmail_sync_response(
            __import__("json").dumps(payload),
            "westu@schoolofrock.com",
            "2026-06-01",
            "2026-06-24",
            "https://mail.google.com/search",
        )

        self.assertEqual(rows, [])

    def test_gmail_sync_parser_skips_group_and_vendor_rows(self):
        payload = [
            0,
            [
                [
                    "thread-f:3|msg-f:3",
                    None,
                    [
                        [
                            "msg-f:3",
                            [
                                [[[1, "westu@schoolofrock.com", "West U"]]],
                                None,
                                None,
                                [[1, "family@example.com", "Family"]],
                                "Private customer subject",
                                [None, [], 0, "https://mail.google.com/message/3"],
                                "Private body text",
                                ["^smartlabel_group"],
                                1782261577245,
                            ],
                        ],
                        [
                            "msg-f:4",
                            [
                                [[[1, "westu@schoolofrock.com", "West U"]]],
                                None,
                                None,
                                [[1, "hello@jumbula.com", "Jumbula"]],
                                "Vendor subject",
                                [None, [], 0, "https://mail.google.com/message/4"],
                                "Vendor body",
                                1782261577245,
                            ],
                        ],
                    ],
                ]
            ],
        ]

        rows = parse_gmail_sync_response(
            __import__("json").dumps(payload),
            "westu@schoolofrock.com",
            "2026-06-01",
            "2026-06-24",
            "https://mail.google.com/search",
        )

        self.assertEqual(rows, [])

    def test_hubspot_timeline_email_events_are_sanitized_and_lead_matched(self):
        payload = {
            "events": [
                {
                    "etype": "eventEmailSend",
                    "timestamp": 1782325469010,
                    "eventData": {
                        "id": "email-event-1",
                        "recipient": "lead@example.com",
                        "subject": "Private subject",
                        "messageId": {"to": "lead@example.com"},
                    },
                },
                {
                    "etype": "eventEmailSend",
                    "timestamp": 1782325469010,
                    "eventData": {
                        "id": "email-event-other",
                        "recipient": "other@example.com",
                    },
                },
            ]
        }

        rows = hubspot_email_events(
            payload,
            {"contact_id": "contact-1", "email_normalized": "lead@example.com", "school": "The Heights"},
            "2026-01-01",
            "2026-06-24",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["direction"], "outbound")
        self.assertEqual(rows[0]["school"], "The Heights")
        self.assertEqual(rows[0]["external_email_normalized"], "lead@example.com")
        self.assertEqual(rows[0]["subject"], "[redacted HubSpot timeline email subject]")
        self.assertNotIn("Private subject", rows[0]["raw_json"])

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
        self.assertEqual(report["rows"][0]["customer_name"], "Private Name")
        self.assertTrue(report["rows"][0]["pre_trial_outreach_found"])
        self.assertTrue(report["rows"][0]["post_trial_outreach_found"])
        self.assertIn("By Customer", markdown)
        self.assertIn("post_no_show_followup_found", markdown)
        self.assertIn("Private Name", markdown)
        self.assertNotIn("lead@example.com", markdown)
        self.assertNotIn("Private body", markdown)

    def test_trial_followup_uses_name_search_when_contact_keys_are_missing(self):
        conn = open_db()
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, school, updated_at
            )
            VALUES ('person-name-only', 'Private Student', 'West U', '2026-05-01T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO pike13_visits (
                visit_id, person_id, service, starts_at, status, school, updated_at
            )
            VALUES ('visit-name-only', 'person-name-only', 'Trial - Guitar',
                    '2026-04-25T13:30:00', 'Complete', 'West U', '2026-05-01T00:00:00+00:00')
            """
        )
        upsert_school_email_message(
            conn,
            {
                "message_id": "email-name-only",
                "thread_id": "thread-name-only",
                "school_mailbox": "westu@schoolofrock.com",
                "school": "West University Place",
                "direction": "outbound",
                "message_at": "2026-04-24T10:00:00",
                "from_email": "westu@schoolofrock.com",
                "from_email_normalized": "westu@schoolofrock.com",
                "to_emails": "[]",
                "to_emails_normalized": "[]",
                "cc_emails": "[]",
                "cc_emails_normalized": "[]",
                "external_email_normalized": "",
                "subject": "Follow-Up for Private Student",
                "snippet": "Private snippet",
                "body": "Private body",
                "source_url": "https://mail.google.com/private",
                "raw_text": "Follow-Up for Private Student",
                "raw_json": "{}",
                "updated_at": utc_now_iso(),
            },
        )

        report = build_trial_followup_report(conn, "2026-04-22", "2026-04-30", "West U")
        markdown = render_trial_followup_markdown(report)

        self.assertEqual(report["rows"][0]["identity_status"], "name_search_only")
        self.assertEqual(report["rows"][0]["customer_name"], "Private Student")
        self.assertTrue(report["rows"][0]["pre_trial_outreach_found"])
        self.assertTrue(report["rows"][0]["name_search_used"])
        self.assertIn("Identity Coverage", markdown)
        self.assertIn("Private Student", markdown)
        self.assertNotIn("Private body", markdown)

    def test_trial_followup_marks_identity_limited_no_outreach(self):
        conn = open_db()
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, school, updated_at
            )
            VALUES ('person-limited', 'West U', '2026-05-01T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO pike13_visits (
                visit_id, person_id, service, starts_at, status, school, updated_at
            )
            VALUES ('visit-limited', 'person-limited', 'Trial - Guitar',
                    '2026-04-25T13:30:00', 'Complete', 'West U', '2026-05-01T00:00:00+00:00')
            """
        )

        report = build_trial_followup_report(conn, "2026-04-22", "2026-04-30", "West U")

        self.assertEqual(report["rows"][0]["identity_status"], "insufficient_identity")
        self.assertEqual(report["rows"][0]["followup_status"], "no_pre_trial_outreach_identity_limited")


if __name__ == "__main__":
    unittest.main()
