import sqlite3
import unittest

from lead_followup_schema import ensure_lead_followup_schema
from scripts.extract_pike13_leads import (
    capture_visit_link_rows,
    capture_related_rows,
    first_date_like,
    is_auth_redirect,
    normalize_date_like,
    parse_person_text,
    person_urls_from_db,
    upsert_person,
    upsert_plan_pass,
    upsert_visit,
)


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


class Pike13ExtractorTests(unittest.TestCase):
    def test_parse_person_text_extracts_identity_fields(self):
        person, _ = parse_person_text(
            "https://westu-sor.pike13.com/people/15046380",
            """
            Maira Example
            Name Maira Example
            Email maira@example.com
            Phone (713) 555-1212
            Membership No membership
            """,
            "West U",
        )

        self.assertEqual(person["person_id"], "15046380")
        self.assertEqual(person["email_normalized"], "maira@example.com")
        self.assertEqual(person["phone_normalized"], "7135551212")
        self.assertEqual(person["membership_state"], "No membership")

    def test_capture_related_rows_extracts_trial_visit_flags_and_plan_state(self):
        text = """
        Adult Band Trial
        Event Adult Band Trial
        Date Apr 28, 2026 6:00 PM
        Status No Show
        Unpaid
        Waiver missing
        /events/292297814
        /visits/987654
        """
        visits, plans = capture_related_rows(
            "15046380",
            "https://westu-sor.pike13.com/events/292297814",
            text,
            "West U",
            "event_page_text",
        )

        self.assertEqual(len(visits), 1)
        self.assertEqual(visits[0]["visit_id"], "987654")
        self.assertEqual(visits[0]["event_id"], "292297814")
        self.assertEqual(visits[0]["starts_at"], "2026-04-28T18:00:00")
        self.assertEqual(visits[0]["status"], "No Show")
        self.assertEqual(visits[0]["no_show_flag"], 1)
        self.assertEqual(visits[0]["unpaid_flag"], 1)
        self.assertEqual(visits[0]["waiver_flag"], 1)
        self.assertEqual(plans, [])

        plan_text = """
        Plans & Passes
        Plan Rock 101 Monthly
        Status Active
        Start Apr 29, 2026
        End Jun 29, 2026
        """
        _, plans = capture_related_rows(
            "15046380",
            "https://westu-sor.pike13.com/people/15046380/balances",
            plan_text,
            "West U",
            "plans_page_text",
        )
        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0]["name"], "Rock 101 Monthly")
        self.assertEqual(plans[0]["status"], "Active")
        self.assertEqual(plans[0]["starts_at"], "2026-04-29")

    def test_capture_visit_link_rows_extracts_row_level_outcomes(self):
        visits = capture_visit_link_rows(
            "15046380",
            [
                {
                    "href": "https://westu-sor.pike13.com/events/292297814/visits/987654",
                    "text": "Adult Band Trial\nApr 28, 2026 at 6:00 PM\nNo Show\nUnpaid",
                },
                {
                    "href": "https://westu-sor.pike13.com/events/292297815",
                    "text": "Rock 101 Trial\nMay 1, 2026 5:30 PM\nComplete",
                },
                {
                    "href": "https://westu-sor.pike13.com/e/292297816",
                    "text": "Rookies Trial\nMay 2, 2026 4:30 PM\nLate Cancel",
                },
            ],
            "West U",
        )

        self.assertEqual(len(visits), 3)
        self.assertEqual(visits[0]["visit_id"], "987654")
        self.assertEqual(visits[0]["event_id"], "292297814")
        self.assertEqual(visits[0]["service"], "Adult Band Trial")
        self.assertEqual(visits[0]["starts_at"], "2026-04-28T18:00:00")
        self.assertEqual(visits[0]["status"], "No Show")
        self.assertEqual(visits[0]["no_show_flag"], 1)
        self.assertTrue(visits[1]["visit_id"].startswith("pike13_visit_"))
        self.assertEqual(visits[1]["event_id"], "292297815")
        self.assertEqual(visits[1]["starts_at"], "2026-05-01T17:30:00")
        self.assertEqual(visits[1]["status"], "Complete")
        self.assertTrue(visits[2]["visit_id"].startswith("pike13_visit_"))
        self.assertEqual(visits[2]["event_id"], "292297816")
        self.assertEqual(visits[2]["status"], "Late Cancel")
        self.assertEqual(visits[2]["canceled_flag"], 1)

    def test_date_normalization_handles_pike13_formats(self):
        self.assertEqual(normalize_date_like("Apr 28, 2026 at 6:00 PM"), "2026-04-28T18:00:00")
        self.assertEqual(normalize_date_like("4/28/2026 6:00 PM"), "2026-04-28T18:00:00")
        self.assertIsNone(normalize_date_like("Cancel"))
        self.assertEqual(first_date_like("When Tuesday, Apr 28, 2026 at 6:00 PM"), "2026-04-28T18:00:00")

    def test_auth_redirect_detection(self):
        self.assertTrue(is_auth_redirect("https://westu-sor.pike13.com/accounts/sign_in"))
        self.assertFalse(is_auth_redirect("https://westu-sor.pike13.com/people/15046380"))

    def test_upserts_are_idempotent_and_person_urls_are_school_scoped(self):
        conn = open_db()
        person, _ = parse_person_text(
            "https://westu-sor.pike13.com/people/15046380",
            "Name Maira Example\nEmail maira@example.com\nPhone (713) 555-1212",
            "West U",
        )
        visits, plans = capture_related_rows(
            "15046380",
            "https://westu-sor.pike13.com/events/292297814",
            "Event Trial\nDate Apr 28, 2026\nStatus Canceled\n/visits/987654",
            "West U",
        )

        for _ in range(2):
            upsert_person(conn, person)
            upsert_visit(conn, visits[0])
            if plans:
                upsert_plan_pass(conn, plans[0])

        self.assertEqual(conn.execute("SELECT COUNT(*) FROM pike13_people").fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM pike13_visits").fetchone()[0], 1)
        self.assertEqual(
            conn.execute("SELECT canceled_flag FROM pike13_visits").fetchone()[0],
            1,
        )

        conn.execute(
            """
            INSERT INTO hubspot_deals (
                deal_id, pike13_person_id, school, updated_at
            )
            VALUES ('westu-deal', '15046380', 'West U', '2026-05-02T00:00:00+00:00'),
                   ('heights-deal', '999999', 'The Heights', '2026-05-02T00:00:00+00:00')
            """
        )
        urls = person_urls_from_db(conn, "https://westu-sor.pike13.com", 25, "West U")
        self.assertEqual(urls, ["https://westu-sor.pike13.com/people/15046380"])

    def test_person_urls_fall_back_to_pike13_people_matched_to_hubspot_contacts(self):
        conn = open_db()
        person, _ = parse_person_text(
            "https://westu-sor.pike13.com/people/15046380",
            "Name Maira Example\nEmail maira@example.com\nPhone (713) 555-1212",
            "West U",
        )
        upsert_person(conn, person)
        conn.execute(
            """
            INSERT INTO hubspot_contacts (
                contact_id, email_normalized, phone_normalized, school, raw_json, updated_at
            )
            VALUES ('contact-1', 'maira@example.com', '7135551212', 'West U',
                    '{"trusted": 1}', '2026-05-02T00:00:00+00:00')
            """
        )

        urls = person_urls_from_db(conn, "https://westu-sor.pike13.com", 25, "West U")

        self.assertEqual(urls, ["https://westu-sor.pike13.com/people/15046380"])


if __name__ == "__main__":
    unittest.main()
