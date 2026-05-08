import sqlite3
import unittest

from lead_followup_schema import ensure_lead_followup_schema
from lead_gap_analysis import classify_gap, fetch_gap_rows, render_gap_markdown, summarize_gap_rows
from source_completeness import refresh_identity_matches


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


def insert_deal(conn, deal_id, stage="Contacted", pike13_person_id=None):
    conn.execute(
        """
        INSERT INTO hubspot_deals (
            deal_id, deal_name, stage, school, create_date, pike13_person_id, updated_at
        )
        VALUES (?, ?, ?, 'West University Place', '2026-04-24', ?, '2026-05-07T00:00:00+00:00')
        """,
        (deal_id, f"Lead {deal_id}", stage, pike13_person_id),
    )


def insert_trusted_contact(conn, contact_id, deal_id, phone, email=None):
    conn.execute(
        """
        INSERT INTO hubspot_contacts (
            contact_id, full_name, email, email_normalized, phone, phone_normalized,
            school, associated_deal_ids, raw_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, 'West University Place', ?, '{"trusted": 1}', '2026-05-07T00:00:00+00:00')
        """,
        (contact_id, f"Contact {contact_id}", email, email, phone, phone, deal_id),
    )


def insert_pike13_person(conn, person_id, phone=None, email=None):
    conn.execute(
        """
        INSERT INTO pike13_people (
            person_id, full_name, email, email_normalized, phone, phone_normalized, school, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, 'West U', '2026-05-07T00:00:00+00:00')
        """,
        (person_id, f"Person {person_id}", email, email, phone, phone),
    )


def insert_first_visit(conn, person_id, conversion=True):
    conn.execute(
        """
        INSERT INTO pike13_visits (
            visit_id, person_id, service, starts_at, status, first_visit_flag,
            attendance_confirmed_flag, checked_in_flag, school, updated_at
        )
        VALUES (?, ?, 'Trial - Guitar', '2026-04-15T19:15:00', 'Complete', 1, 1, 1, 'West U', '2026-05-07T00:00:00+00:00')
        """,
        (f"visit-{person_id}", person_id),
    )
    if conversion:
        conn.execute(
            """
            INSERT INTO pike13_plans_passes (
                plan_pass_id, person_id, name, status, starts_at, payer_name, updated_at
            )
            VALUES (?, ?, 'Lessons Only - 45 Minute Lessons', 'Active', '2026-05-01', 'Payer', '2026-05-07T00:00:00+00:00')
            """,
            (f"plan-{person_id}", person_id),
        )


def insert_dialpad_sms(conn, phone):
    conn.execute(
        """
        INSERT INTO dialpad_sms_threads (
            thread_id, phone, phone_normalized, school, updated_at
        )
        VALUES (?, ?, ?, 'West University Place', '2026-05-07T00:00:00+00:00')
        """,
        (f"thread-{phone}", phone, phone),
    )
    conn.execute(
        """
        INSERT INTO dialpad_sms_messages (
            message_id, thread_id, message_at, direction, body, updated_at
        )
        VALUES (?, ?, '2026-05-01T12:00:00', 'inbound', 'redacted', '2026-05-07T00:00:00+00:00')
        """,
        (f"message-{phone}", f"thread-{phone}"),
    )


class LeadGapReportTests(unittest.TestCase):
    def test_classify_gap_uses_fixed_precedence(self):
        self.assertEqual(classify_gap({"excluded_stage_flag": 1}), "excluded_stage")
        self.assertEqual(classify_gap({}), "missing_hubspot_contact")
        self.assertEqual(classify_gap({"trusted_contact_flag": 1}), "missing_pike13_match")
        self.assertEqual(
            classify_gap({"trusted_contact_flag": 1, "pike13_match_flag": 1}),
            "missing_first_visit",
        )
        self.assertEqual(
            classify_gap({"trusted_contact_flag": 1, "pike13_match_flag": 1, "first_visit_flag": 1}),
            "missing_conversion_signal",
        )
        self.assertEqual(
            classify_gap(
                {
                    "trusted_contact_flag": 1,
                    "pike13_match_flag": 1,
                    "first_visit_flag": 1,
                    "conversion_signal_flag": 1,
                    "targeted_dialpad_found_flag": 1,
                }
            ),
            "targeted_dialpad_not_wired",
        )

    def test_gap_report_flags_complete_and_missing_rows_without_customer_content(self):
        conn = open_db()

        insert_deal(conn, "deal-ready", pike13_person_id="person-ready")
        insert_trusted_contact(conn, "contact-ready", "deal-ready", "7135550001")
        insert_pike13_person(conn, "person-ready", phone="7135550001")
        insert_first_visit(conn, "person-ready", conversion=True)
        insert_dialpad_sms(conn, "7135550001")

        insert_deal(conn, "deal-no-contact")

        insert_deal(conn, "deal-no-pike")
        insert_trusted_contact(conn, "contact-no-pike", "deal-no-pike", "7135550002")

        insert_deal(conn, "deal-no-visit", pike13_person_id="person-no-visit")
        insert_trusted_contact(conn, "contact-no-visit", "deal-no-visit", "7135550003")
        insert_pike13_person(conn, "person-no-visit", phone="7135550003")

        insert_deal(conn, "deal-no-conversion", pike13_person_id="person-no-conversion")
        insert_trusted_contact(conn, "contact-no-conversion", "deal-no-conversion", "7135550004")
        insert_pike13_person(conn, "person-no-conversion", phone="7135550004")
        insert_first_visit(conn, "person-no-conversion", conversion=False)

        insert_deal(conn, "deal-targeted", pike13_person_id="person-targeted")
        insert_trusted_contact(conn, "contact-targeted", "deal-targeted", "7135550005")
        insert_pike13_person(conn, "person-targeted", phone="7135550005")
        insert_first_visit(conn, "person-targeted", conversion=True)
        conn.execute(
            """
            INSERT INTO dialpad_target_searches (
                search_id, deal_id, target_hash, target_type, searched_at, outcome,
                found_call_review_count, updated_at
            )
            VALUES ('search-targeted', 'deal-targeted', 'hash', 'phone', '2026-05-07T00:00:00+00:00',
                    'found_call_review', 1, '2026-05-07T00:00:00+00:00')
            """
        )

        insert_deal(conn, "deal-excluded", stage="Closed Lost")
        insert_trusted_contact(conn, "contact-excluded", "deal-excluded", "7135550006")

        refresh_identity_matches(conn)
        rows = fetch_gap_rows(conn, "West University Place", 20)
        summary = summarize_gap_rows(rows)
        markdown = render_gap_markdown({"summary": summary, "rows": rows}, "West University Place")
        categories = {row["gap_category"] for row in rows}

        self.assertIn("ready_for_review", categories)
        self.assertIn("missing_hubspot_contact", categories)
        self.assertIn("missing_pike13_match", categories)
        self.assertIn("missing_first_visit", categories)
        self.assertIn("missing_conversion_signal", categories)
        self.assertIn("targeted_dialpad_not_wired", categories)
        self.assertIn("excluded_stage", categories)
        self.assertEqual(summary["ready_for_review_rows"], 1)
        self.assertEqual(summary["targeted_dialpad_not_wired_rows"], 1)
        self.assertEqual(summary["by_diagnostic_area"]["identity"], 1)
        self.assertEqual(summary["by_diagnostic_area"]["communication"], 1)
        self.assertIn("Lead Intelligence Gap Report", markdown)
        self.assertIn("Diagnostic Areas", markdown)
        self.assertNotIn("Lead deal-ready", markdown)
        self.assertNotIn("7135550001", markdown)
        self.assertNotIn("redacted", markdown)

    def test_gap_report_can_filter_to_a_date_window(self):
        conn = open_db()
        insert_deal(conn, "deal-window", pike13_person_id="person-window")
        insert_trusted_contact(conn, "contact-window", "deal-window", "7135550101")
        insert_pike13_person(conn, "person-window", phone="7135550101")
        insert_first_visit(conn, "person-window", conversion=True)
        insert_dialpad_sms(conn, "7135550101")

        insert_deal(conn, "deal-old", pike13_person_id="person-old")
        conn.execute("UPDATE hubspot_deals SET create_date = '2026-03-01', updated_at = '2026-03-01' WHERE deal_id = 'deal-old'")
        insert_trusted_contact(conn, "contact-old", "deal-old", "7135550102")
        insert_pike13_person(conn, "person-old", phone="7135550102")

        refresh_identity_matches(conn)
        rows = fetch_gap_rows(
            conn,
            "West University Place",
            20,
            start_date="2026-04-27",
            end_date="2026-05-03",
        )
        lead_refs = {row["lead_ref"] for row in rows}

        self.assertIn(next(row["lead_ref"] for row in rows if row["gap_category"] == "ready_for_review"), lead_refs)
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
