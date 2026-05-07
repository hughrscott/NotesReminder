import sqlite3
import unittest

from lead_followup_schema import ensure_lead_followup_schema
from scripts.pike13_outcome_validation_report import (
    classify_outcome,
    fetch_validation_rows,
    render_report,
    summarize_rows,
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


class Pike13OutcomeValidationReportTests(unittest.TestCase):
    def test_classify_outcome_uses_trial_and_conversion_signals(self):
        self.assertEqual(
            classify_outcome({"starts_at": "2026-05-11T14:45:00", "status": "Enrolled", "enrolled_flag": 1}, today="2026-05-06"),
            "scheduled",
        )
        self.assertEqual(
            classify_outcome({"starts_at": "2026-04-15T19:15:00", "status": "Complete", "attendance_confirmed_flag": 1}, today="2026-05-06"),
            "attended-not-converted",
        )
        self.assertEqual(
            classify_outcome({"starts_at": "2026-04-15T19:15:00", "status": "Complete", "has_conversion_plan": 1}, today="2026-05-06"),
            "converted",
        )
        self.assertEqual(classify_outcome({"status": "Late Cancel"}, today="2026-05-06"), "canceled")
        self.assertEqual(classify_outcome({"status": "No Show"}, today="2026-05-06"), "no-show")

    def test_validation_report_is_sanitized_and_counts_outcomes(self):
        conn = open_db()
        conn.execute(
            """
            INSERT INTO pike13_people (person_id, full_name, email, phone, school, updated_at)
            VALUES ('person-1', 'Francisco Fallon', 'francisco@example.com', '7135551212', 'West U', '2026-05-06T00:00:00+00:00'),
                   ('person-2', 'Jose Alfonso', 'jose@example.com', '3055829682', 'West U', '2026-05-06T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO pike13_visits (
                visit_id, person_id, service, instructor, starts_at, status, first_visit_flag,
                attendance_confirmed_flag, checked_in_flag, enrolled_flag, waiver_flag,
                terms_accepted_flag, school, updated_at
            )
            VALUES
                ('visit-1', 'person-1', 'Trial - Guitar', 'Alexander Peterson', '2026-04-15T19:15:00', 'Complete', 1, 1, 1, 0, 0, 0, 'West U', '2026-05-06T00:00:00+00:00'),
                ('visit-2', 'person-2', 'Trial - Vocals', 'Iman Qureshi', '2026-05-11T14:45:00', 'Enrolled', 1, 0, 0, 1, 1, NULL, 'West U', '2026-05-06T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO pike13_plans_passes (
                plan_pass_id, person_id, name, status, starts_at, payer_name,
                next_invoice_at, terms_accepted_flag, school, updated_at
            )
            VALUES ('plan-1', 'person-1', 'Lessons Only - 45 Minute Lessons', 'Active',
                    '2026-05-01', 'James Fallon', '2026-06-01', 0, 'West U', '2026-05-06T00:00:00+00:00')
            """
        )

        rows = fetch_validation_rows(conn, "West U", "2026-04-01", "2026-05-12", 20)
        summary, classified = summarize_rows(rows, today="2026-05-06")
        markdown = render_report(summary, classified, "West U", "2026-04-01", "2026-05-12")

        self.assertEqual(summary["total"], 2)
        self.assertEqual(summary["converted"], 1)
        self.assertEqual(summary["scheduled"], 1)
        self.assertIn("conversion plan", markdown)
        self.assertIn("terms not accepted", markdown)
        self.assertIn("waiver", markdown)
        self.assertNotIn("Francisco Fallon", markdown)
        self.assertNotIn("francisco@example.com", markdown)
        self.assertNotIn("7135551212", markdown)


if __name__ == "__main__":
    unittest.main()
