import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from date_window_lead_load import (
    DEFAULT_DB,
    build_date_window_report,
    render_date_window_markdown,
    report_to_json,
    validate_target_db,
    validate_window,
)
from lead_followup_schema import ensure_lead_followup_schema


class DateWindowLeadLoadTests(unittest.TestCase):
    def test_validate_window_rejects_bad_order_and_bad_dates(self):
        self.assertEqual(validate_window("2026-04-27", "2026-05-03")[0].isoformat(), "2026-04-27")
        with self.assertRaises(ValueError):
            validate_window("2026-05-03", "2026-04-27")
        with self.assertRaises(ValueError):
            validate_window("04/27/2026", "2026-05-03")

    def test_validate_target_db_rejects_production_and_unexpected_paths(self):
        root = Path("/tmp/notes-reminder-test")
        with self.assertRaises(ValueError):
            validate_target_db(root / "reminders.db", root=root)
        with self.assertRaises(ValueError):
            validate_target_db(root / "some_other.db", root=root)

        expected = root / "outputs" / "lead_intelligence" / "lead_intelligence_working.db"
        self.assertEqual(validate_target_db(expected, root=root), expected.resolve())

    def test_date_window_report_is_sanitized_and_counts_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "lead_intelligence_working.db"
            conn = sqlite3.connect(db)
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
            conn.execute(
                """
                INSERT INTO reminders (
                    lesson_id, school, lesson_date, location, notes_text
                )
                VALUES ('lesson-1', 'westu-sor', '2026-04-29', 'West U', 'customer note body')
                """
            )
            conn.execute(
                """
                INSERT INTO hubspot_deals (
                    deal_id, deal_name, stage, school, create_date, updated_at
                )
                VALUES ('deal-private-name', 'Private Customer Name', 'Contacted',
                        'West University Place', '2026-04-29', '2026-05-01T00:00:00+00:00')
                """
            )
            conn.execute(
                """
                INSERT INTO pike13_people (
                    person_id, full_name, school, updated_at
                )
                VALUES ('person-1', 'Private Customer Name', 'West U', '2026-05-01T00:00:00+00:00')
                """
            )
            conn.execute(
                """
                INSERT INTO pike13_visits (
                    visit_id, person_id, service, starts_at, status, first_visit_flag, school, updated_at
                )
                VALUES ('visit-1', 'person-1', 'Trial - Guitar', '2026-04-29T12:00:00',
                        'Complete', 1, 'West U', '2026-05-01T00:00:00+00:00')
                """
            )
            conn.execute(
                """
                INSERT INTO dialpad_sms_threads (
                    thread_id, phone, phone_normalized, contact_name, school, updated_at
                )
                VALUES ('thread-1', '7135551212', '7135551212', 'Private Customer Name',
                        'West University Place', '2026-05-01T00:00:00+00:00')
                """
            )
            conn.execute(
                """
                INSERT INTO dialpad_sms_messages (
                    message_id, thread_id, message_at, direction, body, updated_at
                )
                VALUES ('msg-1', 'thread-1', '2026-04-29T13:00:00', 'inbound',
                        'private message body', '2026-05-01T00:00:00+00:00')
                """
            )
            conn.commit()
            conn.close()

            report = build_date_window_report(
                db,
                "2026-04-27",
                "2026-05-03",
                "West University Place",
                steps=[
                    {
                        "name": "pike13",
                        "status": "success",
                        "returncode": 0,
                        "duration_seconds": 1.0,
                        "stdout_tail": "loaded rows",
                        "stderr_tail": "",
                        "error": "",
                    }
                ],
            )
            markdown = render_date_window_markdown(report)
            payload = json.loads(report_to_json(report))

            self.assertEqual(payload["source_counts"]["hubspot"]["rows_in_window"], 1)
            self.assertEqual(payload["source_counts"]["pike13"]["rows_in_window"], 1)
            self.assertEqual(payload["source_counts"]["dialpad"]["rows_in_window"], 1)
            self.assertEqual(payload["source_counts"]["notes"]["rows_in_window"], 1)
            self.assertIn("Date-Window Lead Load Report", markdown)
            self.assertNotIn("Private Customer Name", markdown)
            self.assertNotIn("7135551212", markdown)
            self.assertNotIn("private message body", markdown)
            self.assertNotIn("customer note body", markdown)


if __name__ == "__main__":
    unittest.main()
