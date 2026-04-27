import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema
from run_daily import get_lessons_without_notes


class NotesPipelineIsolationTests(unittest.TestCase):
    def test_missing_notes_selection_ignores_additive_lead_tables(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        original_cwd = os.getcwd()
        self.addCleanup(os.chdir, original_cwd)
        os.chdir(tmp.name)

        db_path = Path(tmp.name) / "reminders.db"
        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE reminders (
                lesson_id TEXT PRIMARY KEY,
                instructor_name TEXT,
                lesson_date TEXT,
                lesson_time TEXT,
                lesson_type TEXT,
                students TEXT,
                location TEXT,
                note_completed INTEGER,
                school TEXT
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
                lesson_id, instructor_name, lesson_date, lesson_time,
                lesson_type, students, location, note_completed, school
            )
            VALUES
                ('westu-sor-lesson-1', 'Instructor A', '2026-04-20', '4:00 PM',
                 'Private Lesson', 'Student One', 'Room 1', 0, 'westu-sor'),
                ('westu-sor-lesson-2', 'Instructor B', '2026-04-20', '5:00 PM',
                 'Private Lesson', 'Student Two', 'Room 2', 1, 'westu-sor'),
                ('theheights-sor-lesson-1', 'Instructor C', '2026-04-20', '6:00 PM',
                 'Private Lesson', 'Student Three', 'Room 3', 0, 'theheights-sor')
            """
        )
        conn.execute(
            """
            INSERT INTO hubspot_deals (
                deal_id, deal_name, stage, school, create_date, source_url, raw_text, updated_at
            )
            VALUES (
                'deal-1', 'Lead One | West University Place', 'Scheduled Trial/Tour',
                'West University Place', 'Apr 20, 2026 at 9:00 AM CDT',
                'https://hubspot.example/deal-1', 'raw lead text', '2026-04-20T14:00:00+00:00'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO dialpad_sms_threads (
                thread_id, phone, phone_normalized, contact_name, school, updated_at
            )
            VALUES (
                'thread-1', '(713) 555-1212', '7135551212',
                'Lead One', 'West University Place', '2026-04-20T14:00:00+00:00'
            )
            """
        )
        conn.commit()
        conn.close()

        missing = get_lessons_without_notes(
            "westu-sor",
            start_date="2026-04-20",
            end_date="2026-04-20",
        )

        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0][0], "westu-sor-lesson-1")
        self.assertEqual(missing[0][5], "Student One")


if __name__ == "__main__":
    unittest.main()
