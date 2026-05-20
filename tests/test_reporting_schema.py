import sqlite3
import unittest

from build_reporting_schema import backfill_reporting


def create_reminders_schema(conn):
    conn.execute(
        """
        CREATE TABLE reminders (
            id INTEGER PRIMARY KEY,
            lesson_id TEXT,
            school TEXT,
            instructor_name TEXT,
            lesson_date TEXT,
            lesson_time TEXT,
            lesson_type TEXT,
            students TEXT,
            reminder_sent INTEGER,
            reminder_count INTEGER,
            note_completed INTEGER,
            attendance_status TEXT,
            last_checked TEXT,
            last_reminder_sent TEXT,
            location TEXT,
            notes_text TEXT,
            note_timestamp TEXT,
            pike13_lesson_id TEXT,
            note_score REAL,
            note_score_explanation TEXT,
            note_score_model TEXT,
            note_score_version TEXT,
            note_score_updated_at TEXT,
            note_score_hash TEXT
        )
        """
    )


def seed_lessons(conn):
    rows = [
        (
            "lesson-1",
            "westu-sor",
            "Alex Instructor",
            "2026-05-01",
            "4:00 PM",
            "Guitar",
            "Student One",
            1,
            "present",
            "Good note",
            "2026-05-01T22:00:00",
            "pike-1",
            8.0,
        ),
        (
            "lesson-2",
            "westu-sor",
            "Alex Instructor",
            "2026-05-01",
            "5:00 PM",
            "Drums",
            "Student Two",
            0,
            "present",
            "",
            None,
            "pike-2",
            None,
        ),
        (
            "lesson-group",
            "westu-sor",
            "Alex Instructor",
            "2026-05-01",
            "6:00 PM",
            "Guitar",
            "Student Three, Student Four",
            0,
            "present",
            "",
            None,
            "pike-3",
            None,
        ),
        (
            "lesson-admin",
            "westu-sor",
            "Alex Instructor",
            "2026-05-01",
            "7:00 PM",
            "Admin",
            "Student Five",
            0,
            "present",
            "",
            None,
            "pike-4",
            None,
        ),
        (
            "lesson-trial-instructor",
            "westu-sor",
            "Trial Team",
            "2026-05-01",
            "8:00 PM",
            "Voice",
            "Student Six",
            0,
            "present",
            "",
            None,
            "pike-5",
            None,
        ),
        (
            "lesson-3",
            "theheights-sor",
            "Bailey Instructor",
            "2026-05-01",
            "4:00 PM",
            "Bass",
            "Student Seven",
            1,
            "present",
            "Great note",
            "2026-05-01T22:00:00",
            "pike-6",
            10.0,
        ),
    ]
    conn.executemany(
        """
        INSERT INTO reminders (
            lesson_id, school, instructor_name, lesson_date, lesson_time,
            lesson_type, students, note_completed, attendance_status, notes_text,
            note_timestamp, pike13_lesson_id, note_score
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def seed_call_and_client_data(conn):
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
    conn.executemany(
        """
        INSERT INTO call_logs (
            call_id, external_number, date_started, direction, category,
            school_code, school_name, voicemail_transcript
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "in-1",
                "7135551212",
                "2026-05-01T10:00:00",
                "inbound",
                "missed",
                "westu-sor",
                "West U",
                "",
            ),
            (
                "out-1",
                "7135551212",
                "2026-05-01T11:00:00",
                "outbound",
                "call",
                "westu-sor",
                "West U",
                "",
            ),
        ],
    )
    conn.execute(
        """
        CREATE TABLE pike13_clients (
            "Client" TEXT,
            "Client ID" TEXT,
            "Client Home Location" TEXT,
            "Last Completed Visit Date" TEXT,
            "Completed Visits" TEXT,
            "Future Visits" TEXT,
            "Current Passes/Plans" TEXT,
            "Has Plan on Hold?" TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO pike13_clients (
            "Client", "Client ID", "Client Home Location",
            "Last Completed Visit Date", "Completed Visits", "Future Visits",
            "Current Passes/Plans", "Has Plan on Hold?"
        )
        VALUES ('Private Student', 'client-1', 'West U', '2026-04-01', '4', '0', '', 'No')
        """
    )


class ReportingSchemaTests(unittest.TestCase):
    def test_reporting_tables_views_and_note_quality_are_idempotent(self):
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        create_reminders_schema(conn)
        seed_lessons(conn)
        seed_call_and_client_data(conn)

        backfill_reporting(conn)
        first_counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ["lessons", "lesson_students", "lesson_notes", "lesson_attendance"]
        }
        backfill_reporting(conn)
        second_counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ["lessons", "lesson_students", "lesson_notes", "lesson_attendance"]
        }

        self.assertEqual(first_counts, second_counts)
        self.assertEqual(second_counts["lessons"], 6)
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) FROM lessons WHERE lesson_is_reportable = 1"
            ).fetchone()[0],
            3,
        )
        self.assertEqual(
            conn.execute(
                """
                SELECT total_reportable_lessons, missing_notes, completed_notes, completion_rate
                FROM vw_missing_notes_by_instructor
                WHERE school_code = 'westu-sor' AND instructor_name = 'Alex Instructor'
                """
            ).fetchone(),
            (2, 1, 1, 50.0),
        )
        self.assertEqual(
            conn.execute(
                """
                SELECT total_reportable_lessons, missing_notes, completed_notes
                FROM vw_missing_notes_by_school_day
                WHERE school_code = 'westu-sor' AND lesson_date = '2026-05-01'
                """
            ).fetchone(),
            (2, 1, 1),
        )
        self.assertEqual(
            conn.execute(
                """
                SELECT total_reportable_lessons, lessons_with_notes, scored_lessons,
                       missing_notes, league_score
                FROM vw_note_quality_league_table
                WHERE school_code = 'westu-sor'
                  AND instructor_name = 'Alex Instructor'
                  AND score_month = '2026-05'
                """
            ).fetchone(),
            (2, 1, 1, 1, 40.0),
        )
        self.assertEqual(
            conn.execute(
                """
                SELECT total_reportable_lessons, lessons_with_notes, scored_lessons,
                       missing_notes, league_score
                FROM vw_note_quality_league_table
                WHERE school_code = 'theheights-sor'
                  AND instructor_name = 'Bailey Instructor'
                  AND score_month = '2026-05'
                """
            ).fetchone(),
            (1, 1, 1, 0, 100.0),
        )
        self.assertEqual(
            conn.execute("SELECT callback_hours FROM vw_callback_speed").fetchone()[0],
            1.0,
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM vw_churn_candidates").fetchone()[0],
            1,
        )


if __name__ == "__main__":
    unittest.main()
