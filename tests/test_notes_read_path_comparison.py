import sqlite3
import unittest

from notesreminder.reports.notes_read_path_comparison import build_notes_read_path_comparison
from tests.test_reporting_schema import create_reminders_schema, seed_lessons


class NotesReadPathComparisonTests(unittest.TestCase):
    def test_comparison_is_ready_when_normalized_tables_match_reminders(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_reminders_schema(conn)
        seed_lessons(conn)

        report = build_notes_read_path_comparison(conn, "2026-05-01", "2026-05-01")

        self.assertEqual(report["status"], "ready")
        self.assertEqual(report["mismatch_count"], 0)
        self.assertEqual(report["base_counts"]["reminders"], 6)
        self.assertEqual(report["base_counts"]["lessons"], 6)

    def test_comparison_flags_normalized_mismatch_when_not_rebuilt(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_reminders_schema(conn)
        seed_lessons(conn)
        build_notes_read_path_comparison(conn, "2026-05-01", "2026-05-01")
        conn.execute("DELETE FROM lesson_notes WHERE lesson_id = 'lesson-1'")

        report = build_notes_read_path_comparison(
            conn,
            "2026-05-01",
            "2026-05-01",
            rebuild=False,
        )

        self.assertEqual(report["status"], "mismatch")
        self.assertGreater(report["mismatch_count"], 0)
        self.assertTrue(report["mismatches"]["base"])

    def test_daily_counts_dedupe_duplicate_lesson_display_keys_on_both_paths(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_reminders_schema(conn)
        seed_lessons(conn)
        conn.execute(
            """
            INSERT INTO reminders (
                lesson_id, school, instructor_name, lesson_date, lesson_time,
                lesson_type, students, note_completed, attendance_status,
                notes_text, note_timestamp, pike13_lesson_id, note_score
            )
            VALUES (
                'lesson-1-duplicate', 'westu-sor', 'Alex Instructor',
                '2026-05-01', '4:00 PM', 'Guitar', 'Student One',
                1, 'present', 'Duplicate note', '2026-05-01T22:00:00',
                'pike-1-duplicate', 8.0
            )
            """
        )

        report = build_notes_read_path_comparison(conn, "2026-05-01", "2026-05-01")

        self.assertEqual(report["status"], "ready")
        self.assertEqual(report["mismatch_count"], 0)


if __name__ == "__main__":
    unittest.main()
