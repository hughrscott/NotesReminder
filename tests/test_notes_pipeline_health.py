import sqlite3
import tempfile
import unittest
from pathlib import Path

from notesreminder.reports.notes_pipeline_health import (
    build_notes_pipeline_health,
    is_reportable_lesson,
    render_markdown,
    scan_notes_send_logs,
)


def make_db():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE reminders (
            school TEXT,
            instructor_name TEXT,
            lesson_date TEXT,
            lesson_time TEXT,
            lesson_type TEXT,
            students TEXT,
            note_completed INTEGER,
            last_checked TEXT
        )
        """
    )
    return conn


class NotesPipelineHealthTests(unittest.TestCase):
    def test_reportable_lesson_filter_excludes_group_and_admin_rows(self):
        self.assertTrue(is_reportable_lesson("Guitar Lessons - 45 minutes", "Student One", "Teacher Name"))
        self.assertFalse(is_reportable_lesson("Guitar Lessons - 45 minutes", "A, B", "Teacher Name"))
        self.assertFalse(is_reportable_lesson("Admin Time", "Student One", "Teacher Name"))
        self.assertFalse(is_reportable_lesson("Guitar Lessons - 45 minutes", "Student One", "Admin User"))
        self.assertFalse(is_reportable_lesson("Guitar Lessons - 45 minutes", "Student One", ""))

    def test_scan_notes_send_logs_detects_delivered_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "send.log"
            path.write_text(
                "Sending email 'Lesson notes summary for West U (2026-05-19 to 2026-05-19)' to x\n"
                "Email delivered to SMTP server\n"
            )
            result = scan_notes_send_logs(tmpdir)
        self.assertEqual(result["westu-sor"]["2026-05-19"]["status"], "delivered")

    def test_build_health_report_counts_recent_school_coverage(self):
        conn = make_db()
        rows = [
            ("westu-sor", "Teacher A", "2026-05-19", "3pm", "Guitar Lessons - 45 minutes", "Student A", 1, "2026-05-20"),
            ("westu-sor", "Teacher B", "2026-05-19", "4pm", "Guitar Lessons - 45 minutes", "Student B", 0, "2026-05-20"),
            ("westu-sor", "Teacher C", "2026-05-19", "5pm", "Guitar Lessons - 45 minutes", "A, B", 0, "2026-05-20"),
            ("theheights-sor", "Teacher D", "2026-05-19", "3pm", "Drum Lessons -  45 minutes", "Student D", 1, "2026-05-20"),
        ]
        conn.executemany("INSERT INTO reminders VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "send.log").write_text(
                "Sending email 'Lesson notes summary for West U (2026-05-19 to 2026-05-19)' to x\n"
                "Email delivered to SMTP server\n"
            )
            report = build_notes_pipeline_health(
                conn,
                as_of="2026-05-20",
                lookback_days=1,
                logs_dir=tmpdir,
            )
        westu = next(item for item in report["schools"] if item["school"] == "westu-sor")
        self.assertEqual(westu["window_total_lessons"], 3)
        self.assertEqual(westu["window_reportable_lessons"], 2)
        self.assertEqual(westu["window_missing_notes"], 1)
        self.assertEqual(westu["days"][0]["email_status"], "delivered")
        self.assertIn("Notes Pipeline Health", render_markdown(report))


if __name__ == "__main__":
    unittest.main()
