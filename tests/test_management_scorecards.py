import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import mcp_server
from build_reporting_schema import backfill_reporting
from notesreminder.reports.management_scorecards import (
    build_note_quality_scorecard_for_period,
    render_scorecard_markdown,
    window_for_period,
)


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
            location TEXT,
            note_completed INTEGER,
            attendance_status TEXT,
            notes_text TEXT,
            note_timestamp TEXT,
            pike13_lesson_id TEXT,
            note_score REAL,
            note_score_explanation TEXT,
            note_score_model TEXT,
            note_score_version TEXT,
            note_score_updated_at TEXT,
            note_score_hash TEXT,
            last_checked TEXT
        )
        """
    )


def seed_scorecard_data(conn):
    rows = [
        ("w-alex-1", "westu-sor", "Alex Instructor", "2026-05-01", "Student One", 1, 8.0),
        ("w-alex-2", "westu-sor", "Alex Instructor", "2026-05-02", "Student Two", 0, None),
        ("w-blake-1", "westu-sor", "Blake Instructor", "2026-05-03", "Student Three", 1, 10.0),
        ("w-blake-2", "westu-sor", "Blake Instructor", "2026-05-04", "Student Four", 1, 6.0),
        ("h-casey-1", "theheights-sor", "Casey Instructor", "2026-05-05", "Student Five", 1, 10.0),
        ("w-group", "westu-sor", "Alex Instructor", "2026-05-06", "Student Six, Student Seven", 1, 10.0),
        ("w-admin", "westu-sor", "Admin Team", "2026-05-07", "Student Eight", 1, 10.0),
    ]
    conn.executemany(
        """
        INSERT INTO reminders (
            lesson_id, school, instructor_name, lesson_date, lesson_time,
            lesson_type, students, note_completed, attendance_status,
            notes_text, note_timestamp, pike13_lesson_id, note_score, last_checked
        )
        VALUES (?, ?, ?, ?, '4:00 PM', 'Guitar', ?, ?, 'present',
                'Lesson note', '2026-05-01T22:00:00', ?, ?, '2026-05-08')
        """,
        [(lesson_id, school, instructor, lesson_date, students, completed, lesson_id, score) for lesson_id, school, instructor, lesson_date, students, completed, score in rows],
    )
    backfill_reporting(conn)


class ManagementScorecardTests(unittest.TestCase):
    def test_note_quality_scorecard_ranks_schools_and_instructors(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_reminders_schema(conn)
        seed_scorecard_data(conn)

        scorecard = build_note_quality_scorecard_for_period(
            conn,
            period="custom",
            start_date="2026-05-01",
            end_date="2026-05-31",
        )
        self.assertEqual(scorecard["status"], "ready")
        self.assertFalse(scorecard["sensitive_content_included"])
        self.assertEqual(
            [(row["school_code"], row["rank"], row["league_score"]) for row in scorecard["school_league"]],
            [("theheights-sor", 1, 100.0), ("westu-sor", 2, 60.0)],
        )
        self.assertEqual(
            [
                (row["instructor_name"], row["rank"], row["reportable_lessons"], row["league_score"])
                for row in scorecard["instructor_league"]
            ],
            [
                ("Casey Instructor", 1, 1, 100.0),
                ("Blake Instructor", 2, 2, 80.0),
                ("Alex Instructor", 3, 2, 40.0),
            ],
        )
        west_u = next(row for row in scorecard["school_league"] if row["school_code"] == "westu-sor")
        self.assertEqual(west_u["score_sum"], 2.4)
        self.assertEqual(west_u["zero_inclusive_average_note_score"], 6.0)

    def test_school_filter_and_markdown_are_sanitized(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        create_reminders_schema(conn)
        seed_scorecard_data(conn)

        scorecard = build_note_quality_scorecard_for_period(
            conn,
            period="mtd",
            as_of="2026-05-23",
            school="West U",
        )
        self.assertEqual(len(scorecard["school_league"]), 1)
        self.assertEqual(scorecard["school_league"][0]["school_code"], "westu-sor")
        self.assertEqual(
            [row["instructor_name"] for row in scorecard["instructor_league"]],
            ["Blake Instructor", "Alex Instructor"],
        )
        markdown = render_scorecard_markdown(scorecard)
        self.assertIn("Note Quality Scorecard", markdown)
        self.assertIn("Blake Instructor", markdown)
        self.assertNotIn("Student One", markdown)
        self.assertNotIn("Lesson note", markdown)

    def test_period_windows_are_deterministic(self):
        self.assertEqual(window_for_period("mtd", "2026-05-23"), ("2026-05-01", "2026-05-23"))
        self.assertEqual(window_for_period("prior-week", "2026-05-23"), ("2026-05-11", "2026-05-17"))
        self.assertEqual(window_for_period("prior-month", "2026-05-23"), ("2026-04-01", "2026-04-30"))

    def test_mcp_scorecard_uses_shared_logic(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "scorecards.db"
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        create_reminders_schema(conn)
        seed_scorecard_data(conn)
        conn.commit()
        conn.close()

        original_path = mcp_server.LEAD_DB_PATH
        mcp_server.LEAD_DB_PATH = str(db_path)
        self.addCleanup(setattr, mcp_server, "LEAD_DB_PATH", original_path)

        mcp_scorecard = json.loads(
            mcp_server.note_quality_scorecard(period="mtd", as_of="2026-05-23", school="West U")
        )
        direct_conn = sqlite3.connect(db_path)
        direct_conn.row_factory = sqlite3.Row
        direct_scorecard = build_note_quality_scorecard_for_period(
            direct_conn,
            period="mtd",
            as_of="2026-05-23",
            school="West U",
        )
        direct_conn.close()
        self.assertEqual(mcp_scorecard["school_league"], direct_scorecard["school_league"])
        self.assertEqual(mcp_scorecard["instructor_league"], direct_scorecard["instructor_league"])


if __name__ == "__main__":
    unittest.main()
