import sqlite3
import tempfile
import unittest
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema, utc_now_iso
from scripts.rebuild_lead_working_db import rebuild_lead_working_db


def create_base_db(path, reminders):
    conn = sqlite3.connect(path)
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
            reminder_sent INTEGER DEFAULT 0,
            reminder_count INTEGER DEFAULT 0,
            note_completed INTEGER DEFAULT 0,
            attendance_status TEXT DEFAULT 'unknown',
            last_checked DATE,
            last_reminder_sent TIMESTAMP,
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
    conn.executemany(
        """
        INSERT INTO reminders (
            lesson_id, school, lesson_date, lesson_time, lesson_type, students,
            note_completed, notes_text, note_timestamp, pike13_lesson_id,
            note_score, last_checked
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        reminders,
    )
    conn.commit()
    conn.close()


def create_lead_source_db(path):
    create_base_db(
        path,
        [
            (
                "stale-proof-lesson",
                "westu-sor",
                "2026-04-01",
                "12:00 PM",
                "Trial",
                "Proof Student",
                1,
                "Old proof note",
                "2026-04-01T18:00:00",
                "proof-pike13",
                3.0,
                "2026-04-01T18:05:00",
            )
        ],
    )
    conn = sqlite3.connect(path)
    ensure_lead_followup_schema(conn)
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO source_import_runs (
            id, source, extractor, started_at, finished_at, status,
            rows_seen, rows_inserted, rows_updated
        )
        VALUES (42, 'hubspot', 'extract_hubspot_leads.py', ?, ?, 'success', 1, 1, 0)
        """,
        (now, now),
    )
    conn.execute(
        """
        INSERT INTO hubspot_deals (
            deal_id, deal_name, stage, school, create_date, source_url,
            raw_text, updated_at
        )
        VALUES ('deal-1', 'Sample Deal', 'Scheduled Trial', 'West U',
                '2026-04-29', 'https://example.test/deal-1', 'raw', ?)
        """,
        (now,),
    )
    conn.execute(
        """
        INSERT INTO hubspot_contacts (
            contact_id, full_name, phone, phone_normalized, associated_deal_ids,
            raw_json, updated_at
        )
        VALUES ('contact-1', 'Sample Contact', '(713) 555-1212', '7135551212',
                '["deal-1"]', '{"trusted": 1}', ?)
        """,
        (now,),
    )
    conn.execute(
        """
        INSERT INTO dialpad_voice_events (
            event_id, event_type, phone, phone_normalized, direction, event_at,
            school, source_url, updated_at
        )
        VALUES ('voice-1', 'call', '(713) 555-1212', '7135551212',
                'inbound', '2026-04-29T18:00:00', 'West U',
                'https://dialpad.test/callreview/1', ?)
        """,
        (now,),
    )
    conn.execute(
        """
        INSERT INTO dialpad_call_reviews (
            call_review_id, voice_event_id, call_review_url, event_at,
            transcript_text, recap_text, transcript_available, recap_available,
            audio_available, extraction_status, updated_at
        )
        VALUES ('review-1', 'voice-1', 'https://dialpad.test/callreview/1',
                '2026-04-29T18:00:00', 'transcript', 'recap', 1, 1, 1,
                'success', ?)
        """,
        (now,),
    )
    conn.commit()
    conn.close()


class RebuildLeadWorkingDbTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_rebuild_preserves_current_production_notes_and_copies_lead_tables(self):
        production = self.root / "production.db"
        proof = self.root / "proof.db"
        output = self.root / "lead_working.db"
        create_base_db(
            production,
            [
                (
                    "prod-lesson-1",
                    "westu-sor",
                    "2026-05-01",
                    "4:00 PM",
                    "Guitar",
                    "Current Student",
                    1,
                    "Current note",
                    "2026-05-01T22:00:00",
                    "prod-pike13-1",
                    4.0,
                    "2026-05-01T22:05:00",
                ),
                (
                    "prod-lesson-2",
                    "theheights-sor",
                    "2026-05-01",
                    "5:00 PM",
                    "Drums",
                    "Current Student 2",
                    0,
                    "",
                    None,
                    "prod-pike13-2",
                    None,
                    "2026-05-01T23:05:00",
                ),
            ],
        )
        create_lead_source_db(proof)

        summary = rebuild_lead_working_db(production, proof, output)

        self.assertEqual(summary["integrity"], "ok")
        self.assertEqual(summary["reminders_rows"], 2)
        conn = sqlite3.connect(output)
        self.addCleanup(conn.close)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM reminders").fetchone()[0],
            2,
        )
        self.assertIsNone(
            conn.execute(
                "SELECT 1 FROM reminders WHERE lesson_id = 'stale-proof-lesson'"
            ).fetchone()
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM hubspot_deals").fetchone()[0],
            1,
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM dialpad_call_reviews").fetchone()[0],
            1,
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM source_import_runs").fetchone()[0],
            1,
        )
        self.assertIsNotNone(
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'view' "
                "AND name = 'vw_pike13_lesson_visits'"
            ).fetchone()
        )

    def test_rebuild_can_refresh_production_notes_while_using_existing_output_as_lead_source(self):
        first_production = self.root / "production_first.db"
        second_production = self.root / "production_second.db"
        proof = self.root / "proof.db"
        output = self.root / "lead_working.db"
        create_base_db(
            first_production,
            [
                (
                    "prod-lesson-1",
                    "westu-sor",
                    "2026-05-01",
                    "4:00 PM",
                    "Guitar",
                    "Current Student",
                    1,
                    "Current note",
                    "2026-05-01T22:00:00",
                    "prod-pike13-1",
                    4.0,
                    "2026-05-01T22:05:00",
                )
            ],
        )
        create_base_db(
            second_production,
            [
                (
                    "prod-lesson-2",
                    "westu-sor",
                    "2026-05-02",
                    "4:00 PM",
                    "Guitar",
                    "Next note student",
                    1,
                    "Next note",
                    "2026-05-02T22:00:00",
                    "prod-pike13-2",
                    4.5,
                    "2026-05-02T22:05:00",
                )
            ],
        )
        create_lead_source_db(proof)

        rebuild_lead_working_db(first_production, proof, output)
        rebuild_lead_working_db(second_production, output, output)

        conn = sqlite3.connect(output)
        self.addCleanup(conn.close)
        self.assertEqual(
            conn.execute("SELECT MAX(lesson_date) FROM reminders").fetchone()[0],
            "2026-05-02",
        )
        self.assertIsNone(
            conn.execute(
                "SELECT 1 FROM reminders WHERE lesson_id = 'prod-lesson-1'"
            ).fetchone()
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM hubspot_deals").fetchone()[0],
            1,
        )

    def test_output_cannot_be_production_db(self):
        production = self.root / "production.db"
        proof = self.root / "proof.db"
        create_base_db(production, [])
        create_lead_source_db(proof)

        with self.assertRaises(ValueError):
            rebuild_lead_working_db(production, proof, production)


if __name__ == "__main__":
    unittest.main()
