import sqlite3
import tempfile
import unittest
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema, utc_now_iso
from notesreminder.schema.lead_intel_migration import migrate_lead_intelligence


def create_production_db(path):
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE reminders (
            id INTEGER PRIMARY KEY,
            lesson_id TEXT,
            school TEXT,
            lesson_date TEXT,
            note_score REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE call_logs (
            call_id TEXT PRIMARY KEY,
            external_number TEXT,
            date_started TEXT
        )
        """
    )
    ensure_lead_followup_schema(conn)
    conn.execute(
        """
        INSERT INTO reminders (lesson_id, school, lesson_date, note_score)
        VALUES ('current-lesson', 'West U', '2026-05-20', 9.0)
        """
    )
    conn.execute(
        """
        INSERT INTO call_logs (call_id, external_number, date_started)
        VALUES ('call-prod', '7135550000', '2026-05-20T10:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO recording_downloads (call_id, status, file_path)
        VALUES ('shared-call', 'success', '/prod/audio.mp3')
        """
    )
    conn.commit()
    conn.close()


def create_lead_db(path):
    create_production_db(path)
    conn = sqlite3.connect(path)
    ensure_lead_followup_schema(conn)
    now = utc_now_iso()
    conn.execute("DELETE FROM reminders")
    conn.execute(
        """
        INSERT INTO reminders (lesson_id, school, lesson_date, note_score)
        VALUES ('stale-lead-lesson', 'West U', '2026-04-01', 3.0)
        """
    )
    conn.execute(
        """
        INSERT INTO source_import_runs (
            id, source, extractor, started_at, finished_at, status, rows_seen
        )
        VALUES (11, 'hubspot', 'proof', ?, ?, 'success', 1)
        """,
        (now, now),
    )
    conn.execute(
        """
        INSERT INTO hubspot_deals (
            deal_id, deal_name, stage, school, updated_at
        )
        VALUES ('deal-1', 'Lead One', 'Scheduled Trial', 'West U', ?)
        """,
        (now,),
    )
    conn.execute(
        """
        INSERT INTO dialpad_sms_threads (
            thread_id, phone_normalized, school, updated_at
        )
        VALUES ('thread-1', '7135551212', 'West U', ?)
        """,
        (now,),
    )
    conn.execute(
        """
        INSERT INTO dialpad_sms_messages (
            message_id, thread_id, message_at, direction, body, updated_at
        )
        VALUES ('message-1', 'thread-1', ?, 'inbound', 'private', ?)
        """,
        (now, now),
    )
    conn.execute(
        """
        UPDATE recording_downloads
        SET voice_event_id = 'voice-1', file_sha256 = 'source-sha'
        WHERE call_id = 'shared-call'
        """
    )
    conn.execute(
        """
        INSERT INTO recording_downloads (call_id, status, file_path, voice_event_id)
        VALUES ('lead-only-call', 'success', '/lead/audio.mp3', 'voice-2')
        """
    )
    conn.commit()
    conn.close()


class LeadIntelMigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_copy_mode_reconciles_lead_tables_without_changing_production_tables(self):
        production = self.root / "production.db"
        lead = self.root / "lead.db"
        output = self.root / "unified.db"
        create_production_db(production)
        create_lead_db(lead)

        summary = migrate_lead_intelligence(production, lead, output_db=output)

        self.assertEqual(summary["status"], "ready")
        self.assertEqual(summary["integrity"], "ok")
        self.assertEqual(summary["production_count_changes"], {})
        conn = sqlite3.connect(output)
        self.addCleanup(conn.close)
        self.assertEqual(
            conn.execute("SELECT lesson_id FROM reminders").fetchone()[0],
            "current-lesson",
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM call_logs").fetchone()[0],
            1,
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM hubspot_deals").fetchone()[0],
            1,
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM dialpad_sms_messages").fetchone()[0],
            1,
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM recording_downloads").fetchone()[0],
            2,
        )
        self.assertEqual(
            conn.execute(
                "SELECT voice_event_id FROM recording_downloads WHERE call_id = 'shared-call'"
            ).fetchone()[0],
            "voice-1",
        )

    def test_migration_is_idempotent_against_unified_output(self):
        production = self.root / "production.db"
        lead = self.root / "lead.db"
        output = self.root / "unified.db"
        create_production_db(production)
        create_lead_db(lead)

        migrate_lead_intelligence(production, lead, output_db=output)
        second = migrate_lead_intelligence(output, lead, output_db=output)

        self.assertEqual(second["status"], "ready")
        conn = sqlite3.connect(output)
        self.addCleanup(conn.close)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM hubspot_deals").fetchone()[0],
            1,
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM recording_downloads").fetchone()[0],
            2,
        )

    def test_requires_explicit_output_or_in_place_mode(self):
        production = self.root / "production.db"
        lead = self.root / "lead.db"
        create_production_db(production)
        create_lead_db(lead)

        with self.assertRaises(ValueError):
            migrate_lead_intelligence(production, lead)


if __name__ == "__main__":
    unittest.main()
