import json
import sqlite3
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema
from notesreminder.lib.raw_capture import prune_old_raw_captures, write_raw_capture


class RawCaptureReplayTests(unittest.TestCase):
    def test_raw_capture_writes_file_and_metadata(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        ensure_lead_followup_schema(conn)

        capture = write_raw_capture(
            conn,
            source="hubspot",
            capture_type="hubspot_deal_text",
            content="Deal name\nExample Lead",
            source_url="https://example.test/deal/123",
            metadata={"deal_id": "123"},
            raw_root=Path(tmp.name) / "raw",
            extension="txt",
            label="deal-123",
        )
        row = conn.execute("SELECT * FROM raw_captures WHERE capture_id = ?", (capture["capture_id"],)).fetchone()

        self.assertTrue(Path(capture["file_path"]).exists())
        self.assertEqual(row["source"], "hubspot")
        self.assertEqual(row["capture_type"], "hubspot_deal_text")
        self.assertEqual(row["parse_status"], "captured")
        self.assertEqual(len(row["content_sha256"]), 64)

    def test_replay_hubspot_deal_text_into_scratch_db(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        source_db = Path(tmp.name) / "source.db"
        scratch_db = Path(tmp.name) / "scratch.db"
        raw_root = Path(tmp.name) / "raw"
        conn = sqlite3.connect(source_db)
        conn.row_factory = sqlite3.Row
        ensure_lead_followup_schema(conn)
        write_raw_capture(
            conn,
            source="hubspot",
            capture_type="hubspot_deal_text",
            content="""
Deal name
Example Lead | West U
Deal Stage
Scheduled Trial/Tour
Deal owner
Calvin Barnhill
School
West U
Create Date
2026-05-01
Last Activity Date
2026-05-02
Trial Date
2026-05-08
""",
            source_url="https://app.hubspot.com/record/0-3/123",
            metadata={"deal_id": "123"},
            raw_root=raw_root,
            extension="txt",
            label="deal-123",
        )
        conn.commit()
        conn.close()

        result = subprocess.run(
            [
                "venv/bin/python",
                "scripts/replay_parse.py",
                "--source-db",
                str(source_db),
                "--scratch-db",
                str(scratch_db),
                "--capture-type",
                "hubspot_deal_text",
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["captures_seen"], 1)
        scratch = sqlite3.connect(scratch_db)
        self.addCleanup(scratch.close)
        deal = scratch.execute("SELECT deal_id, school FROM hubspot_deals").fetchone()
        self.assertEqual(deal, ("123", "West U"))

        source = sqlite3.connect(source_db)
        self.addCleanup(source.close)
        status = source.execute("SELECT parse_status FROM raw_captures").fetchone()[0]
        self.assertEqual(status, "replayed")

    def test_retention_dry_run_identifies_old_files_without_deleting(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        ensure_lead_followup_schema(conn)
        capture = write_raw_capture(
            conn,
            source="dialpad",
            capture_type="dialpad_conversation_history_text",
            content="old payload",
            raw_root=Path(tmp.name) / "raw",
            captured_at="2026-01-01T00:00:00+00:00",
        )
        result = prune_old_raw_captures(
            conn,
            retention_days=90,
            now=datetime(2026, 5, 23, tzinfo=timezone.utc),
            dry_run=True,
        )
        self.assertEqual(result["matched"], 1)
        self.assertTrue(Path(capture["file_path"]).exists())


if __name__ == "__main__":
    unittest.main()
