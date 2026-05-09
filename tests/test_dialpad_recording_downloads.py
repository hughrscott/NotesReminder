import sqlite3
import tempfile
import unittest
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema
from scripts.download_dialpad_recordings import (
    ensure_transcription_pending,
    file_sha256,
    has_recording_action,
    recording_row,
    safe_extension,
    upsert_recording_download,
    within_window,
)


class DialpadRecordingDownloadTests(unittest.TestCase):
    def test_recording_metadata_links_asset_and_queues_transcription(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        ensure_lead_followup_schema(conn)
        parsed = {
            "event_id": "voice-1",
            "source_url": "https://dialpad.com/conversationhistory",
            "event_at": "2026-05-09T13:15:00",
            "phone_normalized": "7135551212",
            "contact_name": "Private Contact",
            "school": "West U",
            "raw_json": '{"duration": "1m 2s"}',
        }
        row = recording_row(
            parsed,
            "call-1",
            "success",
            "https://dialpad.test/recording",
            "/private/call-1.mp3",
            "abc123",
            12345,
            None,
        )

        upsert_recording_download(conn, row)
        ensure_transcription_pending(conn, "call-1", "https://dialpad.test/recording", "1m 2s")

        asset = conn.execute("SELECT * FROM recording_downloads WHERE call_id = 'call-1'").fetchone()
        transcript = conn.execute("SELECT * FROM recording_transcripts WHERE call_id = 'call-1'").fetchone()
        self.assertEqual(asset["voice_event_id"], "voice-1")
        self.assertEqual(asset["status"], "success")
        self.assertEqual(asset["transcription_status"], "pending")
        self.assertEqual(asset["file_sha256"], "abc123")
        self.assertEqual(transcript["transcript_status"], "pending")
        self.assertEqual(transcript["recording_duration"], "1m 2s")

    def test_recording_helpers_are_stable(self):
        self.assertEqual(safe_extension("call.MP3"), ".mp3")
        self.assertEqual(safe_extension("download"), ".mp3")
        self.assertTrue(within_window({"event_at": "2026-05-09T10:00:00"}, "2026-05-01", "2026-05-10"))
        self.assertFalse(within_window({"event_at": "2026-04-30T10:00:00"}, "2026-05-01", "2026-05-10"))
        self.assertTrue(has_recording_action({"raw_json": '{"recording_action_visible": true}'}))
        self.assertFalse(has_recording_action({"raw_json": '{"recording_action_visible": false}'}))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.bin"
            path.write_bytes(b"recording")
            self.assertEqual(
                file_sha256(path),
                "3ebb153fb24e4411400e94a9a92b0ec458c3a8473e51e03cd37d4a34c99dfda6",
            )


if __name__ == "__main__":
    unittest.main()
