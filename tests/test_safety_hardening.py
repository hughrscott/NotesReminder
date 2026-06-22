import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import mcp_server
from notesreminder.extractors.call_data import run_import
from notesreminder.lib.phone import normalize_phone
from notesreminder.schema.init_db import initialize_db
from notesreminder.transcription.analyze_transcripts_openai import call_with_backoff
from scripts.db_guard import verify_replace


ROOT = Path(__file__).resolve().parents[1]


def test_mcp_query_sql_uses_read_only_connection(tmp_path):
    db_path = tmp_path / "readonly.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE reminders (lesson_id TEXT PRIMARY KEY)")
    conn.execute("INSERT INTO reminders VALUES ('lesson-1')")
    conn.commit()
    conn.close()

    original_path = mcp_server.DB_PATH
    mcp_server.DB_PATH = str(db_path)
    try:
        payload = json.loads(mcp_server.query_sql("SELECT lesson_id FROM reminders"))
        assert payload["rows"] == [["lesson-1"]]

        try:
            mcp_server.query_sql(
                "WITH deleted AS (DELETE FROM reminders RETURNING lesson_id) "
                "SELECT lesson_id FROM deleted"
            )
        except sqlite3.Error:
            pass
        else:
            raise AssertionError("Writable CTE unexpectedly succeeded through query_sql")
    finally:
        mcp_server.DB_PATH = original_path

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM reminders").fetchone()[0] == 1
    finally:
        conn.close()


def test_initialize_db_honors_requested_path_and_includes_writer_columns(tmp_path):
    db_path = tmp_path / "custom-reminders.db"
    initialize_db(db_path)

    assert db_path.exists()
    assert not (tmp_path / "reminders.db").exists()

    conn = sqlite3.connect(db_path)
    try:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(reminders)").fetchall()
        }
    finally:
        conn.close()

    assert {
        "location",
        "notes_text",
        "note_timestamp",
        "pike13_lesson_id",
        "note_score",
        "note_score_explanation",
        "note_score_model",
        "note_score_version",
        "note_score_updated_at",
        "note_score_hash",
    }.issubset(columns)


def test_run_daily_init_db_refuses_default_s3_upload_without_confirmation(tmp_path):
    db_path = tmp_path / "fresh.db"
    completed = subprocess.run(
        [
            sys.executable,
            "run_daily.py",
            "--init-db",
            "--no-email",
            "--db-path",
            str(db_path),
        ],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )

    assert completed.returncode != 0
    assert db_path.exists()
    assert "--confirm-init-db-upload" in completed.stderr


def test_call_data_import_is_idempotent_and_aggregates_call_logs(tmp_path):
    db_path = tmp_path / "calls.db"
    clients_csv = tmp_path / "clients.csv"
    dialpad_dir = tmp_path / "Call Log"
    dialpad_dir.mkdir()

    clients_csv.write_text(
        "\n".join(
            [
                "Client,Email,Phone,Guardian Email,Account Manager Phones,Mobile Phone,Client ID",
                "Student One,student@example.com,(713) 555-1212,parent@example.com,,713-555-9999,client-1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (dialpad_dir / "Call_Logs.csv").write_text(
        "\n".join(
            [
                "call_id,office_id,external_number,internal_number,date_started,direction,category,name,email,is_internal",
                "call-1,4776436560855040,(713) 555-1212,7135550000,2026-06-01,outbound,connected,Student One,student@example.com,false",
                "call-1,4776436560855040,(713) 555-1212,7135550000,2026-06-01,outbound,connected,Student One,student@example.com,false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (dialpad_dir / "Voicemails.csv").write_text(
        "\n".join(
            [
                "call_id,transcription_text,recording_url,date",
                "call-1,first transcript,https://example.test/v1,2026-06-01",
                "call-1,second transcript,https://example.test/v2,2026-06-02",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (dialpad_dir / "Recordings.csv").write_text(
        "\n".join(
            [
                "call_id,recording_url,duration,date",
                "call-1,https://example.test/r1,10,2026-06-01",
                "call-1,https://example.test/r2,20,2026-06-02",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    run_import(clients_csv, dialpad_dir, db_path)
    run_import(clients_csv, dialpad_dir, db_path)

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM dialpad_calls").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM pike13_clients").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM call_logs").fetchone()[0] == 1
        row = conn.execute(
            """
            SELECT voicemail_transcript, voicemail_recording_url, recording_url, recording_duration
            FROM call_logs
            WHERE call_id = 'call-1'
            """
        ).fetchone()
        assert row == (
            "second transcript",
            "https://example.test/v2",
            "https://example.test/r2",
            "20",
        )
    finally:
        conn.close()


def test_db_guard_rejects_larger_corrupt_incoming_file(tmp_path):
    current = tmp_path / "current.db"
    incoming = tmp_path / "incoming.db"
    conn = sqlite3.connect(current)
    conn.execute("CREATE TABLE reminders (lesson_id TEXT)")
    conn.commit()
    conn.close()
    incoming.write_bytes(b"not sqlite" * 10000)

    try:
        verify_replace(current, incoming, force=True)
    except SystemExit as exc:
        assert "invalid_sqlite" in str(exc)
    else:
        raise AssertionError("Corrupt incoming DB was allowed")


def test_shared_phone_normalization_handles_extensions_and_international_numbers():
    assert normalize_phone("+1 (713) 555-1212 ext. 44") == "7135551212"
    assert normalize_phone("713-555-1212 x9") == "7135551212"
    assert normalize_phone("+44 20 7946 0958") == "442079460958"
    assert normalize_phone("") is None


def test_openai_backoff_retries_transient_errors_once(monkeypatch):
    calls = {"count": 0}
    sleeps = []

    class RetryableError(Exception):
        status_code = 429

    def flaky():
        calls["count"] += 1
        if calls["count"] == 1:
            raise RetryableError("rate limited")
        return "ok"

    monkeypatch.setattr("notesreminder.transcription.analyze_transcripts_openai.time.sleep", sleeps.append)

    assert call_with_backoff(flaky, attempts=2, base_sleep=0.5) == "ok"
    assert calls["count"] == 2
    assert sleeps == [0.5]
