"""Local raw payload capture and replay metadata helpers."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4


DEFAULT_RAW_ROOT = Path("raw")
DEFAULT_RETENTION_DAYS = 90
DEFAULT_PARSER_VERSION = "v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_raw_capture_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_captures (
            capture_id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            capture_type TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            file_path TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,
            parser_version TEXT NOT NULL,
            parse_status TEXT NOT NULL DEFAULT 'captured',
            parsed_at TEXT,
            import_run_id TEXT,
            source_url TEXT,
            metadata_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_raw_captures_source_captured
        ON raw_captures(source, captured_at)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_raw_captures_parse_status
        ON raw_captures(parse_status)
        """
    )


def slugify(value: str | None) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip()).strip("-")
    return cleaned[:80] or "capture"


def _content_bytes(content, extension: str) -> bytes:
    if isinstance(content, bytes):
        return content
    if extension == "json":
        return json.dumps(content, indent=2, sort_keys=True, default=str).encode("utf-8")
    return str(content or "").encode("utf-8")


def write_raw_capture(
    conn: sqlite3.Connection,
    *,
    source: str,
    capture_type: str,
    content,
    source_url: str | None = None,
    metadata: dict | None = None,
    import_run_id: str | int | None = None,
    raw_root: Path | str = DEFAULT_RAW_ROOT,
    parser_version: str = DEFAULT_PARSER_VERSION,
    extension: str = "txt",
    label: str | None = None,
    captured_at: str | None = None,
) -> dict:
    ensure_raw_capture_schema(conn)
    captured_at = captured_at or utc_now_iso()
    captured_dt = datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
    source_slug = slugify(source)
    date_part = captured_dt.date().isoformat()
    timestamp = captured_dt.strftime("%Y%m%dT%H%M%SZ")
    label_slug = slugify(label or capture_type)
    extension = extension.lstrip(".").lower()
    payload = _content_bytes(content, extension)
    digest = hashlib.sha256(payload).hexdigest()
    capture_id = f"raw_{uuid4().hex}"
    relative_path = Path(source_slug) / date_part / f"{timestamp}-{label_slug}-{capture_id[:12]}.{extension}"
    file_path = Path(raw_root) / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(payload)
    metadata_json = json.dumps(metadata or {}, sort_keys=True, default=str)
    conn.execute(
        """
        INSERT INTO raw_captures (
            capture_id, source, capture_type, captured_at, file_path,
            content_sha256, parser_version, parse_status, import_run_id,
            source_url, metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 'captured', ?, ?, ?)
        """,
        (
            capture_id,
            source,
            capture_type,
            captured_at,
            str(file_path),
            digest,
            parser_version,
            str(import_run_id) if import_run_id is not None else None,
            source_url,
            metadata_json,
        ),
    )
    return {
        "capture_id": capture_id,
        "file_path": str(file_path),
        "content_sha256": digest,
        "captured_at": captured_at,
    }


def mark_capture_parsed(
    conn: sqlite3.Connection,
    capture_id: str,
    status: str,
    parsed_at: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE raw_captures
        SET parse_status = ?, parsed_at = ?
        WHERE capture_id = ?
        """,
        (status, parsed_at or utc_now_iso(), capture_id),
    )


def prune_old_raw_captures(
    conn: sqlite3.Connection,
    *,
    raw_root: Path | str = DEFAULT_RAW_ROOT,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    now: datetime | None = None,
    dry_run: bool = True,
) -> dict:
    ensure_raw_capture_schema(conn)
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=retention_days)
    rows = conn.execute(
        """
        SELECT capture_id, file_path, captured_at
        FROM raw_captures
        WHERE captured_at < ?
        ORDER BY captured_at
        """,
        (cutoff.replace(microsecond=0).isoformat(),),
    ).fetchall()
    deleted = []
    for row in rows:
        path = Path(row["file_path"] if isinstance(row, sqlite3.Row) else row[1])
        if not dry_run and path.exists():
            path.unlink()
        deleted.append(str(path))
    if rows and not dry_run:
        conn.executemany(
            "UPDATE raw_captures SET parse_status = 'expired' WHERE capture_id = ?",
            [(row["capture_id"] if isinstance(row, sqlite3.Row) else row[0],) for row in rows],
        )
    return {
        "retention_days": retention_days,
        "cutoff": cutoff.replace(microsecond=0).isoformat(),
        "matched": len(rows),
        "deleted_files": deleted,
        "dry_run": dry_run,
    }
