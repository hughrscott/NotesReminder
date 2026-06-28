#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema  # noqa: E402
from scripts.extract_hubspot_leads import merge_contact_rows, parse_contact_detail_text, upsert_contact  # noqa: E402


def row_for_contact(conn, contact_id):
    row = conn.execute(
        """
        SELECT *
        FROM hubspot_contacts
        WHERE contact_id = ?
        """,
        (contact_id,),
    ).fetchone()
    return dict(row) if row else {}


def iter_contact_captures(conn, start_date, end_date):
    rows = conn.execute(
        """
        SELECT capture_id, source_url, file_path, metadata_json
        FROM raw_captures
        WHERE source = 'hubspot'
          AND capture_type = 'hubspot_contact_text'
        ORDER BY captured_at
        """
    ).fetchall()
    for row in rows:
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata = {}
        contact_id = str(metadata.get("contact_id") or "").strip()
        if not contact_id:
            continue
        existing = row_for_contact(conn, contact_id)
        create_date = str(existing.get("create_date") or "")
        if start_date and create_date and create_date < start_date:
            continue
        if end_date and create_date and create_date > end_date:
            continue
        yield row, contact_id, existing


def run(args):
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    seen = reparsed = updated = skipped_empty = errors = 0
    try:
        for capture, contact_id, existing in iter_contact_captures(conn, args.start_date, args.end_date):
            seen += 1
            path = Path(capture["file_path"])
            if not path.is_absolute():
                path = ROOT / path
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                errors += 1
                continue
            if not text.strip():
                skipped_empty += 1
                continue
            try:
                detail = parse_contact_detail_text(contact_id, capture["source_url"], text, existing)
                merged = merge_contact_rows(existing, detail)
                before = conn.total_changes
                if args.apply:
                    upsert_contact(conn, merged)
                if conn.total_changes > before:
                    updated += conn.total_changes - before
                reparsed += 1
            except Exception:
                errors += 1
        if args.apply:
            conn.commit()
        return {
            "applied": bool(args.apply),
            "seen": seen,
            "reparsed": reparsed,
            "updated": updated,
            "skipped_empty": skipped_empty,
            "errors": errors,
        }
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Reparse captured HubSpot contact text into hubspot_contacts.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date", default="2026-06-24")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
