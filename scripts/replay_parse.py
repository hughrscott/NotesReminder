#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema  # noqa: E402
from notesreminder.lib.raw_capture import mark_capture_parsed  # noqa: E402
from scripts.extract_hubspot_leads import parse_deal_text, upsert_deal  # noqa: E402
from scripts.extract_pike13_leads import (  # noqa: E402
    capture_related_rows,
    parse_person_text,
    upsert_person,
    upsert_plan_pass,
    upsert_visit,
)


def _row_value(row, key):
    return row[key] if isinstance(row, sqlite3.Row) else row[key]


def _metadata(row):
    try:
        return json.loads(row["metadata_json"] or "{}")
    except json.JSONDecodeError:
        return {}


def replay_capture(source_conn, scratch_conn, row):
    capture_type = row["capture_type"]
    source_url = row["source_url"] or ""
    path = Path(row["file_path"])
    text = path.read_text(encoding="utf-8")
    metadata = _metadata(row)

    if capture_type == "hubspot_deal_text":
        deal_id = metadata.get("deal_id") or row["capture_id"]
        parsed = parse_deal_text(deal_id, source_url, text)
        upsert_deal(scratch_conn, parsed)
        return {"rows_written": 1, "parser": capture_type}

    if capture_type == "pike13_person_text":
        person, _ = parse_person_text(source_url, text, metadata.get("school"))
        upsert_person(scratch_conn, person)
        return {"rows_written": 1, "parser": capture_type}

    if capture_type in {"pike13_related_page_text", "pike13_event_text"}:
        person_id = metadata.get("person_id")
        if not person_id:
            raise ValueError(f"{capture_type} replay requires metadata.person_id")
        visits, plans = capture_related_rows(person_id, source_url, text, metadata.get("school"), capture_type)
        rows_written = 0
        for visit in visits:
            upsert_visit(scratch_conn, visit)
            rows_written += 1
        for plan in plans:
            upsert_plan_pass(scratch_conn, plan)
            rows_written += 1
        return {"rows_written": rows_written, "parser": capture_type}

    return {"rows_written": 0, "parser": capture_type, "status": "unsupported"}


def main():
    parser = argparse.ArgumentParser(description="Replay saved raw captures into a scratch DB.")
    parser.add_argument("--source-db", default="reminders.db")
    parser.add_argument("--scratch-db", required=True)
    parser.add_argument("--capture-id", action="append", default=[])
    parser.add_argument("--source", default="")
    parser.add_argument("--capture-type", default="")
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    source_conn = sqlite3.connect(args.source_db)
    source_conn.row_factory = sqlite3.Row
    scratch_conn = sqlite3.connect(args.scratch_db)
    scratch_conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(scratch_conn)
    params = {}
    filters = []
    if args.capture_id:
        placeholders = ", ".join(f":capture_id_{index}" for index, _ in enumerate(args.capture_id))
        filters.append(f"capture_id IN ({placeholders})")
        params.update({f"capture_id_{index}": value for index, value in enumerate(args.capture_id)})
    if args.source:
        filters.append("source = :source")
        params["source"] = args.source
    if args.capture_type:
        filters.append("capture_type = :capture_type")
        params["capture_type"] = args.capture_type
    where = "WHERE " + " AND ".join(filters) if filters else ""
    rows = source_conn.execute(
        f"""
        SELECT *
        FROM raw_captures
        {where}
        ORDER BY captured_at
        LIMIT :limit
        """,
        {**params, "limit": args.limit},
    ).fetchall()

    results = []
    for row in rows:
        try:
            result = replay_capture(source_conn, scratch_conn, row)
            status = "replayed" if result.get("status") != "unsupported" else "unsupported"
        except Exception as exc:
            result = {"rows_written": 0, "error": str(exc)[:240]}
            status = "replay_error"
        mark_capture_parsed(source_conn, row["capture_id"], status)
        results.append({"capture_id": row["capture_id"], "status": status, **result})
    source_conn.commit()
    scratch_conn.commit()
    source_conn.close()
    scratch_conn.close()
    print(json.dumps({"captures_seen": len(rows), "results": results}, indent=2, default=str))


if __name__ == "__main__":
    main()
