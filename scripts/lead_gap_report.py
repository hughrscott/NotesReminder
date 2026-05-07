#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema  # noqa: E402
from lead_gap_analysis import build_gap_report, render_gap_markdown, report_to_json  # noqa: E402
from source_completeness import refresh_identity_matches  # noqa: E402


DEFAULT_DB = "outputs/lead_intelligence/lead_intelligence_working.db"
DEFAULT_MARKDOWN_OUTPUT = "outputs/progress/lead_gap_report.md"
DEFAULT_JSON_OUTPUT = "outputs/progress/lead_gap_report.json"


def build_reports(db_path, school, limit):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    try:
        refresh_identity_matches(conn)
        report = build_gap_report(conn, school, limit)
        conn.commit()
    finally:
        conn.close()
    return report, render_gap_markdown(report, school), report_to_json(report)


def main():
    parser = argparse.ArgumentParser(description="Generate sanitized lead-intelligence source gap reports.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--school", default="West University Place")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--output", default=DEFAULT_MARKDOWN_OUTPUT)
    parser.add_argument("--json-output", default=DEFAULT_JSON_OUTPUT)
    args = parser.parse_args()

    _, markdown, json_text = build_reports(args.db, args.school, args.limit)
    markdown_output = Path(args.output)
    json_output = Path(args.json_output)
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.write_text(markdown, encoding="utf-8")
    json_output.write_text(json_text, encoding="utf-8")
    print(f"Wrote {markdown_output}")
    print(f"Wrote {json_output}")


if __name__ == "__main__":
    main()
