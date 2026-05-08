#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema  # noqa: E402
from source_completeness import refresh_identity_matches  # noqa: E402
from trial_followup_intelligence import (  # noqa: E402
    build_trial_followup_report,
    render_trial_followup_markdown,
    report_to_json,
)


DEFAULT_DB = "outputs/lead_intelligence/lead_intelligence_working.db"
DEFAULT_OUTPUT = "outputs/progress/trial_followup_report.md"
DEFAULT_JSON_OUTPUT = "outputs/progress/trial_followup_report.json"


def main():
    parser = argparse.ArgumentParser(description="Generate sanitized trial follow-up intelligence reports.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--school", default="West U")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--json-output", default=DEFAULT_JSON_OUTPUT)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    try:
        refresh_identity_matches(conn)
        report = build_trial_followup_report(conn, args.start_date, args.end_date, args.school)
        conn.commit()
    finally:
        conn.close()
    output = Path(args.output)
    json_output = Path(args.json_output)
    output.parent.mkdir(parents=True, exist_ok=True)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_trial_followup_markdown(report), encoding="utf-8")
    json_output.write_text(report_to_json(report), encoding="utf-8")
    print(f"Wrote {output}")
    print(f"Wrote {json_output}")


if __name__ == "__main__":
    main()
