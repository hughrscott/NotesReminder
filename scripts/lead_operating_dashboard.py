#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_operating_dashboard import (  # noqa: E402
    DASHBOARD_PERIODS,
    build_snapshot,
    render_snapshot_markdown,
    snapshot_to_json,
)


DEFAULT_DB = "reminders.db"
DEFAULT_OUTPUT_DIR = "outputs/progress"


def write_snapshot(snapshot, output_dir):
    prefix = f"{snapshot['dashboard_type']}_lead_dashboard"
    markdown_path = output_dir / f"{prefix}.md"
    json_path = output_dir / f"{prefix}.json"
    markdown_path.write_text(render_snapshot_markdown(snapshot), encoding="utf-8")
    json_path.write_text(snapshot_to_json(snapshot) + "\n", encoding="utf-8")
    return markdown_path, json_path


def main():
    parser = argparse.ArgumentParser(description="Generate daily, weekly, and monthly lead operating dashboards.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--school", default="West U")
    parser.add_argument("--period", choices=("all", *DASHBOARD_PERIODS), default="all")
    parser.add_argument("--as-of", help="Date used for default period windows, YYYY-MM-DD.")
    parser.add_argument("--start-date", help="Override start date for a single period.")
    parser.add_argument("--end-date", help="Override end date for a single period.")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--print", action="store_true", dest="print_output")
    args = parser.parse_args()

    periods = DASHBOARD_PERIODS if args.period == "all" else (args.period,)
    if (args.start_date or args.end_date) and len(periods) != 1:
        parser.error("--start-date/--end-date can only be used with a single --period.")
    if bool(args.start_date) != bool(args.end_date):
        parser.error("--start-date and --end-date must be provided together.")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        for period in periods:
            snapshot = build_snapshot(
                conn,
                period,
                start_date=args.start_date,
                end_date=args.end_date,
                as_of=args.as_of,
                school=args.school,
                limit=args.limit,
            )
            markdown_path, json_path = write_snapshot(snapshot, output_dir)
            if args.print_output:
                print(render_snapshot_markdown(snapshot))
            else:
                print(f"Wrote {markdown_path} and {json_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
