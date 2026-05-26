#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from notesreminder.reports.operations_dashboard import (  # noqa: E402
    DEFAULT_SCHOOLS,
    build_operations_dashboard,
    write_operations_dashboard,
)


DEFAULT_DB = "reminders.db"
DEFAULT_OUTPUT_DIR = "outputs/dashboards"


def main():
    parser = argparse.ArgumentParser(description="Generate the static operations dashboard.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--period", choices=("daily", "weekly", "monthly"), default="weekly")
    parser.add_argument("--as-of", help="Date used for default period windows, YYYY-MM-DD.")
    parser.add_argument("--school", action="append", dest="schools", help="School label to include. Repeatable.")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    schools = tuple(args.schools) if args.schools else DEFAULT_SCHOOLS
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        report = build_operations_dashboard(
            conn,
            period=args.period,
            as_of=args.as_of,
            schools=schools,
            limit=args.limit,
        )
        html_path, json_path = write_operations_dashboard(report, args.output_dir)
        print(f"Wrote {html_path} and {json_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
