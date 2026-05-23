#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from notesreminder.reports.management_scorecards import (  # noqa: E402
    PERIODS,
    build_note_quality_scorecard_for_period,
    render_scorecard_markdown,
    scorecard_to_json,
)


DEFAULT_DB = "reminders.db"
DEFAULT_OUTPUT_DIR = "outputs/progress"


def main():
    parser = argparse.ArgumentParser(description="Generate shadow management scorecards.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--school", default="")
    parser.add_argument("--period", choices=PERIODS, default="mtd")
    parser.add_argument("--as-of", help="Date used for automatic windows, YYYY-MM-DD.")
    parser.add_argument("--start-date", help="Custom window start date, YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Custom window end date, YYYY-MM-DD.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--prefix", default="note_quality_scorecard")
    parser.add_argument("--print", action="store_true", dest="print_output")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        scorecard = build_note_quality_scorecard_for_period(
            conn,
            period=args.period,
            as_of=args.as_of,
            start_date=args.start_date,
            end_date=args.end_date,
            school=args.school or None,
        )
    finally:
        conn.close()

    markdown = render_scorecard_markdown(scorecard)
    json_output = scorecard_to_json(scorecard) + "\n"
    markdown_path = output_dir / f"{args.prefix}.md"
    json_path = output_dir / f"{args.prefix}.json"
    markdown_path.write_text(markdown, encoding="utf-8")
    json_path.write_text(json_output, encoding="utf-8")
    if args.print_output:
        print(markdown)
    else:
        print(f"Wrote {markdown_path} and {json_path}")


if __name__ == "__main__":
    main()
