#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from notesreminder.reports.communication_insights import (  # noqa: E402
    DEFAULT_MODEL,
    PROMPT_VERSION,
    generate_insights,
    render_review_markdown,
    report_to_json,
)


DEFAULT_OUTPUT_DIR = "outputs/progress"


def main():
    parser = argparse.ArgumentParser(description="Generate experimental, human-review communication insights.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--school", default="")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--prompt-version", default=PROMPT_VERSION)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--prefix", default="communication_insights")
    parser.add_argument("--print", action="store_true", dest="print_output")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        report = generate_insights(
            conn,
            args.start_date,
            args.end_date,
            school=args.school or None,
            limit=args.limit,
            model=args.model,
            prompt_version=args.prompt_version,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            conn.commit()
    finally:
        conn.close()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    markdown = render_review_markdown(report)
    json_output = report_to_json(report) + "\n"
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
