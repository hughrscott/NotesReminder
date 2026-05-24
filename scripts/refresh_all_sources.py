#!/usr/bin/env python3
import argparse
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from notesreminder.orchestration.refresh_all_sources import (
    backup_local_db,
    build_daily_refresh_plan,
    build_weekly_completeness_plan,
    default_run_date,
    run_refresh_plan,
    write_metadata,
)

DEFAULT_OUTPUT_DIR = Path("outputs/progress/refresh_all_sources")


def parse_args():
    parser = argparse.ArgumentParser(description="Plan or run unified NotesReminder refresh workflows.")
    parser.add_argument("--mode", choices=["daily", "weekly-completeness"], required=True)
    parser.add_argument("--date", dest="run_date", help="Daily refresh date, YYYY-MM-DD. Defaults to yesterday.")
    parser.add_argument("--as-of", help="Weekly completeness as-of date, YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--school", action="append", help="School label/subdomain/mailbox/slug. Repeatable.")
    parser.add_argument("--window-days", type=int, default=2, help="Daily source lookback window.")
    parser.add_argument("--weekly-window-days", type=int, default=7)
    parser.add_argument("--dialpad-limit", type=int, default=100)
    parser.add_argument("--call-review-limit", type=int, default=25)
    parser.add_argument("--hubspot-limit", type=int, default=100)
    parser.add_argument("--hubspot-detail-limit", type=int, default=25)
    parser.add_argument("--pike13-limit", type=int, default=25)
    parser.add_argument("--email-limit-per-query", type=int, default=50)
    parser.add_argument("--login-timeout", type=int, default=900)
    parser.add_argument("--interactive-login", action="store_true")
    parser.add_argument("--execute-refresh", action="store_true", help="Execute mutating refresh tasks.")
    parser.add_argument("--execute-verification", action="store_true", help="Execute read-only verification/report tasks.")
    parser.add_argument("--execute-production-notes", action="store_true", help="Allow normal notes wrapper emails/S3.")
    parser.add_argument("--send-email", action="store_true", help="Allow production notes email when used with --execute-production-notes.")
    parser.add_argument("--upload-s3", action="store_true", help="Allow production notes S3 upload when used with --execute-production-notes.")
    parser.add_argument("--backup", action="store_true", help="Create a local DB backup before executed refresh.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    run_date = args.run_date or default_run_date()
    as_of = args.as_of or date.today().isoformat()
    if args.mode == "daily":
        if args.execute_production_notes and not (args.send_email and args.upload_s3):
            raise SystemExit("--execute-production-notes requires --send-email and --upload-s3.")
        tasks = build_daily_refresh_plan(
            run_date,
            root=ROOT,
            db_path=args.db,
            schools=args.school,
            window_days=args.window_days,
            dialpad_limit=args.dialpad_limit,
            call_review_limit=args.call_review_limit,
            hubspot_limit=args.hubspot_limit,
            hubspot_detail_limit=args.hubspot_detail_limit,
            pike13_limit=args.pike13_limit,
            email_limit_per_query=args.email_limit_per_query,
            login_timeout=args.login_timeout,
            upload_s3=args.upload_s3 and args.execute_production_notes,
            send_email=args.send_email and args.execute_production_notes,
            interactive_login=args.interactive_login,
        )
        label = run_date
    else:
        tasks = build_weekly_completeness_plan(
            as_of,
            root=ROOT,
            db_path=args.db,
            schools=args.school,
            window_days=args.weekly_window_days,
        )
        label = as_of

    stamp = date.today().strftime("%Y%m%d")
    backup_path = None
    if args.backup and args.execute_refresh:
        backup_path = backup_local_db(args.db, output_dir, stamp)

    metadata = run_refresh_plan(
        tasks,
        root=ROOT,
        execute_refresh=args.execute_refresh,
        execute_verification=args.execute_verification,
    )
    metadata.update(
        {
            "mode": args.mode,
            "date": run_date if args.mode == "daily" else as_of,
            "db": args.db,
            "local_backup": str(backup_path) if backup_path else "",
        }
    )
    output_path = output_dir / f"{args.mode}_{label}_{metadata['status']}.json"
    write_metadata(metadata, output_path)
    print(f"Wrote {output_path}")
    if metadata["status"] == "action_required":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
