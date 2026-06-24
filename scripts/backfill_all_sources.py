#!/usr/bin/env python3
import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from notesreminder.orchestration.historical_backfill import (  # noqa: E402
    build_monthly_backfill_plan,
    month_windows,
)
from notesreminder.orchestration.refresh_all_sources import (  # noqa: E402
    backup_local_db,
    run_refresh_plan,
    write_metadata,
)

DEFAULT_OUTPUT_DIR = Path("outputs/progress/historical_backfill")


def print_progress(event, payload):
    if event == "task_start":
        mode = "running" if payload.get("will_execute") else "skipping"
        timeout = payload.get("timeout_seconds") or ""
        timeout_text = f", timeout={timeout}s" if timeout else ""
        print(
            f"[{payload['index']}/{payload['total_tasks']}] {mode} {payload['name']} "
            f"({payload['category']}{timeout_text})",
            flush=True,
        )
    elif event == "task_finish":
        print(
            f"[{payload['index']}/{payload['total_tasks']}] {payload['name']} -> {payload['status']}",
            flush=True,
        )


def parse_args():
    parser = argparse.ArgumentParser(description="Plan or run checkpointed historical source backfills.")
    parser.add_argument("--start-date", required=True, help="Backfill start date, YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="Backfill end date, YYYY-MM-DD.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--school", action="append", help="School label/subdomain/mailbox/slug. Repeatable.")
    parser.add_argument("--skip-notes", action="store_true")
    parser.add_argument("--notes-chunk-days", type=int, default=7)
    parser.add_argument("--dialpad-voice-limit-per-view", type=int, default=500)
    parser.add_argument("--dialpad-sms-thread-limit", type=int, default=250)
    parser.add_argument("--call-review-limit", type=int, default=25)
    parser.add_argument("--hubspot-limit", type=int, default=500)
    parser.add_argument("--hubspot-detail-limit", type=int, default=200)
    parser.add_argument("--pike13-limit", type=int, default=250)
    parser.add_argument("--email-limit-per-query", type=int, default=25)
    parser.add_argument("--login-timeout", type=int, default=900)
    parser.add_argument("--interactive-login", action="store_true")
    parser.add_argument("--execute-refresh", action="store_true", help="Execute mutating backfill tasks.")
    parser.add_argument("--execute-verification", action="store_true", help="Execute checkpoint verification tasks.")
    parser.add_argument("--backup", action="store_true", help="Create a local DB backup before each executed month.")
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    windows = month_windows(args.start_date, args.end_date)
    summary = {
        "mode": "historical-backfill",
        "start_date": args.start_date,
        "end_date": args.end_date,
        "db": args.db,
        "execute_refresh": args.execute_refresh,
        "execute_verification": args.execute_verification,
        "checkpoint_strategy": "calendar_month",
        "source_boundary_notes": {
            "bounded_by_month": ["notes", "school_email", "pike13_first_visits"],
            "start_date_only": ["hubspot", "pike13_people", "dialpad_voice", "dialpad_sms"],
            "lead_spine_targeted": ["dialpad_target_search"],
            "limit_only_after_voice": ["dialpad_call_reviews"],
        },
        "months": [],
    }

    overall_status = "dry_run"
    today_stamp = date.today().strftime("%Y%m%d")
    for window in windows:
        month_dir = output_dir / window.label
        month_dir.mkdir(parents=True, exist_ok=True)
        backup_path = None
        if args.backup and args.execute_refresh:
            backup_path = backup_local_db(args.db, month_dir, f"{today_stamp}.{window.label}")
        tasks = build_monthly_backfill_plan(
            window.start_date,
            window.end_date,
            root=ROOT,
            db_path=args.db,
            schools=args.school,
            include_notes=not args.skip_notes,
            notes_chunk_days=args.notes_chunk_days,
            dialpad_voice_limit_per_view=args.dialpad_voice_limit_per_view,
            dialpad_sms_thread_limit=args.dialpad_sms_thread_limit,
            call_review_limit=args.call_review_limit,
            hubspot_limit=args.hubspot_limit,
            hubspot_detail_limit=args.hubspot_detail_limit,
            pike13_limit=args.pike13_limit,
            email_limit_per_query=args.email_limit_per_query,
            login_timeout=args.login_timeout,
            interactive_login=args.interactive_login,
        )
        metadata = run_refresh_plan(
            tasks,
            root=ROOT,
            execute_refresh=args.execute_refresh,
            execute_verification=args.execute_verification,
            progress_callback=print_progress,
        )
        metadata.update(
            {
                "mode": "historical-backfill-month",
                "month": window.label,
                "start_date": window.start_date,
                "end_date": window.end_date,
                "db": args.db,
                "local_backup": str(backup_path) if backup_path else "",
            }
        )
        output_path = month_dir / f"backfill_{window.label}_{metadata['status']}.json"
        write_metadata(metadata, output_path)
        print(f"Wrote {output_path}")
        summary["months"].append(
            {
                "month": window.label,
                "start_date": window.start_date,
                "end_date": window.end_date,
                "status": metadata["status"],
                "metadata_path": str(output_path),
                "local_backup": str(backup_path) if backup_path else "",
            }
        )
        if metadata["status"] == "action_required":
            overall_status = "action_required"
            if not args.continue_on_failure:
                break
        elif metadata["status"] == "success" and overall_status != "action_required":
            overall_status = "success"

    summary["status"] = overall_status
    summary_path = output_dir / f"historical_backfill_{args.start_date}_to_{args.end_date}_{overall_status}.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {summary_path}")
    if overall_status == "action_required":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
