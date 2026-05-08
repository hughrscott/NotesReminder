#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from date_window_lead_load import (  # noqa: E402
    DEFAULT_DB,
    DEFAULT_END_DATE,
    DEFAULT_JSON_OUTPUT,
    DEFAULT_MD_OUTPUT,
    DEFAULT_PIKE13_BASE_URL,
    DEFAULT_PIKE13_SCHOOL,
    DEFAULT_SCHOOL,
    DEFAULT_START_DATE,
    backup_db,
    build_date_window_report,
    python_executable,
    render_date_window_markdown,
    report_to_json,
    rolling_window_days,
    run_command_step,
    validate_target_db,
    validate_window,
)
from lead_followup_schema import ensure_lead_followup_schema  # noqa: E402


def build_source_steps(args):
    py = python_executable()
    db = str(Path(args.db).resolve())
    steps = []
    if not args.skip_hubspot:
        command = [
            py,
            "scripts/extract_hubspot_leads.py",
            "--db",
            db,
            "--profile-dir",
            args.hubspot_profile_dir,
            "--limit",
            str(args.hubspot_limit),
            "--detail-limit",
            str(args.hubspot_detail_limit),
            "--start-date",
            args.start_date,
            "--school",
            args.school,
        ]
        if args.headless:
            command.append("--headless")
        steps.append(("hubspot", command))
    if not args.skip_pike13:
        command = [
            py,
            "scripts/extract_pike13_leads.py",
            "--db",
            db,
            "--profile-dir",
            args.pike13_profile_dir,
            "--base-url",
            args.pike13_base_url,
            "--school",
            args.pike13_school,
            "--first-visits-start-date",
            args.start_date,
            "--first-visits-end-date",
            args.end_date,
            "--first-visits-limit",
            str(args.pike13_limit),
            "--reauth-if-needed",
        ]
        if args.headless:
            command.append("--headless")
        steps.append(("pike13", command))
    if not args.skip_dialpad:
        days = rolling_window_days(args.start_date)
        command = [
            py,
            "scripts/extract_dialpad_daily_intake.py",
            "--db",
            db,
            "--profile-dir",
            args.dialpad_profile_dir,
            "--school",
            "West U",
            "--window-days",
            str(days),
            "--limit",
            str(args.dialpad_daily_limit),
            "--no-route-discovery-on-failure",
        ]
        if args.headless:
            command.append("--headless")
        steps.append(("dialpad_daily_intake", command))
        voice_command = [
            py,
            "scripts/extract_dialpad_voice.py",
            "--db",
            db,
            "--profile-dir",
            args.dialpad_profile_dir,
            "--views",
            "conversation_history,calls,missed,voicemails,recordings",
            "--limit-per-view",
            str(args.dialpad_voice_limit),
            "--start-date",
            args.start_date,
        ]
        if args.headless:
            voice_command.append("--headless")
        steps.append(("dialpad_voice", voice_command))
        sms_command = [
            py,
            "scripts/extract_dialpad_sms.py",
            "--db",
            db,
            "--profile-dir",
            args.dialpad_profile_dir,
            "--thread-limit",
            str(args.dialpad_sms_limit),
            "--start-date",
            args.start_date,
        ]
        if args.headless:
            sms_command.append("--headless")
        steps.append(("dialpad_sms", sms_command))
    if not args.skip_call_reviews:
        command = [
            py,
            "scripts/extract_dialpad_call_reviews.py",
            "--db",
            db,
            "--profile-dir",
            args.dialpad_profile_dir,
            "--limit",
            str(args.call_review_limit),
        ]
        if args.headless:
            command.append("--headless")
        steps.append(("dialpad_call_reviews", command))
    return steps


def ensure_schema(db_path):
    conn = sqlite3.connect(db_path)
    try:
        ensure_lead_followup_schema(conn)
        conn.commit()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Run a sanitized date-window lead-intelligence load into the working DB.")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--school", default=DEFAULT_SCHOOL)
    parser.add_argument("--pike13-school", default=DEFAULT_PIKE13_SCHOOL)
    parser.add_argument("--pike13-base-url", default=DEFAULT_PIKE13_BASE_URL)
    parser.add_argument("--hubspot-profile-dir", default="browser_profiles/hubspot")
    parser.add_argument("--pike13-profile-dir", default="browser_profiles/pike13")
    parser.add_argument("--dialpad-profile-dir", default="browser_profiles/dialpad")
    parser.add_argument("--hubspot-limit", type=int, default=100)
    parser.add_argument("--hubspot-detail-limit", type=int, default=100)
    parser.add_argument("--pike13-limit", type=int, default=500)
    parser.add_argument("--dialpad-daily-limit", type=int, default=250)
    parser.add_argument("--dialpad-voice-limit", type=int, default=75)
    parser.add_argument("--dialpad-sms-limit", type=int, default=50)
    parser.add_argument("--call-review-limit", type=int, default=150)
    parser.add_argument("--source-timeout-seconds", type=int, default=900)
    parser.add_argument("--call-review-timeout-seconds", type=int, default=300)
    parser.add_argument("--gap-limit", type=int, default=500)
    parser.add_argument("--output", default=str(DEFAULT_MD_OUTPUT))
    parser.add_argument("--json-output", default=str(DEFAULT_JSON_OUTPUT))
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--skip-hubspot", action="store_true")
    parser.add_argument("--skip-pike13", action="store_true")
    parser.add_argument("--skip-dialpad", action="store_true")
    parser.add_argument("--skip-call-reviews", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    validate_window(args.start_date, args.end_date)
    db_path = validate_target_db(args.db)
    ensure_schema(db_path)
    backup_path = None if args.dry_run else backup_db(db_path)

    notes = [
        "Pike13 uses exact First Visits report start/end dates.",
        "HubSpot extraction currently applies a start-date proof filter; the date-window report filters loaded rows back to the requested window.",
        "Dialpad daily intake uses Dialpad's rolling conversation-history window, then the report filters loaded rows back to the requested dates.",
    ]

    steps = []
    for name, command in build_source_steps(args):
        if args.dry_run:
            steps.append(
                {
                    "name": name,
                    "command": " ".join(command),
                    "status": "dry_run",
                    "returncode": 0,
                    "duration_seconds": 0,
                    "stdout_tail": "",
                    "stderr_tail": "",
                    "error": "",
                }
            )
        else:
            print(f"Running {name}...", flush=True)
            timeout = args.call_review_timeout_seconds if name == "dialpad_call_reviews" else args.source_timeout_seconds
            step = run_command_step(name, command, cwd=ROOT, timeout=timeout)
            steps.append(step)
            print(f"{name}: {step['status']}", flush=True)

    report = build_date_window_report(
        db_path,
        args.start_date,
        args.end_date,
        args.school,
        steps,
        backup_path=backup_path,
        notes=notes,
        gap_limit=args.gap_limit,
    )
    output = Path(args.output)
    json_output = Path(args.json_output)
    output.parent.mkdir(parents=True, exist_ok=True)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_date_window_markdown(report), encoding="utf-8")
    json_output.write_text(report_to_json(report), encoding="utf-8")
    print(f"Wrote {output}")
    print(f"Wrote {json_output}")

    failed = [step for step in steps if step["status"] not in {"success", "dry_run"}]
    if failed:
        print("One or more source loads failed; see sanitized date-window report for source-specific status.")


if __name__ == "__main__":
    main()
