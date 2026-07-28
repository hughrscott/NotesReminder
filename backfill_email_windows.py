#!/usr/bin/env python3
"""Run email extraction for a date range in weekly chunks to bypass Gmail's 50-row limit."""
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def run(start_date: str, end_date: str, mailbox: str, profile_dir: str, log_path: str):
    """Run a single extraction window and return True on success."""
    cmd = [
        sys.executable, "-u",
        str(ROOT / "scripts" / "extract_school_emails.py"),
        "--profile-dir", profile_dir,
        "--headless",
        "--mailbox", mailbox,
        "--start-date", start_date,
        "--end-date", end_date,
        "--limit-per-query", "50",
        "--login-timeout", "120",
        "--query-timeout", "45",
    ]
    with open(log_path, "a") as log:
        log.write(f"\n=== WINDOW {start_date} → {end_date} ===\n")
        log.flush()
        result = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT)
        return result.returncode == 0


def main():
    if len(sys.argv) < 4:
        print("Usage: backfill_email_windows.py <start_date> <end_date> <profile_dir> [mailbox]")
        sys.exit(1)

    start = date.fromisoformat(sys.argv[1])
    end = date.fromisoformat(sys.argv[2])
    profile_dir = sys.argv[3]
    mailbox = sys.argv[4] if len(sys.argv) > 4 else None

    mailboxes = [mailbox] if mailbox else [
        "huscott@schoolofrock.com",
        "westu@schoolofrock.com",
        "theheights@schoolofrock.com",
    ]

    log_path = "/tmp/backfill_email_windows.log"

    current = start
    window_days = 7

    while current < end:
        window_end = min(current + timedelta(days=window_days - 1), end)
        s = current.isoformat()
        e = window_end.isoformat()

        for mb in mailboxes:
            print(f"Extracting {mb}: {s} → {e}")
            ok = run(s, e, mb, profile_dir, log_path)
            if not ok:
                print(f"  FAILED for {mb}: {s} → {e} — continuing", file=sys.stderr)

        current = window_end + timedelta(days=1)

    print(f"Done. Log: {log_path}")


if __name__ == "__main__":
    main()
