import argparse
import subprocess
import sys
from datetime import datetime, timedelta


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill reminders DB for multiple schools.")
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in YYYY-MM-DD format (defaults to 13 months ago)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date in YYYY-MM-DD format (defaults to yesterday)",
    )
    parser.add_argument(
        "--schools",
        nargs="+",
        default=["westu-sor", "theheights-sor"],
        help="List of Pike13 subdomains",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        help="Skip sending summary emails during backfill",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging in run_daily.py",
    )
    return parser.parse_args()


def default_dates():
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=395)).strftime("%Y-%m-%d")
    return start_date, end_date


def run_school(school, start_date, end_date, no_email, verbose):
    cmd = [
        sys.executable,
        "run_daily.py",
        "--school",
        school,
        "--start-date",
        start_date,
        "--end-date",
        end_date,
    ]
    if no_email:
        cmd.append("--no-email")
    if verbose:
        cmd.append("--verbose")
    print(f"Running: {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def main():
    args = parse_args()
    start_date, end_date = args.start_date, args.end_date
    if not start_date or not end_date:
        start_date, end_date = default_dates()
    for school in args.schools:
        run_school(school, start_date, end_date, args.no_email, args.verbose)


if __name__ == "__main__":
    main()
