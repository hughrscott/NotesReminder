#!/usr/bin/env python3
"""Refresh and generate the actionable School of Rock retention report.

Preview is the default. Pass --send only after the generated report has been reviewed.
"""
from __future__ import annotations

import argparse
import os
import smtplib
import subprocess
import sys
import tomllib
from datetime import date, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
MODELS_DIR = PROJECT_DIR / "models"
DB_PATH = PROJECT_DIR / "reminders.db"
DATA_PYTHON = os.environ.get(
    "NOTESREMINDER_PYTHON", "/home/ubuntu/.hermes/env/bin/python3"
)
SCHOOLS = ("westu-sor", "theheights-sor")
RECIPIENTS = ("vscott@schoolofrock.com", "huscott@schoolofrock.com")


def get_hermes_password() -> str:
    """Read the existing Hermes SMTP password command from Himalaya config."""
    config_path = Path.home() / ".config/himalaya/config.toml"
    with config_path.open("rb") as handle:
        config = tomllib.load(handle)
    for account in config.get("accounts", {}).values():
        command = account.get("backend", {}).get("auth", {}).get("cmd", "")
        if "echo " in command:
            return command.split("echo ", 1)[1].strip().strip('"').strip("'")
    raise RuntimeError("Could not locate Hermes SMTP password command")


def run_step(label: str, command: list[str], timeout: int = 900) -> None:
    """Run one required pipeline step and stop on any incomplete result."""
    print(f"=== {label} ===", flush=True)
    result = subprocess.run(
        command,
        cwd=PROJECT_DIR,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        raise RuntimeError(f"{label} failed with exit code {result.returncode}")


def refresh_sources(as_of: date) -> None:
    start = (as_of - timedelta(days=7)).isoformat()
    end = (as_of + timedelta(days=28)).isoformat()
    for school in SCHOOLS:
        run_step(
            f"REFRESH SCHEDULED LESSONS - {school}",
            [
                DATA_PYTHON,
                "run_daily.py",
                "--school",
                school,
                "--start-date",
                start,
                "--end-date",
                end,
                "--no-email",
                "--skip-note-scoring",
                "--skip-s3-sync",
                "--db-path",
                str(DB_PATH),
            ],
            timeout=1200,
        )
    for school in SCHOOLS:
        run_step(
            f"REFRESH CURRENT MEMBERS - {school}",
            [DATA_PYTHON, "scrape_pike13_current_members.py", "--school", school],
        )
    run_step(
        "REFRESH LATE-CANCELLATION SHADOW SOURCE",
        [
            DATA_PYTHON,
            "scrape_pike13_late_cancels.py",
            "--start-date",
            (as_of - timedelta(days=59)).isoformat(),
            "--end-date",
            as_of.isoformat(),
        ],
        timeout=1200,
    )
    for school in SCHOOLS:
        run_step(
            f"REFRESH HOLD STATUS - {school}",
            [
                DATA_PYTHON,
                "scrape_pike13_holds.py",
                "--school",
                school,
                "--as-of",
                as_of.isoformat(),
            ],
        )
    run_step(
        "REFRESH DETERMINISTIC PERSON IDENTITIES",
        [
            DATA_PYTHON,
            "-c",
            (
                "import sqlite3; "
                "from notesreminder.lib.person_identity import refresh_person_identities; "
                f"c=sqlite3.connect({str(DB_PATH)!r}); "
                "c.row_factory=sqlite3.Row; "
                "print(refresh_person_identities(c)); c.commit(); c.close()"
            ),
        ],
    )


def generate_report(as_of: date) -> tuple[Path, Path]:
    report_path = MODELS_DIR / "actionable_churn_report.txt"
    worklist_path = MODELS_DIR / "hold_return_worklist.csv"
    run_step(
        "GENERATE ACTIONABLE RETENTION REPORT",
        [
            DATA_PYTHON,
            "actionable_churn_report.py",
            "--db",
            str(DB_PATH),
            "--as-of",
            as_of.isoformat(),
            "--output",
            str(report_path),
            "--hold-worklist",
            str(worklist_path),
        ],
    )
    if not report_path.exists() or not report_path.read_text().strip():
        raise RuntimeError("Actionable churn report is missing or empty")
    if not worklist_path.exists():
        raise RuntimeError("Hold-return worklist is missing")
    return report_path, worklist_path


def generate_shadow(as_of: date) -> tuple[Path, Path]:
    """Generate separate research artifacts that never enter GM ranking/email."""
    report_path = MODELS_DIR / "late_cancel_shadow_report.txt"
    observations_path = MODELS_DIR / "late_cancel_shadow_observations.csv"
    run_step(
        "GENERATE LATE-CANCELLATION SHADOW REPORT",
        [
            DATA_PYTHON,
            "late_cancel_shadow.py",
            "--db",
            str(DB_PATH),
            "--as-of",
            as_of.isoformat(),
            "--output",
            str(report_path),
            "--observations",
            str(observations_path),
        ],
    )
    return report_path, observations_path


def send_report(report_path: Path, worklist_path: Path, as_of: date) -> None:
    body = report_path.read_text()
    body.encode("ascii")
    message = MIMEMultipart()
    message["From"] = "hermes@hughrscott.com"
    message["To"] = ", ".join(RECIPIENTS)
    message["Subject"] = f"SCHOOL OF ROCK CHURN PREVENTION - {as_of.strftime('%B %d, %Y').upper()}"
    message.attach(MIMEText(body, "plain", "ascii"))
    attachment = MIMEApplication(worklist_path.read_bytes(), _subtype="csv")
    attachment.add_header(
        "Content-Disposition", "attachment", filename=worklist_path.name
    )
    message.attach(attachment)

    with smtplib.SMTP_SSL("smtp.hostinger.com", 465, timeout=20) as server:
        server.login("hermes@hughrscott.com", get_hermes_password())
        server.sendmail(
            "hermes@hughrscott.com", list(RECIPIENTS), message.as_string()
        )
    print(f"SENT REPORT TO {', '.join(RECIPIENTS)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Generate from already-refreshed sources (preview/testing only)",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="Email the report after all refresh and generation steps succeed",
    )
    args = parser.parse_args()

    if not args.skip_refresh:
        refresh_sources(args.as_of)
    report_path, worklist_path = generate_report(args.as_of)
    shadow_path, shadow_observations_path = generate_shadow(args.as_of)
    print(f"PREVIEW: {report_path}")
    print(f"HOLD WORKLIST: {worklist_path}")
    print(f"SHADOW RESEARCH: {shadow_path}")
    print(f"SHADOW OBSERVATIONS: {shadow_observations_path}")
    if args.send:
        send_report(report_path, worklist_path, args.as_of)
    else:
        print("PREVIEW ONLY; USE --send AFTER REVIEW")


if __name__ == "__main__":
    main()
