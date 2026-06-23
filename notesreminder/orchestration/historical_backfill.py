"""Monthly historical backfill orchestration."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from notesreminder.orchestration.refresh_all_sources import (
    RefreshTask,
    _python,
    _school_filter,
    _timeout_at_least,
)


@dataclass(frozen=True)
class MonthWindow:
    label: str
    start_date: str
    end_date: str


def month_windows(start_date: str, end_date: str) -> list[MonthWindow]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if start > end:
        raise ValueError("start_date must be on or before end_date")

    windows: list[MonthWindow] = []
    current = start
    while current <= end:
        month_last_day = calendar.monthrange(current.year, current.month)[1]
        month_end = date(current.year, current.month, month_last_day)
        window_end = min(month_end, end)
        windows.append(
            MonthWindow(
                label=f"{current.year:04d}-{current.month:02d}",
                start_date=current.isoformat(),
                end_date=window_end.isoformat(),
            )
        )
        current = window_end + timedelta(days=1)
    return windows


def _days_inclusive(start_date: str, end_date: str) -> int:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    return (end - start).days + 1


def date_chunks(start_date: str, end_date: str, chunk_days: int) -> list[tuple[str, str]]:
    if chunk_days < 1:
        raise ValueError("chunk_days must be at least 1")
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if start > end:
        raise ValueError("start_date must be on or before end_date")
    chunks: list[tuple[str, str]] = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)
    return chunks


def build_monthly_backfill_plan(
    start_date: str,
    end_date: str,
    *,
    root: Path | None = None,
    db_path: str = "reminders.db",
    schools: Iterable[str] | None = None,
    include_notes: bool = True,
    notes_chunk_days: int = 7,
    dialpad_voice_limit_per_view: int = 500,
    dialpad_sms_thread_limit: int = 250,
    call_review_limit: int = 25,
    hubspot_limit: int = 500,
    hubspot_detail_limit: int = 200,
    pike13_limit: int = 250,
    email_limit_per_query: int = 25,
    login_timeout: int = 900,
    interactive_login: bool = False,
) -> list[RefreshTask]:
    root = root or Path.cwd()
    py = _python(root)
    selected_schools = _school_filter(schools)
    email_login_args = ["--interactive-login"] if interactive_login else ["--headless"]
    hubspot_login_args = [] if interactive_login else ["--headless"]
    pike13_login_args = ["--headless", "--reauth-if-needed"] if interactive_login else ["--headless"]
    dialpad_login_args = ["--interactive-login"] if interactive_login else ["--headless"]
    checkpoint_days = _days_inclusive(start_date, end_date)
    tasks: list[RefreshTask] = []

    for label, subdomain, base_url, mailbox, slug in selected_schools:
        if include_notes:
            for chunk_start, chunk_end in date_chunks(start_date, end_date, notes_chunk_days):
                tasks.append(
                    RefreshTask(
                        name=f"notes_backfill_{slug}_{chunk_start}_to_{chunk_end}",
                        command=[
                            py,
                            "run_daily.py",
                            "--school",
                            subdomain,
                            "--start-date",
                            chunk_start,
                            "--end-date",
                            chunk_end,
                            "--db-path",
                            db_path,
                            "--skip-s3-sync",
                            "--no-email",
                            "--skip-note-scoring",
                            "--pike13-profile-dir",
                            "browser_profiles/pike13",
                            "--login-timeout",
                            str(login_timeout),
                        ],
                        category="historical_backfill",
                        mutates_db=True,
                        requires_auth=True,
                        enabled_flag="--execute-refresh",
                        timeout_seconds=_timeout_at_least(login_timeout + 300, 600),
                    )
                )
        tasks.extend(
            [
                RefreshTask(
                    name=f"school_email_{slug}",
                    command=[
                        py,
                        "scripts/extract_school_emails.py",
                        "--db",
                        db_path,
                        "--profile-dir",
                        "browser_profiles/sor_okta",
                        "--start-date",
                        start_date,
                        "--end-date",
                        end_date,
                        "--mailbox",
                        mailbox,
                        "--limit-per-query",
                        str(email_limit_per_query),
                        "--login-timeout",
                        str(login_timeout),
                        "--allow-production-db",
                        *email_login_args,
                    ],
                    category="historical_backfill",
                    mutates_db=True,
                    requires_auth=True,
                    enabled_flag="--execute-refresh",
                    timeout_seconds=_timeout_at_least(login_timeout + 300, 600),
                ),
                RefreshTask(
                    name=f"hubspot_leads_{slug}",
                    command=[
                        py,
                        "scripts/extract_hubspot_leads.py",
                        "--db",
                        db_path,
                        "--profile-dir",
                        "browser_profiles/hubspot",
                        "--school",
                        label,
                        "--start-date",
                        start_date,
                        "--limit",
                        str(hubspot_limit),
                        "--detail-limit",
                        str(hubspot_detail_limit),
                        *hubspot_login_args,
                    ],
                    category="historical_backfill_start_date_only",
                    mutates_db=True,
                    requires_auth=True,
                    enabled_flag="--execute-refresh",
                    timeout_seconds=_timeout_at_least(
                        login_timeout + hubspot_detail_limit * 5 + 180,
                        900,
                    ),
                ),
                RefreshTask(
                    name=f"pike13_leads_{slug}",
                    command=[
                        py,
                        "scripts/extract_pike13_leads.py",
                        "--db",
                        db_path,
                        "--profile-dir",
                        "browser_profiles/pike13",
                        "--base-url",
                        base_url,
                        "--school",
                        label,
                        "--start-date",
                        start_date,
                        "--limit",
                        str(pike13_limit),
                        "--first-visits-start-date",
                        start_date,
                        "--first-visits-end-date",
                        end_date,
                        "--first-visits-limit",
                        str(pike13_limit),
                        *pike13_login_args,
                    ],
                    category="historical_backfill",
                    mutates_db=True,
                    requires_auth=True,
                    enabled_flag="--execute-refresh",
                    timeout_seconds=_timeout_at_least(login_timeout + 600, 900),
                ),
            ]
        )

    tasks.extend(
        [
            RefreshTask(
                name="dialpad_voice",
                command=[
                    py,
                    "scripts/extract_dialpad_voice.py",
                    "--db",
                    db_path,
                    "--profile-dir",
                    "browser_profiles/dialpad",
                    "--views",
                    "conversation_history,calls,missed,voicemails,recordings",
                    "--limit-per-view",
                    str(dialpad_voice_limit_per_view),
                    "--start-date",
                    start_date,
                    "--login-timeout",
                    str(login_timeout),
                    *dialpad_login_args,
                ],
                category="historical_backfill_start_date_only",
                mutates_db=True,
                requires_auth=True,
                enabled_flag="--execute-refresh",
                timeout_seconds=_timeout_at_least(login_timeout + 900, 1200),
            ),
            RefreshTask(
                name="dialpad_sms",
                command=[
                    py,
                    "scripts/extract_dialpad_sms.py",
                    "--db",
                    db_path,
                    "--profile-dir",
                    "browser_profiles/dialpad",
                    "--start-date",
                    start_date,
                    "--thread-limit",
                    str(dialpad_sms_thread_limit),
                    "--login-timeout",
                    str(login_timeout),
                    *dialpad_login_args,
                ],
                category="historical_backfill_start_date_only",
                mutates_db=True,
                requires_auth=True,
                enabled_flag="--execute-refresh",
                timeout_seconds=_timeout_at_least(
                    login_timeout + dialpad_sms_thread_limit * 5 + 180,
                    900,
                ),
            ),
            RefreshTask(
                name="dialpad_call_reviews",
                command=[
                    py,
                    "scripts/extract_dialpad_call_reviews.py",
                    "--db",
                    db_path,
                    "--profile-dir",
                    "browser_profiles/dialpad",
                    "--limit",
                    str(call_review_limit),
                    "--login-timeout",
                    str(login_timeout),
                    *dialpad_login_args,
                ],
                category="historical_backfill_limit_only",
                mutates_db=True,
                requires_auth=True,
                enabled_flag="--execute-refresh",
                timeout_seconds=_timeout_at_least(
                    login_timeout + call_review_limit * 15 + 180,
                    900,
                ),
            ),
            RefreshTask(
                name="refresh_person_identities",
                command=[py, "scripts/refresh_person_identities.py", "--db", db_path, "--json"],
                category="checkpoint",
                mutates_db=True,
                enabled_flag="--execute-refresh",
                timeout_seconds=300,
            ),
            RefreshTask(
                name="build_reporting_schema",
                command=[py, "build_reporting_schema.py", "--db", db_path],
                category="checkpoint",
                mutates_db=True,
                enabled_flag="--execute-refresh",
                timeout_seconds=600,
            ),
            RefreshTask(
                name="db_integrity",
                command=["sqlite3", db_path, "PRAGMA integrity_check;"],
                category="checkpoint",
            ),
            RefreshTask(
                name="notes_pipeline_health",
                command=[
                    py,
                    "scripts/notes_pipeline_health.py",
                    "--db",
                    db_path,
                    "--as-of",
                    (date.fromisoformat(end_date) + timedelta(days=1)).isoformat(),
                    "--lookback-days",
                    str(checkpoint_days),
                ],
                category="checkpoint",
            ),
            RefreshTask(
                name="notes_read_path_comparison",
                command=[
                    py,
                    "scripts/notes_read_path_comparison.py",
                    "--db",
                    db_path,
                    "--start-date",
                    start_date,
                    "--end-date",
                    end_date,
                ],
                category="checkpoint",
            ),
            RefreshTask(
                name="source_completeness_cumulative",
                command=[
                    py,
                    "scripts/source_completeness_report.py",
                    "--db",
                    db_path,
                    "--window-days",
                    str(max(7, _days_inclusive(start_date, date.today().isoformat()))),
                    "--pike13-lookahead-days",
                    "30",
                    "--pretty",
                ],
                category="checkpoint",
            ),
        ]
    )
    return tasks
