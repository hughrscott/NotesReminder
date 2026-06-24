"""Unified daily refresh and weekly completeness orchestration."""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable


Runner = Callable[[list[str], Path], subprocess.CompletedProcess]
ProgressCallback = Callable[[str, dict], None]

SCHOOLS = (
    ("West U", "westu-sor", "https://westu-sor.pike13.com", "westu@schoolofrock.com", "westu"),
    (
        "The Heights",
        "theheights-sor",
        "https://theheights-sor.pike13.com",
        "theheights@schoolofrock.com",
        "heights",
    ),
)


@dataclass(frozen=True)
class RefreshTask:
    name: str
    command: list[str]
    category: str
    mutates_db: bool = False
    sends_email: bool = False
    uploads_s3: bool = False
    requires_auth: bool = False
    enabled_flag: str = ""
    timeout_seconds: int = 0
    gates_refresh: bool = False


def _python(root: Path) -> str:
    venv_python = root / "venv" / "bin" / "python"
    return str(venv_python if venv_python.exists() else "python3")


def default_run_date() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def window_start(end_date: str, days: int) -> str:
    end = date.fromisoformat(end_date)
    return (end - timedelta(days=max(0, days - 1))).isoformat()


def _school_filter(selected: Iterable[str] | None) -> list[tuple[str, str, str, str, str]]:
    if not selected:
        return list(SCHOOLS)
    wanted = {item.lower() for item in selected}
    rows = []
    for row in SCHOOLS:
        label, subdomain, _, mailbox, slug = row
        aliases = {label.lower(), subdomain.lower(), mailbox.lower(), slug.lower()}
        if wanted & aliases:
            rows.append(row)
    return rows


def backup_local_db(db_path: str, output_dir: Path, stamp: str) -> Path | None:
    source = Path(db_path)
    if not source.exists():
        return None
    backup_dir = output_dir / "db_backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    target = backup_dir / f"{source.name}.{stamp}.before-refresh-all-sources.bak"
    shutil.copy2(source, target)
    return target


def _timeout_at_least(*values: int) -> int:
    return max(values)


def _auth_probe_timeout(login_timeout: int) -> int:
    return min(max(login_timeout, 30), 90)


def build_auth_preflight_tasks(
    *,
    root: Path | None = None,
    schools: Iterable[str] | None = None,
    login_timeout: int = 900,
) -> list[RefreshTask]:
    root = root or Path.cwd()
    py = _python(root)
    selected_schools = _school_filter(schools)
    probe_timeout = _auth_probe_timeout(login_timeout)
    tasks = [
        RefreshTask(
            name="gmail_auth_preflight",
            command=[
                py,
                "scripts/probe_sor_okta_auth.py",
                "--profile-dir",
                "browser_profiles/sor_okta",
                "--headless",
                "--probe",
                "gmail",
                "--probe-timeout",
                str(probe_timeout),
            ],
            category="auth_preflight",
            requires_auth=True,
            enabled_flag="--execute-refresh",
            timeout_seconds=probe_timeout + 30,
            gates_refresh=True,
        ),
        RefreshTask(
            name="hubspot_auth_preflight",
            command=[
                py,
                "scripts/probe_sor_okta_auth.py",
                "--profile-dir",
                "browser_profiles/hubspot",
                "--headless",
                "--probe",
                "hubspot",
                "--probe-timeout",
                str(probe_timeout),
            ],
            category="auth_preflight",
            requires_auth=True,
            enabled_flag="--execute-refresh",
            timeout_seconds=probe_timeout + 30,
            gates_refresh=True,
        ),
        RefreshTask(
            name="dialpad_auth_preflight",
            command=[
                py,
                "scripts/probe_sor_okta_auth.py",
                "--profile-dir",
                "browser_profiles/dialpad",
                "--headless",
                "--probe",
                "dialpad",
                "--probe-timeout",
                str(probe_timeout),
            ],
            category="auth_preflight",
            requires_auth=True,
            enabled_flag="--execute-refresh",
            timeout_seconds=probe_timeout + 30,
            gates_refresh=True,
        ),
    ]
    for label, _, base_url, _, slug in selected_schools:
        tasks.append(
            RefreshTask(
                name=f"pike13_auth_preflight_{slug}",
                command=[
                    py,
                    "scripts/extract_pike13_leads.py",
                    "--profile-dir",
                    "browser_profiles/pike13",
                    "--base-url",
                    base_url,
                    "--school",
                    label,
                    "--auth-check-only",
                    "--headless",
                ],
                category="auth_preflight",
                requires_auth=True,
                enabled_flag="--execute-refresh",
                timeout_seconds=probe_timeout + 30,
                gates_refresh=True,
            )
        )
    return tasks


def build_daily_refresh_plan(
    run_date: str,
    *,
    root: Path | None = None,
    db_path: str = "reminders.db",
    schools: Iterable[str] | None = None,
    window_days: int = 2,
    dialpad_limit: int = 100,
    call_review_limit: int = 25,
    hubspot_limit: int = 100,
    hubspot_detail_limit: int = 25,
    pike13_limit: int = 25,
    email_limit_per_query: int = 50,
    login_timeout: int = 900,
    upload_s3: bool = False,
    send_email: bool = False,
    interactive_login: bool = False,
    skip_notes_validation: bool = False,
) -> list[RefreshTask]:
    root = root or Path.cwd()
    py = _python(root)
    selected_schools = _school_filter(schools)
    start_date = window_start(run_date, window_days)
    tasks: list[RefreshTask] = build_auth_preflight_tasks(
        root=root,
        schools=schools,
        login_timeout=login_timeout,
    )

    if send_email and upload_s3:
        tasks.append(
            RefreshTask(
                name="production_notes_local_mfa",
                command=["scripts/run_notes_local_mfa.sh", "--date", run_date],
                category="production_notes",
                mutates_db=True,
                sends_email=True,
                uploads_s3=True,
                requires_auth=True,
                enabled_flag="--execute-production-notes",
                timeout_seconds=3600,
            )
        )
    elif not skip_notes_validation:
        for label, subdomain, _, _, slug in selected_schools:
            tasks.append(
                RefreshTask(
                    name=f"notes_smoke_{slug}",
                    command=[
                        py,
                        "run_daily.py",
                        "--school",
                        subdomain,
                        "--start-date",
                        run_date,
                        "--end-date",
                        run_date,
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
                    category="notes_validation",
                    mutates_db=True,
                    requires_auth=True,
                    enabled_flag="--execute-refresh",
                    timeout_seconds=_timeout_at_least(login_timeout + 300, 600),
                )
            )

    for label, _, base_url, mailbox, slug in selected_schools:
        login_args = ["--interactive-login"] if interactive_login else ["--headless"]
        hubspot_login_args = [] if interactive_login else ["--headless"]
        pike13_login_args = ["--headless", "--reauth-if-needed"] if interactive_login else ["--headless"]
        tasks.extend(
            [
                RefreshTask(
                    name=f"dialpad_daily_intake_{slug}",
                    command=[
                        py,
                        "scripts/extract_dialpad_daily_intake.py",
                        "--db",
                        db_path,
                        "--school",
                        label,
                        "--window-days",
                        str(window_days),
                        "--limit",
                        str(dialpad_limit),
                        "--profile-dir",
                        "browser_profiles/dialpad",
                        "--login-timeout",
                        str(login_timeout),
                        *login_args,
                    ],
                    category="source_refresh",
                    mutates_db=True,
                    requires_auth=True,
                    enabled_flag="--execute-refresh",
                    timeout_seconds=_timeout_at_least(login_timeout + 300, 600),
                ),
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
                        run_date,
                        "--mailbox",
                        mailbox,
                        "--limit-per-query",
                        str(email_limit_per_query),
                        "--login-timeout",
                        str(login_timeout),
                        "--allow-production-db",
                        *login_args,
                    ],
                    category="source_refresh",
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
                    category="source_refresh",
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
                        run_date,
                        "--first-visits-limit",
                        str(pike13_limit),
                        *pike13_login_args,
                    ],
                    category="source_refresh",
                    mutates_db=True,
                    requires_auth=True,
                    enabled_flag="--execute-refresh",
                    timeout_seconds=_timeout_at_least(login_timeout + 600, 900),
                ),
                RefreshTask(
                    name=f"dialpad_sms_api_{slug}",
                    command=[
                        py,
                        "scripts/extract_dialpad_sms_api.py",
                        "--db",
                        db_path,
                        "--profile-dir",
                        "browser_profiles/dialpad",
                        "--school",
                        label,
                        "--start-date",
                        start_date,
                        "--contact-limit",
                        str(dialpad_limit),
                        "--feed-limit",
                        "50",
                        "--login-timeout",
                        str(login_timeout),
                        *login_args,
                    ],
                    category="source_refresh",
                    mutates_db=True,
                    requires_auth=True,
                    enabled_flag="--execute-refresh",
                    timeout_seconds=_timeout_at_least(login_timeout + dialpad_limit * 10 + 300, 1200),
                ),
                RefreshTask(
                    name=f"dialpad_target_search_{slug}",
                    command=[
                        py,
                        "scripts/discover_dialpad_targets.py",
                        "--db",
                        db_path,
                        "--profile-dir",
                        "browser_profiles/dialpad",
                        "--school",
                        label,
                        "--candidate-source",
                        "hubspot-contacts",
                        "--start-date",
                        start_date,
                        "--end-date",
                        run_date,
                        "--limit",
                        str(dialpad_limit),
                        "--per-target-limit",
                        "5",
                        "--output",
                        f"outputs/progress/dialpad_target_coverage_{slug}.md",
                        *login_args,
                    ],
                    category="source_refresh",
                    mutates_db=True,
                    requires_auth=True,
                    enabled_flag="--execute-refresh",
                    timeout_seconds=_timeout_at_least(login_timeout + dialpad_limit * 20 + 300, 1200),
                ),
            ]
        )

    tasks.extend(
        [
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
                    *login_args,
                ],
                category="source_refresh",
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
                category="post_refresh",
                mutates_db=True,
                enabled_flag="--execute-refresh",
                timeout_seconds=300,
            ),
            RefreshTask(
                name="build_reporting_schema",
                command=[py, "build_reporting_schema.py", "--db", db_path],
                category="post_refresh",
                mutates_db=True,
                enabled_flag="--execute-refresh",
                timeout_seconds=600,
            ),
            RefreshTask(
                name="db_integrity",
                command=["sqlite3", db_path, "PRAGMA integrity_check;"],
                category="verification",
            ),
            RefreshTask(
                name="notes_pipeline_health",
                command=[
                    py,
                    "scripts/notes_pipeline_health.py",
                    "--db",
                    db_path,
                    "--as-of",
                    (date.fromisoformat(run_date) + timedelta(days=1)).isoformat(),
                    "--lookback-days",
                    "7",
                ],
                category="verification",
            ),
            RefreshTask(
                name="source_completeness",
                command=[
                    py,
                    "scripts/source_completeness_report.py",
                    "--db",
                    db_path,
                    "--window-days",
                    "7",
                    "--pike13-lookahead-days",
                    "30",
                    "--pretty",
                ],
                category="verification",
            ),
        ]
    )
    return tasks


def build_weekly_completeness_plan(
    as_of: str,
    *,
    root: Path | None = None,
    db_path: str = "reminders.db",
    schools: Iterable[str] | None = None,
    window_days: int = 7,
) -> list[RefreshTask]:
    root = root or Path.cwd()
    py = _python(root)
    selected_schools = _school_filter(schools)
    output_root = Path("outputs/progress/weekly_completeness") / as_of
    start_date = window_start(as_of, window_days)
    tasks = [
        RefreshTask(
            name="db_integrity",
            command=["sqlite3", db_path, "PRAGMA integrity_check;"],
            category="verification",
        ),
        RefreshTask(
            name="notes_pipeline_health",
            command=[
                py,
                "scripts/notes_pipeline_health.py",
                "--db",
                db_path,
                "--as-of",
                as_of,
                "--lookback-days",
                str(window_days),
            ],
            category="verification",
        ),
        RefreshTask(
            name="source_completeness",
            command=[
                py,
                "scripts/source_completeness_report.py",
                "--db",
                db_path,
                "--window-days",
                str(window_days),
                "--pike13-lookahead-days",
                "30",
                "--pretty",
            ],
            category="verification",
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
                as_of,
            ],
            category="verification",
        ),
    ]
    for label, _, _, _, slug in selected_schools:
        tasks.extend(
            [
                RefreshTask(
                    name=f"unmatched_inbound_{slug}",
                    command=[
                        py,
                        "scripts/unmatched_inbound_report.py",
                        "--db",
                        db_path,
                        "--school",
                        label,
                        "--window-days",
                        str(window_days),
                        "--output",
                        str(output_root / slug / "unmatched_inbound_report.md"),
                    ],
                    category="weekly_report",
                ),
                RefreshTask(
                    name=f"lead_attention_{slug}",
                    command=[
                        py,
                        "scripts/lead_attention_report.py",
                        "--db",
                        db_path,
                        "--school",
                        label,
                        "--window-days",
                        str(window_days),
                        "--output",
                        str(output_root / slug / "lead_attention_report.md"),
                    ],
                    category="weekly_report",
                ),
                RefreshTask(
                    name=f"lead_operating_dashboard_{slug}",
                    command=[
                        py,
                        "scripts/lead_operating_dashboard.py",
                        "--db",
                        db_path,
                        "--school",
                        label,
                        "--period",
                        "all",
                        "--as-of",
                        as_of,
                        "--output-dir",
                        str(output_root / slug / "dashboard"),
                    ],
                    category="weekly_report",
                ),
                RefreshTask(
                    name=f"note_quality_scorecard_{slug}",
                    command=[
                        py,
                        "scripts/management_scorecards.py",
                        "--db",
                        db_path,
                        "--school",
                        label,
                        "--period",
                        "mtd",
                        "--as-of",
                        as_of,
                        "--output-dir",
                        str(output_root / slug / "scorecard"),
                    ],
                    category="weekly_report",
                ),
            ]
        )
    return tasks


def _default_runner(
    command: list[str],
    cwd: Path,
    timeout_seconds: int = 0,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        command,
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
        timeout=timeout_seconds or None,
    )


def _task_metadata(task: RefreshTask) -> dict:
    return {
        "name": task.name,
        "category": task.category,
        "command": task.command,
        "mutates_db": task.mutates_db,
        "sends_email": task.sends_email,
        "uploads_s3": task.uploads_s3,
        "requires_auth": task.requires_auth,
        "enabled_flag": task.enabled_flag,
        "timeout_seconds": task.timeout_seconds,
        "gates_refresh": task.gates_refresh,
    }


def run_refresh_plan(
    tasks: list[RefreshTask],
    *,
    root: Path | None = None,
    execute_refresh: bool = False,
    execute_verification: bool = False,
    runner: Runner | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict:
    root = root or Path.cwd()
    runner = runner or _default_runner
    task_results = []
    started_at = datetime.now().isoformat(timespec="seconds")
    refresh_blocked_by = ""

    for index, task in enumerate(tasks, start=1):
        result = _task_metadata(task)
        result["started_at"] = datetime.now().isoformat(timespec="seconds")
        result["index"] = index
        result["total_tasks"] = len(tasks)
        should_execute = execute_refresh if task.mutates_db or task.gates_refresh else execute_verification
        if progress_callback:
            progress_callback(
                "task_start",
                {
                    "index": index,
                    "total_tasks": len(tasks),
                    "name": task.name,
                    "category": task.category,
                    "will_execute": should_execute and not (refresh_blocked_by and task.mutates_db),
                    "timeout_seconds": task.timeout_seconds,
                },
            )
        if not should_execute:
            result.update(
                {
                    "status": "dry_run",
                    "ended_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
        elif refresh_blocked_by and task.mutates_db:
            result.update(
                {
                    "status": "blocked",
                    "blocked_by": refresh_blocked_by,
                    "ended_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
        else:
            try:
                if runner is _default_runner:
                    completed = _default_runner(task.command, root, task.timeout_seconds)
                else:
                    completed = runner(task.command, root)
                result.update(
                    {
                        "status": "success" if completed.returncode == 0 else "failed",
                        "returncode": completed.returncode,
                        "stdout_tail": (completed.stdout or "")[-2000:],
                        "stderr_tail": (completed.stderr or "")[-2000:],
                        "ended_at": datetime.now().isoformat(timespec="seconds"),
                    }
                )
            except subprocess.TimeoutExpired as exc:
                stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
                stderr = exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or "")
                result.update(
                    {
                        "status": "timeout",
                        "returncode": None,
                        "stdout_tail": stdout[-2000:],
                        "stderr_tail": stderr[-2000:],
                        "error": f"Timed out after {task.timeout_seconds} seconds.",
                        "ended_at": datetime.now().isoformat(timespec="seconds"),
                    }
                )
        if task.gates_refresh and result["status"] in {"failed", "timeout"} and not refresh_blocked_by:
            refresh_blocked_by = task.name
        task_results.append(result)
        if progress_callback:
            progress_callback("task_finish", result)

    if any(task["status"] in {"failed", "timeout", "blocked"} for task in task_results):
        status = "action_required"
    elif execute_refresh or execute_verification:
        status = "success"
    else:
        status = "dry_run"

    return {
        "started_at": started_at,
        "ended_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "execute_refresh": execute_refresh,
        "execute_verification": execute_verification,
        "refresh_blocked_by": refresh_blocked_by,
        "tasks": task_results,
    }


def write_metadata(metadata: dict, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8")
    return output_path
