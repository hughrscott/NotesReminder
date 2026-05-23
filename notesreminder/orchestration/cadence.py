"""Cadence planning and run metadata for scheduled operations."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable


Runner = Callable[[list[str], Path], subprocess.CompletedProcess]


@dataclass(frozen=True)
class CadenceTask:
    name: str
    command: list[str]
    category: str
    requires_mfa: bool = False
    mutates_db: bool = False
    sends_email: bool = False


def _python(root: Path) -> str:
    venv_python = root / "venv" / "bin" / "python"
    return str(venv_python if venv_python.exists() else "python3")


def build_cadence_plan(run_date: str, root: Path | None = None) -> list[CadenceTask]:
    root = root or Path.cwd()
    py = _python(root)
    dashboard_root = Path("outputs/progress/cadence_dashboards") / run_date
    scorecard_root = Path("outputs/progress/cadence_scorecards") / run_date
    return [
        CadenceTask(
            name="production_notes_local_mfa",
            command=["scripts/run_notes_local_mfa.sh", "--date", run_date],
            category="production_notes",
            requires_mfa=True,
            mutates_db=True,
            sends_email=True,
        ),
        CadenceTask(
            name="notes_pipeline_health",
            command=[py, "scripts/notes_pipeline_health.py", "--db", "reminders.db"],
            category="shadow_report",
        ),
        CadenceTask(
            name="source_completeness",
            command=[
                py,
                "scripts/source_completeness_report.py",
                "--db",
                "reminders.db",
                "--window-days",
                "7",
                "--pike13-lookahead-days",
                "30",
            ],
            category="shadow_report",
        ),
        *[
            CadenceTask(
                name=f"lead_operating_dashboard_{slug}",
                command=[
                    py,
                    "scripts/lead_operating_dashboard.py",
                    "--db",
                    "reminders.db",
                    "--school",
                    school,
                    "--period",
                    "all",
                    "--as-of",
                    run_date,
                    "--output-dir",
                    str(dashboard_root / slug),
                ],
                category="shadow_report",
            )
            for school, slug in (("West U", "westu"), ("The Heights", "heights"))
        ],
        *[
            CadenceTask(
                name=f"note_quality_scorecard_{slug}",
                command=[
                    py,
                    "scripts/management_scorecards.py",
                    "--db",
                    "reminders.db",
                    "--school",
                    school,
                    "--period",
                    "mtd",
                    "--as-of",
                    run_date,
                    "--output-dir",
                    str(scorecard_root / slug),
                ],
                category="shadow_report",
            )
            for school, slug in (("West U", "westu"), ("The Heights", "heights"))
        ],
    ]


def _default_runner(command: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(command, cwd=cwd, check=False, text=True, capture_output=True)


def _task_metadata(task: CadenceTask) -> dict:
    return {
        "name": task.name,
        "category": task.category,
        "command": task.command,
        "requires_mfa": task.requires_mfa,
        "mutates_db": task.mutates_db,
        "sends_email": task.sends_email,
    }


def write_metadata(metadata: dict, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8")
    return output_path


def run_cadence(
    run_date: str | None = None,
    root: Path | None = None,
    execute_shadow: bool = False,
    execute_production: bool = False,
    simulate_expired_auth: bool = False,
    runner: Runner | None = None,
) -> dict:
    root = root or Path.cwd()
    run_date = run_date or date.today().isoformat()
    runner = runner or _default_runner
    started_at = datetime.now().isoformat(timespec="seconds")
    tasks = build_cadence_plan(run_date, root)
    task_results = []

    for task in tasks:
        result = _task_metadata(task)
        result["started_at"] = datetime.now().isoformat(timespec="seconds")
        if simulate_expired_auth and task.requires_mfa:
            result.update(
                {
                    "status": "action_required",
                    "error": "MFA/auth session is expired; approve or renew the browser session before production notes run.",
                    "ended_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
        elif task.category == "production_notes" and not execute_production:
            result.update(
                {
                    "status": "skipped_requires_approval",
                    "error": "Production notes/email execution requires explicit approval.",
                    "ended_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
        elif task.category == "shadow_report" and not execute_shadow:
            result.update(
                {
                    "status": "dry_run",
                    "ended_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
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
        task_results.append(result)

    failure_statuses = {"failed", "action_required"}
    if any(task["status"] in failure_statuses for task in task_results):
        status = "action_required"
    elif execute_shadow:
        status = "shadow_success"
    else:
        status = "dry_run"

    return {
        "run_date": run_date,
        "started_at": started_at,
        "ended_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "execute_shadow": execute_shadow,
        "execute_production": execute_production,
        "simulate_expired_auth": simulate_expired_auth,
        "tasks": task_results,
    }
