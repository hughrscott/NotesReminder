import importlib
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_root_compatibility_shims_import():
    modules = [
        "run_daily",
        "backfill",
        "mcp_server",
        "noteschecker",
        "build_reporting_schema",
        "lead_followup_schema",
        "source_completeness",
        "lead_operating_dashboard",
        "date_window_lead_load",
        "school_email",
        "trial_followup_intelligence",
    ]
    for name in modules:
        importlib.import_module(name)


def test_mcp_server_shim_preserves_module_globals():
    import mcp_server
    from notesreminder.mcp import server

    assert mcp_server is server


def test_run_daily_help_works_through_root_shim():
    completed = subprocess.run(
        [sys.executable, "run_daily.py", "--help"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0
    assert "Run daily notes reminder" in completed.stdout
