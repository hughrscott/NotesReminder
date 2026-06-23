import subprocess
import sys
import unittest
from pathlib import Path

from notesreminder.orchestration.refresh_all_sources import (
    RefreshTask,
    build_daily_refresh_plan,
    build_weekly_completeness_plan,
    run_refresh_plan,
)


class RefreshAllSourcesTests(unittest.TestCase):
    def test_daily_plan_is_gated_and_contains_source_refreshes(self):
        plan = build_daily_refresh_plan(
            "2026-05-23",
            root=Path("/repo"),
            db_path="reminders.db",
            schools=["West U"],
        )
        names = [task.name for task in plan]
        self.assertIn("gmail_auth_preflight", names)
        self.assertIn("hubspot_auth_preflight", names)
        self.assertIn("dialpad_auth_preflight", names)
        self.assertIn("pike13_auth_preflight_westu", names)
        self.assertIn("notes_smoke_westu", names)
        self.assertIn("dialpad_daily_intake_westu", names)
        self.assertIn("school_email_westu", names)
        self.assertIn("hubspot_leads_westu", names)
        self.assertIn("pike13_leads_westu", names)
        self.assertIn("refresh_person_identities", names)
        self.assertIn("source_completeness", names)
        self.assertTrue(any(task.mutates_db for task in plan))
        self.assertFalse(any(task.sends_email for task in plan))
        self.assertFalse(any(task.uploads_s3 for task in plan))

    def test_daily_production_notes_requires_explicit_email_s3_plan(self):
        plan = build_daily_refresh_plan(
            "2026-05-23",
            root=Path("/repo"),
            send_email=True,
            upload_s3=True,
        )
        production = next(task for task in plan if task.name == "production_notes_local_mfa")
        self.assertTrue(production.sends_email)
        self.assertTrue(production.uploads_s3)
        self.assertTrue(production.mutates_db)

    def test_daily_plan_can_skip_notes_validation(self):
        plan = build_daily_refresh_plan(
            "2026-05-24",
            root=Path("/repo"),
            schools=["West U"],
            skip_notes_validation=True,
        )
        names = [task.name for task in plan]
        self.assertNotIn("notes_smoke_westu", names)
        self.assertIn("dialpad_daily_intake_westu", names)

    def test_interactive_daily_plan_uses_supported_auth_flags(self):
        plan = build_daily_refresh_plan(
            "2026-05-24",
            root=Path("/repo"),
            schools=["West U"],
            interactive_login=True,
        )
        hubspot = next(task for task in plan if task.name == "hubspot_leads_westu")
        pike13 = next(task for task in plan if task.name == "pike13_leads_westu")

        self.assertNotIn("--interactive-login", hubspot.command)
        self.assertIn("--reauth-if-needed", pike13.command)
        self.assertGreater(hubspot.timeout_seconds, 0)
        self.assertGreater(pike13.timeout_seconds, 0)

    def test_refresh_auth_preflight_runs_with_execute_refresh(self):
        calls = []

        def runner(command, cwd):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

        metadata = run_refresh_plan(
            [
                RefreshTask(
                    name="auth_gate",
                    command=["auth"],
                    category="auth_preflight",
                    gates_refresh=True,
                )
            ],
            root=Path("/repo"),
            execute_refresh=True,
            runner=runner,
        )

        self.assertEqual(metadata["status"], "success")
        self.assertEqual(calls, [["auth"]])
        self.assertEqual(metadata["tasks"][0]["status"], "success")

    def test_failed_auth_preflight_blocks_mutating_refresh_tasks(self):
        calls = []

        def runner(command, cwd):
            calls.append(command)
            return subprocess.CompletedProcess(command, 2, stdout="", stderr="needs login")

        metadata = run_refresh_plan(
            [
                RefreshTask(
                    name="auth_gate",
                    command=["auth"],
                    category="auth_preflight",
                    gates_refresh=True,
                ),
                RefreshTask(
                    name="db_refresh",
                    command=["refresh"],
                    category="source_refresh",
                    mutates_db=True,
                ),
            ],
            root=Path("/repo"),
            execute_refresh=True,
            runner=runner,
        )

        self.assertEqual(metadata["status"], "action_required")
        self.assertEqual(metadata["refresh_blocked_by"], "auth_gate")
        self.assertEqual([task["status"] for task in metadata["tasks"]], ["failed", "blocked"])
        self.assertEqual(metadata["tasks"][1]["blocked_by"], "auth_gate")
        self.assertEqual(calls, [["auth"]])

    def test_weekly_plan_is_read_only(self):
        plan = build_weekly_completeness_plan(
            "2026-05-24",
            root=Path("/repo"),
            schools=["The Heights"],
        )
        names = [task.name for task in plan]
        self.assertIn("db_integrity", names)
        self.assertIn("notes_pipeline_health", names)
        self.assertIn("source_completeness", names)
        self.assertIn("notes_read_path_comparison", names)
        self.assertIn("lead_attention_heights", names)
        self.assertFalse(any(task.mutates_db for task in plan))
        self.assertFalse(any(task.sends_email for task in plan))

    def test_dry_run_executes_nothing(self):
        calls = []

        def runner(command, cwd):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

        metadata = run_refresh_plan(
            build_weekly_completeness_plan("2026-05-24", root=Path("/repo")),
            root=Path("/repo"),
            runner=runner,
        )
        self.assertEqual(metadata["status"], "dry_run")
        self.assertEqual(calls, [])
        self.assertTrue(all(task["status"] == "dry_run" for task in metadata["tasks"]))

    def test_execute_verification_runs_read_only_tasks(self):
        calls = []

        def runner(command, cwd):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

        metadata = run_refresh_plan(
            build_weekly_completeness_plan("2026-05-24", root=Path("/repo")),
            root=Path("/repo"),
            execute_verification=True,
            runner=runner,
        )
        self.assertEqual(metadata["status"], "success")
        self.assertGreater(len(calls), 0)
        self.assertTrue(all(task["status"] == "success" for task in metadata["tasks"]))

    def test_execute_timeout_returns_action_required(self):
        metadata = run_refresh_plan(
            [
                RefreshTask(
                    name="slow_verification",
                    command=[sys.executable, "-c", "import time; time.sleep(2)"],
                    category="verification",
                    timeout_seconds=1,
                )
            ],
            root=Path.cwd(),
            execute_verification=True,
        )

        self.assertEqual(metadata["status"], "action_required")
        self.assertEqual(metadata["tasks"][0]["status"], "timeout")
        self.assertIn("Timed out after 1 seconds", metadata["tasks"][0]["error"])


if __name__ == "__main__":
    unittest.main()
