import subprocess
import unittest
from pathlib import Path

from notesreminder.orchestration.refresh_all_sources import (
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


if __name__ == "__main__":
    unittest.main()
