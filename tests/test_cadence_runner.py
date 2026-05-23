import subprocess
import unittest
from pathlib import Path

from notesreminder.orchestration.cadence import build_cadence_plan, run_cadence


class CadenceRunnerTests(unittest.TestCase):
    def test_plan_contains_production_gate_and_shadow_reports(self):
        plan = build_cadence_plan("2026-05-23", Path("/repo"))
        names = [task.name for task in plan]
        self.assertIn("production_notes_local_mfa", names)
        self.assertIn("notes_pipeline_health", names)
        self.assertIn("lead_operating_dashboard_westu", names)
        self.assertIn("note_quality_scorecard_heights", names)
        production = next(task for task in plan if task.name == "production_notes_local_mfa")
        self.assertTrue(production.requires_mfa)
        self.assertTrue(production.mutates_db)
        self.assertTrue(production.sends_email)

    def test_dry_run_does_not_execute_commands(self):
        calls = []

        def runner(command, cwd):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

        metadata = run_cadence(
            run_date="2026-05-23",
            root=Path("/repo"),
            runner=runner,
        )
        self.assertEqual(metadata["status"], "dry_run")
        self.assertEqual(calls, [])
        statuses = {task["name"]: task["status"] for task in metadata["tasks"]}
        self.assertEqual(statuses["production_notes_local_mfa"], "skipped_requires_approval")
        self.assertEqual(statuses["notes_pipeline_health"], "dry_run")

    def test_execute_shadow_runs_only_shadow_commands(self):
        calls = []

        def runner(command, cwd):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

        metadata = run_cadence(
            run_date="2026-05-23",
            root=Path("/repo"),
            execute_shadow=True,
            runner=runner,
        )
        self.assertEqual(metadata["status"], "shadow_success")
        self.assertGreater(len(calls), 0)
        self.assertFalse(any("run_notes_local_mfa.sh" in part for call in calls for part in call))
        statuses = {task["name"]: task["status"] for task in metadata["tasks"]}
        self.assertEqual(statuses["production_notes_local_mfa"], "skipped_requires_approval")
        self.assertEqual(statuses["notes_pipeline_health"], "success")

    def test_expired_auth_simulation_is_actionable(self):
        metadata = run_cadence(
            run_date="2026-05-23",
            root=Path("/repo"),
            simulate_expired_auth=True,
        )
        self.assertEqual(metadata["status"], "action_required")
        production = next(task for task in metadata["tasks"] if task["name"] == "production_notes_local_mfa")
        self.assertEqual(production["status"], "action_required")
        self.assertIn("MFA/auth session is expired", production["error"])


if __name__ == "__main__":
    unittest.main()
