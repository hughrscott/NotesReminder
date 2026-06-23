import unittest
from pathlib import Path

from notesreminder.orchestration.historical_backfill import (
    build_monthly_backfill_plan,
    date_chunks,
    month_windows,
)


class HistoricalBackfillTests(unittest.TestCase):
    def test_month_windows_split_calendar_months(self):
        windows = month_windows("2026-01-15", "2026-03-02")

        self.assertEqual(
            [(window.label, window.start_date, window.end_date) for window in windows],
            [
                ("2026-01", "2026-01-15", "2026-01-31"),
                ("2026-02", "2026-02-01", "2026-02-28"),
                ("2026-03", "2026-03-01", "2026-03-02"),
            ],
        )

    def test_monthly_backfill_plan_has_checkpoint_tasks(self):
        plan = build_monthly_backfill_plan(
            "2026-01-01",
            "2026-01-31",
            root=Path("/repo"),
            schools=["West U"],
        )
        names = [task.name for task in plan]

        self.assertIn("gmail_auth_preflight", names)
        self.assertIn("hubspot_auth_preflight", names)
        self.assertIn("dialpad_auth_preflight", names)
        self.assertIn("pike13_auth_preflight_westu", names)
        self.assertIn("notes_backfill_westu_2026-01-01_to_2026-01-07", names)
        self.assertIn("notes_backfill_westu_2026-01-29_to_2026-01-31", names)
        self.assertIn("school_email_westu", names)
        self.assertIn("hubspot_leads_westu", names)
        self.assertIn("pike13_leads_westu", names)
        self.assertIn("dialpad_voice", names)
        self.assertIn("dialpad_sms", names)
        self.assertIn("dialpad_call_reviews", names)
        self.assertIn("refresh_person_identities", names)
        self.assertIn("db_integrity", names)
        self.assertIn("notes_read_path_comparison", names)
        self.assertEqual(names[0], "gmail_auth_preflight")
        self.assertTrue(any(task.category == "historical_backfill_start_date_only" for task in plan))
        self.assertTrue(any(task.category == "checkpoint" for task in plan))

    def test_date_chunks_split_monthly_notes_work(self):
        self.assertEqual(
            date_chunks("2026-01-01", "2026-01-10", 7),
            [("2026-01-01", "2026-01-07"), ("2026-01-08", "2026-01-10")],
        )

    def test_monthly_backfill_plan_can_skip_notes(self):
        plan = build_monthly_backfill_plan(
            "2026-01-01",
            "2026-01-31",
            root=Path("/repo"),
            schools=["The Heights"],
            include_notes=False,
        )
        names = [task.name for task in plan]

        self.assertFalse(any(name.startswith("notes_backfill_heights") for name in names))
        self.assertIn("school_email_heights", names)

    def test_interactive_monthly_plan_uses_supported_auth_flags(self):
        plan = build_monthly_backfill_plan(
            "2026-01-01",
            "2026-01-31",
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


if __name__ == "__main__":
    unittest.main()
