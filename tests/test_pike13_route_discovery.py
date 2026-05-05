import json
import unittest

from scripts.discover_pike13_routes import render_route_report, route_row, route_signals, route_status


class Pike13RouteDiscoveryTests(unittest.TestCase):
    def test_route_signals_identify_visit_trial_outcome_exposure(self):
        signals = route_signals(
            "Past Visits\nAdult Band Trial\nNo Show\nUnpaid\nActive Membership",
            [{"href": "https://westu-sor.pike13.com/events/292297814/visits/987654", "text": "visit"}],
        )

        self.assertEqual(signals["visit_signal_visible"], 1)
        self.assertEqual(signals["trial_signal_visible"], 1)
        self.assertEqual(signals["outcome_signal_visible"], 1)
        self.assertEqual(signals["plan_signal_visible"], 1)
        self.assertEqual(signals["visit_link_count"], 1)

    def test_route_report_shows_sanitized_capabilities(self):
        text = "Past Visits\nAdult Band Trial\nComplete"
        links = [{"href": "https://westu-sor.pike13.com/visits/987654", "text": "Visit"}]
        signals = route_signals(text, links)
        status, blocker = route_status(text, signals)
        row = route_row(
            1,
            "pike13",
            "known_person_visits",
            "https://westu-sor.pike13.com/people/15046380/visits",
            status,
            text,
            links,
            ["visit history", "attendance state"],
            blocker,
        )

        raw = json.loads(row["raw_json"])
        self.assertTrue(raw["route_capabilities"]["visits_or_attendance"])
        self.assertTrue(raw["route_capabilities"]["trials_or_first_visits"])
        self.assertTrue(raw["route_capabilities"]["outcomes"])

        report = render_route_report([row])
        self.assertIn("| known_person_visits | usable |", report)
        self.assertIn("| Route | Status | Rows | Links | Visit | Trial | Outcome | Plans |", report)
        self.assertNotIn("15046380", report)


if __name__ == "__main__":
    unittest.main()
