import json
import unittest

from scripts.extract_hubspot_leads import parse_deal_text


class HubSpotExtractorTests(unittest.TestCase):
    def test_parse_deal_text_records_field_diagnostics(self):
        row = parse_deal_text(
            "123",
            "https://app.hubspot.com/contacts/1/record/0-3/123",
            "\n".join(
                [
                    "Deal name",
                    "Sample Lead",
                    "Deal Stage",
                    "Scheduled Trial",
                    "Deal owner",
                    "Owner A",
                    "https://westu-sor.pike13.com/people/15046380",
                ]
            ),
        )
        metadata = json.loads(row["raw_json"])
        self.assertEqual(row["deal_id"], "123")
        self.assertEqual(row["pike13_person_id"], "15046380")
        self.assertIn("deal_name", metadata["fields_found"])
        self.assertIn("trial_date", metadata["fields_missing"])


if __name__ == "__main__":
    unittest.main()
