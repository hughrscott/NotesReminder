import json
import unittest

from scripts.extract_hubspot_leads import (
    filter_deal_rows_by_school,
    merge_deal_rows,
    parse_deal_text,
    parse_hubspot_board_cards,
    parse_hubspot_table_rows,
    row_to_deal,
)


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

    def test_parse_deal_text_skips_details_placeholder_for_dates(self):
        row = parse_deal_text(
            "456",
            "https://app.hubspot.com/contacts/1/record/0-3/456",
            "\n".join(
                [
                    "Sofia Shanley | West University Place",
                    "Deal Stage:",
                    "Campers",
                    "School Name - Deal",
                    "Details",
                    "Create Date",
                    "Details",
                    "Deal Stage",
                    "Details",
                    "Campers",
                    "Last Activity Date",
                    "Details",
                    "Follow Up Needed",
                    "Details",
                    "--",
                    "Deal Activity",
                    "Apr 25, 2026 at 8:47 AM CDT",
                    "Created",
                    "Apr 25, 2026 at 8:47 AM CDT",
                ]
            ),
        )

        self.assertEqual(row["deal_name"], "Sofia Shanley | West University Place")
        self.assertEqual(row["stage"], "Campers")
        self.assertEqual(row["school"], "West University Place")
        self.assertEqual(row["create_date"], "Apr 25, 2026 at 8:47 AM CDT")
        self.assertNotEqual(row["create_date"], "Details")
        self.assertIsNone(row["follow_up_needed"])

    def test_parse_hubspot_table_rows_extracts_spine_fields(self):
        rows = parse_hubspot_table_rows(
            "\n".join(
                [
                    "DEAL NAME",
                    "DEAL STAGE",
                    "CLOSE DATE",
                    "DEAL OWNER",
                    "AMOUNT",
                    "Sofia Shanley | West University Place",
                    "Campers (Lead Pipeline)",
                    "--",
                    "SU",
                    "SOR West U (westu@schoolofrock.com)",
                    "--",
                    "Jose Perez | West University Place",
                    "Scheduled Trial/Tour (Lead Pipeline)",
                    "--",
                    "SU",
                    "SOR West U (westu@schoolofrock.com)",
                    "--",
                ]
            )
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["deal_name"], "Sofia Shanley | West University Place")
        self.assertEqual(rows[0]["stage"], "Campers")
        self.assertEqual(rows[0]["school"], "West University Place")
        self.assertIn("westu@schoolofrock.com", rows[0]["owner"])

    def test_parse_hubspot_board_cards_extracts_create_date(self):
        rows = parse_hubspot_board_cards(
            "\n".join(
                [
                    "Scheduled Trial/Tour",
                    "Maira Example | West University Place",
                    "Create date: Apr 21, 2026 7:00 PM CDT",
                    "Last contacted: Apr 22, 2026 9:15 AM CDT",
                    "Trial Date (Deal): Apr 28, 2026",
                    "Follow Up Needed:",
                    "Yes",
                ]
            )
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["stage"], "Scheduled Trial/Tour")
        self.assertEqual(rows[0]["create_date"], "Apr 21, 2026 7:00 PM CDT")
        self.assertEqual(rows[0]["follow_up_needed"], "Yes")

    def test_merge_deal_rows_preserves_spine_fields(self):
        spine = row_to_deal(
            "789",
            "https://app.hubspot.com/contacts/1/record/0-3/789",
            {
                "deal_name": "Maira Example | West University Place",
                "stage": "Scheduled Trial/Tour",
                "school": "West University Place",
                "create_date": "Apr 21, 2026 7:00 PM CDT",
                "raw_text": "spine row",
            },
            "deal_board_card",
        )
        detail = parse_deal_text(
            "789",
            "https://app.hubspot.com/contacts/1/record/0-3/789",
            "\n".join(
                [
                    "Deal name",
                    "Maira Example | West University Place",
                    "Create Date",
                    "Details",
                    "Deal Stage",
                    "Details",
                    "Contacted",
                ]
            ),
        )

        merged = merge_deal_rows(spine, detail)
        self.assertEqual(merged["stage"], "Scheduled Trial/Tour")
        self.assertEqual(merged["create_date"], "Apr 21, 2026 7:00 PM CDT")
        self.assertIn("spine row", merged["raw_text"])

    def test_filter_deal_rows_by_school(self):
        west_u = row_to_deal(
            "1",
            "https://hubspot/deal/1",
            {"deal_name": "A Lead | West University Place", "school": "West University Place"},
            "deal_table_row",
        )
        heights = row_to_deal(
            "2",
            "https://hubspot/deal/2",
            {"deal_name": "B Lead | The Heights", "school": "The Heights"},
            "deal_table_row",
        )

        rows = filter_deal_rows_by_school(
            [("1", {"href": "https://hubspot/deal/1"}, west_u), ("2", {"href": "https://hubspot/deal/2"}, heights)],
            "West University Place",
        )

        self.assertEqual([row[0] for row in rows], ["1"])


if __name__ == "__main__":
    unittest.main()
