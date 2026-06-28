import json
import sqlite3
import unittest

from lead_followup_schema import ensure_lead_followup_schema
from scripts.extract_hubspot_leads import (
    apply_pike13_match_from_db,
    associated_deal_summaries,
    contact_rows_from_dataset,
    filter_deal_rows_by_school,
    goto_hubspot_url,
    is_hubspot_auth_page,
    merge_deal_rows,
    merge_contact_rows,
    parse_contact_detail_text,
    parse_contact_from_text,
    parse_contact_report_rows,
    parse_deal_text,
    parse_hubspot_board_cards,
    parse_hubspot_table_rows,
    pike13_trial_reconciliation,
    row_to_deal,
    upsert_contact,
)


class HubSpotExtractorTests(unittest.TestCase):
    def test_goto_hubspot_url_uses_auth_launch_before_target(self):
        class FakePage:
            def __init__(self):
                self.url = "about:blank"
                self.visited = []

            def goto(self, url, **_kwargs):
                self.url = url
                self.visited.append(url)

            def wait_for_load_state(self, *_args, **_kwargs):
                return None

        page = FakePage()

        goto_hubspot_url(page, "https://app.hubspot.com/reports-dashboard/1", "https://sor.okta.com/home/hubspotsaml/app")

        self.assertEqual(
            page.visited,
            [
                "https://sor.okta.com/home/hubspotsaml/app",
                "https://app.hubspot.com/reports-dashboard/1",
            ],
        )

    def test_auth_page_detection(self):
        self.assertTrue(
            is_hubspot_auth_page(
                "https://app.hubspot.com/login/?loginRedirectUrl=https%3A%2F%2Fapp.hubspot.com%2Fcontacts",
                "Sign in with your account to access HubSpot\nPowered by Okta",
            )
        )
        self.assertFalse(
            is_hubspot_auth_page(
                "https://app.hubspot.com/contacts/6841203/objects/0-3/views/all/list",
                "Deals\nAll deals\nNew deals this month",
            )
        )

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

    def test_parse_deal_text_rejects_placeholder_enrichment_fields(self):
        row = parse_deal_text(
            "456",
            "https://app.hubspot.com/contacts/1/record/0-3/456",
            "\n".join(
                [
                    "Sofia Shanley | West University Place",
                    "Follow Up Needed",
                    "Details",
                    "Trial Date - Display Deal",
                    "Details",
                    "- Display Deal",
                    "Trial No Show",
                    "Details",
                    "Maybe",
                    "Area of Interest",
                    "Details",
                    "Details",
                    "Instrument Type",
                    "Details",
                    "- Deal",
                    "Lead Source - Deal",
                    "Details",
                    "- Deal",
                    "Marketing Source - Deal",
                    "Details",
                    "GA UTM Term - Deal",
                ]
            ),
        )

        for field in (
            "follow_up_needed",
            "trial_date",
            "trial_no_show",
            "area_of_interest",
            "instrument_type",
            "lead_source",
            "marketing_source",
        ):
            self.assertIsNone(row[field], field)

    def test_parse_deal_text_accepts_valid_enrichment_fields(self):
        row = parse_deal_text(
            "456",
            "https://app.hubspot.com/contacts/1/record/0-3/456",
            "\n".join(
                [
                    "Sofia Shanley | West University Place",
                    "Follow Up Needed",
                    "Yes",
                    "Trial Date (Deal)",
                    "Apr 28, 2026",
                    "Trial No Show",
                    "No",
                    "Area of Interest",
                    "Rock 101",
                    "Instrument Type",
                    "Guitar",
                    "Lead Source - Deal",
                    "Online",
                    "Marketing Source - Deal",
                    "Paid Search",
                    "Last Contacted",
                    "Apr 25, 2026 at 8:47 AM CDT",
                ]
            ),
        )

        self.assertEqual(row["follow_up_needed"], "Yes")
        self.assertEqual(row["trial_date"], "Apr 28, 2026")
        self.assertEqual(row["trial_no_show"], "No")
        self.assertEqual(row["area_of_interest"], "Rock 101")
        self.assertEqual(row["instrument_type"], "Guitar")
        self.assertEqual(row["lead_source"], "Online")
        self.assertEqual(row["marketing_source"], "Paid Search")
        self.assertEqual(row["last_contacted"], "Apr 25, 2026 at 8:47 AM CDT")

    def test_parse_contact_from_text_rejects_internal_email_and_records_diagnostics(self):
        row = parse_contact_from_text(
            "deal-1",
            "https://app.hubspot.com/contacts/1/record/0-3/deal-1",
            "\n".join(
                [
                    "Maira Pirzada",
                    "School of Rock West University Place",
                    "maira@example.com",
                    "(713) 555-1212",
                    "Email - Thank You from Calvin Barnhill",
                    "to Maira Pirzada",
                    "calvin@schoolofrock.com",
                ]
            ),
        )

        metadata = json.loads(row["raw_json"])
        self.assertTrue(metadata["trusted"])
        self.assertEqual(row["email_normalized"], "maira@example.com")
        self.assertEqual(row["phone_normalized"], "7135551212")
        self.assertEqual(row["full_name"], "Maira Pirzada")
        self.assertEqual(metadata["rejected_emails"][0]["email"], "calvin@schoolofrock.com")

    def test_parse_contact_report_rows_from_drilldown_table(self):
        rows = parse_contact_report_rows(
            "\n".join(
                [
                    "5/1/2026",
                    "5/3/2026",
                    "5/5/2026",
                    "Report details",
                    "2 Contacts",
                    "CONTACT",
                    "EMAIL",
                    "CREATE DATE",
                    "Hannie A",
                    "-",
                    "5/1/2026",
                    "Bere A",
                    "bere0384@gmail.com",
                    "5/1/2026",
                ]
            )
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["full_name"], "Hannie A")
        self.assertEqual(rows[0]["create_date"], "5/1/2026")
        self.assertIsNone(rows[0]["email_normalized"])
        self.assertEqual(rows[1]["email_normalized"], "bere0384@gmail.com")

    def test_contact_rows_from_dataset_extracts_labels_links_and_create_dates(self):
        rows, total, offset = contact_rows_from_dataset(
            "2026-05-01",
            {
                "result": {
                    "primaryDataSet": {
                        "data": [
                            {
                                "vid": "219226282497",
                                "hs_object_id": "219226282497",
                                "createdate": "1777663573758",
                                "email": "bere0384@gmail.com",
                            }
                        ],
                        "identifiers": {
                            "vid": {
                                "219226282497": {
                                    "references": {
                                        "label": "Bere A",
                                    },
                                },
                            },
                        },
                        "links": {
                            "vid": {
                                "219226282497": {
                                    "219226282497": "/contacts/6841203/contact/219226282497",
                                },
                            },
                        },
                        "pagination": {
                            "offset": 1,
                            "total": 1,
                        },
                    },
                },
            },
        )

        self.assertEqual(total, 1)
        self.assertEqual(offset, 1)
        self.assertEqual(rows[0][0], "219226282497")
        self.assertEqual(rows[0][2]["full_name"], "Bere A")
        self.assertEqual(rows[0][2]["create_date"], "2026-05-01")
        self.assertEqual(rows[0][2]["email_normalized"], "bere0384@gmail.com")
        self.assertEqual(rows[0][2]["source_url"], "https://app.hubspot.com/contacts/6841203/contact/219226282497")

    def test_parse_contact_detail_text_uses_parent_guardian_identity(self):
        row = parse_contact_detail_text(
            "219179482293",
            "https://app.hubspot.com/contacts/6841203/record/0-1/219179482293",
            "\n".join(
                [
                    "Hannie A",
                    "School of Rock West University Place",
                    "About This Student",
                    "Create Date",
                    "Details",
                    "Overview",
                    "Created",
                    "May 1, 2026 at 9:26 PM GMT+2",
                    "This contact was created from Offline Sources fromLarger Scope Workflow App",
                    "Parent/Guardian (1)",
                    "Bere A",
                    "Email:bere0384@gmail.com",
                    "Phone Number:8328966375",
                ]
            ),
            {"full_name": "Hannie A", "create_date": "5/1/2026"},
        )

        self.assertEqual(row["full_name"], "Hannie A")
        self.assertEqual(row["email_normalized"], "bere0384@gmail.com")
        self.assertEqual(row["phone_normalized"], "8328966375")
        self.assertEqual(row["school"], "West University Place")
        self.assertEqual(row["lead_source"], "Offline Sources")

    def test_parse_contact_detail_text_captures_associated_trial_deal(self):
        row = parse_contact_detail_text(
            "contact-1",
            "https://app.hubspot.com/contacts/6841203/record/0-1/contact-1",
            "\n".join(
                [
                    "Jamie Example",
                    "School of Rock The Heights",
                    "Associated deals",
                    "DEAL NAME",
                    "DEAL STAGE",
                    "Jamie Example | The Heights",
                    "Student for Deal",
                    "Scheduled Trial/Tour (Lead Pipeline)",
                    "Trial Date (Deal)",
                    "May 23, 2026",
                ]
            ),
            {"full_name": "Jamie Example", "create_date": "5/20/2026"},
        )

        self.assertEqual(row["hubspot_deal_name"], "Jamie Example | The Heights")
        self.assertEqual(row["hubspot_deal_stage"], "Scheduled Trial/Tour")
        self.assertEqual(row["hubspot_trial_date"], "May 23, 2026")
        self.assertEqual(row["hubspot_trial_scheduled_flag"], 1)
        self.assertEqual(row["school"], "The Heights")
        self.assertEqual(associated_deal_summaries(row["raw_text"])[0]["stage"], "Scheduled Trial/Tour")

    def test_parse_contact_detail_text_captures_pike13_loaded_signal(self):
        row = parse_contact_detail_text(
            "219179482293",
            "https://app.hubspot.com/contacts/6841203/record/0-1/219179482293",
            "\n".join(
                [
                    "Hannie A",
                    "Email",
                    "hannie@example.com",
                    "Create Date",
                    "5/1/2026",
                    "School",
                    "West University Place",
                    "Record source detail 3",
                    "Offline Sources",
                    "Registration Method",
                    "Web form",
                    "https://westu-sor.pike13.com/people/15046380",
                ]
            ),
            {"full_name": "Hannie A", "create_date": "5/1/2026"},
        )

        self.assertEqual(row["contact_id"], "219179482293")
        self.assertEqual(row["email_normalized"], "hannie@example.com")
        self.assertEqual(row["school"], "West University Place")
        self.assertEqual(row["record_source_detail"], "Offline Sources")
        self.assertEqual(row["registration_method"], "Web form")
        self.assertEqual(row["pike13_person_id"], "15046380")
        self.assertEqual(row["pike13_loaded_flag"], 1)

    def test_contact_upsert_preserves_spine_date_and_canonical_school(self):
        conn = sqlite3.connect(":memory:")
        ensure_lead_followup_schema(conn)
        detail_row = parse_contact_detail_text(
            "contact-school-noise",
            "https://app.hubspot.com/contacts/6841203/record/0-1/contact-school-noise",
            "\n".join(
                [
                    "Private Student",
                    "School",
                    "Contact Activity",
                    "Associated deals",
                    "DEAL NAME",
                    "DEAL STAGE",
                    "Private Student | The Heights",
                    "Scheduled Trial/Tour (Lead Pipeline)",
                ]
            ),
            {"full_name": "Private Student", "create_date": "2026-01-21"},
        )
        merged = merge_contact_rows(
            {"full_name": "Private Student", "create_date": "2026-01-21"},
            detail_row,
        )

        upsert_contact(conn, merged)
        stored = conn.execute(
            "SELECT create_date, school FROM hubspot_contacts WHERE contact_id = ?",
            ("contact-school-noise",),
        ).fetchone()

        self.assertEqual(stored[0], "2026-01-21")
        self.assertEqual(stored[1], "The Heights")

    def test_contact_row_marks_existing_pike13_person_by_email(self):
        conn = sqlite3.connect(":memory:")
        ensure_lead_followup_schema(conn)
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, email, email_normalized, school, updated_at
            )
            VALUES ('pike-person-1', 'Hannie A', 'hannie@example.com',
                    'hannie@example.com', 'West U', '2026-05-31T00:00:00+00:00')
            """
        )
        row = {
            "contact_id": "219179482293",
            "email_normalized": "hannie@example.com",
            "pike13_loaded_flag": 0,
        }

        matched = apply_pike13_match_from_db(conn, row)

        self.assertEqual(matched["pike13_person_id"], "pike-person-1")
        self.assertEqual(matched["pike13_loaded_flag"], 1)
        self.assertEqual(matched["pike13_match_method"], "existing_pike13_email")
        self.assertEqual(matched["school"], "West U")

    def test_contact_row_fills_blank_school_from_existing_pike13_match(self):
        conn = sqlite3.connect(":memory:")
        ensure_lead_followup_schema(conn)
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, email, email_normalized, school, updated_at
            )
            VALUES ('pike-school-fill', 'School Fill', 'fill@example.com',
                    'fill@example.com', 'The Heights', '2026-05-31T00:00:00+00:00')
            """
        )
        row = {
            "contact_id": "contact-school-fill",
            "email_normalized": "fill@example.com",
            "pike13_loaded_flag": 0,
        }

        matched = apply_pike13_match_from_db(conn, row)

        self.assertEqual(matched["pike13_person_id"], "pike-school-fill")
        self.assertEqual(matched["school"], "The Heights")

    def test_contact_row_marks_existing_pike13_person_by_name_and_school(self):
        conn = sqlite3.connect(":memory:")
        ensure_lead_followup_schema(conn)
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, school, updated_at
            )
            VALUES ('pike-person-2', 'Delilah Buque', 'The Heights',
                    '2026-05-31T00:00:00+00:00')
            """
        )
        row = {
            "contact_id": "contact-delilah",
            "full_name": "Delilah Buque",
            "school": "TX - The Heights",
            "pike13_loaded_flag": 0,
        }

        matched = apply_pike13_match_from_db(conn, row)

        self.assertEqual(matched["pike13_person_id"], "pike-person-2")
        self.assertEqual(matched["pike13_loaded_flag"], 1)
        self.assertEqual(matched["pike13_match_method"], "existing_pike13_name_school")

    def test_contact_row_matches_pike13_student_from_hubspot_deal_name(self):
        conn = sqlite3.connect(":memory:")
        ensure_lead_followup_schema(conn)
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, school, updated_at
            )
            VALUES ('pike-person-3', 'Delilah Buque', 'The Heights',
                    '2026-05-31T00:00:00+00:00')
            """
        )
        row = {
            "contact_id": "contact-parent",
            "full_name": "Mercedes Sanchez",
            "school": "75% complete",
            "hubspot_deal_name": "Delilah Buque | The Heights",
            "pike13_loaded_flag": 0,
        }

        matched = apply_pike13_match_from_db(conn, row)

        self.assertEqual(matched["pike13_person_id"], "pike-person-3")
        self.assertEqual(matched["pike13_loaded_flag"], 1)
        self.assertEqual(matched["pike13_match_method"], "existing_pike13_deal_name_school")

    def test_contact_row_matches_duplicate_token_student_name_from_deal_name(self):
        conn = sqlite3.connect(":memory:")
        ensure_lead_followup_schema(conn)
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, school, updated_at
            )
            VALUES ('pike-person-4', 'Abel Aleman', 'The Heights',
                    '2026-05-31T00:00:00+00:00')
            """
        )
        row = {
            "contact_id": "contact-abel",
            "full_name": "Joseph Aleman",
            "hubspot_deal_name": "Abel Aleman Aleman | The Heights",
            "create_date": "2026-05-08",
            "pike13_loaded_flag": 0,
        }

        matched = apply_pike13_match_from_db(conn, row)

        self.assertEqual(matched["pike13_person_id"], "pike-person-4")
        self.assertEqual(matched["pike13_match_method"], "existing_pike13_deal_name_school")

    def test_contact_row_uses_trial_date_to_break_duplicate_name_matches(self):
        conn = sqlite3.connect(":memory:")
        ensure_lead_followup_schema(conn)
        conn.executescript(
            """
            INSERT INTO pike13_people (
                person_id, full_name, school, updated_at
            )
            VALUES ('old-fiona', 'Fiona Gibson', 'The Heights',
                    '2026-05-31T00:00:00+00:00'),
                   ('new-fiona', 'Fiona Gibson', 'The Heights',
                    '2026-05-31T00:00:00+00:00');
            INSERT INTO pike13_visits (
                visit_id, person_id, service, starts_at, first_visit_flag, school, updated_at
            )
            VALUES ('old-visit', 'old-fiona', 'Rock 101 Camp', '2025-07-21T09:30:00', 1, 'The Heights',
                    '2026-05-31T00:00:00+00:00'),
                   ('new-visit', 'new-fiona', 'Trial - Drums', '2026-03-16T16:00:00', 1, 'The Heights',
                    '2026-05-31T00:00:00+00:00');
            """
        )
        row = {
            "contact_id": "contact-fiona",
            "full_name": "Marian Gibson",
            "hubspot_deal_name": "Fiona Gibson | The Heights",
            "create_date": "2026-03-08",
            "pike13_loaded_flag": 0,
        }

        matched = apply_pike13_match_from_db(conn, row)

        self.assertEqual(matched["pike13_person_id"], "new-fiona")
        self.assertEqual(matched["pike13_match_method"], "existing_pike13_deal_name_school")

    def test_pike13_trial_reconciliation_returns_trial_outcome_and_conversion(self):
        conn = sqlite3.connect(":memory:")
        ensure_lead_followup_schema(conn)
        conn.execute(
            """
            INSERT INTO pike13_people (
                person_id, full_name, email, email_normalized, school, updated_at
            )
            VALUES ('person-1', 'Jamie Example', 'jamie@example.com',
                    'jamie@example.com', 'The Heights', '2026-05-31T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO pike13_visits (
                visit_id, person_id, service, starts_at, status, first_visit_flag,
                attendance_confirmed_flag, checked_in_flag, school, updated_at
            )
            VALUES ('visit-1', 'person-1', 'Trial - Drums', '2026-05-23T10:45:00',
                    'Complete', 1, 1, 1, 'The Heights', '2026-05-31T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO pike13_plans_passes (
                plan_pass_id, person_id, name, status, starts_at, school, updated_at
            )
            VALUES ('plan-1', 'person-1', 'Rock 101 Membership', 'Active',
                    '2026-05-24T00:00:00', 'The Heights', '2026-05-31T00:00:00+00:00')
            """
        )

        reconciliation = pike13_trial_reconciliation(
            conn,
            {
                "contact_id": "contact-1",
                "email_normalized": "jamie@example.com",
                "pike13_loaded_flag": 0,
            },
        )

        self.assertEqual(reconciliation["pike13_person_id"], "person-1")
        self.assertTrue(reconciliation["trial_found"])
        self.assertEqual(reconciliation["trial"]["starts_at"], "2026-05-23T10:45:00")
        self.assertEqual(reconciliation["trial"]["happened_flag"], 1)
        self.assertTrue(reconciliation["conversion_found"])
        self.assertEqual(reconciliation["conversion"]["starts_at"], "2026-05-24T00:00:00")

    def test_parse_contact_from_text_does_not_accept_internal_email_only(self):
        row = parse_contact_from_text(
            "deal-1",
            "https://app.hubspot.com/contacts/1/record/0-3/deal-1",
            "\n".join(
                [
                    "Email - Thank You from Calvin Barnhill",
                    "calvin@schoolofrock.com",
                ]
            ),
        )

        self.assertIsNone(row)

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
