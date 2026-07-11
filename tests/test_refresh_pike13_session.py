from datetime import datetime, timezone

import refresh_pike13_session as refresh


SAMPLE_ENVELOPES = """
| ID     | FLAGS | SUBJECT                                                                      | FROM                                              | DATE                   |
|--------|-------|------------------------------------------------------------------------------|---------------------------------------------------|------------------------|
| 158478 |  *    | Your verification code for School of Rock West U, School of Rock The Heights | School of Rock West U, School of Rock The Heights | 2026-07-11 05:03+00:00 |
| 158477 |       | Your verification code for School of Rock West U, School of Rock The Heights | School of Rock West U, School of Rock The Heights | 2026-07-11 05:01+00:00 |
"""


def test_parse_envelopes_filters_by_request_timestamp():
    requested_at = datetime(2026, 7, 11, 5, 2, 30, tzinfo=timezone.utc)

    rows = refresh.parse_fresh_verification_envelopes(SAMPLE_ENVELOPES, requested_at)

    assert [row[0] for row in rows] == ["158478"]


def test_extract_code_accepts_reused_code_from_fresh_message():
    body = "Your code: 123456\nThis code expires in 10 minutes."

    assert refresh.extract_verification_code(body) == "123456"
