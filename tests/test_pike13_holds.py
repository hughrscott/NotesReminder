import pytest

from scrape_pike13_holds import normalize_record, report_url


def test_recent_hold_report_uses_verified_filter_key():
    url = report_url("westu-sor", "last_hold_end_date", ["2026-06-18", "2026-07-18"])
    assert "last_hold_end_date" in url
    assert "2026-06-18" in url
    assert "2026-07-18" in url


def test_active_hold_report_uses_on_hold_filter():
    assert "is_on_hold" in report_url("westu-sor", "is_on_hold")


def test_normalize_record_preserves_recent_hold_evidence():
    record = normalize_record(
        {
            "Client": "Caleb Shannon",
            "Plan Name": "Little Wing",
            "On Hold?": "No",
            "Last Hold Start Date": "Jun 1, 2026",
            "Last Hold End Date": "Jun 30, 2026",
            "Account Managers": "Steven Shannon",
            "Ended?": "No",
        },
        "westu-sor",
        "2026-07-18",
    )
    assert record["client"] == "Caleb Shannon"
    assert record["on_hold"] is False
    assert record["hold_end"] == "Jun 30, 2026"
    assert record["scraped_at"] == "2026-07-18"


def test_normalize_record_rejects_missing_client():
    with pytest.raises(ValueError, match="no Client"):
        normalize_record({}, "westu-sor", "2026-07-18")
