from urllib.parse import unquote

from scrape_pike13_late_cancels import build_report_path


def test_report_path_requests_late_canceled_state_and_date_window():
    path = build_report_path("2026-05-21", "2026-07-19")
    decoded = unquote(path)
    assert path.startswith("/desk/reports#/enrollments/details?")
    assert "%27" in path
    assert "state:!((eq:!(late_canceled)))" in decoded
    assert "service_date:!((btw:!('2026-05-21','2026-07-19')))" in decoded
