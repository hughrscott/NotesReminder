from datetime import date

import run_retention_cron


def test_refresh_sources_collects_60_day_late_cancel_window(monkeypatch):
    calls = []

    def capture(label, command, timeout=900):
        calls.append((label, command, timeout))

    monkeypatch.setattr(run_retention_cron, "run_step", capture)
    run_retention_cron.refresh_sources(date(2026, 7, 19))

    late = [command for label, command, _ in calls if label == "REFRESH LATE-CANCELLATION SHADOW SOURCE"]
    assert len(late) == 1
    assert late[0][-4:] == ["--start-date", "2026-05-21", "--end-date", "2026-07-19"]


def test_generate_shadow_is_explicitly_separate_from_actionable_report(tmp_path, monkeypatch):
    calls = []

    def capture(label, command, timeout=900):
        calls.append((label, command))

    monkeypatch.setattr(run_retention_cron, "run_step", capture)
    monkeypatch.setattr(run_retention_cron, "MODELS_DIR", tmp_path)

    report_path, observations_path = run_retention_cron.generate_shadow(date(2026, 7, 19))

    assert calls[0][0] == "GENERATE LATE-CANCELLATION SHADOW REPORT"
    assert "late_cancel_shadow.py" in calls[0][1]
    assert report_path.name == "late_cancel_shadow_report.txt"
    assert observations_path.name == "late_cancel_shadow_observations.csv"
