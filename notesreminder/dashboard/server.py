from __future__ import annotations

import html
import json
import sqlite3
from pathlib import Path

from notesreminder.reports.lead_operating_dashboard import build_snapshot, render_snapshot_markdown
from notesreminder.reports.operations_dashboard import build_operations_dashboard, render_operations_dashboard_html
from notesreminder.reports.source_completeness import build_source_completeness_report


DEFAULT_DB = "reminders.db"


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def normalize_school_slug(value: str) -> str:
    text = (value or "").strip().lower().replace("_", "-")
    if text in {"heights", "the-heights", "theheights"}:
        return "The Heights"
    if text in {"westu", "west-u", "west", "west-university", "west-university-place"}:
        return "West U"
    return value or "West U"


def _window_kwargs(period: str, as_of: str = "", start_date: str = "", end_date: str = ""):
    if period not in {"daily", "weekly", "monthly"}:
        raise ValueError("period must be daily, weekly, or monthly.")
    if bool(start_date) != bool(end_date):
        raise ValueError("start_date and end_date must be provided together.")
    kwargs = {"period": period, "as_of": as_of or None}
    if start_date and end_date:
        kwargs.update({"start_date": start_date, "end_date": end_date})
    return kwargs


def lead_dashboard_payload(
    db_path=DEFAULT_DB,
    *,
    school="West U",
    period="monthly",
    as_of="",
    start_date="",
    end_date="",
    limit=50,
):
    conn = _connect(db_path)
    try:
        kwargs = _window_kwargs(period, as_of=as_of, start_date=start_date, end_date=end_date)
        return build_snapshot(conn, school=normalize_school_slug(school), limit=limit, **kwargs)
    finally:
        conn.close()


def lead_dashboard_html(payload: dict) -> str:
    markdown = render_snapshot_markdown(payload)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(payload.get('school', 'Lead'))} Lead Dashboard</title>
  <style>
    body {{ margin: 0; background: #f7f8fa; color: #1d2430; font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; }}
    main {{ max-width: 1180px; margin: 0 auto; padding: 24px; }}
    pre {{ white-space: pre-wrap; background: #fff; border: 1px solid #d9dee7; border-radius: 6px; padding: 18px; overflow-x: auto; }}
  </style>
</head>
<body><main><pre>{html.escape(markdown)}</pre></main></body>
</html>"""


def operations_dashboard_payload(db_path=DEFAULT_DB, *, period="monthly", as_of="", limit=25):
    conn = _connect(db_path)
    try:
        return build_operations_dashboard(conn, period=period, as_of=as_of or None, limit=limit)
    finally:
        conn.close()


def source_completeness_payload(db_path=DEFAULT_DB, *, window_days=7, pike13_lookahead_days=30):
    conn = _connect(db_path)
    try:
        return build_source_completeness_report(conn, window_days, pike13_lookahead_days)
    finally:
        conn.close()


def create_app(db_path=DEFAULT_DB):
    try:
        from fastapi import FastAPI
        from fastapi.encoders import jsonable_encoder
        from fastapi.responses import HTMLResponse, JSONResponse
    except ImportError as exc:  # pragma: no cover - exercised by CLI/runtime environment
        raise RuntimeError("FastAPI is required for the local dashboard service. Install requirements.txt.") from exc

    app = FastAPI(title="NotesReminder Dashboards")
    resolved_db = str(Path(db_path))

    @app.get("/api/dashboard/lead/{school}")
    def api_lead_dashboard(
        school: str,
        period: str = "monthly",
        as_of: str = "",
        start_date: str = "",
        end_date: str = "",
        limit: int = 50,
    ):
        return JSONResponse(
            jsonable_encoder(
                lead_dashboard_payload(
                    resolved_db,
                    school=school,
                    period=period,
                    as_of=as_of,
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit,
                )
            )
        )

    @app.get("/dashboard/lead/{school}", response_class=HTMLResponse)
    def html_lead_dashboard(
        school: str,
        period: str = "monthly",
        as_of: str = "",
        start_date: str = "",
        end_date: str = "",
        limit: int = 50,
    ):
        payload = lead_dashboard_payload(
            resolved_db,
            school=school,
            period=period,
            as_of=as_of,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        return HTMLResponse(lead_dashboard_html(payload))

    @app.get("/api/dashboard/operations")
    def api_operations_dashboard(period: str = "monthly", as_of: str = "", limit: int = 25):
        return JSONResponse(
            jsonable_encoder(
                operations_dashboard_payload(resolved_db, period=period, as_of=as_of, limit=limit)
            )
        )

    @app.get("/dashboard/operations", response_class=HTMLResponse)
    def html_operations_dashboard(period: str = "monthly", as_of: str = "", limit: int = 25):
        payload = operations_dashboard_payload(resolved_db, period=period, as_of=as_of, limit=limit)
        return HTMLResponse(render_operations_dashboard_html(payload))

    @app.get("/api/source-completeness")
    def api_source_completeness(window_days: int = 7, pike13_lookahead_days: int = 30):
        return JSONResponse(
            jsonable_encoder(
                source_completeness_payload(
                    resolved_db,
                    window_days=window_days,
                    pike13_lookahead_days=pike13_lookahead_days,
                )
            )
        )

    @app.get("/")
    def root():
        return JSONResponse(
            {
                "service": "NotesReminder Dashboards",
                "routes": [
                    "/dashboard/operations",
                    "/dashboard/lead/westu",
                    "/dashboard/lead/heights",
                    "/api/dashboard/operations",
                    "/api/dashboard/lead/{school}",
                    "/api/source-completeness",
                ],
            }
        )

    return app


def payload_to_json(payload) -> str:
    return json.dumps(payload, indent=2, default=str)
