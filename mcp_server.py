import json
import os
import re
import sqlite3
from datetime import datetime

import boto3
from mcp.server.fastmcp import FastMCP

from import_call_data import run_import
from lead_followup_schema import ensure_lead_followup_schema
from lead_operating_dashboard import (
    build_exception_queue,
    build_snapshot,
    lead_evidence_timeline as build_lead_evidence_timeline,
)
from source_completeness import build_source_completeness_report

DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), "reminders.db")
DEFAULT_LEAD_DB_PATH = os.path.join(
    os.path.dirname(__file__),
    "outputs",
    "lead_intelligence",
    "lead_intelligence_working.db",
)
DB_PATH = os.getenv("REMINDERS_DB_PATH", DEFAULT_DB_PATH)
USE_UNIFIED_DB = os.getenv("NOTESREMINDER_UNIFIED_DB", "").lower() in {"1", "true", "yes"}
LEAD_DB_PATH = os.getenv(
    "LEAD_INTELLIGENCE_DB_PATH",
    DB_PATH if USE_UNIFIED_DB else DEFAULT_LEAD_DB_PATH,
)
S3_BUCKET = os.getenv("REMINDERS_S3_BUCKET", "notesreminder-db")
S3_KEY = os.getenv("REMINDERS_S3_KEY", "reminders.db")
MAX_ROWS_DEFAULT = int(os.getenv("REMINDERS_MAX_ROWS", "200"))

mcp = FastMCP("notesreminder")


def _download_db():
    s3 = boto3.client("s3")
    s3.download_file(S3_BUCKET, S3_KEY, DB_PATH)


def _connect(db_path=DB_PATH):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"{db_path} not found.")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _connect_lead():
    return _connect(LEAD_DB_PATH)


def _rows_as_json(columns, rows, max_rows):
    data = [list(row) for row in rows]
    return json.dumps(
        {
            "columns": columns,
            "rows": data,
            "row_count": len(data),
            "truncated": len(data) >= max_rows,
        },
        indent=2,
        default=str,
    )


def _query_rows(sql, params=None, max_rows=MAX_ROWS_DEFAULT):
    conn = _connect()
    try:
        cursor = conn.execute(sql, params or {})
        rows = cursor.fetchmany(max_rows)
        columns = [col[0] for col in cursor.description] if cursor.description else []
    finally:
        conn.close()
    return _rows_as_json(columns, rows, max_rows)


@mcp.tool()
def sync_db_from_s3() -> str:
    """Download the latest reminders.db from S3 to the local DB path."""
    _download_db()
    stat = os.stat(DB_PATH)
    timestamp = datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds")
    size_kb = stat.st_size / 1024.0
    return f"Downloaded s3://{S3_BUCKET}/{S3_KEY} to {DB_PATH} ({size_kb:.1f} KB, mtime {timestamp})."


@mcp.tool()
def db_status() -> str:
    """Return basic info about the local SQLite file."""
    statuses = {}
    for label, path in (("reminders", DB_PATH), ("lead_intelligence", LEAD_DB_PATH)):
        if not os.path.exists(path):
            statuses[label] = {"path": path, "exists": False}
            continue
        stat = os.stat(path)
        statuses[label] = {
            "path": path,
            "exists": True,
            "size_kb": round(stat.st_size / 1024.0, 1),
            "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
        }
    return json.dumps(statuses, indent=2)


@mcp.tool()
def list_tables() -> str:
    """List tables available in the SQLite database."""
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    finally:
        conn.close()
    table_names = [row["name"] for row in rows]
    return json.dumps({"tables": table_names}, indent=2)


@mcp.tool()
def describe_table(table_name: str) -> str:
    """Describe columns for a given table."""
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError("Table name must be alphanumeric or underscore only.")
    conn = _connect()
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    finally:
        conn.close()
    columns = [
        {
            "cid": row["cid"],
            "name": row["name"],
            "type": row["type"],
            "notnull": row["notnull"],
            "default": row["dflt_value"],
            "pk": row["pk"],
        }
        for row in rows
    ]
    return json.dumps({"table": table_name, "columns": columns}, indent=2)


@mcp.tool()
def query_sql(sql: str, max_rows: int = MAX_ROWS_DEFAULT) -> str:
    """Run a read-only SQL query (SELECT/CTE only) against reminders.db."""
    cleaned = sql.strip().rstrip(";")
    lowered = cleaned.lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise ValueError("Only SELECT queries (including CTEs) are allowed.")
    if ";" in cleaned:
        raise ValueError("Only a single SQL statement is allowed.")
    conn = _connect()
    try:
        cursor = conn.execute(cleaned)
        rows = cursor.fetchmany(max_rows)
        columns = [col[0] for col in cursor.description] if cursor.description else []
    finally:
        conn.close()
    data = [list(row) for row in rows]
    return _rows_as_json(columns, rows, max_rows)


@mcp.tool()
def import_call_data(clients_csv: str, dialpad_dir: str = "Call Log", db_path: str = DB_PATH) -> str:
    """Import Dialpad + Pike13 client CSVs into the SQLite DB and build matches."""
    run_import(clients_path=clients_csv, dialpad_dir=dialpad_dir, db_path=db_path, enable_fuzzy=False)
    return "Imported Dialpad + Pike13 data and rebuilt call_client_matches."


@mcp.tool()
def initialize_lead_followup_schema() -> str:
    """Create the additive V1 lead follow-up tables and curated views."""
    conn = _connect()
    try:
        ensure_lead_followup_schema(conn)
        conn.commit()
    finally:
        conn.close()
    return "Lead follow-up schema and views are ready."


@mcp.tool()
def source_completeness(window_days: int = 7, pike13_lookahead_days: int = 30) -> str:
    """Report whether HubSpot, Dialpad, and Pike13 are complete enough for lead timelines."""
    conn = _connect()
    try:
        report = build_source_completeness_report(conn, window_days, pike13_lookahead_days)
        conn.commit()
    finally:
        conn.close()
    return json.dumps(report, indent=2, default=str)


@mcp.tool()
def daily_snapshot(as_of: str = "", school: str = "West U", limit: int = 50) -> str:
    """Return the sanitized daily lead operating dashboard snapshot for yesterday/today."""
    conn = _connect_lead()
    try:
        snapshot = build_snapshot(conn, "daily", as_of=as_of or None, school=school, limit=limit)
    finally:
        conn.close()
    return json.dumps(snapshot, indent=2, default=str)


@mcp.tool()
def weekly_snapshot(as_of: str = "", school: str = "West U", limit: int = 50) -> str:
    """Return the sanitized weekly lead operating dashboard snapshot for the prior closed Monday-Sunday week."""
    conn = _connect_lead()
    try:
        snapshot = build_snapshot(conn, "weekly", as_of=as_of or None, school=school, limit=limit)
    finally:
        conn.close()
    return json.dumps(snapshot, indent=2, default=str)


@mcp.tool()
def monthly_snapshot(as_of: str = "", school: str = "West U", limit: int = 50) -> str:
    """Return the sanitized monthly lead operating dashboard snapshot."""
    conn = _connect_lead()
    try:
        snapshot = build_snapshot(conn, "monthly", as_of=as_of or None, school=school, limit=limit)
    finally:
        conn.close()
    return json.dumps(snapshot, indent=2, default=str)


@mcp.tool()
def exception_queue(start_date: str, end_date: str, school: str = "West U", limit: int = 50) -> str:
    """Return sanitized lead/trial follow-up exceptions for a date window."""
    conn = _connect_lead()
    try:
        queue = build_exception_queue(conn, start_date, end_date, school, limit)
    finally:
        conn.close()
    return json.dumps(queue, indent=2, default=str)


@mcp.tool()
def lead_evidence_timeline(
    search: str,
    start_date: str = "",
    end_date: str = "",
    limit: int = 100,
    include_sensitive: bool = False,
) -> str:
    """Return a cross-system lead evidence timeline. Broad results are sanitized unless include_sensitive is true."""
    conn = _connect_lead()
    try:
        timeline = build_lead_evidence_timeline(conn, search, start_date, end_date, limit, include_sensitive)
    finally:
        conn.close()
    return json.dumps(timeline, indent=2, default=str)


@mcp.tool()
def stale_leads(school: str = "", days: int = 7, limit: int = 50) -> str:
    """Return active leads with stale touches, follow-up-needed flags, or overdue tasks."""
    limit = max(1, min(limit, MAX_ROWS_DEFAULT))
    return _query_rows(
        """
        SELECT *
        FROM vw_stale_leads
        WHERE (:school = '' OR LOWER(COALESCE(school, '')) LIKE '%' || LOWER(:school) || '%')
          AND (
              days_since_last_touch IS NULL
              OR days_since_last_touch >= :days
              OR risk_reason IN ('follow_up_needed', 'overdue_task', 'missing_touch_date')
          )
        ORDER BY
          CASE risk_reason
              WHEN 'overdue_task' THEN 1
              WHEN 'follow_up_needed' THEN 2
              WHEN 'missing_touch_date' THEN 3
              ELSE 4
          END,
          days_since_last_touch DESC
        LIMIT :limit
        """,
        {"school": school or "", "days": days, "limit": limit},
        limit,
    )


@mcp.tool()
def lead_timeline(search: str, limit: int = 100) -> str:
    """Return a cross-system timeline for a lead name, phone, deal ID, or Pike13 person ID."""
    if not search or not search.strip():
        raise ValueError("search is required.")
    limit = max(1, min(limit, MAX_ROWS_DEFAULT))
    needle = f"%{search.strip().lower()}%"
    return _query_rows(
        """
        SELECT *
        FROM vw_lead_timeline
        WHERE LOWER(COALESCE(deal_id, '')) LIKE :needle
           OR LOWER(COALESCE(contact_id, '')) LIKE :needle
           OR LOWER(COALESCE(pike13_person_id, '')) LIKE :needle
           OR LOWER(COALESCE(person_or_lead, '')) LIKE :needle
           OR LOWER(COALESCE(detail, '')) LIKE :needle
           OR LOWER(COALESCE(title, '')) LIKE :needle
        ORDER BY event_at
        LIMIT :limit
        """,
        {"needle": needle, "limit": limit},
        limit,
    )


@mcp.tool()
def unanswered_messages(school: str = "", days: int = 7, limit: int = 50) -> str:
    """Return inbound SMS messages with no later outbound follow-up in the same thread."""
    limit = max(1, min(limit, MAX_ROWS_DEFAULT))
    return _query_rows(
        """
        SELECT *
        FROM vw_unanswered_messages
        WHERE (:school = '' OR LOWER(COALESCE(school, '')) LIKE '%' || LOWER(:school) || '%')
          AND (days_since_inbound IS NULL OR days_since_inbound <= :days)
        ORDER BY message_at DESC
        LIMIT :limit
        """,
        {"school": school or "", "days": days, "limit": limit},
        limit,
    )


@mcp.tool()
def unanswered_communications(school: str = "", days: int = 7, limit: int = 50) -> str:
    """Return inbound SMS, missed calls, and voicemails with no later outbound follow-up."""
    limit = max(1, min(limit, MAX_ROWS_DEFAULT))
    return _query_rows(
        """
        SELECT *
        FROM vw_unanswered_communications
        WHERE (:school = '' OR LOWER(COALESCE(school, '')) LIKE '%' || LOWER(:school) || '%')
          AND (days_since_inbound IS NULL OR days_since_inbound <= :days)
        ORDER BY event_at DESC
        LIMIT :limit
        """,
        {"school": school or "", "days": days, "limit": limit},
        limit,
    )


@mcp.tool()
def no_show_followup(school: str = "", days: int = 30, limit: int = 50) -> str:
    """Return Pike13 no-shows with HubSpot follow-up context where available."""
    limit = max(1, min(limit, MAX_ROWS_DEFAULT))
    return _query_rows(
        """
        SELECT *
        FROM vw_no_show_followup
        WHERE (:school = '' OR LOWER(COALESCE(school, '')) LIKE '%' || LOWER(:school) || '%')
          AND (days_since_no_show IS NULL OR days_since_no_show <= :days)
        ORDER BY starts_at DESC
        LIMIT :limit
        """,
        {"school": school or "", "days": days, "limit": limit},
        limit,
    )


@mcp.tool()
def lead_conversion_path(search: str, limit: int = 50) -> str:
    """Show the lead-created to trial/enrollment path for a person, deal, school, or Pike13 ID."""
    if not search or not search.strip():
        raise ValueError("search is required.")
    limit = max(1, min(limit, MAX_ROWS_DEFAULT))
    needle = f"%{search.strip().lower()}%"
    return _query_rows(
        """
        SELECT *
        FROM vw_lead_conversion_path
        WHERE LOWER(COALESCE(deal_id, '')) LIKE :needle
           OR LOWER(COALESCE(deal_name, '')) LIKE :needle
           OR LOWER(COALESCE(school, '')) LIKE :needle
           OR LOWER(COALESCE(owner, '')) LIKE :needle
           OR LOWER(COALESCE(pike13_person_id, '')) LIKE :needle
        ORDER BY lead_created_at DESC
        LIMIT :limit
        """,
        {"needle": needle, "limit": limit},
        limit,
    )


if __name__ == "__main__":
    mcp.run()
