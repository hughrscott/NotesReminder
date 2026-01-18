import json
import os
import re
import sqlite3
from datetime import datetime

import boto3
from mcp.server.fastmcp import FastMCP

from import_call_data import run_import

DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), "reminders.db")
DB_PATH = os.getenv("REMINDERS_DB_PATH", DEFAULT_DB_PATH)
S3_BUCKET = os.getenv("REMINDERS_S3_BUCKET", "notesreminder-db")
S3_KEY = os.getenv("REMINDERS_S3_KEY", "reminders.db")
MAX_ROWS_DEFAULT = int(os.getenv("REMINDERS_MAX_ROWS", "200"))

mcp = FastMCP("notesreminder")


def _download_db():
    s3 = boto3.client("s3")
    s3.download_file(S3_BUCKET, S3_KEY, DB_PATH)


def _connect():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"{DB_PATH} not found. Run sync_db_from_s3 first.")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
    if not os.path.exists(DB_PATH):
        return f"{DB_PATH} not found."
    stat = os.stat(DB_PATH)
    timestamp = datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds")
    size_kb = stat.st_size / 1024.0
    return f"{DB_PATH} exists ({size_kb:.1f} KB, mtime {timestamp})."


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


@mcp.tool()
def import_call_data(clients_csv: str, dialpad_dir: str = "Call Log", db_path: str = DB_PATH) -> str:
    """Import Dialpad + Pike13 client CSVs into the SQLite DB and build matches."""
    run_import(clients_path=clients_csv, dialpad_dir=dialpad_dir, db_path=db_path, enable_fuzzy=False)
    return "Imported Dialpad + Pike13 data and rebuilt call_client_matches."


if __name__ == "__main__":
    mcp.run()
