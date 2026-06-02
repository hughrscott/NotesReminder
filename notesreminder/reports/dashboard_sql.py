from __future__ import annotations

import re
import sqlite3
from datetime import date, datetime


_HUBSPOT_TIME_RE = re.compile(r"\s+at\s+.+$", re.IGNORECASE)


def normalize_dashboard_date(*values) -> str | None:
    """Return YYYY-MM-DD for common source date formats used in report filters."""
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue

        iso_candidate = text[:10]
        try:
            return date.fromisoformat(iso_candidate).isoformat()
        except ValueError:
            pass

        text = _HUBSPOT_TIME_RE.sub("", text).strip()
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(text, fmt).date().isoformat()
            except ValueError:
                continue
    return None


def register_dashboard_sql_functions(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.create_function("dashboard_date", -1, normalize_dashboard_date, deterministic=True)
    return conn
