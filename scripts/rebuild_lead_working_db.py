#!/usr/bin/env python3
import argparse
import json
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema


DEFAULT_LEAD_PROOF_DB = (
    "outputs/db_backups/reminders.db.20260501-211741.before-local-mfa-notes-run.bak"
)
DEFAULT_OUTPUT_DB = "outputs/lead_intelligence/lead_intelligence_working.db"

LEAD_TABLES = [
    "source_import_runs",
    "hubspot_deals",
    "hubspot_contacts",
    "hubspot_tasks",
    "hubspot_activities",
    "dialpad_sms_threads",
    "dialpad_sms_messages",
    "dialpad_voice_events",
    "dialpad_call_reviews",
    "dialpad_target_searches",
    "dialpad_route_discoveries",
    "source_route_discoveries",
    "pike13_people",
    "pike13_visits",
    "pike13_plans_passes",
    "identity_matches",
    "communication_ai_insights",
]


def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def table_exists(conn, schema, table):
    return (
        conn.execute(
            f"""
            SELECT 1
            FROM {quote_identifier(schema)}.sqlite_master
            WHERE type = 'table'
              AND name = ?
            """,
            (table,),
        ).fetchone()
        is not None
    )


def table_info(conn, schema, table):
    return conn.execute(
        f"PRAGMA {quote_identifier(schema)}.table_info({quote_identifier(table)})"
    ).fetchall()


def copy_table(conn, table):
    if not table_exists(conn, "lead_source", table):
        conn.execute(f"DELETE FROM {quote_identifier(table)}")
        return {"table": table, "source_exists": False, "rows": 0}

    dest_info = table_info(conn, "main", table)
    source_info = table_info(conn, "lead_source", table)
    dest_columns = [row["name"] for row in dest_info]
    source_columns = {row["name"] for row in source_info}

    missing_required = [
        row["name"]
        for row in dest_info
        if row["name"] not in source_columns and (row["notnull"] or row["pk"])
    ]
    if missing_required:
        raise RuntimeError(
            f"Cannot copy {table}: source is missing required columns "
            f"{', '.join(missing_required)}"
        )

    copy_columns = [column for column in dest_columns if column in source_columns]
    column_sql = ", ".join(quote_identifier(column) for column in copy_columns)
    conn.execute(f"DELETE FROM {quote_identifier(table)}")
    conn.execute(
        f"""
        INSERT INTO {quote_identifier(table)} ({column_sql})
        SELECT {column_sql}
        FROM lead_source.{quote_identifier(table)}
        """
    )
    rows = conn.execute(f"SELECT COUNT(*) FROM {quote_identifier(table)}").fetchone()[0]
    return {"table": table, "source_exists": True, "rows": rows}


def scalar(conn, sql):
    return conn.execute(sql).fetchone()[0]


def rebuild_lead_working_db(production_db, lead_proof_db, output_db):
    production_db = Path(production_db).expanduser().resolve()
    lead_proof_db = Path(lead_proof_db).expanduser().resolve()
    output_db = Path(output_db).expanduser().resolve()

    if not production_db.exists():
        raise FileNotFoundError(f"Production DB not found: {production_db}")
    if not lead_proof_db.exists():
        raise FileNotFoundError(f"Lead proof DB not found: {lead_proof_db}")
    if output_db == production_db:
        raise ValueError("Output DB must not be the production reminders.db")

    output_db.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="lead-working-db-", dir=output_db.parent) as tmp:
        tmp_dir = Path(tmp)
        source_for_attach = lead_proof_db
        if lead_proof_db == output_db:
            source_for_attach = tmp_dir / "lead_source.db"
            shutil.copy2(lead_proof_db, source_for_attach)

        staged_output = tmp_dir / "lead_working.db"
        shutil.copy2(production_db, staged_output)

        conn = sqlite3.connect(staged_output)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = OFF")
            ensure_lead_followup_schema(conn)
            conn.execute("ATTACH DATABASE ? AS lead_source", (str(source_for_attach),))

            copied_tables = [copy_table(conn, table) for table in LEAD_TABLES]
            conn.commit()
            conn.execute("DETACH DATABASE lead_source")
            ensure_lead_followup_schema(conn)

            integrity = scalar(conn, "PRAGMA integrity_check")
            if integrity != "ok":
                raise RuntimeError(f"Integrity check failed: {integrity}")

            summary = {
                "production_db": str(production_db),
                "lead_source_db": str(lead_proof_db),
                "output_db": str(output_db),
                "integrity": integrity,
                "reminders_rows": scalar(conn, "SELECT COUNT(*) FROM reminders"),
                "latest_lesson_date": scalar(conn, "SELECT MAX(lesson_date) FROM reminders"),
                "copied_tables": copied_tables,
            }
            conn.commit()
        finally:
            conn.close()

        output_db.unlink(missing_ok=True)
        shutil.move(str(staged_output), output_db)

    return summary


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild the local lead-intelligence working DB from the current "
            "production notes DB plus additive lead proof tables."
        )
    )
    parser.add_argument("--production-db", default="reminders.db")
    parser.add_argument("--lead-proof-db", default=DEFAULT_LEAD_PROOF_DB)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_DB)
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args()

    summary = rebuild_lead_working_db(
        args.production_db,
        args.lead_proof_db,
        args.output,
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
        return

    print(f"Lead working DB: {summary['output_db']}")
    print(f"Integrity: {summary['integrity']}")
    print(f"Reminders: {summary['reminders_rows']}")
    print(f"Latest lesson date: {summary['latest_lesson_date']}")
    for table in summary["copied_tables"]:
        status = "copied" if table["source_exists"] else "missing source"
        print(f"{table['table']}: {table['rows']} rows ({status})")


if __name__ == "__main__":
    main()
