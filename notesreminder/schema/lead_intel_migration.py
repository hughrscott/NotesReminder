"""Copy lead-intelligence tables into a production database copy."""

import argparse
import json
import shutil
import sqlite3
import tempfile
from pathlib import Path

from lead_followup_schema import ensure_lead_followup_schema


REPLACE_TABLES = [
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
    "school_email_messages",
    "pike13_people",
    "pike13_visits",
    "pike13_plans_passes",
    "identity_matches",
    "communication_ai_insights",
]

MERGE_TABLES = [
    "recording_downloads",
    "recording_transcripts",
]

PRODUCTION_OWNED_TABLES = [
    "reminders",
    "call_logs",
    "call_client_matches",
    "dialpad_calls",
    "dialpad_daily_stats",
    "dialpad_recordings",
    "dialpad_user_stats",
    "dialpad_voicemails",
    "pike13_clients",
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


def table_columns(conn, schema, table):
    return [row["name"] for row in table_info(conn, schema, table)]


def primary_key_columns(conn, schema, table):
    columns = [
        (row["pk"], row["name"])
        for row in table_info(conn, schema, table)
        if row["pk"]
    ]
    return [name for _, name in sorted(columns)]


def scalar(conn, sql):
    return conn.execute(sql).fetchone()[0]


def count_rows(conn, schema, table):
    return scalar(conn, f"SELECT COUNT(*) FROM {quote_identifier(schema)}.{quote_identifier(table)}")


def common_columns(conn, table):
    target_columns = table_columns(conn, "main", table)
    source_columns = set(table_columns(conn, "lead_source", table))
    return [column for column in target_columns if column in source_columns]


def validate_required_columns(conn, table, columns):
    source_columns = set(columns)
    missing_required = [
        row["name"]
        for row in table_info(conn, "main", table)
        if row["name"] not in source_columns and (row["notnull"] or row["pk"])
    ]
    if missing_required:
        raise RuntimeError(
            f"Cannot copy {table}: lead source is missing required columns "
            f"{', '.join(missing_required)}"
        )


def replace_table(conn, table):
    if not table_exists(conn, "lead_source", table):
        raise RuntimeError(f"Lead source is missing required table: {table}")

    before = count_rows(conn, "main", table)
    source = count_rows(conn, "lead_source", table)
    columns = common_columns(conn, table)
    validate_required_columns(conn, table, columns)

    column_sql = ", ".join(quote_identifier(column) for column in columns)
    conn.execute(f"DELETE FROM {quote_identifier(table)}")
    if columns:
        conn.execute(
            f"""
            INSERT INTO {quote_identifier(table)} ({column_sql})
            SELECT {column_sql}
            FROM lead_source.{quote_identifier(table)}
            """
        )

    after = count_rows(conn, "main", table)
    status = "ok" if after == source else "mismatch"
    return {
        "table": table,
        "strategy": "replace",
        "source_rows": source,
        "target_rows_before": before,
        "target_rows_after": after,
        "missing_source_rows": 0 if status == "ok" else source - after,
        "status": status,
    }


def join_predicate(pk_columns, target_alias, source_alias):
    return " AND ".join(
        f"{target_alias}.{quote_identifier(column)} = {source_alias}.{quote_identifier(column)}"
        for column in pk_columns
    )


def missing_source_rows_sql(table, pk_columns):
    join_sql = join_predicate(pk_columns, "target", "source")
    null_check = f"target.{quote_identifier(pk_columns[0])} IS NULL"
    return f"""
        SELECT COUNT(*)
        FROM lead_source.{quote_identifier(table)} source
        LEFT JOIN {quote_identifier(table)} target
          ON {join_sql}
        WHERE {null_check}
    """


def fill_missing_columns(conn, table, pk_columns, columns):
    non_pk_columns = [column for column in columns if column not in pk_columns]
    for column in non_pk_columns:
        join_sql = join_predicate(pk_columns, "target", "source")
        conn.execute(
            f"""
            UPDATE {quote_identifier(table)} AS target
            SET {quote_identifier(column)} = (
                SELECT source.{quote_identifier(column)}
                FROM lead_source.{quote_identifier(table)} AS source
                WHERE {join_sql}
            )
            WHERE target.{quote_identifier(column)} IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM lead_source.{quote_identifier(table)} AS source
                  WHERE {join_sql}
                    AND source.{quote_identifier(column)} IS NOT NULL
              )
            """
        )


def merge_table(conn, table):
    if not table_exists(conn, "lead_source", table):
        raise RuntimeError(f"Lead source is missing required table: {table}")

    pk_columns = primary_key_columns(conn, "main", table)
    if not pk_columns:
        raise RuntimeError(f"Cannot merge {table}: target table has no primary key")

    before = count_rows(conn, "main", table)
    source = count_rows(conn, "lead_source", table)
    columns = common_columns(conn, table)
    validate_required_columns(conn, table, columns)
    column_sql = ", ".join(quote_identifier(column) for column in columns)

    if columns:
        conn.execute(
            f"""
            INSERT OR IGNORE INTO {quote_identifier(table)} ({column_sql})
            SELECT {column_sql}
            FROM lead_source.{quote_identifier(table)}
            """
        )
        fill_missing_columns(conn, table, pk_columns, columns)

    after = count_rows(conn, "main", table)
    missing = scalar(conn, missing_source_rows_sql(table, pk_columns))
    return {
        "table": table,
        "strategy": "merge_fill_missing",
        "source_rows": source,
        "target_rows_before": before,
        "target_rows_after": after,
        "missing_source_rows": missing,
        "status": "ok" if missing == 0 else "mismatch",
    }


def production_table_counts(conn):
    counts = {}
    for table in PRODUCTION_OWNED_TABLES:
        if table_exists(conn, "main", table):
            counts[table] = count_rows(conn, "main", table)
    return counts


def migrate_lead_intelligence(production_db, lead_db, output_db=None, in_place=False):
    production_db = Path(production_db).expanduser().resolve()
    lead_db = Path(lead_db).expanduser().resolve()
    output_db = Path(output_db).expanduser().resolve() if output_db else None

    if not production_db.exists():
        raise FileNotFoundError(f"Production DB not found: {production_db}")
    if not lead_db.exists():
        raise FileNotFoundError(f"Lead DB not found: {lead_db}")
    if output_db is None and not in_place:
        raise ValueError("Pass --output for copy mode, or --in-place explicitly.")
    if output_db and output_db == lead_db:
        raise ValueError("Output DB must not overwrite the lead source DB.")

    target_db = production_db if in_place else output_db
    if not in_place:
        target_db.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            prefix=f"{target_db.name}.",
            suffix=".tmp",
            dir=target_db.parent,
            delete=False,
        ) as tmp:
            staged = Path(tmp.name)
        try:
            shutil.copy2(production_db, staged)
            shutil.move(str(staged), target_db)
        finally:
            staged.unlink(missing_ok=True)

    conn = sqlite3.connect(target_db)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        before_counts = production_table_counts(conn)
        ensure_lead_followup_schema(conn)
        conn.execute("ATTACH DATABASE ? AS lead_source", (str(lead_db),))

        table_results = []
        for table in REPLACE_TABLES:
            table_results.append(replace_table(conn, table))
        for table in MERGE_TABLES:
            table_results.append(merge_table(conn, table))

        conn.commit()
        conn.execute("DETACH DATABASE lead_source")
        ensure_lead_followup_schema(conn)
        after_counts = production_table_counts(conn)
        integrity = scalar(conn, "PRAGMA integrity_check")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    failed = [row for row in table_results if row["status"] != "ok"]
    production_count_changes = {
        table: {"before": before_counts.get(table), "after": after_counts.get(table)}
        for table in sorted(set(before_counts) | set(after_counts))
        if before_counts.get(table) != after_counts.get(table)
    }
    return {
        "production_db": str(production_db),
        "lead_db": str(lead_db),
        "output_db": str(target_db),
        "mode": "in_place" if in_place else "copy",
        "integrity": integrity,
        "status": "ready" if integrity == "ok" and not failed and not production_count_changes else "blocked",
        "failed_tables": failed,
        "production_count_changes": production_count_changes,
        "table_results": table_results,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Merge lead-intelligence tables into a production DB copy and reconcile counts."
    )
    parser.add_argument("--production-db", default="reminders.db")
    parser.add_argument(
        "--lead-db",
        default="outputs/lead_intelligence/lead_intelligence_working.db",
    )
    parser.add_argument("--output", help="Output DB path for copy mode.")
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Modify --production-db directly. Use only after the copy-mode gate passes.",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args(argv)

    summary = migrate_lead_intelligence(
        args.production_db,
        args.lead_db,
        output_db=args.output,
        in_place=args.in_place,
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
        return

    print(f"Unified DB: {summary['output_db']}")
    print(f"Mode: {summary['mode']}")
    print(f"Integrity: {summary['integrity']}")
    print(f"Status: {summary['status']}")
    for result in summary["table_results"]:
        print(
            "{table}: {strategy}, source={source_rows}, "
            "before={target_rows_before}, after={target_rows_after}, "
            "missing_source={missing_source_rows}, status={status}".format(**result)
        )
    if summary["production_count_changes"]:
        print("Production-owned table count changes:")
        for table, counts in summary["production_count_changes"].items():
            print(f"  {table}: {counts['before']} -> {counts['after']}")


if __name__ == "__main__":
    main()
