#!/usr/bin/env python3
"""
run_retention_pull.py — Pull the verified date-bounded Pike13 report
fields in ONE authenticated session and load them into reminders.db
(table `pike13_report_pulls`, keyed (pull_id, person_id)).

Verified date-bounded field tokens (probed live 2026-07-16):
  - last_membership_end : memberships ENDING in window  -> renewal/retention
  - last_visit_date     : last VISIT in window           -> engagement/disengagement
  - first_visit_date    : first VISIT in window          -> trial->conversion

(NOT date filters: created_at, expiry_date, membership_start, etc.
 return the entire client base = 5154 regardless of window, so they're
 ignored. The dashboard KPI cards themselves are hard-coded windows.)

Usage:
  python3 run_retention_pull.py --school westu-sor \
      --from 2026-07-15 --to 2026-08-11 [--db ...] [--dry-run]
"""
import asyncio
import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from pike13_report_puller import pull_reports_batch

DB_DEFAULT = HERE / "reminders.db"
TABLE = "pike13_report_pulls"

# field token -> human label / loop it feeds
FIELDS = {
    "last_membership_end": "membership_ending",
    "last_visit_date": "last_visit",
    "first_visit_date": "first_visit",
}


def ensure_schema(con):
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        pull_id      INTEGER NOT NULL,
        person_id    TEXT NOT NULL,
        full_name    TEXT,
        email        TEXT,
        phone        TEXT,
        address      TEXT,
        current_plans TEXT,
        raw_row      TEXT,
        school       TEXT,
        field        TEXT,
        field_label  TEXT,
        from_date    TEXT,
        to_date      TEXT,
        pulled_at    TEXT NOT NULL,
        PRIMARY KEY (pull_id, person_id, field)
    )""")
    # add any columns missing from a prior-schema table
    cur = con.cursor()
    cur.execute(f"PRAGMA table_info({TABLE})")
    existing = {r[1] for r in cur.fetchall()}
    for col, typ in [
        ("field_label", "TEXT"), ("from_date", "TEXT"),
        ("to_date", "TEXT"), ("school", "TEXT"), ("current_plans", "TEXT"),
    ]:
        if col not in existing:
            cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {col} {typ}")
    con.commit()


def run(school, frm, to, db_path=DB_DEFAULT, dry_run=False, verbose=False):
    print(f"[1/4] Single-session pull: {school} {frm}..{to} fields={list(FIELDS)}")
    batch = asyncio.run(pull_reports_batch(
        school, frm, to, list(FIELDS.keys()), op="btw", verbose=verbose))

    if dry_run:
        print("[2/4] DRY-RUN: no DB write.")
        for f, (rows, meta) in batch.items():
            print(f"  {FIELDS[f]:20s} HTTP={meta.get('http_status')} "
                  f"rows={len(rows)} total={meta.get('total_count')}")
        return batch

    con = sqlite3.connect(str(db_path))
    ensure_schema(con)
    cur = con.cursor()
    cur.execute(f"SELECT COALESCE(MAX(pull_id),0)+1 FROM {TABLE}")
    pull_id = cur.fetchone()[0]

    total_ins = 0
    joins = 0
    for f, (rows, meta) in batch.items():
        field_names = meta.get("field_names") or [
            "full_name", "email", "phone", "address", "current_plans", "person_id"]
        for r in rows:
            rec = dict(zip(field_names, r)) if field_names else {}
            pid = str(rec.get("person_id") or (r[-1] if r else None))
            cur.execute(
                f"""INSERT OR REPLACE INTO {TABLE}
                   (pull_id, person_id, full_name, email, phone, address,
                    current_plans, raw_row, school, field, field_label,
                    from_date, to_date, pulled_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (pull_id, pid, rec.get("full_name"), rec.get("email"),
                 rec.get("phone"), rec.get("address"), rec.get("current_plans"),
                 json.dumps(r, default=str), meta.get("school"),
                 f, FIELDS[f], frm, to,
                 datetime.now(timezone.utc).isoformat()))
            total_ins += 1

    # join check vs pike13_people
    cur.execute(
        f"""SELECT COUNT(DISTINCT pp.person_id) FROM {TABLE} pp
           JOIN pike13_people p ON p.person_id = pp.person_id
           WHERE pp.pull_id=?""", (pull_id,))
    joins = cur.fetchone()[0]

    con.commit()
    cur.execute(
        """INSERT INTO source_import_runs
           (source, extractor, started_at, finished_at, status,
            window_start, window_end, rows_seen, rows_inserted, rows_updated,
            metadata_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        ("pike13_report_pulls", "run_retention_pull.py",
         datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat(),
         "success", frm, to, total_ins, total_ins, 0,
         json.dumps({"pull_id": pull_id, "school": school,
                     "fields": {f: len(r) for f, (r, m) in batch.items()},
                     "joined_to_pike13_people": joins}, default=str)))
    con.commit()
    con.close()

    print(f"[2/4] Inserted {total_ins} rows (pull_id={pull_id})")
    for f, (rows, meta) in batch.items():
        print(f"      {FIELDS[f]:20s} HTTP={meta.get('http_status')} "
              f"rows={len(rows)} total={meta.get('total_count')}")
    print(f"[3/4] Join: {joins} distinct person_ids matched pike13_people")
    print(f"[4/4] Audit logged. DB={db_path}")
    return batch


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--school", default="westu-sor")
    ap.add_argument("--from", dest="frm", required=True)
    ap.add_argument("--to", dest="to", required=True)
    ap.add_argument("--db", default=str(DB_DEFAULT))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()
    run(a.school, a.frm, a.to, db_path=a.db,
         dry_run=a.dry_run, verbose=a.verbose)


if __name__ == "__main__":
    main()
