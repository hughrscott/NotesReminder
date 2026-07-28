#!/usr/bin/env python3
"""
upload_report_to_db.py — Pull a date-bounded Pike13 report and upsert it into
the SOR analysis DB (reminders.db).

Design:
  - New table `pike13_report_pulls` holds every pull's rows, keyed by
    (pull_id, person_id) so re-pulls are auditable, not destructive.
  - person_id (8-digit Pike13 people ID from /queries) JOINs directly to
    pike13_people.person_id (verified match — not the 23-char Client hash).
  - Every run logs to source_import_runs (existing audit convention).

Usage:
  python3 upload_report_to_db.py --school westu-sor \
      --from 2026-07-15 --to 2026-08-11 [--field last_membership_end] \
      [--dry-run] [--db /path/reminders.db]
"""
import asyncio
import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from pike13_report_puller import pull_report, build_report_url

DB_DEFAULT = HERE / "reminders.db"
TABLE = "pike13_report_pulls"


def ensure_schema(con):
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        pull_id     INTEGER NOT NULL,
        person_id   TEXT NOT NULL,
        full_name   TEXT,
        email       TEXT,
        phone       TEXT,
        address     TEXT,
        current_plans TEXT,
        raw_row     TEXT,
        school      TEXT,
        field       TEXT,
        from_date   TEXT,
        to_date     TEXT,
        pulled_at   TEXT NOT NULL,
        PRIMARY KEY (pull_id, person_id)
    )""")
    con.commit()


def run(school, frm, to, field="last_membership_end", op="btw",
        db_path=DB_DEFAULT, dry_run=False, verbose=False):
    print(f"[1/4] Pulling {school} report {field} {frm}..{to} ...")
    rows, meta = asyncio.run(pull_report(
        school, frm, to, field=field, op=op, verbose=verbose))

    if meta.get("error"):
        print(f"  !! pull error: {meta['error']}")
    n = len(rows)
    field_names = meta.get("field_names") or [
        "full_name", "email", "phone", "address", "current_plans", "person_id"]
    print(f"     HTTP {meta.get('http_status')} rows={n} "
          f"total_count={meta.get('total_count')} fields={field_names}")

    if dry_run:
        print("[2/4] DRY-RUN: not writing to DB.")
        print(json.dumps({"meta": meta, "sample": rows[:3]}, indent=2, default=str)[:1200])
        return meta, rows

    con = sqlite3.connect(str(db_path))
    ensure_schema(con)
    cur = con.cursor()

    # new pull_id (monotonic)
    cur.execute(f"SELECT COALESCE(MAX(pull_id),0)+1 FROM {TABLE}")
    pull_id = cur.fetchone()[0]

    ins = 0
    for r in rows:
        # rows are positional per field_names
        rec = dict(zip(field_names, r)) if field_names else {}
        pid = str(rec.get("person_id") or (r[-1] if r else None))
        full_name = rec.get("full_name")
        email = rec.get("email")
        phone = rec.get("phone")
        address = rec.get("address")
        current_plans = rec.get("current_plans")
        cur.execute(
            f"""INSERT INTO {TABLE}
               (pull_id, person_id, full_name, email, phone, address,
                current_plans, raw_row, school, field, from_date, to_date, pulled_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(pull_id, person_id) DO UPDATE SET
                 full_name=excluded.full_name, email=excluded.email,
                 phone=excluded.phone, address=excluded.address,
                 current_plans=excluded.current_plans, raw_row=excluded.raw_row,
                 pulled_at=excluded.pulled_at""",
            (pull_id, str(pid), full_name, email, phone, address,
             current_plans, json.dumps(r, default=str),
             meta.get("school"), field, frm, to,
             datetime.now(timezone.utc).isoformat()),
        )
        ins += 1

    # join sanity: how many pulled person_ids exist in pike13_people
    joined = 0
    if ins:
        ph = ",".join("?" * ins)
        pids = [str(r[-1]) if r else None for r in rows]
        pids = [p for p in pids if p]
        if pids:
            cur.execute(
                f"SELECT COUNT(*) FROM pike13_people WHERE person_id IN ({','.join('?'*len(pids))})",
                pids)
            joined = cur.fetchone()[0]

    con.commit()
    # audit row
    cur.execute(
        """INSERT INTO source_import_runs
           (source, extractor, started_at, finished_at, status,
            window_start, window_end, rows_seen, rows_inserted, rows_updated,
            metadata_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        ("pike13_report_pulls", "upload_report_to_db.py",
         datetime.now(timezone.utc).isoformat(),
         datetime.now(timezone.utc).isoformat(),
         "success", frm, to, n, ins, 0,
         json.dumps({"pull_id": pull_id, "school": school, "field": field,
                     "op": op, "joined_to_pike13_people": joined,
                     "report_url": meta.get("report_url"),
                     "http_status": meta.get("http_status"),
                     "total_count": meta.get("total_count")}, default=str)),
    )
    con.commit()
    con.close()

    print(f"[2/4] Inserted {ins} rows under pull_id={pull_id}")
    print(f"[3/4] Join check: {joined}/{ins} person_ids matched pike13_people.person_id")
    print(f"[4/4] Audit logged to source_import_runs. DB={db_path}")
    return meta, rows


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--school", default="westu-sor")
    ap.add_argument("--from", dest="frm", required=True)
    ap.add_argument("--to", dest="to", required=True)
    ap.add_argument("--field", default="last_membership_end")
    ap.add_argument("--op", default="btw")
    ap.add_argument("--db", default=str(DB_DEFAULT))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()
    run(a.school, a.frm, a.to, field=a.field, op=a.op,
         db_path=a.db, dry_run=a.dry_run, verbose=a.verbose)


if __name__ == "__main__":
    main()
