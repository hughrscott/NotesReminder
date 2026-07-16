#!/usr/bin/env python3
"""
build_cross_source_views.py — Materialize the SOR "Operating System" spine:
join the date-bounded Pike13 report pulls to the unified person hub
(`persons`) and on to Dialpad / HubSpot / School Email, so we can
answer "engaged-but-uncontacted", "trial-no-touch", etc.

BRIDGE (VERIFIED 2026-07-16, do NOT guess):
  pike13_report_pulls.person_id (8-digit Pike13 numeric)
    -> pike13_people.person_id
    -> pike13_people.person_identity_id  (= "person_XXXX")
    -> persons.person_id

  From persons, the source tables link by person_id EXCEPT:
    dialpad_sms_threads.person_id, dialpad_voice_events.person_id  (NOT dialpad_calls)
    hubspot_contacts.person_id, hubspot_deals.person_id
    school_email_messages.person_id

COVERAGE CAVEAT: this DB snapshot only bridges 17/46 pulled people
to `persons`; the rest have no unified identity yet. The view uses LEFT
joins so it shows coverage truthfully (NULLs = no cross-source footprint),
not a false "clean" list.
"""
import sqlite3, sys
from pathlib import Path
from datetime import datetime, timezone

HERE = Path(__file__).resolve().parent
DB = HERE / "reminders.db"
TABLE = "pike13_report_pulls"


def build(con):
    cur = con.cursor()

    # 1) Unified per-person spine: every pulled person + their Pike13 signals
    cur.execute(f"""
    DROP VIEW IF EXISTS vw_pull_spine""")
    cur.execute(f"""
    CREATE VIEW vw_pull_spine AS
    SELECT
        pp.person_id              AS pike13_person_id,
        pp.full_name,
        pp.current_plans,
        pp.field_label,
        pp.from_date, pp.to_date,
        p.person_identity_id     AS unified_person_id,
        ps.person_id             AS in_persons_hub,
        ps.display_name           AS hub_name,
        ps.primary_email,
        ps.primary_phone,
        ps.school
    FROM {TABLE} pp
    LEFT JOIN pike13_people p
        ON p.person_id = pp.person_id
    LEFT JOIN persons ps
        ON ps.person_id = p.person_identity_id
    WHERE pp.pull_id = (SELECT MAX(pull_id) FROM {TABLE})
    """)

    # 2) Cross-source footprint: for each pulled person, what DID we hear/do?
    cur.execute("DROP VIEW IF EXISTS vw_pull_cross_source")
    cur.execute(f"""
    CREATE VIEW vw_pull_cross_source AS
    SELECT
        s.pike13_person_id,
        s.full_name,
        s.current_plans,
        s.in_persons_hub,
        (SELECT COUNT(*) FROM dialpad_sms_threads t  WHERE t.person_id = s.unified_person_id) AS dialpad_sms_threads,
        (SELECT COUNT(*) FROM dialpad_voice_events v  WHERE v.person_id = s.unified_person_id) AS dialpad_voice_events,
        (SELECT COUNT(*) FROM hubspot_contacts h  WHERE h.person_id = s.unified_person_id) AS hubspot_contacts,
        (SELECT COUNT(*) FROM hubspot_deals d    WHERE d.person_id = s.unified_person_id) AS hubspot_deals,
        (SELECT COUNT(*) FROM school_email_messages e WHERE e.person_id = s.unified_person_id) AS school_emails,
        -- the actionable flag: Pike13 signals them, but ZERO other-source footprint
        CASE WHEN s.in_persons_hub IS NULL THEN 'no_unified_identity'
             WHEN (SELECT COUNT(*) FROM dialpad_sms_threads t WHERE t.person_id = s.unified_person_id)
                + (SELECT COUNT(*) FROM dialpad_voice_events v WHERE v.person_id = s.unified_person_id)
                + (SELECT COUNT(*) FROM hubspot_contacts h WHERE h.person_id = s.unified_person_id)
                + (SELECT COUNT(*) FROM hubspot_deals d WHERE d.person_id = s.unified_person_id)
                + (SELECT COUNT(*) FROM school_email_messages e WHERE e.person_id = s.unified_person_id) = 0
             THEN 'pike13_only__no_cross_touch'
             ELSE 'has_cross_source'
        END AS os_status
    FROM vw_pull_spine s
    """)

    con.commit()


def report(con):
    cur = con.cursor()
    print("=== vw_pull_spine (latest pull, per-person) ===")
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT pike13_person_id) FROM vw_pull_spine")
    tot, dist = cur.fetchone()
    print(f"  rows={tot}  distinct pike13 people={dist}")

    print("\n=== vw_pull_cross_source: OS status breakdown ===")
    cur.execute("""SELECT os_status, COUNT(*) FROM vw_pull_cross_source
                  GROUP BY os_status ORDER BY COUNT(*) DESC""")
    for r in cur.fetchall():
        print(f"  {r[0]:32s} {r[1]}")

    print("\n=== ENGAGED-BUT-UNTOUCHED (Pike13 signal, no cross-source footprint) ===")
    cur.execute("""SELECT pike13_person_id, full_name, current_plans, os_status
                  FROM vw_pull_cross_source
                  WHERE os_status = 'pike13_only__no_cross_touch'
                  ORDER BY full_name LIMIT 25""")
    rows = cur.fetchall()
    print(f"  ({len(rows)} shown)")
    for r in rows:
        print(f"   {r[0]} | {r[1]} | {str(r[2])[:40]}")

    print("\n=== coverage: how many pulled people reached persons hub? ===")
    cur.execute("""SELECT COUNT(*), SUM(CASE WHEN in_persons_hub IS NOT NULL THEN 1 ELSE 0 END)
                  FROM vw_pull_spine""")
    a, b = cur.fetchone()
    print(f"  {b}/{a} bridged to unified persons hub")


def main():
    con = sqlite3.connect(str(DB))
    build(con)
    print("Views built: vw_pull_spine, vw_pull_cross_source")
    report(con)
    con.close()


if __name__ == "__main__":
    main()
