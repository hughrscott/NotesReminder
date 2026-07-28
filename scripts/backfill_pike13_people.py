#!/usr/bin/env python3
"""Backfill pike13_people from pike13_report_pulls for records missing from the client scrape."""
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from lead_followup_schema import normalize_email, normalize_phone, utc_now_iso

def main():
    db = ROOT / "reminders.db"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT person_id, full_name, email, phone, current_plans, school, raw_row
        FROM pike13_report_pulls
        WHERE pull_id = 2 
        AND person_id NOT IN (SELECT person_id FROM pike13_people)
    """).fetchall()

    now = utc_now_iso()
    inserted = 0

    for row in rows:
        email_norm = normalize_email(row['email']) if row['email'] else None
        phone_norm = normalize_phone(row['phone']) if row['phone'] else None
        
        conn.execute("""
            INSERT OR IGNORE INTO pike13_people 
            (person_id, full_name, email, email_normalized, phone, phone_normalized, 
             school, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['person_id'],
            row['full_name'],
            row['email'],
            email_norm,
            row['phone'],
            phone_norm,
            row['school'],
            now
        ))
        if conn.total_changes > 0:
            inserted += 1

    conn.commit()
    print(f"Inserted {inserted} records into pike13_people")

    count = conn.execute("""
        SELECT COUNT(*) FROM pike13_people 
        WHERE person_id IN (SELECT person_id FROM pike13_report_pulls WHERE pull_id = 2)
    """).fetchone()[0]
    print(f"Pull-2 people now in pike13_people: {count}/46")
    conn.close()

if __name__ == "__main__":
    main()
