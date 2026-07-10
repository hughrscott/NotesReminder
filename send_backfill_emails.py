#!/usr/bin/env python3
"""Send summary emails for backfilled dates without re-scraping."""
import os
import sys
import sqlite3
from datetime import datetime, timedelta

# Load credentials
env = {}
for line in open('/home/ubuntu/.hermes/SOR/.sorenv'):
    line = line.strip()
    if not line or line.startswith('#'):
        continue
    if '=' in line:
        k, v = line.split('=', 1)
        v = v.strip().strip('"').strip("'")
        env[k] = v
for line in open(os.path.expanduser('~/.hermes/.env')):
    line = line.strip()
    if line.startswith('SOR_APP_PASSWORD='):
        env['SOR_APP_PASSWORD'] = line.split('=',1)[1].strip()
        break
for k, v in env.items():
    os.environ[k] = v

sys.path.insert(0, '.')
from run_daily import (
    get_lessons_without_notes,
    send_email_report,
    normalize_lesson_time,
    normalize_students_field,
    should_skip_lesson,
    DB_PATH,
)

DATES = ["2026-07-06", "2026-07-07", "2026-07-08", "2026-07-09"]
SCHOOLS = ["westu-sor", "theheights-sor"]
TO = ["hughrscott@mac.com", "vivian@schoolofrock.com"]
CC = ["hugh.scott@gmail.com"]

for school in SCHOOLS:
    for date in DATES:
        print(f"\nSending email for {school} {date}...")
        try:
            all_missing = get_lessons_without_notes(school, date, date)

            report_missing = []
            seen = set()
            for note in all_missing:
                lesson_id, instructor, lesson_date, time_value, lesson_type, students, location = note
                normalized_time = normalize_lesson_time(time_value or "")
                instructor_clean = (instructor or "").strip()
                lesson_type_clean = (lesson_type or "").strip()
                students_clean = ' '.join(students.split()) if isinstance(students, str) else (students or "")
                dedup_key = (instructor_clean, lesson_date, normalized_time, lesson_type_clean, students_clean)
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
                if should_skip_lesson(lesson_type, students, instructor_clean):
                    continue
                report_missing.append({
                    'date': lesson_date,
                    'time': normalized_time,
                    'instructor': instructor_clean,
                    'students': students_clean,
                    'lesson_type': lesson_type_clean,
                    'location': location
                })

            # Get completed notes from DB
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.execute("""
                SELECT l.lesson_date, l.lesson_time, l.lesson_type, l.students_raw,
                       ln.notes_text, ln.note_score
                FROM lessons l
                JOIN lesson_notes ln ON l.lesson_id = ln.lesson_id
                WHERE l.school_id = (SELECT school_id FROM schools WHERE school_code = ?)
                  AND l.lesson_date = ?
                  AND ln.notes_text IS NOT NULL AND TRIM(ln.notes_text) != ''
            """, (school, date))
            report_completed = []
            seen_completed = set()
            for row in cursor.fetchall():
                lesson_date, lesson_time, lesson_type, students_raw, notes_text, note_score = row
                normalized_time = normalize_lesson_time(lesson_time or "")
                instructor_clean = ""
                lesson_type_clean = (lesson_type or "").strip()
                students_clean = normalize_students_field(students_raw)
                dedup_key = (instructor_clean, lesson_date, normalized_time, lesson_type_clean, students_clean)
                if dedup_key in seen_completed:
                    continue
                seen_completed.add(dedup_key)
                report_completed.append({
                    'date': lesson_date,
                    'time': normalized_time,
                    'instructor': instructor_clean,
                    'lesson_type': lesson_type_clean,
                    'students': students_clean,
                    'notes': notes_text[:200],
                    'score': note_score,
                })
            conn.close()

            total = len(report_missing) + len(report_completed)
            print(f"  {len(report_completed)} notes completed, {len(report_missing)} missing, {total} total")

            send_email_report(
                report_missing,
                report_completed,
                school,
                date,
                date,
                include_missing=True,
                include_notes=True,
                to_recipients=TO,
                cc_recipients=CC,
                total_lessons_override=total,
                notes_count_override=len(report_completed),
                missing_count_override=len(report_missing),
            )
            print(f"  Email sent!")
        except Exception as e:
            print(f"  ERROR: {e}")

print("\nAll emails sent!")