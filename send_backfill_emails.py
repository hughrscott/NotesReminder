#!/usr/bin/env python3
"""Send summary emails for backfilled dates without re-scraping.
Uses the correct key names expected by send_email_report."""
import os
import sys
import sqlite3

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
            # Get missing notes (same as run_daily.py does)
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

            # Get completed notes WITH scores from DB
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.execute("""
                SELECT l.lesson_date, l.lesson_time, l.lesson_type, l.students_raw, l.location,
                       i.instructor_name,
                       ln.notes_text, ln.note_score, ln.note_score_explanation
                FROM lessons l
                JOIN lesson_notes ln ON l.lesson_id = ln.lesson_id
                LEFT JOIN instructors i ON l.instructor_id = i.instructor_id
                WHERE l.school_id = (SELECT school_id FROM schools WHERE school_code = ?)
                  AND l.lesson_date = ?
                  AND ln.notes_text IS NOT NULL AND TRIM(ln.notes_text) != ''
            """, (school, date))
            report_completed = []
            seen_completed = set()
            for row in cursor.fetchall():
                (lesson_date, lesson_time, lesson_type, students_raw, location,
                 instructor_name, notes_text, note_score, note_score_explanation) = row
                normalized_time = normalize_lesson_time(lesson_time or "")
                instructor_clean = (instructor_name or "").strip()
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
                    'location': location or '',
                    'note_text': notes_text[:200],
                    'note_score': note_score,
                    'note_score_explanation': note_score_explanation or '',
                })
            conn.close()

            total = len(report_missing) + len(report_completed)
            print(f"  {len(report_completed)} notes completed, {len(report_missing)} missing, {total} total")
            print(f"  Sample score: {report_completed[0]['note_score'] if report_completed else 'none'}")

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
            import traceback
            traceback.print_exc()

print("\nAll emails sent!")