# -*- coding: utf-8 -*-
import asyncio
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from instructormapping import get_instructor_email
import argparse
import boto3
import os
import re
from dotenv import load_dotenv

load_dotenv()

from noteschecker import scrape_lessons

VERBOSE = False

def log(message, force=False):
    if VERBOSE or force:
        print(message)

# Email configuration
SMTP_SERVER = "smtp.mail.me.com"
SMTP_PORT = 587
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")

S3_BUCKET = 'notesreminder-db'
S3_KEY = 'reminders.db'

def parse_args():
    parser = argparse.ArgumentParser(description="Run daily notes reminder for a specific school.")
    parser.add_argument('--school', type=str, default='westu-sor', help='Pike13 subdomain (e.g., westu-sor or theheights-sor)')
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format')
    parser.add_argument('--init-db', action='store_true', help='Initialize and upload a fresh database')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging output')
    parser.add_argument('--summary', choices=['none', 'notes', 'missing', 'both'], default='none',
                        help='Select which sections to include in the summary email')
    parser.add_argument('--to', nargs='+', required=True,
                        help='One or more recipient email addresses')
    parser.add_argument('--cc', nargs='*', default=[],
                        help='Optional list of extra email addresses to CC on the summary email')
    return parser.parse_args()

def get_lessons_without_notes(school_subdomain, start_date=None, end_date=None):
    """Get all lessons from the database that don't have notes for a specific school."""
    conn = sqlite3.connect('reminders.db')
    cursor = conn.cursor()
    
    params = [school_subdomain]
    query = '''
        SELECT lesson_id, instructor_name, lesson_date, lesson_time, lesson_type, students, location
        FROM reminders
        WHERE note_completed = 0
        AND school = ?
    '''
    
    if start_date and end_date:
        query += ' AND lesson_date BETWEEN ? AND ?'
        params.extend([start_date, end_date])
    else:
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        query += ' AND lesson_date >= ?'
        params.append(seven_days_ago)

    query += ' ORDER BY lesson_date, lesson_time'
    cursor.execute(query, tuple(params))
    
    lessons = cursor.fetchall()
    conn.close()
    return lessons

def update_lesson_status(lesson_id, has_notes):
    """Update the note_completed status in the database."""
    conn = sqlite3.connect('reminders.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        UPDATE reminders
        SET note_completed = ?,
            last_checked = CURRENT_DATE
        WHERE lesson_id = ?
    ''', (1 if has_notes else 0, lesson_id))
    
    conn.commit()
    conn.close()

def format_school_label(school_subdomain):
    return (school_subdomain.replace('-sor', '')
            .replace('westu', 'West U')
            .replace('theheights', 'The Heights')
            .strip()
            .title())

def format_date_range(start_date, end_date):
    if start_date == end_date:
        return datetime.strptime(start_date, "%Y-%m-%d").strftime("%-m/%-d/%y")
    start_str = datetime.strptime(start_date, "%Y-%m-%d").strftime("%-m/%-d/%y")
    end_str = datetime.strptime(end_date, "%Y-%m-%d").strftime("%-m/%-d/%y")
    return f"{start_str} and {end_str}"


def normalize_lesson_time(time_str):
    if not time_str:
        return ""
    cleaned = time_str.strip()
    if " on " in cleaned:
        cleaned = cleaned.split(" on ", 1)[0].strip()
    return cleaned

def send_email_report(missing_notes, completed_notes, school_subdomain, start_date, end_date,
                      include_missing, include_notes, to_recipients, cc_recipients=None):
    if not include_missing and not include_notes:
        return
    if not to_recipients:
        print("⚠️ No recipients specified; skipping email send.")
        return
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("⚠️ Missing SENDER_EMAIL or SENDER_PASSWORD; skipping email send.")
        return

    missing_available = include_missing and missing_notes
    notes_available = include_notes and completed_notes
    if not missing_available and not notes_available:
        return

    school_label = format_school_label(school_subdomain)
    date_phrase = format_date_range(start_date, end_date)

    missing_count = len(missing_notes) if missing_available else 0
    notes_count = len(completed_notes) if notes_available else 0
    total_count = missing_count + notes_count

    plain_lines = [
        f"For {date_phrase} there were {total_count} lessons "
        f"({notes_count} with notes, {missing_count} without notes) at {school_label}.\n"
    ]
    summary_paragraph = (
        f"<p><strong>Total lessons:</strong> {total_count} &nbsp;|&nbsp;"
        f"<strong>With notes:</strong> {notes_count} &nbsp;|&nbsp;"
        f"<strong>Missing notes:</strong> {missing_count}</p>"
    )
    html_sections = [summary_paragraph]

    if missing_available:
        missing_sorted = sorted(missing_notes, key=lambda n: (n['date'], n['time'], n['instructor']))
        plain_lines.append("\nLessons with missing notes:\n")
        missing_rows = []
        for note in missing_sorted:
            location_text = f" | Location: {note['location']}" if note.get('location') else ""
            plain_lines.append(
                f"  - {note['date']} {note['time']} | {note['instructor']} | "
                f"{note['students']} ({note['lesson_type']}){location_text}\n"
            )
            missing_rows.append(
                f"<tr>"
                f"<td>{note['date']}</td>"
                f"<td>{note['instructor']}</td>"
                f"<td>{note['time']}</td>"
                f"<td>{note['students']}</td>"
                f"<td>{note['lesson_type']}</td>"
                f"<td>{note.get('location', '') or ''}</td>"
                f"</tr>"
            )

        html_sections.append(f"""
            <h3>Lessons with missing notes</h3>
            <table style="border-collapse:collapse;width:100%;">
                <thead>
                    <tr>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Date</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Instructor</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Time</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Student</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Lesson Type</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Location</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(missing_rows)}
                </tbody>
            </table>
        """)

    if notes_available:
        notes_sorted = sorted(completed_notes, key=lambda n: (n['date'], n['time'], n['instructor']))
        plain_lines.append("\nLessons with notes:\n")
        notes_rows = []
        for lesson in notes_sorted:
            location_text = f" | Location: {lesson['location']}" if lesson.get('location') else ""
            plain_lines.append(
                f"  - {lesson['date']} {lesson['time']} | {lesson['instructor']} | "
                f"{lesson['students']} ({lesson['lesson_type']}){location_text}\n"
                f"    Notes: {lesson['snippet']}\n"
            )
            notes_rows.append(
                f"<tr>"
                f"<td>{lesson['date']}</td>"
                f"<td>{lesson['instructor']}</td>"
                f"<td>{lesson['time']}</td>"
                f"<td>{lesson['students']}</td>"
                f"<td>{lesson['lesson_type']}</td>"
                f"<td>{lesson.get('location', '') or ''}</td>"
                f"<td>{lesson['snippet']}</td>"
                f"</tr>"
            )

        html_sections.append(f"""
            <h3>Lessons with notes</h3>
            <table style="border-collapse:collapse;width:100%;">
                <thead>
                    <tr>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Date</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Instructor</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Time</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Student</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Lesson Type</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Location</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Note Snippet</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(notes_rows)}
                </tbody>
            </table>
        """)

    plain_body = "".join(plain_lines)
    html_body = f"""
    <html>
        <body style=\"font-family:Arial,sans-serif;font-size:14px;color:#222;\">
            {''.join(html_sections)}
        </body>
    </html>
    """

    msg = MIMEMultipart('alternative')
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(to_recipients)
    if cc_recipients:
        msg['Cc'] = ", ".join(cc_recipients)
    msg['Subject'] = f"Lesson notes summary for {school_label} ({start_date} to {end_date})"

    msg.attach(MIMEText(plain_body, 'plain'))
    msg.attach(MIMEText(html_body, 'html'))

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)
        server.quit()
        log("✅ Email report sent successfully")
    except Exception as e:
        print(f"⚠️ Error sending email: {e}")

def upload_db_to_s3(local_path, bucket, s3_key):
    """Upload the database to S3"""
    log("\n🔍 Checking AWS credentials...")
    log(f"AWS_ACCESS_KEY_ID: {'*' * 5}{os.getenv('AWS_ACCESS_KEY_ID')[-4:] if os.getenv('AWS_ACCESS_KEY_ID') else 'Not found'}")
    log(f"AWS_SECRET_ACCESS_KEY: {'*' * 5}{os.getenv('AWS_SECRET_ACCESS_KEY')[-4:] if os.getenv('AWS_SECRET_ACCESS_KEY') else 'Not found'}")
    log(f"AWS_DEFAULT_REGION: {os.getenv('AWS_DEFAULT_REGION', 'Not found')}")
    
    try:
        s3 = boto3.client('s3')
        log(f"\n📤 Uploading {local_path} to s3://{bucket}/{s3_key}")
        s3.upload_file(local_path, bucket, s3_key)
        log("✅ Database uploaded successfully")
    except Exception as e:
        print(f"⚠️ Error uploading to S3: {str(e)}")
        raise

def download_db_from_s3(local_path, bucket, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket, s3_key, local_path)
        log(f"✅ Downloaded {s3_key} from s3://{bucket} to {local_path}")
    except Exception as e:
        print(f"⚠️ Could not download {s3_key} from s3://{bucket}: {e}")

def ensure_location_column():
    conn = sqlite3.connect('reminders.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(reminders)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'location' not in columns:
        cursor.execute("ALTER TABLE reminders ADD COLUMN location TEXT")
        conn.commit()
    cursor.execute("SELECT lesson_id, lesson_time, location FROM reminders")
    updates = []
    for lesson_id, lesson_time, existing_location in cursor.fetchall():
        if not lesson_time or ' - ' not in lesson_time:
            continue
        time_part, _, location_part = lesson_time.rpartition(' - ')
        time_part = time_part.strip()
        time_part = normalize_lesson_time(time_part)
        location_part = location_part.strip()
        if not location_part:
            continue
        existing_clean = existing_location.strip() if isinstance(existing_location, str) else ""
        new_location = existing_clean or location_part
        updates.append((time_part, new_location, lesson_id))
    if updates:
        cursor.executemany(
            "UPDATE reminders SET lesson_time = ?, location = ? WHERE lesson_id = ?",
            updates
        )
        conn.commit()
    cursor.execute("SELECT lesson_id, lesson_time FROM reminders")
    normalize_updates = []
    for lesson_id, lesson_time in cursor.fetchall():
        if not lesson_time:
            continue
        normalized = normalize_lesson_time(lesson_time)
        if normalized and normalized != lesson_time.strip():
            normalize_updates.append((normalized, lesson_id))
    if normalize_updates:
        cursor.executemany(
            "UPDATE reminders SET lesson_time = ? WHERE lesson_id = ?",
            normalize_updates
        )
        conn.commit()
    conn.close()

def should_skip_lesson(lesson_type, students, instructor=None):
    lt = (lesson_type or "").lower()
    if "admin" in lt or "meeting" in lt:
        return True
    if students and isinstance(students, str) and ',' in students:
        return True
    if instructor:
        instructor_clean = instructor.strip().lower()
        if not re.search(r"[a-zA-Z]", instructor_clean):
            return True
        if "admin" in instructor_clean or "trial" in instructor_clean or "rookies" in instructor_clean:
            return True
    return False

def format_note_snippet(note_text, word_limit=10):
    words = note_text.split()
    if len(words) <= word_limit:
        return note_text
    return ' '.join(words[:word_limit]) + "..."

async def main():
    args = parse_args()
    global VERBOSE
    VERBOSE = args.verbose
    school_subdomain = args.school

    # If init-db flag is set, create a fresh database and upload it
    if args.init_db:
        log("Initializing fresh database...")
        from init_db import initialize_db
        initialize_db()
        upload_db_to_s3('reminders.db', S3_BUCKET, S3_KEY)
        log("Fresh database initialized and uploaded to S3")
        return

    # Download the latest DB from S3 (if it exists)
    download_db_from_s3('reminders.db', S3_BUCKET, S3_KEY)
    ensure_location_column()

    # Scrape recent lessons (last 7 days by default, or custom range)
    if args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    if args.verbose:
        print(f"🔍 Scraping lessons from {start_date} to {end_date} for {school_subdomain}")
    await scrape_lessons(school_subdomain, start_date=start_date, end_date=end_date, verbose=args.verbose)

    # Read the scraped data
    csv_file = f"{school_subdomain}_lessons_{start_date}_to_{end_date}.csv"
    df = pd.read_csv(csv_file)
    has_location_column = 'Location' in df.columns
    completed_lessons = []

    # Process each lesson from the scraped data
    # This part updates the database based on the latest scrape
    for index, row in df.iterrows():
        lesson_id = f"{row['Lesson Type']}-{row['Date']}-{row['Time']}-{row['Students']}"
        instructor_name = row['Instructor']
        lesson_date = row['Date']
        lesson_time = row['Time']
        lesson_type = row['Lesson Type']
        students = row['Students']
        location_value = row['Location'] if has_location_column else None
        if isinstance(location_value, str):
            location_clean = location_value.strip()
        elif pd.notna(location_value):
            location_clean = str(location_value).strip()
        else:
            location_clean = None
        if location_clean == "":
            location_clean = None
        notes_value = row['Notes']
        notes_str = (notes_value if isinstance(notes_value, str) else str(notes_value or "")).strip()
        normalized_notes = notes_str.lower()
        has_notes = notes_str != "" and normalized_notes not in ("no notes", "nan", "none")

        # Check if the lesson already exists in the database
        conn = sqlite3.connect('reminders.db')
        cursor = conn.cursor()
        cursor.execute('''SELECT lesson_id FROM reminders WHERE lesson_id = ? AND school = ?''', (lesson_id, school_subdomain))
        existing_lesson = cursor.fetchone()

        if existing_lesson:
            cursor.execute('''
                UPDATE reminders
                SET note_completed = ?,
                    last_checked = CURRENT_DATE,
                    instructor_name = ?,
                    lesson_date = ?,
                    lesson_time = ?,
                    lesson_type = ?,
                    students = ?,
                    location = COALESCE(?, location),
                    attendance_status = ?
                WHERE lesson_id = ? AND school = ?
            ''', (
                1 if has_notes else 0,
                instructor_name,
                lesson_date,
                lesson_time,
                lesson_type,
                students,
                location_clean,
                row.get('Attendance Status', 'unknown'),
                lesson_id,
                school_subdomain
            ))
        else:
            cursor.execute('''
                INSERT INTO reminders (lesson_id, school, instructor_name, lesson_date, lesson_time, lesson_type, students, location, note_completed, attendance_status, last_checked)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE)
            ''', (
                lesson_id,
                school_subdomain,
                instructor_name,
                lesson_date,
                lesson_time,
                lesson_type,
                students,
                location_clean,
                1 if has_notes else 0,
                row.get('Attendance Status', 'unknown')
            ))
        conn.commit()
        conn.close()

        if has_notes and not should_skip_lesson(lesson_type, students, instructor_name):
            completed_lessons.append({
                'date': lesson_date,
                'time': lesson_time,
                'instructor': instructor_name,
                'students': students,
                'lesson_type': lesson_type,
                'location': location_clean,
                'snippet': format_note_snippet(notes_str)
            })

    # Now, retrieve missing notes from the DB within the requested window
    all_missing_notes = get_lessons_without_notes(school_subdomain, start_date, end_date)

    # Filter the notes for the report based on your criteria
    report_missing_notes = []
    seen_lessons = set()
    for note in all_missing_notes:
        lesson_id, instructor, date, time_value, lesson_type, students, location = note
        normalized_time = normalize_lesson_time(time_value or "")
        instructor_clean = (instructor or "").strip()
        lesson_type_clean = (lesson_type or "").strip()
        students_clean = ' '.join(students.split()) if isinstance(students, str) else (students or "")
        dedup_key = (instructor_clean, date, normalized_time, lesson_type_clean, students_clean)
        if dedup_key in seen_lessons:
            continue
        seen_lessons.add(dedup_key)

        if should_skip_lesson(lesson_type, students, instructor_clean):
            continue

        report_missing_notes.append({
            'date': date,
            'time': normalized_time,
            'instructor': instructor_clean,
            'students': students_clean,
            'lesson_type': lesson_type_clean,
            'location': location
        })

    include_missing_section = args.summary in ('none', 'missing', 'both')
    include_notes_section = args.summary in ('notes', 'both')
    send_email_report(
        report_missing_notes,
        completed_lessons,
        school_subdomain,
        start_date,
        end_date,
        include_missing_section,
        include_notes_section,
        to_recipients=args.to,
        cc_recipients=args.cc
    )
    if include_missing_section and not report_missing_notes:
        log(f"✅ All lessons for {school_subdomain} (from {start_date} to {end_date}) have notes (or were filtered out)!")

    # At the end, upload the DB to S3
    upload_db_to_s3('reminders.db', S3_BUCKET, S3_KEY)

if __name__ == "__main__":
    asyncio.run(main())
