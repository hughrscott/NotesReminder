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
from dotenv import load_dotenv

load_dotenv()

from noteschecker import scrape_lessons

# Email configuration
SMTP_SERVER = "smtp.mail.me.com"
SMTP_PORT = 587
SENDER_EMAIL = "hughrscott@mac.com"
SENDER_PASSWORD = "lamk-mwgz-snxk-wusw"
RECIPIENT_EMAILS = ["hughrscott@mac.com", "vivianscott@mac.com"]  # Current recipients

S3_BUCKET = 'notesreminder-db'
S3_KEY = 'reminders.db'

def parse_args():
    parser = argparse.ArgumentParser(description="Run daily notes reminder for a specific school.")
    parser.add_argument('--school', type=str, default='westu-sor', help='Pike13 subdomain (e.g., westu-sor or theheights-sor)')
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format')
    parser.add_argument('--init-db', action='store_true', help='Initialize and upload a fresh database')
    return parser.parse_args()

def get_lessons_without_notes(school_subdomain):
    """Get all lessons from the database that don't have notes for a specific school."""
    conn = sqlite3.connect('reminders.db')
    cursor = conn.cursor()
    
    # Get lessons from the last 7 days that don't have notes for the specific school
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    cursor.execute('''
        SELECT lesson_id, instructor_name, lesson_date, lesson_time, lesson_type, students
        FROM reminders
        WHERE note_completed = 0
        AND lesson_date >= ?
        AND school = ?
        ORDER BY lesson_date, lesson_time
    ''', (seven_days_ago, school_subdomain))
    
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

def send_email_report(missing_notes, school_subdomain, start_date, end_date):
    """Send an email report of missing notes."""
    if not missing_notes:
        return

    # Group by date and then by instructor for easier formatting later
    report_content = {}
    for note in missing_notes:
        date = note['date']
        instructor = note['instructor']
        if date not in report_content:
            report_content[date] = {}
        if instructor not in report_content[date]:
            report_content[date][instructor] = []
        report_content[date][instructor].append({
            'time': note['time'],
            'students': note['students'],
            'lesson_type': note['lesson_type']
        })

    # Sort dates and instructors for consistent reporting
    sorted_dates = sorted(report_content.keys())

    plain_body_lines = ["This report lists all lessons with missing notes.\n"]
    table_rows = []

    for date in sorted_dates:
        plain_body_lines.append(f"Date: {date}\n")
        sorted_instructors = sorted(report_content[date].keys())
        for instructor in sorted_instructors:
            plain_body_lines.append(f"Instructor: {instructor}\n")
            for lesson in report_content[date][instructor]:
                plain_body_lines.append(f"  {lesson['time']} - {lesson['students']} ({lesson['lesson_type']})\n")
                table_rows.append(
                    f"<tr>"
                    f"<td>{date}</td>"
                    f"<td>{instructor}</td>"
                    f"<td>{lesson['time']}</td>"
                    f"<td>{lesson['students']}</td>"
                    f"<td>{lesson['lesson_type']}</td>"
                    f"</tr>"
                )
            plain_body_lines.append("\n")

    plain_body = "".join(plain_body_lines)
    html_body = f"""
    <html>
        <body style="font-family:Arial,sans-serif;font-size:14px;color:#222;">
            <p>This report lists all lessons with missing notes.</p>
            <table style="border-collapse:collapse;width:100%;">
                <thead>
                    <tr>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Date</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Instructor</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Time</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Student</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Lesson Type</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(table_rows)}
                </tbody>
            </table>
        </body>
    </html>
    """

    msg = MIMEMultipart('alternative')
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECIPIENT_EMAILS)
    msg['Subject'] = f"Missing Notes for {school_subdomain.replace('-sor', '').replace('westu', 'West U').replace('theheights', 'The Heights').title()} ({start_date} to {end_date})"

    msg.attach(MIMEText(plain_body, 'plain'))
    msg.attach(MIMEText(html_body, 'html'))

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("✅ Email report sent successfully")
    except Exception as e:
        print(f"⚠️ Error sending email: {e}")

def upload_db_to_s3(local_path, bucket, s3_key):
    """Upload the database to S3"""
    print("\n🔍 Checking AWS credentials...")
    print(f"AWS_ACCESS_KEY_ID: {'*' * 5}{os.getenv('AWS_ACCESS_KEY_ID')[-4:] if os.getenv('AWS_ACCESS_KEY_ID') else 'Not found'}")
    print(f"AWS_SECRET_ACCESS_KEY: {'*' * 5}{os.getenv('AWS_SECRET_ACCESS_KEY')[-4:] if os.getenv('AWS_SECRET_ACCESS_KEY') else 'Not found'}")
    print(f"AWS_DEFAULT_REGION: {os.getenv('AWS_DEFAULT_REGION', 'Not found')}")
    
    try:
        s3 = boto3.client('s3')
        print(f"\n📤 Uploading {local_path} to s3://{bucket}/{s3_key}")
        s3.upload_file(local_path, bucket, s3_key)
        print("✅ Database uploaded successfully")
    except Exception as e:
        print(f"⚠️ Error uploading to S3: {str(e)}")
        raise

def download_db_from_s3(local_path, bucket, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket, s3_key, local_path)
        print(f"✅ Downloaded {s3_key} from s3://{bucket} to {local_path}")
    except Exception as e:
        print(f"⚠️ Could not download {s3_key} from s3://{bucket}: {e}")

async def main():
    args = parse_args()
    school_subdomain = args.school

    # If init-db flag is set, create a fresh database and upload it
    if args.init_db:
        print("Initializing fresh database...")
        from init_db import initialize_db
        initialize_db()
        upload_db_to_s3('reminders.db', S3_BUCKET, S3_KEY)
        print("Fresh database initialized and uploaded to S3")
        return

    # Download the latest DB from S3 (if it exists)
    download_db_from_s3('reminders.db', S3_BUCKET, S3_KEY)

    # Get lessons without notes from the database
    # We're retrieving all records first to determine current status, then filtering for the report
    lessons_without_notes_from_db = get_lessons_without_notes(school_subdomain)

    # Scrape recent lessons (last 7 days by default, or custom range)
    if args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    print(f"🔍 Scraping lessons from {start_date} to {end_date} for {school_subdomain}")
    await scrape_lessons(school_subdomain, start_date=start_date, end_date=end_date)

    # Read the scraped data
    csv_file = f"{school_subdomain}_lessons_{start_date}_to_{end_date}.csv"
    df = pd.read_csv(csv_file)

    # Process each lesson from the scraped data
    # This part updates the database based on the latest scrape
    for index, row in df.iterrows():
        lesson_id = f"{row['Lesson Type']}-{row['Date']}-{row['Time']}-{row['Students']}"
        instructor_name = row['Instructor']
        lesson_date = row['Date']
        lesson_time = row['Time']
        lesson_type = row['Lesson Type']
        students = row['Students']
        notes = row['Notes']

        has_notes = (notes != "No notes" and notes.strip() != "") # Check for empty notes after stripping

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
                    attendance_status = ?
                WHERE lesson_id = ? AND school = ?
            ''', (1 if has_notes else 0, instructor_name, lesson_date, lesson_time, lesson_type, students, row.get('Attendance Status', 'unknown'), lesson_id, school_subdomain))
        else:
            cursor.execute('''
                INSERT INTO reminders (lesson_id, school, instructor_name, lesson_date, lesson_time, lesson_type, students, note_completed, attendance_status, last_checked)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE)
            ''', (lesson_id, school_subdomain, instructor_name, lesson_date, lesson_time, lesson_type, students, 1 if has_notes else 0, row.get('Attendance Status', 'unknown')))
        conn.commit()
        conn.close()

    # Now, retrieve all currently missing notes from the DB to generate the report
    # This ensures we capture previously missing notes that might not be in the current scrape range
    all_missing_notes = get_lessons_without_notes(school_subdomain)

    # Filter the notes for the report based on your criteria
    report_missing_notes = []
    for note in all_missing_notes:
        # Unpack the tuple from get_lessons_without_notes
        lesson_id, instructor, date, time, lesson_type, students = note

        # Filter out 'Admin Time' lessons
        if lesson_type == "Admin Time":
            continue

        # Filter out group lessons (more than one student)
        if students and ',' in students:
            continue

        report_missing_notes.append({
            'date': date,
            'time': time,
            'instructor': instructor,
            'students': students,
            'lesson_type': lesson_type
        })

    # Send email report if there are missing notes after filtering
    if report_missing_notes:
        send_email_report(report_missing_notes, school_subdomain, start_date, end_date)
    else:
        print(f"✅ All lessons for {school_subdomain} (from {start_date} to {end_date}) have notes (or were filtered out)!")

    # At the end, upload the DB to S3
    upload_db_to_s3('reminders.db', S3_BUCKET, S3_KEY)

if __name__ == "__main__":
    asyncio.run(main())
