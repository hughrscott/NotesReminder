# -*- coding: utf-8 -*-
import asyncio
import sqlite3
from datetime import datetime, timedelta
import hashlib
import json
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
DELAY_NOTICE_FALLBACK_EMAIL = "hughrscott@mac.com"

def log(message, force=False):
    if VERBOSE or force:
        print(message)


class FatalScoringError(RuntimeError):
    pass

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
    parser.add_argument('--summary', choices=['none', 'notes', 'missing', 'both'], default='both',
                        help='Select which sections to include in the summary email')
    parser.add_argument('--to', nargs='+', default=[],
                        help='One or more recipient email addresses')
    parser.add_argument('--cc', nargs='*', default=[],
                        help='Optional list of extra email addresses to CC on the summary email')
    parser.add_argument('--no-email', action='store_true',
                        help='Skip sending the summary email (useful for backfills)')
    parser.add_argument('--skip-note-scoring', action='store_true',
                        help='Skip LLM note scoring and email score columns')
    parser.add_argument('--note-score-model', default='gpt-4o-mini',
                        help='OpenAI model for note scoring (default: gpt-4o-mini)')
    parser.add_argument('--note-score-version', default='v1-note-quality',
                        help='Version tag for note scoring prompt/rubric')
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


def normalize_students_field(value):
    if isinstance(value, str):
        return " ".join(value.split())
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return " ".join(str(value).split())


NOTE_SCORING_SYSTEM_PROMPT = """You score music lesson notes from 1 to 10.
Return strict JSON with keys: score, explanation.

Scoring guidance:
- 1-3: extremely weak or empty note quality.
- 4-6: acceptable but generic; lacks specificity.
- 7-8: strong quality with clear specifics and progress.
- 9-10: excellent, specific, actionable, and clearly student-focused.

Be strict. Use decimal-free integers 1-10 only.
"""


def is_scoring_auth_error(exc) -> bool:
    message = str(exc).lower()
    exc_type = exc.__class__.__name__.lower()
    auth_markers = [
        "invalid_api_key",
        "incorrect api key",
        "authentication",
        "unauthorized",
        "401",
        "api key provided",
        "openai_api_key not set",
    ]
    return "auth" in exc_type or any(marker in message for marker in auth_markers)


def build_github_run_url():
    server = os.getenv("GITHUB_SERVER_URL")
    repo = os.getenv("GITHUB_REPOSITORY")
    run_id = os.getenv("GITHUB_RUN_ID")
    if server and repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return None


def normalize_email_list(addresses):
    seen = set()
    normalized = []
    for address in addresses or []:
        addr = (address or "").strip()
        if not addr:
            continue
        lowered = addr.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(addr)
    return normalized


def send_multipart_email(subject, plain_body, html_body, to_recipients, cc_recipients=None):
    if not to_recipients:
        print("⚠️ No recipients specified; skipping email send.")
        return
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("⚠️ Missing SENDER_EMAIL or SENDER_PASSWORD; skipping email send.")
        return

    msg = MIMEMultipart('alternative')
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(to_recipients)
    if cc_recipients:
        msg['Cc'] = ", ".join(cc_recipients)
    msg['Subject'] = subject

    msg.attach(MIMEText(plain_body, 'plain'))
    msg.attach(MIMEText(html_body, 'html'))

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    try:
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)
    finally:
        server.quit()


def send_delay_notice(school_subdomain, start_date, end_date, issue_summary, to_recipients):
    school_label = format_school_label(school_subdomain)
    date_phrase = format_date_range(start_date, end_date)
    run_url = build_github_run_url()
    effective_recipients = normalize_email_list(
        list(to_recipients or []) + [DELAY_NOTICE_FALLBACK_EMAIL]
    )

    plain_lines = [
        f"The lesson notes summary for {school_label} ({date_phrase}) is delayed.\n",
        f"Issue: {issue_summary}\n",
        "Reason: note scoring could not run because the OpenAI API key is invalid or misconfigured.\n",
        "The normal summary email was not sent and will be delayed until the issue is fixed.\n",
    ]
    if run_url:
        plain_lines.append(f"GitHub Actions run: {run_url}\n")
    plain_body = "".join(plain_lines)

    html_lines = [
        f"<p>The lesson notes summary for <strong>{school_label}</strong> ({date_phrase}) is delayed.</p>",
        f"<p><strong>Issue:</strong> {issue_summary}</p>",
        "<p>The normal summary email was not sent because note scoring could not run due to an invalid or misconfigured OpenAI API key.</p>",
        "<p>The summary will be delayed until the issue is fixed.</p>",
    ]
    if run_url:
        html_lines.append(f'<p>GitHub Actions run: <a href="{run_url}">{run_url}</a></p>')
    html_body = (
        '<html><body style="font-family:Arial,sans-serif;font-size:14px;color:#222;">'
        + "".join(html_lines)
        + "</body></html>"
    )

    send_multipart_email(
        subject=f"Lesson notes summary delayed for {school_label} ({start_date} to {end_date})",
        plain_body=plain_body,
        html_body=html_body,
        to_recipients=effective_recipients,
    )


def preflight_note_scoring(model_name):
    try:
        from openai import OpenAI
    except Exception as exc:
        raise FatalScoringError("OpenAI package unavailable for note scoring.") from exc

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise FatalScoringError("OPENAI_API_KEY is missing.")

    client = OpenAI()
    try:
        client.responses.create(
            model=model_name,
            input=[{"role": "user", "content": "Return exactly: ok"}],
            temperature=0,
            max_output_tokens=5,
        )
    except Exception as exc:
        if is_scoring_auth_error(exc):
            raise FatalScoringError("OpenAI API key is invalid or misconfigured.") from exc
        raise


def score_note_quality(note_text, lesson_type, model_name):
    if not note_text or not note_text.strip():
        return None, None
    try:
        from openai import OpenAI
    except Exception:
        raise FatalScoringError("OpenAI package unavailable for note scoring.")

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise FatalScoringError("OPENAI_API_KEY is missing.")

    client = OpenAI()
    user_payload = {
        "lesson_type": lesson_type or "",
        "note_text": note_text.strip(),
    }
    try:
        response = client.responses.create(
            model=model_name,
            input=[
                {"role": "system", "content": NOTE_SCORING_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=True)},
            ],
            temperature=0,
        )
        text = response.output_text.strip()
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}")
            if start == -1 or end == -1 or end <= start:
                return None, None
            parsed = json.loads(text[start : end + 1])
        raw_score = parsed.get("score")
        try:
            score = int(raw_score)
        except Exception:
            return None, None
        score = max(1, min(10, score))
        explanation = str(parsed.get("explanation", "")).strip()
        if not explanation:
            explanation = "No explanation returned."
        return score, explanation
    except Exception as exc:
        if is_scoring_auth_error(exc):
            raise FatalScoringError("OpenAI API key is invalid or misconfigured.") from exc
        return None, None

def send_email_report(missing_notes, completed_notes, school_subdomain, start_date, end_date,
                      include_missing, include_notes, to_recipients, cc_recipients=None,
                      total_lessons_override=None, notes_count_override=None, missing_count_override=None):
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

    # Header totals should reflect true reportable counts, independent of which
    # sections are included in the body.
    missing_count = (
        missing_count_override
        if missing_count_override is not None
        else len(missing_notes)
    )
    notes_count = (
        notes_count_override
        if notes_count_override is not None
        else len(completed_notes)
    )
    total_count = (
        total_lessons_override
        if total_lessons_override is not None
        else (missing_count + notes_count)
    )

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
                f"    Note: {lesson.get('note_text', '')}\n"
                f"    Score: {lesson.get('note_score', 'N/A')} | "
                f"Why: {lesson.get('note_score_explanation', '')}\n"
            )
            notes_rows.append(
                f"<tr>"
                f"<td>{lesson['date']}</td>"
                f"<td>{lesson['instructor']}</td>"
                f"<td>{lesson['time']}</td>"
                f"<td>{lesson['students']}</td>"
                f"<td>{lesson['lesson_type']}</td>"
                f"<td>{lesson.get('location', '') or ''}</td>"
                f"<td>{lesson.get('note_text', '') or ''}</td>"
                f"<td>{lesson.get('note_score', '') if lesson.get('note_score') is not None else ''}</td>"
                f"<td>{lesson.get('note_score_explanation', '') or ''}</td>"
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
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Note</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Score (1-10)</th>
                        <th style="border:1px solid #ccc;padding:8px;background:#f6f6f6;text-align:left;">Score Explanation</th>
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

    try:
        send_multipart_email(
            subject=f"Lesson notes summary for {school_label} ({start_date} to {end_date})",
            plain_body=plain_body,
            html_body=html_body,
            to_recipients=to_recipients,
            cc_recipients=cc_recipients,
        )
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

def ensure_unique_lesson_ids():
    conn = sqlite3.connect('reminders.db')
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM reminders WHERE lesson_id LIKE school || '-%'")
    prefixed_count = cursor.fetchone()[0]
    if prefixed_count == 0:
        cursor.execute("""
            UPDATE reminders
            SET lesson_id = school || '-' || lesson_id
        """)
        conn.commit()
    conn.close()

def ensure_notes_columns():
    conn = sqlite3.connect('reminders.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(reminders)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'notes_text' not in columns:
        cursor.execute("ALTER TABLE reminders ADD COLUMN notes_text TEXT")
    if 'note_timestamp' not in columns:
        cursor.execute("ALTER TABLE reminders ADD COLUMN note_timestamp TEXT")
    conn.commit()
    conn.close()


def ensure_note_score_columns():
    conn = sqlite3.connect('reminders.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(reminders)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'note_score' not in columns:
        cursor.execute("ALTER TABLE reminders ADD COLUMN note_score REAL")
    if 'note_score_explanation' not in columns:
        cursor.execute("ALTER TABLE reminders ADD COLUMN note_score_explanation TEXT")
    if 'note_score_model' not in columns:
        cursor.execute("ALTER TABLE reminders ADD COLUMN note_score_model TEXT")
    if 'note_score_version' not in columns:
        cursor.execute("ALTER TABLE reminders ADD COLUMN note_score_version TEXT")
    if 'note_score_updated_at' not in columns:
        cursor.execute("ALTER TABLE reminders ADD COLUMN note_score_updated_at TEXT")
    if 'note_score_hash' not in columns:
        cursor.execute("ALTER TABLE reminders ADD COLUMN note_score_hash TEXT")
    conn.commit()
    conn.close()

def ensure_pike13_column():
    conn = sqlite3.connect('reminders.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(reminders)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'pike13_lesson_id' not in columns:
        cursor.execute("ALTER TABLE reminders ADD COLUMN pike13_lesson_id TEXT")
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

async def main():
    args = parse_args()
    global VERBOSE
    VERBOSE = args.verbose
    school_subdomain = args.school
    if not args.no_email and not args.to:
        raise SystemExit("Missing --to recipients (or use --no-email to skip sending).")

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
    ensure_notes_columns()
    ensure_note_score_columns()
    ensure_pike13_column()
    ensure_unique_lesson_ids()

    # Scrape recent lessons (last 7 days by default, or custom range)
    if args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    if not args.skip_note_scoring:
        try:
            preflight_note_scoring(args.note_score_model)
        except FatalScoringError as exc:
            if not args.no_email:
                try:
                    send_delay_notice(
                        school_subdomain=school_subdomain,
                        start_date=start_date,
                        end_date=end_date,
                        issue_summary=str(exc),
                        to_recipients=args.to,
                    )
                except Exception as email_exc:
                    print(f"⚠️ Failed to send delay notice: {email_exc}")
            raise SystemExit(f"Fatal note scoring error: {exc}")

    if args.verbose:
        print(f"🔍 Scraping lessons from {start_date} to {end_date} for {school_subdomain}")
    await scrape_lessons(school_subdomain, start_date=start_date, end_date=end_date, verbose=args.verbose)

    # Read the scraped data
    csv_file = f"{school_subdomain}_lessons_{start_date}_to_{end_date}.csv"
    if not os.path.exists(csv_file) or os.path.getsize(csv_file) == 0:
        raise SystemExit(
            f"CSV file {csv_file} is missing or empty. "
            "Scrape likely failed (login/permissions/network)."
        )
    try:
        df = pd.read_csv(csv_file)
    except pd.errors.EmptyDataError:
        raise SystemExit(
            f"CSV file {csv_file} has no columns. "
            "Scrape likely returned no data (login/permissions/network)."
        )
    has_location_column = 'Location' in df.columns
    completed_lessons = []

    # Process each lesson from the scraped data
    # This part updates the database based on the latest scrape
    for index, row in df.iterrows():
        base_lesson_id = f"{row['Lesson Type']}-{row['Date']}-{row['Time']}-{row['Students']}"
        pike13_lesson_id = row.get('Lesson ID', None)
        if isinstance(pike13_lesson_id, float) and pd.isna(pike13_lesson_id):
            pike13_lesson_id = None
        if pike13_lesson_id:
            lesson_id = f"{school_subdomain}-{pike13_lesson_id}"
        else:
            lesson_id = f"{school_subdomain}-{base_lesson_id}"
        instructor_name = row['Instructor']
        lesson_date = row['Date']
        lesson_time = row['Time']
        lesson_type = row['Lesson Type']
        students = normalize_students_field(row['Students'])
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
        notes_text = notes_str if has_notes else None
        note_timestamp = row.get('Note Timestamp', None)

        note_hash = hashlib.sha256(notes_text.encode("utf-8")).hexdigest() if notes_text else None
        note_score = None
        note_score_explanation = None

        # Check if the lesson already exists in the database
        conn = sqlite3.connect('reminders.db')
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT lesson_id, note_score, note_score_explanation, note_score_hash
            FROM reminders
            WHERE lesson_id = ? AND school = ?
            ''',
            (lesson_id, school_subdomain),
        )
        existing_lesson = cursor.fetchone()
        if not existing_lesson:
            legacy_lesson_id = base_lesson_id
            cursor.execute('''
                SELECT lesson_id FROM reminders
                WHERE lesson_id = ? AND school = ?
            ''', (legacy_lesson_id, school_subdomain))
            legacy_lesson = cursor.fetchone()
            if legacy_lesson:
                cursor.execute('''
                    UPDATE reminders
                    SET lesson_id = ?
                    WHERE lesson_id = ? AND school = ?
                ''', (lesson_id, legacy_lesson_id, school_subdomain))
                conn.commit()
                cursor.execute(
                    '''
                    SELECT lesson_id, note_score, note_score_explanation, note_score_hash
                    FROM reminders
                    WHERE lesson_id = ? AND school = ?
                    ''',
                    (lesson_id, school_subdomain),
                )
                existing_lesson = cursor.fetchone()
        if not existing_lesson and pike13_lesson_id:
            legacy_prefixed = f"{school_subdomain}-{base_lesson_id}"
            cursor.execute('''
                SELECT lesson_id FROM reminders
                WHERE lesson_id = ? AND school = ?
            ''', (legacy_prefixed, school_subdomain))
            legacy_prefixed_row = cursor.fetchone()
            if legacy_prefixed_row:
                cursor.execute('''
                    UPDATE reminders
                    SET lesson_id = ?
                    WHERE lesson_id = ? AND school = ?
                ''', (lesson_id, legacy_prefixed, school_subdomain))
                conn.commit()
                cursor.execute(
                    '''
                    SELECT lesson_id, note_score, note_score_explanation, note_score_hash
                    FROM reminders
                    WHERE lesson_id = ? AND school = ?
                    ''',
                    (lesson_id, school_subdomain),
                )
                existing_lesson = cursor.fetchone()

        if has_notes:
            if (
                existing_lesson
                and existing_lesson[1] is not None
                and existing_lesson[3]
                and existing_lesson[3] == note_hash
            ):
                note_score = existing_lesson[1]
                note_score_explanation = existing_lesson[2]
            elif args.skip_note_scoring:
                note_score = None
                note_score_explanation = None
            else:
                try:
                    note_score, note_score_explanation = score_note_quality(
                        notes_text,
                        lesson_type,
                        args.note_score_model,
                    )
                except FatalScoringError as exc:
                    conn.close()
                    if not args.no_email:
                        try:
                            send_delay_notice(
                                school_subdomain=school_subdomain,
                                start_date=start_date,
                                end_date=end_date,
                                issue_summary=str(exc),
                                to_recipients=args.to,
                            )
                        except Exception as email_exc:
                            print(f"⚠️ Failed to send delay notice: {email_exc}")
                    raise SystemExit(f"Fatal note scoring error: {exc}")

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
                    attendance_status = ?,
                    notes_text = ?,
                    note_timestamp = ?,
                    pike13_lesson_id = COALESCE(?, pike13_lesson_id),
                    note_score = ?,
                    note_score_explanation = ?,
                    note_score_model = ?,
                    note_score_version = ?,
                    note_score_updated_at = ?,
                    note_score_hash = ?
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
                notes_text,
                note_timestamp,
                pike13_lesson_id,
                note_score if has_notes else None,
                note_score_explanation if has_notes else None,
                args.note_score_model if has_notes else None,
                args.note_score_version if has_notes else None,
                datetime.now().isoformat(timespec="seconds") if has_notes else None,
                note_hash if has_notes else None,
                lesson_id,
                school_subdomain
            ))
            if args.verbose:
                print(f"🔁 Updated lesson {lesson_id}")
        else:
            cursor.execute('''
                INSERT INTO reminders (
                    lesson_id,
                    school,
                    instructor_name,
                    lesson_date,
                    lesson_time,
                    lesson_type,
                    students,
                    location,
                    note_completed,
                    attendance_status,
                    notes_text,
                    note_timestamp,
                    pike13_lesson_id,
                    note_score,
                    note_score_explanation,
                    note_score_model,
                    note_score_version,
                    note_score_updated_at,
                    note_score_hash,
                    last_checked
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE)
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
                row.get('Attendance Status', 'unknown'),
                notes_text,
                note_timestamp,
                pike13_lesson_id,
                note_score if has_notes else None,
                note_score_explanation if has_notes else None,
                args.note_score_model if has_notes else None,
                args.note_score_version if has_notes else None,
                datetime.now().isoformat(timespec="seconds") if has_notes else None,
                note_hash if has_notes else None,
            ))
            if args.verbose:
                print(f"🆕 Inserted lesson {lesson_id}")
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
                'note_text': notes_str,
                'note_score': note_score,
                'note_score_explanation': note_score_explanation,
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

    # Deduplicate and normalize completed lessons for consistent reporting counts.
    report_completed_notes = []
    seen_completed = set()
    for lesson in completed_lessons:
        dedup_key = (
            lesson.get('instructor', '').strip(),
            lesson.get('date', '').strip(),
            normalize_lesson_time(lesson.get('time', '')),
            lesson.get('lesson_type', '').strip(),
            normalize_students_field(lesson.get('students'))
        )
        if dedup_key in seen_completed:
            continue
        seen_completed.add(dedup_key)
        report_completed_notes.append({
            **lesson,
            'time': normalize_lesson_time(lesson.get('time', '')),
            'instructor': lesson.get('instructor', '').strip(),
            'lesson_type': lesson.get('lesson_type', '').strip(),
            'students': normalize_students_field(lesson.get('students'))
        })

    total_reportable_lessons = len(report_missing_notes) + len(report_completed_notes)

    include_missing_section = args.summary in ('none', 'missing', 'both')
    include_notes_section = args.summary in ('notes', 'both')
    if not args.no_email:
        send_email_report(
            report_missing_notes,
            report_completed_notes,
            school_subdomain,
            start_date,
            end_date,
            include_missing_section,
            include_notes_section,
            to_recipients=args.to,
            cc_recipients=args.cc,
            total_lessons_override=total_reportable_lessons,
            notes_count_override=len(report_completed_notes),
            missing_count_override=len(report_missing_notes),
        )
    if include_missing_section and not report_missing_notes:
        log(f"✅ All lessons for {school_subdomain} (from {start_date} to {end_date}) have notes (or were filtered out)!")

    # At the end, upload the DB to S3
    upload_db_to_s3('reminders.db', S3_BUCKET, S3_KEY)

if __name__ == "__main__":
    asyncio.run(main())
