import argparse
import sqlite3


def initialize_db(db_path="reminders.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS reminders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lesson_id TEXT UNIQUE,
        school TEXT,
        instructor_name TEXT,
        lesson_date TEXT,
        lesson_time TEXT,
        lesson_type TEXT,
        students TEXT,
        location TEXT,
        notes_text TEXT,
        note_timestamp TEXT,
        pike13_lesson_id TEXT,
        note_score REAL,
        note_score_explanation TEXT,
        note_score_model TEXT,
        note_score_version TEXT,
        note_score_updated_at TEXT,
        note_score_hash TEXT,
        reminder_sent INTEGER DEFAULT 0,
        reminder_count INTEGER DEFAULT 0,
        note_completed INTEGER DEFAULT 0,
        attendance_status TEXT DEFAULT 'unknown',
        last_checked DATE,
        last_reminder_sent TIMESTAMP
    );
    ''')

    conn.commit()
    conn.close()
    print(f"SQLite database initialized at {db_path}.")


def parse_args():
    parser = argparse.ArgumentParser(description="Initialize the NotesReminder SQLite database.")
    parser.add_argument(
        "--db-path",
        default="reminders.db",
        help="SQLite DB path to initialize (default: reminders.db).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    initialize_db(args.db_path)

if __name__ == "__main__":
    main()
