import argparse
import sqlite3


def format_school_label(school_code):
    if not school_code:
        return None
    cleaned = (
        school_code.replace("-sor", "")
        .replace("westu", "West U")
        .replace("theheights", "The Heights")
        .strip()
        .title()
    )
    return cleaned


def split_students(value):
    if not value or not isinstance(value, str):
        return []
    parts = [p.strip() for p in value.split(",")]
    return [p for p in parts if p]


def ensure_reporting_tables(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schools (
            school_id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_code TEXT NOT NULL UNIQUE,
            school_name TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS instructors (
            instructor_id INTEGER PRIMARY KEY AUTOINCREMENT,
            instructor_name TEXT NOT NULL,
            school_id INTEGER,
            UNIQUE(instructor_name, school_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS students (
            student_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_name TEXT NOT NULL,
            school_id INTEGER,
            UNIQUE(student_name, school_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lessons (
            lesson_id TEXT PRIMARY KEY,
            pike13_lesson_id TEXT,
            school_id INTEGER,
            instructor_id INTEGER,
            lesson_date TEXT,
            lesson_time TEXT,
            lesson_type TEXT,
            location TEXT,
            students_raw TEXT,
            lesson_is_group INTEGER,
            lesson_student_count INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lesson_students (
            lesson_id TEXT NOT NULL,
            student_id INTEGER NOT NULL,
            is_primary INTEGER DEFAULT 0,
            PRIMARY KEY (lesson_id, student_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lesson_notes (
            lesson_id TEXT PRIMARY KEY,
            note_completed INTEGER,
            notes_text TEXT,
            note_timestamp TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lesson_attendance (
            lesson_id TEXT PRIMARY KEY,
            attendance_status TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lesson_note_scores_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lesson_id TEXT NOT NULL,
            pike13_lesson_id TEXT,
            score REAL NOT NULL,
            scoring_source TEXT NOT NULL,
            scoring_model TEXT,
            scoring_version TEXT NOT NULL,
            justification TEXT,
            strength_or_weakness TEXT,
            improvement TEXT,
            scored_at TEXT NOT NULL,
            import_run_id TEXT NOT NULL,
            source_db_fingerprint TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(lesson_id, scoring_source, scoring_version, scored_at)
        )
        """
    )

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lessons_school_date ON lessons(school_id, lesson_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lesson_students_student ON lesson_students(student_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lesson_note_scores_history_lesson "
        "ON lesson_note_scores_history(lesson_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lesson_note_scores_history_scored_at "
        "ON lesson_note_scores_history(scored_at)"
    )


def load_school_map(conn):
    schools = {}
    for school_id, school_code in conn.execute(
        "SELECT school_id, school_code FROM schools"
    ):
        schools[school_code] = school_id
    return schools


def get_or_create_school(conn, schools, school_code):
    if not school_code:
        return None
    if school_code in schools:
        return schools[school_code]
    school_name = format_school_label(school_code)
    conn.execute(
        "INSERT OR IGNORE INTO schools (school_code, school_name) VALUES (?, ?)",
        (school_code, school_name),
    )
    school_id = conn.execute(
        "SELECT school_id FROM schools WHERE school_code = ?",
        (school_code,),
    ).fetchone()[0]
    schools[school_code] = school_id
    return school_id


def get_or_create_instructor(conn, cache, school_id, instructor_name):
    if not instructor_name:
        return None
    key = (school_id, instructor_name)
    if key in cache:
        return cache[key]
    conn.execute(
        "INSERT OR IGNORE INTO instructors (instructor_name, school_id) VALUES (?, ?)",
        (instructor_name, school_id),
    )
    row = conn.execute(
        "SELECT instructor_id FROM instructors WHERE instructor_name = ? AND school_id IS ?",
        (instructor_name, school_id),
    ).fetchone()
    instructor_id = row[0] if row else None
    cache[key] = instructor_id
    return instructor_id


def get_or_create_student(conn, cache, school_id, student_name):
    if not student_name:
        return None
    key = (school_id, student_name)
    if key in cache:
        return cache[key]
    conn.execute(
        "INSERT OR IGNORE INTO students (student_name, school_id) VALUES (?, ?)",
        (student_name, school_id),
    )
    row = conn.execute(
        "SELECT student_id FROM students WHERE student_name = ? AND school_id IS ?",
        (student_name, school_id),
    ).fetchone()
    student_id = row[0] if row else None
    cache[key] = student_id
    return student_id


def backfill_reporting(conn):
    ensure_reporting_tables(conn)

    schools = load_school_map(conn)
    instructors = {}
    students = {}

    rows = conn.execute(
        """
        SELECT
            lesson_id,
            pike13_lesson_id,
            school,
            instructor_name,
            lesson_date,
            lesson_time,
            lesson_type,
            students,
            location,
            attendance_status,
            note_completed,
            notes_text,
            note_timestamp
        FROM reminders
        """
    ).fetchall()

    for row in rows:
        (
            lesson_id,
            pike13_lesson_id,
            school_code,
            instructor_name,
            lesson_date,
            lesson_time,
            lesson_type,
            students_raw,
            location,
            attendance_status,
            note_completed,
            notes_text,
            note_timestamp,
        ) = row

        if not lesson_id:
            continue

        school_id = get_or_create_school(conn, schools, school_code)
        instructor_id = get_or_create_instructor(conn, instructors, school_id, instructor_name)

        student_list = split_students(students_raw)
        student_count = len(student_list)
        lesson_is_group = 1 if student_count > 1 else 0

        conn.execute(
            """
            INSERT OR REPLACE INTO lessons (
                lesson_id,
                pike13_lesson_id,
                school_id,
                instructor_id,
                lesson_date,
                lesson_time,
                lesson_type,
                location,
                students_raw,
                lesson_is_group,
                lesson_student_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lesson_id,
                pike13_lesson_id,
                school_id,
                instructor_id,
                lesson_date,
                lesson_time,
                lesson_type,
                location,
                students_raw,
                lesson_is_group,
                student_count,
            ),
        )

        conn.execute(
            """
            INSERT OR REPLACE INTO lesson_notes (
                lesson_id,
                note_completed,
                notes_text,
                note_timestamp
            ) VALUES (?, ?, ?, ?)
            """,
            (lesson_id, note_completed, notes_text, note_timestamp),
        )

        conn.execute(
            """
            INSERT OR REPLACE INTO lesson_attendance (
                lesson_id,
                attendance_status
            ) VALUES (?, ?)
            """,
            (lesson_id, attendance_status),
        )

        conn.execute("DELETE FROM lesson_students WHERE lesson_id = ?", (lesson_id,))
        if student_list:
            is_primary = 1 if student_count == 1 else 0
            for student_name in student_list:
                student_id = get_or_create_student(
                    conn, students, school_id, student_name
                )
                if student_id:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO lesson_students (
                            lesson_id,
                            student_id,
                            is_primary
                        ) VALUES (?, ?, ?)
                        """,
                        (lesson_id, student_id, is_primary),
                    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build and backfill reporting tables from reminders."
    )
    parser.add_argument(
        "--db",
        default="reminders.db",
        help="Path to the SQLite database (default: reminders.db)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    conn = sqlite3.connect(args.db)
    try:
        backfill_reporting(conn)
        conn.commit()
    finally:
        conn.close()

    print("✅ Reporting tables created/backfilled.")


if __name__ == "__main__":
    main()
