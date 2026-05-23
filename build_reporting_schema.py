import argparse
import sqlite3
import re


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


def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def table_columns(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({quote_identifier(table)})")}


def add_column_if_missing(conn, table, column, definition):
    if column not in table_columns(conn, table):
        conn.execute(
            f"ALTER TABLE {quote_identifier(table)} "
            f"ADD COLUMN {quote_identifier(column)} {definition}"
        )


def table_exists(conn, table):
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (table,),
        ).fetchone()
    )


def is_reportable_lesson(lesson_type, students, instructor=None):
    lesson_type = (lesson_type or "").lower()
    if "admin" in lesson_type or "meeting" in lesson_type:
        return False
    if students and isinstance(students, str) and "," in students:
        return False
    if instructor:
        instructor_clean = instructor.strip().lower()
        if not re.search(r"[a-zA-Z]", instructor_clean):
            return False
        if (
            "admin" in instructor_clean
            or "trial" in instructor_clean
            or "rookies" in instructor_clean
        ):
            return False
    return True


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
            lesson_student_count INTEGER,
            lesson_is_reportable INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lesson_students (
            lesson_id TEXT NOT NULL,
            student_id INTEGER NOT NULL,
            person_id TEXT,
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
            note_timestamp TEXT,
            note_score REAL,
            note_score_explanation TEXT,
            note_score_model TEXT,
            note_score_version TEXT,
            note_score_updated_at TEXT,
            note_score_hash TEXT
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
        CREATE TABLE IF NOT EXISTS call_logs (
            call_id TEXT PRIMARY KEY,
            external_number TEXT,
            date_started TEXT,
            direction TEXT,
            category TEXT,
            name TEXT,
            school_code TEXT,
            school_name TEXT,
            voicemail_transcript TEXT,
            voicemail_recording_url TEXT,
            recording_url TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS call_client_matches (
            match_id INTEGER PRIMARY KEY AUTOINCREMENT,
            call_id TEXT,
            client_id TEXT,
            match_type TEXT,
            confidence REAL,
            match_value TEXT,
            matched_on TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recording_transcripts (
            call_id TEXT PRIMARY KEY,
            recording_url TEXT,
            transcript_text TEXT,
            outcome TEXT,
            summary TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pike13_clients (
            "Client" TEXT,
            "Client ID" TEXT,
            "Client Home Location" TEXT,
            "Last Completed Visit Date" TEXT,
            "Completed Visits" TEXT,
            "Future Visits" TEXT,
            "Current Passes/Plans" TEXT,
            "Has Plan on Hold?" TEXT
        )
        """
    )
    add_column_if_missing(conn, "lessons", "lesson_is_reportable", "INTEGER")
    add_column_if_missing(conn, "lesson_students", "person_id", "TEXT")
    add_column_if_missing(conn, "lesson_notes", "note_score", "REAL")
    add_column_if_missing(conn, "lesson_notes", "note_score_explanation", "TEXT")
    add_column_if_missing(conn, "lesson_notes", "note_score_model", "TEXT")
    add_column_if_missing(conn, "lesson_notes", "note_score_version", "TEXT")
    add_column_if_missing(conn, "lesson_notes", "note_score_updated_at", "TEXT")
    add_column_if_missing(conn, "lesson_notes", "note_score_hash", "TEXT")
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
        "CREATE INDEX IF NOT EXISTS idx_lessons_instructor_date ON lessons(instructor_id, lesson_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lesson_students_student ON lesson_students(student_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lesson_students_person ON lesson_students(person_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lesson_note_scores_history_lesson "
        "ON lesson_note_scores_history(lesson_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lesson_note_scores_history_scored_at "
        "ON lesson_note_scores_history(scored_at)"
    )
    create_reporting_views(conn)


def create_reporting_views(conn):
    for view_name in [
        "vw_missing_notes_by_instructor",
        "vw_note_completion_rate",
        "vw_missing_notes_by_school_day",
        "vw_note_quality_league_table",
        "vw_callback_speed",
        "vw_churn_candidates",
    ]:
        conn.execute(f"DROP VIEW IF EXISTS {view_name}")

    conn.execute(
        """
        CREATE VIEW vw_missing_notes_by_instructor AS
        SELECT
            s.school_code,
            s.school_name,
            i.instructor_name,
            COUNT(*) AS total_reportable_lessons,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 0 THEN 1 ELSE 0 END) AS missing_notes,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) AS completed_notes,
            ROUND(
                100.0 * SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0),
                1
            ) AS completion_rate,
            MAX(l.lesson_date) AS latest_lesson_date
        FROM lessons l
        JOIN schools s ON s.school_id = l.school_id
        LEFT JOIN instructors i ON i.instructor_id = l.instructor_id
        LEFT JOIN lesson_notes n ON n.lesson_id = l.lesson_id
        WHERE COALESCE(l.lesson_is_reportable, 0) = 1
        GROUP BY s.school_code, s.school_name, i.instructor_name
        HAVING missing_notes > 0
        """
    )
    conn.execute(
        """
        CREATE VIEW vw_note_completion_rate AS
        SELECT
            s.school_code,
            s.school_name,
            i.instructor_name,
            COUNT(*) AS total_reportable_lessons,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) AS completed_notes,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 0 THEN 1 ELSE 0 END) AS missing_notes,
            ROUND(
                100.0 * SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0),
                1
            ) AS completion_rate
        FROM lessons l
        JOIN schools s ON s.school_id = l.school_id
        LEFT JOIN instructors i ON i.instructor_id = l.instructor_id
        LEFT JOIN lesson_notes n ON n.lesson_id = l.lesson_id
        WHERE COALESCE(l.lesson_is_reportable, 0) = 1
        GROUP BY s.school_code, s.school_name, i.instructor_name
        """
    )
    conn.execute(
        """
        CREATE VIEW vw_missing_notes_by_school_day AS
        SELECT
            s.school_code,
            s.school_name,
            l.lesson_date,
            COUNT(*) AS total_reportable_lessons,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 0 THEN 1 ELSE 0 END) AS missing_notes,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) AS completed_notes
        FROM lessons l
        JOIN schools s ON s.school_id = l.school_id
        LEFT JOIN lesson_notes n ON n.lesson_id = l.lesson_id
        WHERE COALESCE(l.lesson_is_reportable, 0) = 1
        GROUP BY s.school_code, s.school_name, l.lesson_date
        """
    )
    conn.execute(
        """
        CREATE VIEW vw_note_quality_league_table AS
        SELECT
            s.school_code,
            s.school_name,
            i.instructor_name,
            substr(l.lesson_date, 1, 7) AS score_month,
            COUNT(*) AS total_reportable_lessons,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 1 THEN 1 ELSE 0 END) AS lessons_with_notes,
            SUM(CASE WHEN n.note_score IS NOT NULL THEN 1 ELSE 0 END) AS scored_lessons,
            SUM(CASE WHEN COALESCE(n.note_completed, 0) = 0 THEN 1 ELSE 0 END) AS missing_notes,
            ROUND(
                100.0 * SUM(
                    CASE
                        WHEN n.note_score IS NOT NULL THEN n.note_score / 10.0
                        ELSE 0
                    END
                ) / NULLIF(COUNT(*), 0),
                1
            ) AS league_score
        FROM lessons l
        JOIN schools s ON s.school_id = l.school_id
        LEFT JOIN instructors i ON i.instructor_id = l.instructor_id
        LEFT JOIN lesson_notes n ON n.lesson_id = l.lesson_id
        WHERE COALESCE(l.lesson_is_reportable, 0) = 1
        GROUP BY s.school_code, s.school_name, i.instructor_name, substr(l.lesson_date, 1, 7)
        """
    )
    conn.execute(
        """
        CREATE VIEW vw_callback_speed AS
        SELECT
            inbound.call_id AS inbound_call_id,
            inbound.school_code,
            inbound.school_name,
            inbound.external_number,
            inbound.date_started AS inbound_at,
            (
                SELECT MIN(outbound.date_started)
                FROM call_logs outbound
                WHERE outbound.external_number = inbound.external_number
                  AND LOWER(COALESCE(outbound.direction, '')) = 'outbound'
                  AND outbound.date_started > inbound.date_started
            ) AS next_outbound_at,
            ROUND(
                24.0 * (
                    julianday((
                        SELECT MIN(outbound.date_started)
                        FROM call_logs outbound
                        WHERE outbound.external_number = inbound.external_number
                          AND LOWER(COALESCE(outbound.direction, '')) = 'outbound'
                          AND outbound.date_started > inbound.date_started
                    )) - julianday(inbound.date_started)
                ),
                2
            ) AS callback_hours,
            inbound.category,
            inbound.voicemail_transcript
        FROM call_logs inbound
        WHERE LOWER(COALESCE(inbound.direction, '')) = 'inbound'
          AND (
              LOWER(COALESCE(inbound.category, '')) LIKE '%miss%'
              OR COALESCE(inbound.voicemail_transcript, '') != ''
          )
        """
    )
    conn.execute(
        """
        CREATE VIEW vw_churn_candidates AS
        SELECT
            "Client ID" AS client_id,
            "Client" AS client_name,
            "Client Home Location" AS school_name,
            "Last Completed Visit Date" AS last_completed_visit_date,
            CAST(NULLIF("Completed Visits", '') AS INTEGER) AS completed_visits,
            CAST(NULLIF("Future Visits", '') AS INTEGER) AS future_visits,
            "Current Passes/Plans" AS current_passes_plans,
            "Has Plan on Hold?" AS has_plan_on_hold
        FROM pike13_clients
        WHERE COALESCE(CAST(NULLIF("Future Visits", '') AS INTEGER), 0) = 0
          AND COALESCE("Current Passes/Plans", '') = ''
        """
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


def school_aliases(value):
    normalized = " ".join(str(value or "").replace("-sor", "").lower().split())
    if normalized in {"westu", "west u", "west university place", "west university"}:
        return {"westu", "west u", "west university place", "west university"}
    if normalized in {"theheights", "the heights", "heights"}:
        return {"theheights", "the heights", "heights"}
    return {normalized} if normalized else set()


def schools_match(left, right):
    return bool(school_aliases(left).intersection(school_aliases(right)))


def resolve_student_person_id(conn, student_name, school_code):
    if not student_name or not table_exists(conn, "persons"):
        return None
    rows = conn.execute(
        """
        SELECT person_id, school
        FROM persons
        WHERE LOWER(COALESCE(display_name, '')) = LOWER(?)
        """,
        (student_name.strip(),),
    ).fetchall()
    matches = [
        row[0]
        for row in rows
        if not row[1] or schools_match(school_code, row[1])
    ]
    return matches[0] if len(set(matches)) == 1 else None


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
            note_timestamp,
            note_score,
            note_score_explanation,
            note_score_model,
            note_score_version,
            note_score_updated_at,
            note_score_hash
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
            note_score,
            note_score_explanation,
            note_score_model,
            note_score_version,
            note_score_updated_at,
            note_score_hash,
        ) = row

        if not lesson_id:
            continue

        school_id = get_or_create_school(conn, schools, school_code)
        instructor_id = get_or_create_instructor(conn, instructors, school_id, instructor_name)

        student_list = split_students(students_raw)
        student_count = len(student_list)
        lesson_is_group = 1 if student_count > 1 else 0
        lesson_is_reportable = 1 if is_reportable_lesson(
            lesson_type, students_raw, instructor_name
        ) else 0

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
                lesson_student_count,
                lesson_is_reportable
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                lesson_is_reportable,
            ),
        )

        conn.execute(
            """
            INSERT OR REPLACE INTO lesson_notes (
                lesson_id,
                note_completed,
                notes_text,
                note_timestamp,
                note_score,
                note_score_explanation,
                note_score_model,
                note_score_version,
                note_score_updated_at,
                note_score_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lesson_id,
                note_completed,
                notes_text,
                note_timestamp,
                note_score,
                note_score_explanation,
                note_score_model,
                note_score_version,
                note_score_updated_at,
                note_score_hash,
            ),
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
                            person_id,
                            is_primary
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (
                            lesson_id,
                            student_id,
                            resolve_student_person_id(conn, student_name, school_code),
                            is_primary,
                        ),
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
