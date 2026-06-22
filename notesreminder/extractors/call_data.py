import argparse
import csv
import re
import sqlite3
from pathlib import Path

from notesreminder.lib.phone import normalize_phone


def normalize_email(value):
    if not value:
        return None
    return value.strip().lower()


def split_multi(value):
    if not value:
        return []
    parts = re.split(r"[;,]", value)
    return [p.strip() for p in parts if p.strip()]


def create_table(conn, table_name, columns):
    if not table_exists(conn, table_name):
        cols = ", ".join([f"\"{col}\" TEXT" for col in columns])
        conn.execute(f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({cols})")
        return
    existing = {row[1] for row in conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()}
    for column in columns:
        if column not in existing:
            conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{column}" TEXT')


def read_csv_rows(csv_path, required_headers=()):
    last_error = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with csv_path.open(newline="", encoding=encoding) as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []
                missing = [header for header in required_headers if header not in headers]
                if missing:
                    print(f"⚠️ {csv_path} missing expected headers: {', '.join(missing)}")
                return headers, list(reader)
        except UnicodeDecodeError as exc:
            last_error = exc
    raise last_error


def union_headers(header_sets, extra_columns=()):
    seen = set()
    headers = []
    for header_set in header_sets:
        for header in header_set:
            if header not in seen:
                seen.add(header)
                headers.append(header)
    for header in extra_columns:
        if header not in seen:
            seen.add(header)
            headers.append(header)
    return headers


def reset_staging_tables(conn):
    for table_name in (
        "pike13_clients",
        "dialpad_calls",
        "dialpad_recordings",
        "dialpad_voicemails",
    ):
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    if table_exists(conn, "call_client_matches"):
        conn.execute("DELETE FROM call_client_matches")
    if table_exists(conn, "call_logs"):
        conn.execute("DELETE FROM call_logs")


def ensure_indexes(conn):
    if table_exists(conn, "dialpad_calls"):
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_email ON dialpad_calls(email_lower)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_phone ON dialpad_calls(external_number_digits)")
    if table_exists(conn, "pike13_clients"):
        conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_email ON pike13_clients(email_lower)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_guardian_email ON pike13_clients(guardian_email_lower)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_phone ON pike13_clients(phone_digits)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_mobile ON pike13_clients(mobile_digits)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_account_phone ON pike13_clients(account_manager_phone_digits)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_emergency_phone ON pike13_clients(emergency_contact_phone_digits)")


def import_csv(conn, table_name, csv_path, extra_columns=None, unique_key=None):
    raw_headers, rows = read_csv_rows(csv_path)
    headers = union_headers([raw_headers], extra_columns or [])
    create_table(conn, table_name, headers)
    placeholders = ", ".join(["?"] * len(headers))
    columns_sql = ", ".join([f"\"{col}\"" for col in headers])
    insert_sql = f"INSERT INTO \"{table_name}\" ({columns_sql}) VALUES ({placeholders})"
    if unique_key:
        insert_sql = (
            f"INSERT OR IGNORE INTO \"{table_name}\" ({columns_sql}) VALUES ({placeholders})"
        )
    for row in rows:
        values = []
        for col in headers:
            values.append(row.get(col))
        conn.execute(insert_sql, values)


def load_pike13_clients(conn, client_csv):
    extra = [
        "email_lower",
        "guardian_email_lower",
        "phone_digits",
        "mobile_digits",
        "account_manager_phone_digits",
        "emergency_contact_phone_digits",
    ]
    create_table(
        conn,
        "pike13_clients",
        [
            "Client",
            "Email",
            "Phone",
            "Street Address",
            "Street Address 2",
            "City",
            "State Code",
            "Postal Code",
            "Country Code",
            "Tenure",
            "Last Completed Visit Date",
            "Last Completed Visit Service",
            "Completed Visits",
            "Future Visits",
            "Has Plan on Hold?",
            "Payment on File?",
            "Last Membership End Date",
            "Next Pass/Plan End Date",
            "Current Passes/Plans",
            "Primary Staff Member",
            "Dependents",
            "Account Manager Emails",
            "Account Manager Phones",
            "Guardian Name",
            "Guardian Email",
            "Client Home Location",
            "First Completed Visit Date",
            "Last Signed Waiver Name",
            "First Name",
            "Middle Name",
            "Last Name",
            "Emergency Contact Name",
            "Emergency Contact Number",
            "Gender",
            "Instrument",
            "Mobile Phone",
            "Referral Source",
            "Skill Level",
            "Academic School",
            "Customer ID",
            "First Visit",
            "Lead Source",
            "Marketing Source",
            "Zendesk Ticket ID",
            "Client ID",
        ]
        + extra,
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pike13_clients_client_id_unique
        ON pike13_clients("Client ID")
        WHERE "Client ID" IS NOT NULL AND "Client ID" != ''
        """
    )

    raw_headers, rows = read_csv_rows(client_csv, required_headers=("Client ID",))
    create_table(conn, "pike13_clients", union_headers([raw_headers], extra))
    for row in rows:
        email_lower = normalize_email(row.get("Email"))
        guardian_email_lower = normalize_email(row.get("Guardian Email"))
        phone_digits = normalize_phone(row.get("Phone"))
        mobile_digits = normalize_phone(row.get("Mobile Phone"))
        emergency_contact_phone_digits = normalize_phone(row.get("Emergency Contact Number"))
        account_manager_phones = split_multi(row.get("Account Manager Phones"))
        account_manager_phone_digits = (
            normalize_phone(account_manager_phones[0]) if account_manager_phones else None
        )
        row["email_lower"] = email_lower
        row["guardian_email_lower"] = guardian_email_lower
        row["phone_digits"] = phone_digits
        row["mobile_digits"] = mobile_digits
        row["account_manager_phone_digits"] = account_manager_phone_digits
        row["emergency_contact_phone_digits"] = emergency_contact_phone_digits

        cols = list(row.keys())
        placeholders = ", ".join(["?"] * len(cols))
        columns_sql = ", ".join([f"\"{col}\"" for col in cols])
        conn.execute(
            f"INSERT OR REPLACE INTO pike13_clients ({columns_sql}) VALUES ({placeholders})",
            [row.get(col) for col in cols],
        )

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_clients_client_id ON pike13_clients(\"Client ID\")"
    )


def load_dialpad_calls(conn, call_csvs):
    extra = ["email_lower", "external_number_digits", "internal_number_digits", "name_lower"]
    loaded = [(*read_csv_rows(path, required_headers=("call_id",)), path) for path in call_csvs]
    if not loaded:
        return
    all_headers = union_headers([headers for headers, _, _ in loaded], extra)
    for path in call_csvs:
        for headers, rows, loaded_path in loaded:
            if loaded_path != path:
                continue
            create_table(conn, "dialpad_calls", all_headers)
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_dialpad_calls_call_id_unique
                ON dialpad_calls(call_id)
                WHERE call_id IS NOT NULL AND call_id != ''
                """
            )
            insert_cols = all_headers
            placeholders = ", ".join(["?"] * len(insert_cols))
            columns_sql = ", ".join([f"\"{col}\"" for col in insert_cols])
            insert_sql = f"INSERT OR REPLACE INTO dialpad_calls ({columns_sql}) VALUES ({placeholders})"

            for row in rows:
                row["email_lower"] = normalize_email(row.get("email"))
                row["external_number_digits"] = normalize_phone(row.get("external_number"))
                row["internal_number_digits"] = normalize_phone(row.get("internal_number"))
                row["name_lower"] = (row.get("name") or "").strip().lower() or None
                conn.execute(insert_sql, [row.get(col) for col in insert_cols])


def load_dialpad_generic(conn, table_name, csvs, key_column=None):
    loaded = [(*read_csv_rows(path, required_headers=((key_column,) if key_column else ())), path) for path in csvs]
    if not loaded:
        return
    headers = union_headers([item[0] for item in loaded])
    create_table(conn, table_name, headers)
    placeholders = ", ".join(["?"] * len(headers))
    columns_sql = ", ".join([f"\"{col}\"" for col in headers])
    insert_sql = f"INSERT OR IGNORE INTO \"{table_name}\" ({columns_sql}) VALUES ({placeholders})"
    for _, rows, path in loaded:
        for row in rows:
            conn.execute(insert_sql, [row.get(col) for col in headers])
            if key_column:
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{key_column} "
                    f"ON \"{table_name}\"(\"{key_column}\")"
                )


def build_matches(conn, enable_fuzzy=False):
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
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_matches_unique "
        "ON call_client_matches(call_id, client_id, match_type, match_value)"
    )

    # Email matches (client email + guardian email + account manager email).
    email_matches = conn.execute(
        """
        SELECT c.call_id, p."Client ID", c.email_lower
        FROM dialpad_calls c
        JOIN pike13_clients p
          ON c.email_lower IS NOT NULL
         AND (
             c.email_lower = p.email_lower
             OR c.email_lower = p.guardian_email_lower
             OR c.email_lower = LOWER(p."Account Manager Emails")
         )
        """
    ).fetchall()
    for call_id, client_id, value in email_matches:
        conn.execute(
            """
            INSERT OR IGNORE INTO call_client_matches
            (call_id, client_id, match_type, confidence, match_value, matched_on)
            VALUES (?, ?, 'email_exact', 0.95, ?, 'dialpad_calls.email')
            """,
            (call_id, client_id, value),
        )

    # Phone matches (external number vs client phone/mobile/account manager phones).
    phone_matches = conn.execute(
        """
        SELECT c.call_id, p."Client ID", c.external_number_digits
        FROM dialpad_calls c
        JOIN pike13_clients p
          ON c.external_number_digits IS NOT NULL
         AND (
             c.external_number_digits = p.phone_digits
             OR c.external_number_digits = p.mobile_digits
             OR c.external_number_digits = p.account_manager_phone_digits
             OR c.external_number_digits = p.emergency_contact_phone_digits
         )
        """
    ).fetchall()
    for call_id, client_id, value in phone_matches:
        conn.execute(
            """
            INSERT OR IGNORE INTO call_client_matches
            (call_id, client_id, match_type, confidence, match_value, matched_on)
            VALUES (?, ?, 'phone_exact', 0.90, ?, 'dialpad_calls.external_number')
            """,
            (call_id, client_id, value),
        )

    # Name matches (exact match on client or guardian name).
    name_matches = conn.execute(
        """
        SELECT c.call_id, p."Client ID", c.name_lower
        FROM dialpad_calls c
        JOIN pike13_clients p
          ON c.name_lower IS NOT NULL
         AND (
             c.name_lower = LOWER(p."Client")
             OR c.name_lower = LOWER(p."Guardian Name")
         )
        """
    ).fetchall()
    for call_id, client_id, value in name_matches:
        conn.execute(
            """
            INSERT OR IGNORE INTO call_client_matches
            (call_id, client_id, match_type, confidence, match_value, matched_on)
            VALUES (?, ?, 'name_exact', 0.70, ?, 'dialpad_calls.name')
            """,
            (call_id, client_id, value),
        )

    if enable_fuzzy:
        # Optional fuzzy matching can be added later; skip by default.
        pass


def table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def build_reporting_call_logs(conn):
    if not table_exists(conn, "dialpad_calls"):
        return

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dialpad_voicemails (
            call_id TEXT,
            transcription_text TEXT,
            recording_url TEXT,
            date TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dialpad_recordings (
            call_id TEXT,
            recording_url TEXT,
            duration TEXT,
            date TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS call_logs (
            call_id TEXT PRIMARY KEY,
            office_id TEXT,
            external_number TEXT,
            internal_number TEXT,
            date_started TEXT,
            direction TEXT,
            category TEXT,
            name TEXT,
            email TEXT,
            is_internal INTEGER,
            school_code TEXT,
            school_name TEXT,
            voicemail_transcript TEXT,
            voicemail_recording_url TEXT,
            recording_url TEXT,
            recording_duration TEXT,
            voicemail_date TEXT,
            recording_date TEXT
        )
        """
    )
    if table_exists(conn, "call_logs"):
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(call_logs)").fetchall()
        }
        if "office_id" not in columns:
            conn.execute("ALTER TABLE call_logs ADD COLUMN office_id TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_call_logs_date_started ON call_logs(date_started)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_call_logs_school_code ON call_logs(school_code)"
    )

    conn.execute(
        """
        INSERT OR REPLACE INTO call_logs (
            call_id,
            office_id,
            external_number,
            internal_number,
            date_started,
            direction,
            category,
            name,
            email,
            is_internal,
            school_code,
            school_name,
            voicemail_transcript,
            voicemail_recording_url,
            recording_url,
            recording_duration,
            voicemail_date,
            recording_date
        )
        WITH voicemail_one AS (
            SELECT
                call_id,
                MAX(transcription_text) AS transcription_text,
                MAX(recording_url) AS recording_url,
                MAX(date) AS date
            FROM dialpad_voicemails
            GROUP BY call_id
        ),
        recording_one AS (
            SELECT
                call_id,
                MAX(recording_url) AS recording_url,
                MAX(duration) AS duration,
                MAX(date) AS date
            FROM dialpad_recordings
            GROUP BY call_id
        )
        SELECT
            c.call_id,
            c.office_id,
            c.external_number,
            c.internal_number,
            c.date_started,
            c.direction,
            c.category,
            c.name,
            c.email,
            CASE
                WHEN c.is_internal = 'true' THEN 1
                WHEN c.is_internal = 'false' THEN 0
                ELSE NULL
            END AS is_internal,
            CASE
                WHEN c.office_id = '4776436560855040' THEN 'westu-sor'
                WHEN c.office_id = '6438038951198720' THEN 'theheights-sor'
                WHEN lower(c.name) LIKE '%west u%' OR lower(c.name) LIKE '%westu%' THEN 'westu-sor'
                WHEN lower(c.name) LIKE '%heights%' THEN 'theheights-sor'
                ELSE NULL
            END AS school_code,
            CASE
                WHEN c.office_id = '4776436560855040' THEN 'West U'
                WHEN c.office_id = '6438038951198720' THEN 'The Heights'
                WHEN lower(c.name) LIKE '%west u%' OR lower(c.name) LIKE '%westu%' THEN 'West U'
                WHEN lower(c.name) LIKE '%heights%' THEN 'The Heights'
                ELSE NULL
            END AS school_name,
            v.transcription_text,
            v.recording_url,
            r.recording_url,
            r.duration,
            v.date,
            r.date
        FROM dialpad_calls c
        LEFT JOIN voicemail_one v ON v.call_id = c.call_id
        LEFT JOIN recording_one r ON r.call_id = c.call_id
        """
    )


def run_import(clients_path, dialpad_dir, db_path="reminders.db", enable_fuzzy=False):
    db_path = Path(db_path)
    dialpad_dir = Path(dialpad_dir)
    client_csv = Path(clients_path)

    call_csvs = sorted(dialpad_dir.rglob("Call_Logs*.csv"))
    recording_csvs = sorted(dialpad_dir.rglob("Recordings*.csv"))
    voicemail_csvs = sorted(dialpad_dir.rglob("Voicemails*.csv"))
    user_stats_csvs = sorted(dialpad_dir.rglob("User_Statistics*.csv"))
    daily_stats_csvs = sorted(dialpad_dir.rglob("Daily_Statistics*.csv"))

    conn = sqlite3.connect(db_path)
    try:
        reset_staging_tables(conn)
        load_pike13_clients(conn, client_csv)
        load_dialpad_calls(conn, call_csvs)
        load_dialpad_generic(conn, "dialpad_recordings", recording_csvs, key_column="call_id")
        load_dialpad_generic(conn, "dialpad_voicemails", voicemail_csvs, key_column="call_id")
        load_dialpad_generic(conn, "dialpad_user_stats", user_stats_csvs)
        load_dialpad_generic(conn, "dialpad_daily_stats", daily_stats_csvs)
        ensure_indexes(conn)
        build_matches(conn, enable_fuzzy=enable_fuzzy)
        build_reporting_call_logs(conn)
        conn.commit()
    finally:
        conn.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Import Dialpad + Pike13 CSVs into reminders.db")
    parser.add_argument(
        "--db",
        default="reminders.db",
        help="Path to the SQLite database (default: reminders.db)",
    )
    parser.add_argument(
        "--dialpad-dir",
        default="Call Log",
        help="Directory containing Dialpad CSV exports",
    )
    parser.add_argument(
        "--clients",
        required=True,
        help="Path to Pike13 client CSV export",
    )
    parser.add_argument(
        "--enable-fuzzy",
        action="store_true",
        help="Enable fuzzy name matching (optional, slower)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    run_import(
        clients_path=args.clients,
        dialpad_dir=args.dialpad_dir,
        db_path=args.db,
        enable_fuzzy=args.enable_fuzzy,
    )
    print("✅ Imported Dialpad + Pike13 data and built call_client_matches.")


if __name__ == "__main__":
    main()
