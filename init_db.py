import sqlite3

def initialize_db():
    conn = sqlite3.connect('reminders.db')
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
        reminder_sent INTEGER DEFAULT 0,
        reminder_count INTEGER DEFAULT 0,
        note_completed INTEGER DEFAULT 0,
        last_checked DATE,
        last_reminder_sent TIMESTAMP
    );
    ''')

    conn.commit()
    conn.close()
    print("📂 SQLite database initialized clearly.")

if __name__ == "__main__":
    initialize_db()
