#!/usr/bin/env python3
import argparse
import csv
import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def ensure_history_table(conn: sqlite3.Connection):
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
        "CREATE INDEX IF NOT EXISTS idx_lesson_note_scores_history_lesson_id "
        "ON lesson_note_scores_history(lesson_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lesson_note_scores_history_scored_at "
        "ON lesson_note_scores_history(scored_at)"
    )
    conn.commit()


def fingerprint(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Merge matched legacy scores CSV into lesson_note_scores_history."
    )
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--matched-csv", default="outputs/matched_legacy_scores.csv")
    parser.add_argument("--source-db", required=True, help="Source DB used to generate match CSV.")
    parser.add_argument("--scoring-source", default="legacy-recovery")
    parser.add_argument("--scoring-model", default="unknown")
    parser.add_argument("--scoring-version", default="legacy-import-v1")
    parser.add_argument(
        "--import-run-id",
        default=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    csv_path = Path(args.matched_csv).expanduser().resolve()
    source_db = Path(args.source_db).expanduser().resolve()
    if not db_path.exists():
        raise SystemExit(f"db not found: {db_path}")
    if not csv_path.exists():
        raise SystemExit(f"matched csv not found: {csv_path}")
    if not source_db.exists():
        raise SystemExit(f"source db not found: {source_db}")

    source_fingerprint = fingerprint(source_db)
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    conn = sqlite3.connect(str(db_path))
    try:
        ensure_history_table(conn)
        inserted = 0
        skipped = 0
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "empty" in row:
                    continue
                lesson_id = (row.get("matched_lesson_id") or "").strip()
                if not lesson_id:
                    skipped += 1
                    continue
                score_raw = (row.get("source_score") or "").strip()
                try:
                    score = float(score_raw)
                except ValueError:
                    skipped += 1
                    continue
                scored_at = (
                    (row.get("source_scored_at") or "").strip() or now_iso
                )
                values = (
                    lesson_id,
                    (row.get("matched_pike13_lesson_id") or "").strip() or None,
                    score,
                    args.scoring_source,
                    args.scoring_model,
                    args.scoring_version,
                    (row.get("source_justification") or "").strip() or None,
                    (row.get("source_strength_or_weakness") or "").strip() or None,
                    (row.get("source_improvement") or "").strip() or None,
                    scored_at,
                    args.import_run_id,
                    source_fingerprint,
                )
                if not args.dry_run:
                    cur = conn.execute(
                        """
                        INSERT OR IGNORE INTO lesson_note_scores_history (
                            lesson_id,
                            pike13_lesson_id,
                            score,
                            scoring_source,
                            scoring_model,
                            scoring_version,
                            justification,
                            strength_or_weakness,
                            improvement,
                            scored_at,
                            import_run_id,
                            source_db_fingerprint
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        values,
                    )
                    if cur.rowcount == 1:
                        inserted += 1
                    else:
                        skipped += 1
                else:
                    inserted += 1
        if not args.dry_run:
            conn.commit()
    finally:
        conn.close()

    mode = "dry-run" if args.dry_run else "merge"
    print(f"mode={mode} inserted={inserted} skipped_or_duplicate={skipped}")


if __name__ == "__main__":
    main()
