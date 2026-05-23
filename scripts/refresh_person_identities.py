#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from notesreminder.lib.person_identity import refresh_person_identities  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Refresh deterministic person identity tables.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        summary = refresh_person_identities(conn)
        conn.commit()
    finally:
        conn.close()

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(
            "Person identity refresh complete: "
            f"persons={summary['persons']} "
            f"identities={summary['person_identities']} "
            f"conflicts={summary['conflicts']} "
            f"linked_sources={summary['linked_sources']}"
        )


if __name__ == "__main__":
    main()
