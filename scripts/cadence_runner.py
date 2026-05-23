#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from notesreminder.orchestration.cadence import run_cadence, write_metadata  # noqa: E402


DEFAULT_OUTPUT_DIR = "outputs/progress/cadence_runs"


def main():
    parser = argparse.ArgumentParser(description="Dry-run or execute the NotesReminder cadence scaffold.")
    parser.add_argument("--date", dest="run_date", help="Cadence date, YYYY-MM-DD.")
    parser.add_argument("--execute-shadow", action="store_true", help="Run shadow report commands.")
    parser.add_argument(
        "--execute-production",
        action="store_true",
        help="Run production notes/email command. Requires explicit human approval before use.",
    )
    parser.add_argument(
        "--simulate-expired-auth",
        action="store_true",
        help="Simulate an expired MFA/browser session and verify actionable failure metadata.",
    )
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    metadata = run_cadence(
        run_date=args.run_date,
        root=ROOT,
        execute_shadow=args.execute_shadow,
        execute_production=args.execute_production,
        simulate_expired_auth=args.simulate_expired_auth,
    )
    output_path = Path(args.output_dir) / f"cadence_{metadata['run_date']}_{metadata['status']}.json"
    write_metadata(metadata, output_path)
    print(f"Wrote {output_path}")
    if metadata["status"] == "action_required":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
