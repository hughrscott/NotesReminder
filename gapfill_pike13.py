#!/usr/bin/env python3
"""Gap-fill Pike13: only the monthly windows that timed out (Feb-Jun 2026).
July + already-present Jan are skipped. Each month gets a 90-min cap
(>25min was the original killer). Runs the extractor directly, idempotent
(upserts existing lessons, inserts new ones)."""
import subprocess, sys, datetime
from pathlib import Path

REPO = Path("/home/ubuntu/projects/hughrscott/NotesReminder")
PY = "/home/ubuntu/.hermes/env/bin/python"
SCHOOLS = ["westu-sor", "theheights-sor"]
# (year, month) windows that failed with rc=-9 (full-month > 25min timeout)
WINDOWS = [(2026, m) for m in (2, 3, 4, 5, 6)]

cmd_tmpl = (
    "{py} -u run_daily.py --school {school} --start-date {sd} --end-date {ed} "
    "--pike13-profile-dir {repo}/browser_profiles/sor_shared --no-email "
    "--skip-note-scoring --skip-s3-sync --db-path {repo}/reminders.db"
)

def month_bounds(y, m):
    sd = datetime.date(y, m, 1)
    if m == 12:
        nxt = datetime.date(y + 1, 1, 1)
    else:
        nxt = datetime.date(y, m + 1, 1)
    ed = nxt - datetime.timedelta(days=1)
    return sd.isoformat(), ed.isoformat()

results = []
for school in SCHOOLS:
    for (y, m) in WINDOWS:
        sd, ed = month_bounds(y, m)
        cmd = cmd_tmpl.format(py=PY, school=school, sd=sd, ed=ed, repo=REPO)
        print(f"\n=== {school} {y}-{m:02d} ({sd}..{ed}) ===", flush=True)
        try:
            r = subprocess.run(cmd, shell=True, cwd=str(REPO),
                               timeout=5400, capture_output=False)
            rc = r.returncode
        except subprocess.TimeoutExpired:
            rc = -9
            print(f"TIMEOUT {school} {y}-{m:02d}", flush=True)
        results.append((school, y, m, rc))
        print(f"<<< {school} {y}-{m:02d} rc={rc}", flush=True)

print("\n=== GAPFILL SUMMARY ===", flush=True)
for school, y, m, rc in results:
    print(f"  {school} {y}-{m:02d}: rc={rc} {'OK' if rc == 0 else 'FAIL'}", flush=True)
fails = [r for r in results if r[3] != 0]
print("GAPFILL", "ALL OK" if not fails else f"{len(fails)} FAIL", flush=True)
