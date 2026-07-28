#!/usr/bin/env python3
"""
probe_fields.py — Empirically discover which Pike13 report field tokens
actually return data for a given date window. We try a candidate list
against the live /queries endpoint (ONE auth session) and keep any token
that returns >=1 row. No guessing of final semantics — we just record
what works + the returned field names (which tell us what the report is).

Candidate tokens are educated guesses from the report labels; the verdict
comes from the live response, not from the guess.
"""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
from pike13_report_puller import pull_reports_batch

SCHOOL = "westu-sor"

# educated candidate tokens (snake_case, singular/plural variants)
CANDIDATES = [
    "last_membership_end", "last_memberships_end", "membership_end_date",
    "last_membership_start", "first_membership_start", "membership_start",
    "created_at", "client_created_at", "created",
    "last_visit", "last_visited", "last_visit_date",
    "first_visit", "first_visited", "first_visit_date",
    "expiring_membership", "expiring", "expiry_date",
    "email", "full_name", "status",
]


async def main():
    frm, to = "2026-07-15", "2026-08-11"
    print(f"Probing {len(CANDIDATES)} candidate field tokens for {SCHOOL} {frm}..{to} ...")
    results = await pull_reports_batch(SCHOOL, frm, to, CANDIDATES, op="btw", verbose=True)

    good = {}
    print("\n=== VERDICT ===")
    for f, (rows, meta) in results.items():
        n = len(rows)
        status = meta.get("http_status")
        names = meta.get("field_names")
        ok = (n and n > 0) or (meta.get("total_count"))
        mark = "OK " if ok else "---"
        print(f"  [{mark}] {f:28s} HTTP={status} rows={n} total={meta.get('total_count')} names={names}")
        if ok:
            good[f] = {"rows": n, "total_count": meta.get("total_count"),
                        "field_names": names, "sample": rows[0] if rows else None}

    out = {
        "school": SCHOOL, "from": frm, "to": to,
        "probelled_at": datetime.now(timezone.utc).isoformat(),
        "working_fields": good,
        "all_status": {f: (len(r), m.get("http_status")) for f, (r, m) in results.items()},
    }
    OUT = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models/pike13_fields_probed.json")
    OUT.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nSaved -> {OUT}")
    print(f"Working fields: {list(good.keys())}")


if __name__ == "__main__":
    asyncio.run(main())
