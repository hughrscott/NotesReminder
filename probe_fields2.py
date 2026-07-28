#!/usr/bin/env python3
"""probe_fields2.py — Clean isolated re-probe of candidate tokens that came
back as HTTP=None (listener-timing artifact of the batch run, not a verdict)
or 0-rows (valid but empty in-window). One field per session = no
listener bleed. We record real HTTP status + rows + field names."""
import asyncio, sys, json
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
from pike13_report_puller import pull_report, build_report_url

SCHOOL = "westu-sor"
CANDIDATES = [
    "first_visit_date", "created_at", "client_created_at",
    "expiry_date", "membership_start", "first_membership_start",
    "last_membership_start", "start_date", "signup_date",
]


async def main():
    frm, to = "2026-07-15", "2026-08-11"
    # widen window for created/signup-like fields (they may predate the window)
    wide_frm, wide_to = "2026-01-01", "2026-08-11"
    results = {}
    for f in CANDIDATES:
        ff, tt = (wide_frm, wide_to) if "creat" in f or "signup" in f or "start" in f else (frm, to)
        try:
            rows, meta = await pull_report(SCHOOL, ff, tt, field=f, op="btw", verbose=False)
            results[f] = {
                "http": meta.get("http_status"),
                "rows": len(rows),
                "total": meta.get("total_count"),
                "names": meta.get("field_names"),
                "window": [ff, tt],
            }
            print(f"  {f:26s} HTTP={meta.get('http_status')} rows={len(rows)} "
                  f"total={meta.get('total_count')} names={meta.get('field_names')}")
        except Exception as e:
            results[f] = {"error": str(e)[:80]}
            print(f"  {f:26s} ERR {str(e)[:60]}")

    out = {"school": SCHOOL, "probe_at": datetime.now(timezone.utc).isoformat(), "results": results}
    OUT = Path("/home/ubuntu/projects/hughrscott/NotesReminder/models/pike13_fields_probed2.json")
    OUT.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nSaved -> {OUT}")
    working = [f for f, r in results.items() if (r.get("rows") or r.get("total")) and not r.get("error")]
    print(f"Working (incl. wide window): {working}")


if __name__ == "__main__":
    asyncio.run(main())
