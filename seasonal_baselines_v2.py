#!/usr/bin/env python3
"""Compute seasonal baselines from PRE-2025 data (2023-2024).
These baselines are computed from data BEFORE the training period,
eliminating the circular normalization problem.

HTTP-only approach — no Playwright auth needed, uses the same
public front-end API the churn model's scraper now uses.
"""

import json, os, pickle, sys
import calendar
from collections import defaultdict
from datetime import date
from urllib.request import Request, urlopen

CLIENT_ID = "WWgvG1fId8iDU3rgoFXvz4A2kLnxDBSsOFacfk8X"
SCHOOLS = {
    "westu-sor": 1,
    "theheights-sor": 2,
}
MDIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")


def fetch_month(school: str, year: int, month: int) -> int:
    """Fetch event count for a school×month from Pike13 front-end API."""
    last_day = calendar.monthrange(year, month)[1]
    from_ts = f"{year}-{month:02d}-01T06:00:00Z"
    to_ts = f"{year}-{month:02d}-{last_day}T05:59:59Z"
    url = f"https://{school}.pike13.com/api/v2/front/event_occurrences.json?client_id={CLIENT_ID}&from={from_ts}&to={to_ts}"
    try:
        req = Request(url, headers={"User-Agent": "Hermes/1.0"})
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        return len(data.get("event_occurrences", []))
    except Exception as e:
        print(f"  ⚠️ {school} {year}-{month:02d}: API error: {e}", file=sys.stderr)
        return -1


def main():
    # Fetch monthly event counts for 2023-2024
    counts = defaultdict(lambda: defaultdict(dict))  # school → year → month → count

    for school in SCHOOLS:
        print(f"Fetching {school}...")
        for year in [2023, 2024]:
            for month in range(1, 13):
                count = fetch_month(school, year, month)
                if count > 0:
                    counts[school][year][month] = count
                elif count == -1:
                    print(f"  ⚠️ {year}-{month:02d}: API error, skipping")
                # 0-count months (holidays?) — still record

    # Compute seasonal index for each school×month
    # index = avg_monthly_count / avg_all_months
    baselines = {}
    for school, years in counts.items():
        school_id = SCHOOLS[school]
        # Gather all monthly counts across both years
        all_counts = []
        monthly_totals = defaultdict(list)
        for year, months in years.items():
            for month, cnt in months.items():
                monthly_totals[month].append(cnt)
                all_counts.append(cnt)

        if not all_counts:
            print(f"  No data for {school}, skipping")
            continue

        avg_all = sum(all_counts) / len(all_counts)

        for month in range(1, 13):
            vals = monthly_totals.get(month, [])
            if vals:
                avg_month = sum(vals) / len(vals)
                # Seasonal index: how much above/below average this month is
                seasonal_index = avg_month / avg_all if avg_all > 0 else 1.0
            else:
                seasonal_index = 1.0

            # Store as (school_id, month) → seasonal_index
            baselines[(school_id, month)] = {
                "seasonal_index": round(seasonal_index, 3),
                "monthly_counts": [round(x, 1) for x in vals],
                "avg_all": round(avg_all, 1),
            }
            print(f"  school={school_id} month={month:02d}: "
                  f"index={seasonal_index:.3f} (counts={[round(x,1) for x in vals]})")

    # Save
    path = os.path.join(MDIR, "seasonal_baselines_v2.pkl")
    with open(path, "wb") as f:
        pickle.dump(baselines, f)
    print(f"\nSaved {len(baselines)} baselines → {path}")

    # Summary: peak and trough months
    for school_id in sorted(set(sid for sid, _ in baselines)):
        months = [(m, baselines[(school_id, m)]["seasonal_index"]) for m in range(1, 13) if (school_id, m) in baselines]
        if months:
            peak = max(months, key=lambda x: x[1])
            trough = min(months, key=lambda x: x[1])
            print(f"\n  School {school_id}: peak={peak[1]:.3f} (month {peak[0]}), "
                  f"trough={trough[1]:.3f} (month {trough[0]}), "
                  f"swing={peak[1]/trough[1]:.1f}×")


if __name__ == "__main__":
    main()
