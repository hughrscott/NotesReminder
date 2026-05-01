#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import sqlite3
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema, finish_import_run, start_import_run, utc_now_iso  # noqa: E402
from scripts.extract_pike13_leads import DEFAULT_URL, person_urls_from_db, wait_until_ready  # noqa: E402


DEFAULT_OUTPUT = "outputs/progress/pike13_route_discovery.md"


def stable_id(*parts):
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()
    return f"pike13_route_{digest[:24]}"


def visible_row_count(text):
    return len([line for line in (text or "").splitlines() if line.strip()])


def route_signals(text, links):
    lowered = (text or "").lower()
    link_values = [f"{link.get('href', '')} {link.get('text', '')}".lower() for link in links or []]
    return {
        "source_timestamp_visible": int(any(token in lowered for token in ("date", "time", "starts", "visit", "event"))),
        "transcript_link_visible": 0,
        "recording_link_visible": 0,
        "visit_signal_visible": int(any(token in lowered for token in ("visit", "attendance", "no show", "cancel"))),
        "plan_signal_visible": int(any(token in lowered for token in ("plan", "pass", "membership"))),
        "person_signal_visible": int(any(token in lowered for token in ("phone", "email", "membership", "client"))),
        "login_signal_visible": int(any(token in lowered for token in ("sign in", "login", "password"))),
        "link_count": len(links or []),
        "person_link_count": sum(1 for value in link_values if "/people/" in value),
        "visit_link_count": sum(1 for value in link_values if "/visits/" in value or "/events/" in value),
    }


def route_status(text, signals):
    if signals["login_signal_visible"] and not signals["person_signal_visible"] and not signals["visit_signal_visible"]:
        return "blocked", "Login/session required."
    if signals["person_signal_visible"] or signals["visit_signal_visible"] or signals["plan_signal_visible"]:
        return "usable", None
    if visible_row_count(text) > 0:
        return "partial", "Route loaded, but expected Pike13 fields were not visible."
    return "blocked", "Route did not expose visible Pike13 content."


def extract_links(page):
    return page.locator("a").evaluate_all(
        """
        links => links.map(a => ({href: a.href || '', text: a.innerText || a.textContent || ''}))
                      .filter(a => a.href)
        """
    )


def route_row(run_id, source, route_name, route_url, status, text, links, expected_controls, blocker=None):
    now = utc_now_iso()
    signals = route_signals(text, links)
    return {
        "route_id": f"{run_id}:{route_name}",
        "run_id": run_id,
        "source": source,
        "route_name": route_name,
        "route_url": route_url,
        "status": status,
        "loaded_at": now,
        "visible_row_count": visible_row_count(text),
        "visible_link_count": len(links or []),
        "source_timestamp_visible": signals["source_timestamp_visible"],
        "transcript_link_visible": signals["transcript_link_visible"],
        "recording_link_visible": signals["recording_link_visible"],
        "expected_controls_json": json.dumps(expected_controls, sort_keys=True),
        "blocker": blocker,
        "raw_json": json.dumps(
            {
                "signals": signals,
                "sensitive_values_redacted_from_reports": True,
                "raw_page_text_not_stored": True,
            },
            sort_keys=True,
        ),
        "updated_at": now,
    }


def upsert_source_route_discovery(conn, row):
    conn.execute(
        """
        INSERT INTO source_route_discoveries (
            route_id, run_id, source, route_name, route_url, status, loaded_at,
            visible_row_count, visible_link_count, source_timestamp_visible,
            transcript_link_visible, recording_link_visible, expected_controls_json,
            blocker, raw_json, updated_at
        )
        VALUES (
            :route_id, :run_id, :source, :route_name, :route_url, :status, :loaded_at,
            :visible_row_count, :visible_link_count, :source_timestamp_visible,
            :transcript_link_visible, :recording_link_visible, :expected_controls_json,
            :blocker, :raw_json, :updated_at
        )
        ON CONFLICT(route_id) DO UPDATE SET
            route_url = excluded.route_url,
            status = excluded.status,
            loaded_at = excluded.loaded_at,
            visible_row_count = excluded.visible_row_count,
            visible_link_count = excluded.visible_link_count,
            source_timestamp_visible = excluded.source_timestamp_visible,
            transcript_link_visible = excluded.transcript_link_visible,
            recording_link_visible = excluded.recording_link_visible,
            expected_controls_json = excluded.expected_controls_json,
            blocker = excluded.blocker,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


def routes_for_probe(conn, base_url, limit):
    base = base_url.rstrip("/")
    routes = [
        {
            "name": "base_session",
            "url": base,
            "expected_controls": ["authenticated Pike13 navigation", "client search or dashboard content"],
        }
    ]
    person_urls = person_urls_from_db(conn, base, max(limit, 1))
    if person_urls:
        routes.append(
            {
                "name": "known_person",
                "url": person_urls[0],
                "expected_controls": ["person profile", "visits/events", "plans/passes", "contact fields"],
            }
        )
    else:
        routes.append(
            {
                "name": "people_index",
                "url": f"{base}/people",
                "expected_controls": ["people list", "person search", "person links"],
            }
        )
    routes.extend(
        [
            {
                "name": "visits_or_events",
                "url": f"{base}/events",
                "expected_controls": ["events or visits list", "date controls", "attendance state"],
            },
            {
                "name": "plans_or_passes",
                "url": f"{base}/plans",
                "expected_controls": ["plans", "passes", "membership state"],
            },
        ]
    )
    return routes[:limit]


def render_route_report(rows):
    lines = [
        "# Pike13 Route Discovery",
        "",
        "This report is sanitized and records route capability only.",
        "",
        "| Route | Status | Rows | Links | Source timestamp visible | Blocker |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            "| {route_name} | {status} | {visible_row_count} | {visible_link_count} | {source_timestamp_visible} | {blocker} |".format(
                route_name=row["route_name"],
                status=row["status"],
                visible_row_count=row["visible_row_count"],
                visible_link_count=row["visible_link_count"],
                source_timestamp_visible="yes" if row["source_timestamp_visible"] else "no",
                blocker=(row["blocker"] or "").replace("|", "/"),
            )
        )
    lines.extend(
        [
            "",
            "_No customer names, phone numbers, emails, note text, page text, SMS bodies, transcripts, or recaps are included._",
            "",
        ]
    )
    return "\n".join(lines)


def run_pike13_route_discovery(db_path, profile_dir, base_url=DEFAULT_URL, interactive_login=False, headless=False, limit=4, output=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    routes = routes_for_probe(conn, base_url, limit)
    run_id = start_import_run(
        conn,
        "pike13_route_discovery",
        Path(__file__).name,
        metadata={"base_url": base_url, "route_count": len(routes)},
    )
    conn.commit()
    rows = []
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                str(profile_dir),
                headless=headless and not interactive_login,
                viewport={"width": 1440, "height": 1000},
            )
            page = context.pages[0] if context.pages else context.new_page()
            if interactive_login:
                page.goto(base_url, wait_until="domcontentloaded", timeout=60000)
                wait_until_ready(page)
                print("Complete Pike13 login/navigation in the browser, then press Enter here.")
                input()
            for route in routes:
                try:
                    page.goto(route["url"], wait_until="domcontentloaded", timeout=60000)
                    wait_until_ready(page)
                    text = page.locator("body").inner_text(timeout=30000)
                    links = extract_links(page)
                    signals = route_signals(text, links)
                    status, blocker = route_status(text, signals)
                    row = route_row(
                        run_id,
                        "pike13",
                        route["name"],
                        page.url,
                        status,
                        text,
                        links,
                        route["expected_controls"],
                        blocker=blocker,
                    )
                except Exception as exc:
                    row = route_row(
                        run_id,
                        "pike13",
                        route["name"],
                        route["url"],
                        "blocked",
                        "",
                        [],
                        route["expected_controls"],
                        blocker=str(exc).replace("\n", " ")[:240],
                    )
                upsert_source_route_discovery(conn, row)
                rows.append(row)
                conn.commit()
            context.close()
        status = "success" if any(row["status"] in {"usable", "partial"} for row in rows) else "blocked"
        finish_import_run(
            conn,
            run_id,
            status,
            rows_seen=len(routes),
            rows_inserted=len(rows),
            rows_updated=0,
            metadata={"statuses": {status: sum(1 for row in rows if row["status"] == status) for status in {row["status"] for row in rows}}},
        )
        conn.commit()
    except Exception as exc:
        finish_import_run(conn, run_id, "blocked", rows_seen=len(routes), error=str(exc).replace("\n", " ")[:240])
        conn.commit()
        raise
    finally:
        conn.close()
    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(render_route_report(rows), encoding="utf-8")
    return rows


def main():
    parser = argparse.ArgumentParser(description="Probe Pike13 routes and record sanitized route capability diagnostics.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--profile-dir", default="browser_profiles/pike13-westu")
    parser.add_argument("--base-url", default=DEFAULT_URL)
    parser.add_argument("--interactive-login", action="store_true")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--limit", type=int, default=4)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--print", action="store_true", dest="print_output")
    args = parser.parse_args()

    rows = run_pike13_route_discovery(
        db_path=args.db,
        profile_dir=args.profile_dir,
        base_url=args.base_url,
        interactive_login=args.interactive_login,
        headless=args.headless,
        limit=args.limit,
        output=args.output,
    )
    markdown = render_route_report(rows)
    if args.print_output:
        print(markdown)
    else:
        print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
