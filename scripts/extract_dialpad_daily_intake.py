#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import (  # noqa: E402
    ensure_lead_followup_schema,
    finish_import_run,
    start_import_run,
)
from scripts.discover_dialpad_targets import (  # noqa: E402
    clear_conversation_history_filters,
    run_route_discovery,
    sanitize_error,
    try_apply_conversation_history_filters,
)
from scripts.extract_dialpad_voice import (  # noqa: E402
    extract_conversation_history_rows_from_dom,
    extract_links,
    rows_from_visible_text,
    summarize_view,
    upsert_voice_event,
    wait_for_authenticated_page,
    wait_until_ready,
)


DEFAULT_SCHOOL = "West U"
DEFAULT_OUTPUT_PROFILE = "browser_profiles/dialpad"


def conversation_history_window_url(window_days):
    days = max(int(window_days), 0)
    return f"https://dialpad.com/conversationhistory?days=0-{days}"


def enrich_daily_row(row, window_days, school, filter_diagnostics):
    raw = {}
    try:
        raw = json.loads(row.get("raw_json") or "{}")
    except json.JSONDecodeError:
        raw = {"raw_json_parse_error": True}
    raw.update(
        {
            "daily_intake": True,
            "window_days": window_days,
            "requested_school": school,
            "filter_diagnostics": filter_diagnostics or {},
            "source_timestamp_field": "event_at",
            "import_timestamp_field": "updated_at",
        }
    )
    row["raw_json"] = json.dumps(raw, sort_keys=True)
    return row


def run_fallback_route_discovery(db_path, profile_dir, school, interactive_login, headless, chrome_channel):
    try:
        return run_route_discovery(
            db_path=db_path,
            profile_dir=profile_dir,
            school=school,
            interactive_login=interactive_login,
            headless=headless,
            chrome_channel=chrome_channel,
        )
    except Exception as exc:
        return {"fallback_error": sanitize_error(exc)}


def run_daily_intake(
    db_path,
    profile_dir=DEFAULT_OUTPUT_PROFILE,
    school=DEFAULT_SCHOOL,
    window_days=2,
    limit=100,
    interactive_login=False,
    headless=False,
    login_timeout=300,
    route_discovery_on_failure=True,
    chrome_channel=False,
):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    url = conversation_history_window_url(window_days)
    run_id = start_import_run(
        conn,
        "dialpad_daily_intake",
        Path(__file__).name,
        metadata={
            "school": school,
            "window_days": window_days,
            "limit": limit,
            "url": url,
            "chrome_channel": chrome_channel,
        },
    )
    conn.commit()
    rows_seen = rows_inserted = rows_updated = 0
    view_summary = {}
    fallback_result = None
    status = "success"
    error = None
    try:
        with sync_playwright() as p:
            launch_kwargs = {
                "headless": headless and not interactive_login,
                "viewport": {"width": 1440, "height": 1000},
            }
            if chrome_channel:
                launch_kwargs["channel"] = "chrome"
            context = p.chromium.launch_persistent_context(str(profile_dir), **launch_kwargs)
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            wait_until_ready(page)
            wait_for_authenticated_page(page, url, interactive_login, login_timeout)
            clear_diagnostics = clear_conversation_history_filters(page)
            if clear_diagnostics.get("clear_clicked"):
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                wait_until_ready(page)
            filter_diagnostics = try_apply_conversation_history_filters(page, school)
            text = page.locator("body").inner_text(timeout=30000)
            links = extract_links(page)
            rows = extract_conversation_history_rows_from_dom(page, limit)
            if not rows:
                rows = rows_from_visible_text("conversation_history", page.url, text, limit, links=links)
            view_summary = summarize_view("conversation_history", page.url, rows, links)
            view_summary["filter_diagnostics"] = filter_diagnostics
            view_summary["clear_diagnostics"] = clear_diagnostics
            rows_seen = len(rows)
            for row in rows:
                row = enrich_daily_row(row, window_days, school, filter_diagnostics)
                exists = conn.execute(
                    "SELECT 1 FROM dialpad_voice_events WHERE event_id = ?",
                    (row["event_id"],),
                ).fetchone()
                upsert_voice_event(conn, row)
                if exists:
                    rows_updated += 1
                else:
                    rows_inserted += 1
            context.close()
        if rows_seen == 0:
            status = "partial"
            error = "Conversation History was reachable, but no rows were parsed."
            if route_discovery_on_failure:
                fallback_result = run_fallback_route_discovery(
                    db_path,
                    profile_dir,
                    school,
                    interactive_login,
                    headless,
                    chrome_channel,
                )
        metadata = {
            "school": school,
            "window_days": window_days,
            "limit": limit,
            "conversation_history_url": url,
            "view_summaries": {"conversation_history": view_summary},
            "fallback_result": fallback_result,
        }
        finish_import_run(
            conn,
            run_id,
            status,
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            error=error,
            metadata=metadata,
        )
        conn.commit()
        return {
            "run_id": run_id,
            "status": status,
            "rows_seen": rows_seen,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "fallback_result": fallback_result,
        }
    except Exception as exc:
        error = sanitize_error(exc)
        status = "blocked"
        if route_discovery_on_failure:
            fallback_result = run_fallback_route_discovery(
                db_path,
                profile_dir,
                school,
                interactive_login,
                headless,
                chrome_channel,
            )
        finish_import_run(
            conn,
            run_id,
            status,
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            error=error,
            metadata={
                "school": school,
                "window_days": window_days,
                "limit": limit,
                "conversation_history_url": url,
                "view_summaries": {"conversation_history": view_summary},
                "fallback_result": fallback_result,
            },
        )
        conn.commit()
        return {
            "run_id": run_id,
            "status": status,
            "rows_seen": rows_seen,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "error": error,
            "fallback_result": fallback_result,
        }
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Load recent Dialpad Conversation History rows as daily communications intake.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--profile-dir", default=DEFAULT_OUTPUT_PROFILE)
    parser.add_argument("--school", default=DEFAULT_SCHOOL)
    parser.add_argument("--window-days", type=int, default=2)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--interactive-login", action="store_true")
    parser.add_argument("--login-timeout", type=int, default=300)
    parser.add_argument("--no-route-discovery-on-failure", action="store_true")
    parser.add_argument("--chrome-channel", action="store_true", help="Use the local Chrome channel for the persistent profile.")
    args = parser.parse_args()

    result = run_daily_intake(
        db_path=args.db,
        profile_dir=args.profile_dir,
        school=args.school,
        window_days=args.window_days,
        limit=args.limit,
        interactive_login=args.interactive_login,
        headless=args.headless,
        login_timeout=args.login_timeout,
        route_discovery_on_failure=not args.no_route_discovery_on_failure,
        chrome_channel=args.chrome_channel,
    )
    print(
        "Dialpad daily intake: "
        f"status={result['status']} rows_seen={result['rows_seen']} "
        f"inserted={result['rows_inserted']} updated={result['rows_updated']}"
    )
    if result.get("error"):
        print(f"Blocked: {result['error']}")


if __name__ == "__main__":
    main()
