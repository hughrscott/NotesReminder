#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import (  # noqa: E402
    ensure_lead_followup_schema,
    finish_import_run,
    normalize_phone,
    start_import_run,
    utc_now_iso,
)
from scripts.extract_dialpad_voice import (  # noqa: E402
    HISTORY_URLS,
    PHONE_RE,
    extract_conversation_history_rows_from_dom,
    extract_links,
    is_dialpad_app_page,
    rows_from_visible_text,
    upsert_voice_event,
    wait_for_authenticated_page,
    wait_until_ready,
)
from scripts.lead_attention_report import (  # noqa: E402
    DEFAULT_SCHOOL,
    deal_phone_keys,
    fetch_candidate_leads,
)


DEFAULT_OUTPUT = "outputs/progress/dialpad_target_coverage.md"
OUTCOMES = {
    "found_sms",
    "found_call",
    "found_call_review",
    "not_found",
    "auth_blocked",
    "ui_blocked",
    "parse_error",
}
SEARCH_ROUTES = (
    ("global_search", "https://dialpad.com/app/history/all"),
    ("messages", "https://dialpad.com/app/messages"),
    ("conversation_history", "https://dialpad.com/conversationhistory"),
    ("calls", HISTORY_URLS["calls"]),
    ("missed", HISTORY_URLS["missed"]),
    ("voicemails", HISTORY_URLS["voicemails"]),
)
SENSITIVE_HINT_RE = re.compile(
    r"(\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b|[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})",
    re.IGNORECASE,
)


def target_hash(value):
    normalized = normalize_phone(value) or str(value or "").strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def visible_phone_keys(text):
    return {normalize_phone(match.group(0)) for match in PHONE_RE.finditer(text or "") if normalize_phone(match.group(0))}


def call_review_url_count(links):
    return sum(1 for link in links or [] if "callhistory/callreview" in (link.get("href") or ""))


def classify_target_search_result(text, links, target_phone):
    phone = normalize_phone(target_phone)
    text = text or ""
    links = links or []
    if phone and phone not in visible_phone_keys(text):
        return "not_found"
    lowered = text.lower()
    if call_review_url_count(links) or "callhistory/callreview" in lowered:
        return "found_call_review"
    if any(term in lowered for term in ("message", "sms", "text message", "thread")):
        return "found_sms"
    if any(term in lowered for term in ("call", "missed", "voicemail", "recording")):
        return "found_call"
    return "not_found"


def sanitize_error(value):
    value = str(value or "").strip()
    value = SENSITIVE_HINT_RE.sub("[redacted]", value)
    return value[:240]


def select_target_candidates(conn, school=DEFAULT_SCHOOL, window_days=7, limit=25):
    ensure_lead_followup_schema(conn)
    conn.row_factory = sqlite3.Row
    candidates, window_start = fetch_candidate_leads(conn, school, window_days, limit)
    targets = []
    seen = set()
    for candidate in candidates:
        phones = deal_phone_keys(conn, candidate["deal_id"])
        for phone in phones:
            normalized = normalize_phone(phone)
            if not normalized:
                continue
            key = (candidate["deal_id"], normalized)
            if key in seen:
                continue
            seen.add(key)
            targets.append(
                {
                    "deal_id": candidate["deal_id"],
                    "school": candidate["school"] or school,
                    "target_type": "phone",
                    "target_value": normalized,
                    "target_hash": target_hash(normalized),
                    "window_start": window_start,
                }
            )
    return targets


def try_fill_search(page, target):
    selectors = [
        "input[placeholder*='Search' i]",
        "input[aria-label*='Search' i]",
        "[contenteditable='true'][aria-label*='Search' i]",
        "[contenteditable='true']",
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if locator.count() == 0:
                continue
            locator.click(timeout=1500)
            locator.fill(target, timeout=1500)
            page.keyboard.press("Enter")
            return True, selector
        except Exception:
            continue
    try:
        page.keyboard.press("Meta+K")
        time.sleep(0.5)
        active = page.locator(":focus")
        active.fill(target, timeout=1500)
        page.keyboard.press("Enter")
        return True, "Meta+K"
    except Exception:
        return False, "no_search_input"


def text_snapshot(page):
    try:
        return page.locator("body").inner_text(timeout=15000)
    except Exception as exc:
        raise RuntimeError(f"Unable to read Dialpad page text: {sanitize_error(exc)}") from exc


def parse_found_rows(page, outcome, target_phone, limit):
    text = text_snapshot(page)
    links = extract_links(page)
    voice_rows = []
    if outcome in {"found_call", "found_call_review"}:
        if "conversationhistory" in page.url:
            try:
                voice_rows = extract_conversation_history_rows_from_dom(page, limit)
            except Exception:
                voice_rows = []
        if not voice_rows:
            source_view = "conversation_history" if "conversationhistory" in page.url else "target_search"
            voice_rows = rows_from_visible_text(source_view, page.url, text, limit, links=links)
        target_key = normalize_phone(target_phone)
        if target_key:
            for row in voice_rows:
                if not row.get("phone_normalized"):
                    row["phone"] = target_phone
                    row["phone_normalized"] = target_key
    return text, links, voice_rows


def upsert_target_search(conn, row):
    conn.execute(
        """
        INSERT INTO dialpad_target_searches (
            search_id, run_id, deal_id, contact_id, target_hash, target_type,
            school, searched_at, search_paths_json, outcome, found_sms_count,
            found_voice_count, found_call_review_count, source_url_count,
            first_event_at, latest_event_at, raw_json, updated_at
        )
        VALUES (
            :search_id, :run_id, :deal_id, :contact_id, :target_hash, :target_type,
            :school, :searched_at, :search_paths_json, :outcome, :found_sms_count,
            :found_voice_count, :found_call_review_count, :source_url_count,
            :first_event_at, :latest_event_at, :raw_json, :updated_at
        )
        ON CONFLICT(search_id) DO UPDATE SET
            run_id = excluded.run_id,
            school = excluded.school,
            searched_at = excluded.searched_at,
            search_paths_json = excluded.search_paths_json,
            outcome = excluded.outcome,
            found_sms_count = excluded.found_sms_count,
            found_voice_count = excluded.found_voice_count,
            found_call_review_count = excluded.found_call_review_count,
            source_url_count = excluded.source_url_count,
            first_event_at = excluded.first_event_at,
            latest_event_at = excluded.latest_event_at,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


def search_target(page, target, per_target_limit):
    path_results = []
    best = {
        "outcome": "not_found",
        "text": "",
        "links": [],
        "voice_rows": [],
        "matched_url": None,
    }
    for path_name, url in SEARCH_ROUTES:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            wait_until_ready(page)
            typed, selector = try_fill_search(page, target["target_value"])
            time.sleep(2)
            text = text_snapshot(page)
            links = extract_links(page)
            outcome = classify_target_search_result(text, links, target["target_value"])
            path_results.append(
                {
                    "path": path_name,
                    "url": url,
                    "typed": typed,
                    "selector": selector,
                    "outcome": outcome,
                    "visible_target_phone": outcome != "not_found",
                    "call_review_url_count": call_review_url_count(links),
                }
            )
            if outcome != "not_found":
                text, links, voice_rows = parse_found_rows(page, outcome, target["target_value"], per_target_limit)
                best = {
                    "outcome": outcome,
                    "text": text,
                    "links": links,
                    "voice_rows": voice_rows,
                    "matched_url": page.url,
                }
                break
            if not typed and path_name in {"global_search", "conversation_history"}:
                best["outcome"] = "ui_blocked"
        except Exception as exc:
            path_results.append(
                {
                    "path": path_name,
                    "url": url,
                    "outcome": "parse_error",
                    "error": sanitize_error(exc),
                }
            )
    if best["outcome"] == "not_found" and any(row.get("outcome") == "ui_blocked" for row in path_results):
        best["outcome"] = "ui_blocked"
    return best, path_results


def diagnostics_row(run_id, target, result, path_results, rows_inserted, rows_updated):
    now = utc_now_iso()
    voice_rows = result.get("voice_rows") or []
    event_times = sorted(row.get("event_at") for row in voice_rows if row.get("event_at"))
    links = result.get("links") or []
    outcome = result.get("outcome") if result.get("outcome") in OUTCOMES else "parse_error"
    return {
        "search_id": f"{run_id}:{target['deal_id']}:{target['target_hash']}",
        "run_id": run_id,
        "deal_id": target["deal_id"],
        "contact_id": None,
        "target_hash": target["target_hash"],
        "target_type": target["target_type"],
        "school": target.get("school"),
        "searched_at": now,
        "search_paths_json": json.dumps(path_results, sort_keys=True),
        "outcome": outcome,
        "found_sms_count": 1 if outcome == "found_sms" else 0,
        "found_voice_count": len(voice_rows) if outcome in {"found_call", "found_call_review"} else 0,
        "found_call_review_count": call_review_url_count(links),
        "source_url_count": len({link.get("href") for link in links if link.get("href")}),
        "first_event_at": event_times[0] if event_times else None,
        "latest_event_at": event_times[-1] if event_times else None,
        "raw_json": json.dumps(
            {
                "matched_url": result.get("matched_url"),
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "diagnostic_only": True,
                "sensitive_values_redacted_from_reports": True,
            },
            sort_keys=True,
        ),
        "updated_at": now,
    }


def run_discovery(db_path, profile_dir, school, window_days, limit, per_target_limit, interactive_login, headless):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    targets = select_target_candidates(conn, school=school, window_days=window_days, limit=limit)
    run_id = start_import_run(
        conn,
        "dialpad_target_search",
        "discover_dialpad_targets.py",
        metadata={"school": school, "window_days": window_days, "target_count": len(targets)},
    )
    conn.commit()
    rows_inserted = 0
    rows_updated = 0
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(str(profile_dir), headless=headless)
            page = context.pages[0] if context.pages else context.new_page()
            page.goto("https://dialpad.com/app/history/all", wait_until="domcontentloaded", timeout=30000)
            wait_for_authenticated_page(
                page,
                "https://dialpad.com/app/history/all",
                interactive_login=interactive_login,
                timeout_seconds=300,
            )
            if not is_dialpad_app_page(page.url, text_snapshot(page)):
                for target in targets:
                    result = {"outcome": "auth_blocked", "links": [], "voice_rows": [], "matched_url": page.url}
                    row = diagnostics_row(run_id, target, result, [], 0, 0)
                    upsert_target_search(conn, row)
                finish_import_run(
                    conn,
                    run_id,
                    "blocked",
                    rows_seen=len(targets),
                    error="Dialpad profile is not authenticated.",
                    metadata={"outcomes": {"auth_blocked": len(targets)}},
                )
                conn.commit()
                context.close()
                return run_id
            for target in targets:
                before = conn.total_changes
                result, path_results = search_target(page, target, per_target_limit)
                for voice_row in result.get("voice_rows") or []:
                    upsert_voice_event(conn, voice_row)
                changed = conn.total_changes - before
                if changed:
                    rows_updated += changed
                row = diagnostics_row(run_id, target, result, path_results, changed, 0)
                upsert_target_search(conn, row)
                conn.commit()
            context.close()
        outcomes = target_search_summary(conn, run_id)["outcomes"]
        finish_import_run(
            conn,
            run_id,
            "success",
            rows_seen=len(targets),
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            metadata={"outcomes": outcomes},
        )
        conn.commit()
        return run_id
    except Exception as exc:
        finish_import_run(
            conn,
            run_id,
            "error",
            rows_seen=len(targets),
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            error=sanitize_error(exc),
        )
        conn.commit()
        raise
    finally:
        conn.close()


def target_search_summary(conn, run_id=None):
    where = ""
    params = {}
    if run_id is None:
        row = conn.execute(
            """
            SELECT id
            FROM source_import_runs
            WHERE source = 'dialpad_target_search'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        run_id = row["id"] if row else None
    if run_id is not None:
        where = "WHERE run_id = :run_id"
        params["run_id"] = run_id
    rows = [dict(row) for row in conn.execute(f"SELECT * FROM dialpad_target_searches {where}", params).fetchall()]
    outcomes = Counter(row["outcome"] for row in rows)
    return {
        "run_id": run_id,
        "targets_searched": len(rows),
        "targets_found": sum(outcomes[outcome] for outcome in ("found_sms", "found_call", "found_call_review")),
        "targets_with_sms": outcomes["found_sms"],
        "targets_with_calls_or_call_reviews": outcomes["found_call"] + outcomes["found_call_review"],
        "targets_not_found": outcomes["not_found"],
        "outcomes": dict(sorted(outcomes.items())),
        "found_call_review_rows": sum(row.get("found_call_review_count") or 0 for row in rows),
        "found_voice_rows": sum(row.get("found_voice_count") or 0 for row in rows),
        "source_url_rows": sum(1 for row in rows if (row.get("source_url_count") or 0) > 0),
    }


def render_target_coverage_report(summary, school=DEFAULT_SCHOOL, window_days=7):
    outcomes = summary.get("outcomes") or {}
    if summary.get("targets_found", 0):
        interpretation = "Targeted discovery found Dialpad evidence for at least one current lead-attention candidate; the lead-attention report can now confirm whether it flows through matching."
    elif summary.get("targets_searched", 0):
        interpretation = "First-value readiness remains blocked until at least one current lead-attention candidate has matched Dialpad evidence, or all targets have clear not-found/blocked outcomes."
    else:
        interpretation = "No targeted Dialpad searches have been recorded yet."
    lines = [
        "# Dialpad Target Coverage Report",
        "",
        f"School: **{school}**",
        f"Window: **last {window_days} days**",
        f"Run ID: **{summary.get('run_id') or 'none'}**",
        "",
        "## Summary",
        "",
        f"- Candidate targets searched: {summary.get('targets_searched', 0)}",
        f"- Targets found: {summary.get('targets_found', 0)}",
        f"- Targets with SMS evidence: {summary.get('targets_with_sms', 0)}",
        f"- Targets with call/call-review evidence: {summary.get('targets_with_calls_or_call_reviews', 0)}",
        f"- Targets not found: {summary.get('targets_not_found', 0)}",
        f"- Call-review links found during target search: {summary.get('found_call_review_rows', 0)}",
        f"- Voice rows parsed during target search: {summary.get('found_voice_rows', 0)}",
        "",
        "## Outcomes",
        "",
    ]
    if outcomes:
        for outcome, rows in sorted(outcomes.items()):
            lines.append(f"- {outcome}: {rows}")
    else:
        lines.append("- No target searches recorded.")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            f"- {interpretation}",
            "- This report intentionally excludes customer names, phone numbers, emails, SMS bodies, transcripts, recaps, and note text.",
            "",
        ]
    )
    return "\n".join(lines)


def build_report(db_path, output, school, window_days, run_id=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_lead_followup_schema(conn)
        summary = target_search_summary(conn, run_id)
    finally:
        conn.close()
    markdown = render_target_coverage_report(summary, school=school, window_days=window_days)
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    return markdown


def main():
    parser = argparse.ArgumentParser(description="Run sanitized targeted Dialpad discovery for current lead-attention phone keys.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--school", default=DEFAULT_SCHOOL)
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument("--profile-dir", default="browser_profiles/dialpad")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--per-target-limit", type=int, default=5)
    parser.add_argument("--interactive-login", action="store_true")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument("--print", action="store_true", dest="print_output")
    args = parser.parse_args()

    run_id = None
    if not args.report_only:
        run_id = run_discovery(
            args.db,
            args.profile_dir,
            args.school,
            args.window_days,
            args.limit,
            args.per_target_limit,
            args.interactive_login,
            args.headless,
        )
    markdown = build_report(args.db, args.output, args.school, args.window_days, run_id)
    if args.print_output:
        print(markdown)
    else:
        print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
