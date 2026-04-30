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
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

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
DEFAULT_ROUTE_MAP_OUTPUT = "outputs/progress/dialpad_route_map.md"
OUTCOMES = {
    "found_sms",
    "found_call",
    "found_voicemail",
    "found_call_review",
    "not_found_after_route_search",
    "filter_not_supported",
    "auth_blocked",
    "parse_error",
    # Legacy values remain readable for older diagnostic runs.
    "not_found",
    "ui_blocked",
}
ROUTES = (
    {
        "name": "global_search",
        "url": "https://dialpad.com/app",
        "daily_refresh": True,
        "targeted_search": True,
        "date_filter": False,
        "school_filter": False,
        "keyword_filter": True,
        "required_filter_state": "Authenticated Dialpad inbox shell; use the top Search Dialpad box, type only, never press Enter.",
    },
    {
        "name": "messages",
        "url": "https://dialpad.com/app/messages",
        "daily_refresh": True,
        "targeted_search": True,
        "date_filter": False,
        "school_filter": False,
        "keyword_filter": True,
        "required_filter_state": "Authenticated Dialpad Messages route; target phone/name search if search input is exposed.",
    },
    {
        "name": "conversation_history",
        "url": "https://dialpad.com/conversationhistory",
        "daily_refresh": True,
        "targeted_search": True,
        "date_filter": True,
        "school_filter": True,
        "keyword_filter": True,
        "required_filter_state": "Office/group set to West U, date set to proof window, Call participant filter applied as external_endpoint=<phone>.",
    },
    {
        "name": "calls",
        "url": HISTORY_URLS["calls"],
        "daily_refresh": True,
        "targeted_search": True,
        "date_filter": False,
        "school_filter": False,
        "keyword_filter": True,
        "required_filter_state": "Authenticated calls route; department/school visible where Dialpad exposes it.",
    },
    {
        "name": "missed",
        "url": HISTORY_URLS["missed"],
        "daily_refresh": True,
        "targeted_search": True,
        "date_filter": False,
        "school_filter": False,
        "keyword_filter": True,
        "required_filter_state": "Authenticated missed calls route; department/school visible where Dialpad exposes it.",
    },
    {
        "name": "voicemails",
        "url": HISTORY_URLS["voicemails"],
        "daily_refresh": True,
        "targeted_search": True,
        "date_filter": False,
        "school_filter": False,
        "keyword_filter": True,
        "required_filter_state": "Authenticated voicemails route; voicemail transcript visible if Dialpad exposes it in route.",
    },
    {
        "name": "recordings",
        "url": HISTORY_URLS["recordings"],
        "daily_refresh": True,
        "targeted_search": True,
        "date_filter": False,
        "school_filter": False,
        "keyword_filter": True,
        "required_filter_state": "Authenticated recordings route; recording/call-review links retained, audio not downloaded.",
    },
)
SEARCH_ROUTES = (
    ("conversation_history", "https://dialpad.com/conversationhistory"),
    ("global_search", "https://dialpad.com/app"),
    ("messages", "https://dialpad.com/app/messages"),
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


def conversation_history_participant_url(phone, days="0-30"):
    return f"https://dialpad.com/conversationhistory?{urlencode({'days': days, 'external_endpoint': normalize_phone(phone) or phone})}"


def sanitize_dialpad_url(url):
    if not url:
        return url
    parts = urlsplit(url)
    if "dialpad.com" not in parts.netloc:
        return url
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.lower() not in {"external_endpoint", "phone", "q", "keyword"}
    ]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def route_config(route_name):
    for route in ROUTES:
        if route["name"] == route_name:
            return route
    return {
        "name": route_name,
        "url": "",
        "daily_refresh": False,
        "targeted_search": False,
        "date_filter": False,
        "school_filter": False,
        "keyword_filter": False,
        "required_filter_state": "",
    }


def page_signals(text, links):
    text = text or ""
    lowered = text.lower()
    link_values = [f"{link.get('href', '')} {link.get('text', '')}".lower() for link in links or []]
    return {
        "sms_signal_visible": int(any(term in lowered for term in ("messages", "sms", "text message", "thread"))),
        "voice_signal_visible": int(any(term in lowered for term in ("call", "calls", "missed", "recording", "duration connected"))),
        "voicemail_signal_visible": int("voicemail" in lowered),
        "transcript_link_visible": int(any("transcript" in value or "callhistory/callreview" in value for value in link_values)),
        "recording_link_visible": int(any("recording" in value or "call audio" in lowered for value in link_values)),
        "download_link_visible": int(any("download" in value for value in link_values)),
        "call_review_url_count": call_review_url_count(links),
    }


def classify_target_search_result(text, links, target_phone):
    phone = normalize_phone(target_phone)
    text = text or ""
    links = links or []
    if phone and phone not in visible_phone_keys(text):
        return "not_found_after_route_search"
    lowered = text.lower()
    if call_review_url_count(links) or "callhistory/callreview" in lowered:
        return "found_call_review"
    if "voicemail" in lowered:
        return "found_voicemail"
    if any(term in lowered for term in ("message", "sms", "text message", "thread")):
        return "found_sms"
    if any(term in lowered for term in ("call", "missed", "voicemail", "recording")):
        return "found_call"
    return "not_found_after_route_search"


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
        "input[placeholder='Search Dialpad']",
        "input[aria-label='Search Dialpad']",
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
            page.keyboard.press("Meta+A")
            page.keyboard.press("Backspace")
            locator.fill(target, timeout=1500)
            return True, selector
        except Exception:
            continue
    return False, "no_search_input"


def try_apply_conversation_history_filters(page, school):
    diagnostics = {
        "school_filter_attempted": False,
        "school_filter_applied": False,
        "date_filter_visible": False,
        "keyword_filter_visible": False,
    }
    try:
        text = page.locator("body").inner_text(timeout=5000)
    except Exception:
        return diagnostics
    lowered = text.lower()
    diagnostics["date_filter_visible"] = any(token in lowered for token in ("past 7 days", "past 30 days", "date & time"))
    diagnostics["keyword_filter_visible"] = "keyword" in lowered
    if "conversation history" not in lowered:
        return diagnostics
    diagnostics["school_filter_attempted"] = True
    school_terms = ["West U", "West University", school or ""]
    for term in school_terms:
        if not term:
            continue
        try:
            if page.get_by_text(term, exact=False).count():
                diagnostics["school_filter_applied"] = True
                break
        except Exception:
            continue
    return diagnostics


def clear_conversation_history_filters(page):
    diagnostics = {"clear_attempted": False, "clear_clicked": False}
    try:
        text = page.locator("body").inner_text(timeout=5000)
    except Exception:
        return diagnostics
    if "conversation history" not in text.lower():
        return diagnostics
    diagnostics["clear_attempted"] = True
    for locator in (
        page.get_by_text("Clear", exact=True),
        page.locator("button").filter(has_text="Clear"),
        page.locator("a").filter(has_text="Clear"),
    ):
        try:
            if locator.count() == 0:
                continue
            locator.first.click(timeout=2000)
            page.wait_for_timeout(1000)
            diagnostics["clear_clicked"] = True
            break
        except Exception:
            continue
    return diagnostics


def text_snapshot(page):
    try:
        return page.locator("body").inner_text(timeout=15000)
    except Exception as exc:
        raise RuntimeError(f"Unable to read Dialpad page text: {sanitize_error(exc)}") from exc


def parse_found_rows(page, outcome, target_phone, limit):
    text = text_snapshot(page)
    links = extract_links(page)
    voice_rows = []
    if "conversationhistory" in page.url or outcome in {"found_call", "found_voicemail", "found_call_review"}:
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
                row["source_url"] = sanitize_dialpad_url(row.get("source_url"))
                try:
                    raw_json = json.loads(row.get("raw_json") or "{}")
                except json.JSONDecodeError:
                    raw_json = {}
                raw_json.update(
                    {
                        "target_filter": "call_participant",
                        "target_filter_parameter": "external_endpoint",
                        "source_url_sanitized": True,
                    }
                )
                row["raw_json"] = json.dumps(raw_json, sort_keys=True)
    return text, links, voice_rows


def outcome_from_voice_rows(voice_rows, fallback):
    if not voice_rows:
        return fallback
    if any("dialpad.com/callhistory/callreview/" in (row.get("source_url") or "") for row in voice_rows):
        return "found_call_review"
    if any((row.get("event_type") or "").lower() == "voicemail" for row in voice_rows):
        return "found_voicemail"
    return "found_call"


def route_status(text, links, typed=False, filter_diagnostics=None):
    filter_diagnostics = filter_diagnostics or {}
    if not text:
        return "blocked"
    signals = page_signals(text, links)
    if signals["sms_signal_visible"] or signals["voice_signal_visible"] or signals["call_review_url_count"]:
        if typed or filter_diagnostics.get("school_filter_applied") or filter_diagnostics.get("date_filter_visible"):
            return "usable"
        return "partial"
    return "blocked"


def visible_row_count(text):
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    return sum(1 for line in lines if re.search(r"\b(call|voicemail|recording|message|sms|duration)\b", line, re.IGNORECASE))


def route_probe_row(run_id, route, status, text, links, filter_diagnostics, error=None):
    now = utc_now_iso()
    signals = page_signals(text, links)
    route_id = f"{run_id}:{route['name']}"
    diagnostics = {
        "filter_diagnostics": filter_diagnostics,
        "error": sanitize_error(error) if error else None,
        "sanitized": True,
        "raw_customer_content_omitted": True,
    }
    return {
        "route_id": route_id,
        "run_id": run_id,
        "route_name": route["name"],
        "route_url": route["url"],
        "status": status,
        "loaded_at": now,
        "supports_daily_refresh": 1 if route.get("daily_refresh") else 0,
        "supports_targeted_search": 1 if route.get("targeted_search") else 0,
        "supports_date_filter": 1 if route.get("date_filter") or filter_diagnostics.get("date_filter_visible") else 0,
        "supports_school_filter": 1 if route.get("school_filter") or filter_diagnostics.get("school_filter_applied") else 0,
        "supports_keyword_filter": 1 if route.get("keyword_filter") or filter_diagnostics.get("keyword_filter_visible") else 0,
        "visible_row_count": visible_row_count(text),
        "visible_link_count": len(links or []),
        "call_review_url_count": signals["call_review_url_count"],
        "transcript_link_visible": signals["transcript_link_visible"],
        "recording_link_visible": signals["recording_link_visible"],
        "download_link_visible": signals["download_link_visible"],
        "sms_signal_visible": signals["sms_signal_visible"],
        "voice_signal_visible": signals["voice_signal_visible"],
        "voicemail_signal_visible": signals["voicemail_signal_visible"],
        "required_filter_state": route.get("required_filter_state"),
        "raw_json": json.dumps(diagnostics, sort_keys=True),
        "updated_at": now,
    }


def upsert_route_discovery(conn, row):
    conn.execute(
        """
        INSERT INTO dialpad_route_discoveries (
            route_id, run_id, route_name, route_url, status, loaded_at,
            supports_daily_refresh, supports_targeted_search, supports_date_filter,
            supports_school_filter, supports_keyword_filter, visible_row_count,
            visible_link_count, call_review_url_count, transcript_link_visible,
            recording_link_visible, download_link_visible, sms_signal_visible,
            voice_signal_visible, voicemail_signal_visible, required_filter_state,
            raw_json, updated_at
        )
        VALUES (
            :route_id, :run_id, :route_name, :route_url, :status, :loaded_at,
            :supports_daily_refresh, :supports_targeted_search, :supports_date_filter,
            :supports_school_filter, :supports_keyword_filter, :visible_row_count,
            :visible_link_count, :call_review_url_count, :transcript_link_visible,
            :recording_link_visible, :download_link_visible, :sms_signal_visible,
            :voice_signal_visible, :voicemail_signal_visible, :required_filter_state,
            :raw_json, :updated_at
        )
        ON CONFLICT(route_id) DO UPDATE SET
            route_url = excluded.route_url,
            status = excluded.status,
            loaded_at = excluded.loaded_at,
            supports_daily_refresh = excluded.supports_daily_refresh,
            supports_targeted_search = excluded.supports_targeted_search,
            supports_date_filter = excluded.supports_date_filter,
            supports_school_filter = excluded.supports_school_filter,
            supports_keyword_filter = excluded.supports_keyword_filter,
            visible_row_count = excluded.visible_row_count,
            visible_link_count = excluded.visible_link_count,
            call_review_url_count = excluded.call_review_url_count,
            transcript_link_visible = excluded.transcript_link_visible,
            recording_link_visible = excluded.recording_link_visible,
            download_link_visible = excluded.download_link_visible,
            sms_signal_visible = excluded.sms_signal_visible,
            voice_signal_visible = excluded.voice_signal_visible,
            voicemail_signal_visible = excluded.voicemail_signal_visible,
            required_filter_state = excluded.required_filter_state,
            raw_json = excluded.raw_json,
            updated_at = excluded.updated_at
        """,
        row,
    )


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
        route = route_config(path_name)
        try:
            if path_name == "conversation_history":
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                wait_until_ready(page)
                clear_diagnostics = clear_conversation_history_filters(page)
                url = conversation_history_participant_url(target["target_value"])
            else:
                clear_diagnostics = {}
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            wait_until_ready(page)
            filter_diagnostics = try_apply_conversation_history_filters(page, target.get("school")) if path_name == "conversation_history" else {}
            filter_diagnostics.update(clear_diagnostics)
            if path_name == "conversation_history":
                typed, selector = True, "external_endpoint_url"
            else:
                typed, selector = try_fill_search(page, target["target_value"])
            time.sleep(2)
            text = text_snapshot(page)
            links = extract_links(page)
            outcome = classify_target_search_result(text, links, target["target_value"])
            if path_name == "conversation_history":
                text, links, voice_rows = parse_found_rows(page, outcome, target["target_value"], per_target_limit)
                outcome = outcome_from_voice_rows(voice_rows, outcome)
            else:
                voice_rows = []
            if not typed and route.get("targeted_search"):
                outcome = "filter_not_supported"
            path_results.append(
                {
                    "path": path_name,
                    "url": sanitize_dialpad_url(url),
                    "typed": typed,
                    "selector": selector,
                    "outcome": outcome,
                    "visible_target_phone": outcome.startswith("found_"),
                    "call_review_url_count": call_review_url_count(links),
                    "filter_diagnostics": filter_diagnostics,
                }
            )
            if outcome.startswith("found_"):
                if not voice_rows:
                    text, links, voice_rows = parse_found_rows(page, outcome, target["target_value"], per_target_limit)
                best = {
                    "outcome": outcome,
                    "text": text,
                    "links": links,
                    "voice_rows": voice_rows,
                    "matched_url": sanitize_dialpad_url(page.url),
                }
                break
            if outcome == "filter_not_supported":
                best["outcome"] = "filter_not_supported"
        except Exception as exc:
            path_results.append(
                {
                    "path": path_name,
                    "url": url,
                    "outcome": "parse_error",
                    "error": sanitize_error(exc),
                }
            )
    conversation_history_result = next(
        (row for row in path_results if row.get("path") == "conversation_history" and row.get("selector") == "external_endpoint_url"),
        None,
    )
    if (
        best["outcome"] in {"not_found", "not_found_after_route_search", "filter_not_supported"}
        and conversation_history_result
        and conversation_history_result.get("outcome") == "not_found_after_route_search"
    ):
        best["outcome"] = "not_found_after_route_search"
    elif best["outcome"] in {"not_found", "not_found_after_route_search"} and any(row.get("outcome") == "filter_not_supported" for row in path_results):
        best["outcome"] = "filter_not_supported"
    elif best["outcome"] == "not_found":
        best["outcome"] = "not_found_after_route_search"
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
        "found_voice_count": len(voice_rows) if outcome in {"found_call", "found_voicemail", "found_call_review"} else 0,
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


def launch_browser_context(playwright, profile_dir, headless=False, chrome_channel=False):
    kwargs = {"headless": headless}
    if chrome_channel:
        kwargs["channel"] = "chrome"
    try:
        return playwright.chromium.launch_persistent_context(str(profile_dir), **kwargs)
    except Exception:
        if not chrome_channel:
            raise
        fallback_kwargs = {"headless": headless}
        return playwright.chromium.launch_persistent_context(str(profile_dir), **fallback_kwargs)


def run_route_discovery(db_path, profile_dir, school, interactive_login, headless, chrome_channel):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_lead_followup_schema(conn)
    run_id = start_import_run(
        conn,
        "dialpad_route_discovery",
        "discover_dialpad_targets.py",
        metadata={"school": school, "route_count": len(ROUTES), "chrome_channel": chrome_channel},
    )
    conn.commit()
    rows_seen = rows_inserted = rows_updated = 0
    try:
        with sync_playwright() as p:
            context = launch_browser_context(p, profile_dir, headless=headless, chrome_channel=chrome_channel)
            page = context.pages[0] if context.pages else context.new_page()
            page.goto("https://dialpad.com/app/history/all", wait_until="domcontentloaded", timeout=30000)
            wait_for_authenticated_page(
                page,
                "https://dialpad.com/app/history/all",
                interactive_login=interactive_login,
                timeout_seconds=300,
            )
            for route in ROUTES:
                rows_seen += 1
                exists = conn.execute(
                    """
                    SELECT 1 FROM dialpad_route_discoveries
                    WHERE route_id = ?
                    """,
                    (f"{run_id}:{route['name']}",),
                ).fetchone()
                try:
                    page.goto(route["url"], wait_until="domcontentloaded", timeout=30000)
                    wait_until_ready(page)
                    filter_diagnostics = (
                        try_apply_conversation_history_filters(page, school)
                        if route["name"] == "conversation_history"
                        else {}
                    )
                    text = text_snapshot(page)
                    links = extract_links(page)
                    status = route_status(text, links, filter_diagnostics=filter_diagnostics)
                    row = route_probe_row(run_id, route, status, text, links, filter_diagnostics)
                except Exception as exc:
                    row = route_probe_row(run_id, route, "blocked", "", [], {}, error=exc)
                upsert_route_discovery(conn, row)
                conn.commit()
                if exists:
                    rows_updated += 1
                else:
                    rows_inserted += 1
            context.close()
        statuses = route_discovery_summary(conn, run_id)["statuses"]
        finish_import_run(
            conn,
            run_id,
            "success",
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            metadata={"statuses": statuses, "chrome_channel": chrome_channel},
        )
        conn.commit()
        return run_id
    except Exception as exc:
        finish_import_run(
            conn,
            run_id,
            "error",
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            error=sanitize_error(exc),
            metadata={"chrome_channel": chrome_channel},
        )
        conn.commit()
        raise
    finally:
        conn.close()


def run_discovery(db_path, profile_dir, school, window_days, limit, per_target_limit, interactive_login, headless, chrome_channel=False):
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
            context = launch_browser_context(p, profile_dir, headless=headless, chrome_channel=chrome_channel)
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
    not_found_count = outcomes["not_found"] + outcomes["not_found_after_route_search"]
    blocked_count = outcomes["ui_blocked"] + outcomes["filter_not_supported"] + outcomes["auth_blocked"]
    return {
        "run_id": run_id,
        "targets_searched": len(rows),
        "targets_found": sum(outcomes[outcome] for outcome in ("found_sms", "found_call", "found_voicemail", "found_call_review")),
        "targets_with_sms": outcomes["found_sms"],
        "targets_with_calls_or_call_reviews": outcomes["found_call"] + outcomes["found_voicemail"] + outcomes["found_call_review"],
        "targets_not_found": not_found_count,
        "targets_blocked_or_unsupported": blocked_count,
        "outcomes": dict(sorted(outcomes.items())),
        "found_call_review_rows": sum(row.get("found_call_review_count") or 0 for row in rows),
        "found_voice_rows": sum(row.get("found_voice_count") or 0 for row in rows),
        "source_url_rows": sum(1 for row in rows if (row.get("source_url_count") or 0) > 0),
    }


def route_discovery_summary(conn, run_id=None):
    where = ""
    params = {}
    if run_id is None:
        row = conn.execute(
            """
            SELECT id
            FROM source_import_runs
            WHERE source = 'dialpad_route_discovery'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        run_id = row["id"] if row else None
    if run_id is not None:
        where = "WHERE run_id = :run_id"
        params["run_id"] = run_id
    rows = [dict(row) for row in conn.execute(f"SELECT * FROM dialpad_route_discoveries {where}", params).fetchall()]
    statuses = Counter(row["status"] for row in rows)
    return {
        "run_id": run_id,
        "routes_checked": len(rows),
        "statuses": dict(sorted(statuses.items())),
        "usable_routes": statuses["usable"],
        "partial_routes": statuses["partial"],
        "blocked_routes": statuses["blocked"],
        "sms_routes": sum(1 for row in rows if row.get("sms_signal_visible")),
        "voice_routes": sum(1 for row in rows if row.get("voice_signal_visible")),
        "voicemail_routes": sum(1 for row in rows if row.get("voicemail_signal_visible")),
        "call_review_routes": sum(1 for row in rows if (row.get("call_review_url_count") or 0) > 0),
        "date_filter_routes": sum(1 for row in rows if row.get("supports_date_filter")),
        "school_filter_routes": sum(1 for row in rows if row.get("supports_school_filter")),
        "keyword_filter_routes": sum(1 for row in rows if row.get("supports_keyword_filter")),
        "rows": rows,
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
        f"- Targets blocked/filter unsupported: {summary.get('targets_blocked_or_unsupported', 0)}",
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


def render_route_map_report(summary, school=DEFAULT_SCHOOL):
    rows = summary.get("rows") or []
    lines = [
        "# Dialpad Route Map",
        "",
        f"School: **{school}**",
        f"Run ID: **{summary.get('run_id') or 'none'}**",
        "",
        "## Summary",
        "",
        f"- Routes checked: {summary.get('routes_checked', 0)}",
        f"- Usable routes: {summary.get('usable_routes', 0)}",
        f"- Partial routes: {summary.get('partial_routes', 0)}",
        f"- Blocked routes: {summary.get('blocked_routes', 0)}",
        f"- Routes with SMS signal: {summary.get('sms_routes', 0)}",
        f"- Routes with voice signal: {summary.get('voice_routes', 0)}",
        f"- Routes with voicemail signal: {summary.get('voicemail_routes', 0)}",
        f"- Routes with call-review links: {summary.get('call_review_routes', 0)}",
        f"- Routes with date filters: {summary.get('date_filter_routes', 0)}",
        f"- Routes with school filters: {summary.get('school_filter_routes', 0)}",
        f"- Routes with keyword filters: {summary.get('keyword_filter_routes', 0)}",
        "",
        "## Route Details",
        "",
        "| Route | Status | Daily refresh | Target search | Date filter | School filter | Keyword filter | SMS | Voice | Voicemail | Call-review URLs | Required filter state |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    if rows:
        for row in rows:
            lines.append(
                "| {route_name} | {status} | {supports_daily_refresh} | {supports_targeted_search} | "
                "{supports_date_filter} | {supports_school_filter} | {supports_keyword_filter} | "
                "{sms_signal_visible} | {voice_signal_visible} | {voicemail_signal_visible} | "
                "{call_review_url_count} | {required_filter_state} |".format(
                    **{
                        key: str(value or "").replace("|", "/").replace("\n", " ").strip()
                        for key, value in row.items()
                    }
                )
            )
    else:
        lines.append("| none | none | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | none |")
    lines.extend(
        [
            "",
            "_This route map is sanitized. It records route capabilities and counts only, not customer names, phone numbers, message bodies, transcripts, recaps, or note text._",
            "",
        ]
    )
    return "\n".join(lines)


def build_report(db_path, output, school, window_days, run_id=None, route_output=None, route_run_id=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_lead_followup_schema(conn)
        summary = target_search_summary(conn, run_id)
        route_summary = route_discovery_summary(conn, route_run_id)
    finally:
        conn.close()
    markdown = render_target_coverage_report(summary, school=school, window_days=window_days)
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    if route_output:
        route_markdown = render_route_map_report(route_summary, school=school)
        route_output_path = Path(route_output)
        route_output_path.parent.mkdir(parents=True, exist_ok=True)
        route_output_path.write_text(route_markdown, encoding="utf-8")
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
    parser.add_argument("--chrome-channel", action="store_true", help="Launch the installed Chrome channel with the selected persistent profile directory.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--route-map-output", default=DEFAULT_ROUTE_MAP_OUTPUT)
    parser.add_argument("--route-discovery", action="store_true", help="Probe Dialpad routes and write sanitized route-map diagnostics before target search.")
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument("--print", action="store_true", dest="print_output")
    args = parser.parse_args()

    run_id = None
    route_run_id = None
    if not args.report_only:
        if args.route_discovery:
            route_run_id = run_route_discovery(
                args.db,
                args.profile_dir,
                args.school,
                args.interactive_login,
                args.headless,
                args.chrome_channel,
            )
        run_id = run_discovery(
            args.db,
            args.profile_dir,
            args.school,
            args.window_days,
            args.limit,
            args.per_target_limit,
            args.interactive_login,
            args.headless,
            args.chrome_channel,
        )
    markdown = build_report(
        args.db,
        args.output,
        args.school,
        args.window_days,
        run_id,
        route_output=args.route_map_output,
        route_run_id=route_run_id,
    )
    if args.print_output:
        print(markdown)
    else:
        print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
