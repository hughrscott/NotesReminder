#!/usr/bin/env python3
import argparse
import json
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lead_followup_schema import ensure_lead_followup_schema, normalize_phone, utc_now_iso  # noqa: E402


def canonical_school(value):
    value = (value or "").strip()
    if value in {"West U", "West University Place"}:
        return "West U"
    if value == "The Heights":
        return "The Heights"
    return value


def name_key(value):
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())


def name_match_keys(value):
    words = re.findall(r"[a-z0-9]+", (value or "").lower())
    if not words:
        return set()
    keys = {name_key(" ".join(words))}
    if len(words) >= 2:
        keys.add(name_key(" ".join([words[-1], *words[:-1]])))
    return {key for key in keys if key}


def table_exists(conn, table):
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (table,),
        ).fetchone()
        is not None
    )


def unique_identity_name_phone_map(conn):
    grouped = defaultdict(set)
    sources = defaultdict(set)
    for table, method in (
        ("hubspot_contacts", "hubspot_unique_same_school_name"),
        ("pike13_people", "pike13_unique_same_school_name"),
    ):
        if not table_exists(conn, table):
            continue
        for row in conn.execute(
            f"""
            SELECT school, full_name, phone_normalized, phone
            FROM {table}
            WHERE COALESCE(full_name, '') != ''
              AND COALESCE(COALESCE(phone_normalized, phone), '') != ''
            """
        ):
            school = canonical_school(row["school"])
            phone = normalize_phone(row["phone_normalized"] or row["phone"])
            if school and phone:
                for key in name_match_keys(row["full_name"]):
                    grouped[(school, key)].add(phone)
                    sources[(school, key, phone)].add(method)
    unique = {}
    for key, phones in grouped.items():
        if len(phones) != 1:
            continue
        phone = next(iter(phones))
        methods = sources[(key[0], key[1], phone)]
        method = "hubspot_pike13_unique_same_school_name" if len(methods) > 1 else next(iter(methods))
        unique[key] = {"phone": phone, "method": method}
    return unique


def unique_hubspot_name_phone_map(conn):
    return {
        key: value["phone"]
        for key, value in unique_identity_name_phone_map(conn).items()
        if value["method"] in {"hubspot_unique_same_school_name", "hubspot_pike13_unique_same_school_name"}
    }


def _phone_from_contact_label(value):
    phone = normalize_phone(value)
    return phone if phone and len(phone) == 10 else None


def _raw_json_with_backfill(raw_json, *, method, now):
    try:
        data = json.loads(raw_json or "{}")
    except json.JSONDecodeError:
        data = {}
    data.update(
        {
            "phone_backfill_method": method,
            "phone_backfilled_at": now,
            "raw_customer_content_redacted": True,
        }
    )
    return json.dumps(data, sort_keys=True)


def _matched_phone_for_name(phone_by_name, school, contact_name):
    direct_phone = _phone_from_contact_label(contact_name)
    if direct_phone:
        return direct_phone, "contact_label_phone"
    for key in name_match_keys(contact_name):
        match = phone_by_name.get((canonical_school(school), key))
        if isinstance(match, dict):
            return match["phone"], match["method"]
        if match:
            return match, "hubspot_unique_same_school_name"
    return None, None


def backfill_sms_thread_phones(conn, execute=False):
    ensure_lead_followup_schema(conn)
    conn.row_factory = sqlite3.Row
    phone_by_name = unique_identity_name_phone_map(conn)
    candidates = []
    for row in conn.execute(
        """
        SELECT thread_id, school, contact_name, raw_json
        FROM dialpad_sms_threads
        WHERE COALESCE(phone_normalized, '') = ''
          AND COALESCE(contact_name, '') != ''
        """
    ):
        phone, method = _matched_phone_for_name(phone_by_name, row["school"], row["contact_name"])
        if phone:
            candidates.append((row, phone, method))
    if not execute:
        return {"matched_threads": len(candidates), "updated_threads": 0}

    now = utc_now_iso()
    updated = 0
    for row, phone, method in candidates:
        before = conn.total_changes
        conn.execute(
            """
            UPDATE dialpad_sms_threads
            SET phone = COALESCE(phone, ?),
                phone_normalized = ?,
                raw_json = ?,
                updated_at = ?
            WHERE thread_id = ?
              AND COALESCE(phone_normalized, '') = ''
            """,
            (phone, phone, _raw_json_with_backfill(row["raw_json"], method=method, now=now), now, row["thread_id"]),
        )
        updated += conn.total_changes - before
    conn.commit()
    return {"matched_threads": len(candidates), "updated_threads": updated}


def backfill_voice_event_phones(conn, execute=False):
    ensure_lead_followup_schema(conn)
    conn.row_factory = sqlite3.Row
    phone_by_name = unique_identity_name_phone_map(conn)
    candidates = []
    for row in conn.execute(
        """
        SELECT event_id, school, contact_name, raw_json
        FROM dialpad_voice_events
        WHERE COALESCE(phone_normalized, '') = ''
          AND COALESCE(contact_name, '') != ''
        """
    ):
        phone, method = _matched_phone_for_name(phone_by_name, row["school"], row["contact_name"])
        if phone:
            candidates.append((row, phone, method))
    if not execute:
        return {"matched_events": len(candidates), "updated_events": 0}

    now = utc_now_iso()
    updated = 0
    for row, phone, method in candidates:
        before = conn.total_changes
        conn.execute(
            """
            UPDATE dialpad_voice_events
            SET phone = COALESCE(phone, ?),
                phone_normalized = ?,
                raw_json = ?,
                updated_at = ?
            WHERE event_id = ?
              AND COALESCE(phone_normalized, '') = ''
            """,
            (phone, phone, _raw_json_with_backfill(row["raw_json"], method=method, now=now), now, row["event_id"]),
        )
        updated += conn.total_changes - before
    conn.commit()
    return {"matched_events": len(candidates), "updated_events": updated}


def backfill_dialpad_contact_phones(conn, execute=False):
    sms = backfill_sms_thread_phones(conn, execute=execute)
    voice = backfill_voice_event_phones(conn, execute=execute)
    return {"sms": sms, "voice": voice}


def main():
    parser = argparse.ArgumentParser(
        description="Backfill Dialpad SMS thread phone identity from unique same-school HubSpot exact-name matches."
    )
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--execute", action="store_true", help="Apply updates. Without this flag, only reports matches.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        result = backfill_dialpad_contact_phones(conn, execute=args.execute)
    finally:
        conn.close()
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
