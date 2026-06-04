from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone

from notesreminder.reports.dashboard_sql import register_dashboard_sql_functions


def target_hash(value):
    normalized = normalize_phone(value) or str(value or "").strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def normalize_phone(value):
    if not value:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return None
    return digits[-10:] if len(digits) >= 10 else digits


def expected_conversation_history_scope(school):
    value = (school or "").strip().lower()
    if "height" in value:
        return "The Heights"
    if "west" in value:
        return "West U"
    return None


def school_scope_matches(active_scope, expected_scope):
    active = (active_scope or "").strip().lower()
    expected = (expected_scope or "").strip().lower()
    return bool(active and expected and active == expected)


def conversation_history_days_for_target(target, today=None, minimum_days=30, maximum_days=365):
    today = today or datetime.now(timezone.utc).date()
    candidates = []
    for key in ("lead_date", "create_date", "window_start"):
        value = target.get(key)
        if not value:
            continue
        try:
            candidates.append(date.fromisoformat(str(value)[:10]))
        except ValueError:
            continue
    if not candidates:
        return f"0-{minimum_days}"
    oldest_required_date = min(candidates)
    lookback_days = max(minimum_days, (today - oldest_required_date).days + 1)
    lookback_days = min(maximum_days, max(0, lookback_days))
    return f"0-{lookback_days}"


def target_window_start_date(target):
    for key in ("lead_date", "create_date", "window_start"):
        value = target.get(key)
        if not value:
            continue
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            continue
    return None


def filter_voice_rows_to_target_window(voice_rows, target):
    start = target_window_start_date(target)
    if not start:
        return voice_rows
    filtered = []
    for row in voice_rows or []:
        event_at = row.get("event_at")
        if not event_at:
            continue
        try:
            if date.fromisoformat(str(event_at)[:10]) >= start:
                filtered.append(row)
        except ValueError:
            continue
    return filtered


def voice_rows_relative_to_lead_date(voice_rows, target):
    start = target_window_start_date(target)
    rows = voice_rows or []
    if not start:
        return {"pre_lead": [], "post_lead": rows, "undated": []}
    pre_lead = []
    post_lead = []
    undated = []
    for row in rows:
        event_at = row.get("event_at")
        if not event_at:
            undated.append(row)
            continue
        try:
            event_date = date.fromisoformat(str(event_at)[:10])
        except ValueError:
            undated.append(row)
            continue
        if event_date < start:
            pre_lead.append(row)
        else:
            post_lead.append(row)
    return {"pre_lead": pre_lead, "post_lead": post_lead, "undated": undated}


def _first_associated_deal_id(value):
    if not value:
        return None
    for part in str(value).replace(",", ";").split(";"):
        cleaned = part.strip()
        if cleaned:
            return cleaned
    return None


def _contact_school_clause(school):
    value = (school or "").lower()
    if "height" in value:
        aliases = ("the heights", "heights")
    elif "west" in value:
        aliases = ("west university place", "west university", "west u", "westu")
    else:
        aliases = tuple([value]) if value else tuple()
    if not aliases:
        return "1=1", {}
    params = {f"school_{index}": alias for index, alias in enumerate(aliases)}
    exact = ", ".join(f":{key}" for key in params)
    like_params = {f"school_like_{index}": f"%{alias}%" for index, alias in enumerate(aliases)}
    like_sql = " OR ".join(
        f"LOWER(COALESCE(hubspot_deal_name, '')) LIKE :school_like_{index}"
        for index in range(len(aliases))
    )
    return (
        f"(LOWER(COALESCE(school, '')) IN ({exact}) OR {like_sql})",
        {**params, **like_params},
    )


def select_hubspot_contact_targets(conn, school, start_date, end_date, limit=500):
    register_dashboard_sql_functions(conn)
    school_sql, school_params = _contact_school_clause(school)
    rows = conn.execute(
        f"""
        SELECT
            contact_id,
            full_name,
            school,
            dashboard_date(NULLIF(create_date, ''), NULLIF(updated_at, '')) AS lead_date,
            phone_normalized,
            phone,
            associated_deal_ids
        FROM hubspot_contacts
        WHERE dashboard_date(NULLIF(create_date, ''), NULLIF(updated_at, ''))
            BETWEEN dashboard_date(:start) AND dashboard_date(:end)
          AND {school_sql}
          AND COALESCE(phone_normalized, phone, '') != ''
        ORDER BY dashboard_date(NULLIF(create_date, ''), NULLIF(updated_at, '')), contact_id
        LIMIT :limit
        """,
        {"start": start_date, "end": end_date, "limit": limit, **school_params},
    ).fetchall()
    targets = []
    seen = set()
    for row in rows:
        phone = normalize_phone(row["phone_normalized"] or row["phone"])
        if not phone:
            continue
        deal_id = _first_associated_deal_id(row["associated_deal_ids"]) or f"contact:{row['contact_id']}"
        key = (row["contact_id"], phone)
        if key in seen:
            continue
        seen.add(key)
        targets.append(
            {
                "deal_id": deal_id,
                "contact_id": row["contact_id"],
                "school": row["school"] or school,
                "target_type": "phone",
                "target_value": phone,
                "target_hash": target_hash(phone),
                "lead_date": row["lead_date"],
                "window_start": start_date,
            }
        )
    return targets
