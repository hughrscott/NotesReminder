import hashlib
import json
import sqlite3
from datetime import datetime

from school_email import communication_label, delay_bucket


def opaque_ref(prefix, value):
    digest = hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:10]}"


def parse_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        try:
            return datetime.fromisoformat(str(value).split()[0]).replace(tzinfo=None)
        except ValueError:
            return None


def hours_between(start, end):
    if not start or not end:
        return None
    return round((end - start).total_seconds() / 3600, 2)


def fetch_trials(conn, start_date, end_date, school="West U"):
    return conn.execute(
        """
        SELECT
            v.visit_id,
            v.person_id,
            p.full_name,
            p.email_normalized,
            p.phone_normalized,
            v.service,
            v.starts_at,
            v.status,
            v.no_show_flag,
            v.canceled_flag,
            v.first_visit_flag,
            COALESCE(v.school, p.school) AS school
        FROM pike13_visits v
        LEFT JOIN pike13_people p ON p.person_id = v.person_id
        WHERE date(v.starts_at) BETWEEN date(:start) AND date(:end)
          AND (:school = '' OR COALESCE(v.school, p.school, '') = :school)
          AND (
            COALESCE(v.first_visit_flag, 0) = 1
            OR LOWER(COALESCE(v.service, '')) LIKE '%trial%'
          )
        ORDER BY v.starts_at, v.visit_id
        """,
        {"start": start_date, "end": end_date, "school": school},
    ).fetchall()


def clean_name(value):
    return " ".join(str(value or "").strip().lower().split())


def name_terms(value):
    terms = [term for term in clean_name(value).replace("|", " ").split() if len(term) > 1]
    return terms[:3]


def deal_lead_name(value):
    return clean_name(str(value or "").split("|")[0])


def hubspot_contact_keys(conn, person_id):
    emails = set()
    phones = set()
    deal_ids = set()
    rows = conn.execute(
        """
        SELECT DISTINCT hc.email_normalized, hc.phone_normalized, hc.associated_deal_ids
        FROM identity_matches im
        JOIN hubspot_contacts hc ON hc.contact_id = im.source_id
        WHERE im.source_table = 'hubspot_contacts'
          AND im.target_table = 'pike13_people'
          AND im.target_id = ?
        UNION
        SELECT DISTINCT hc.email_normalized, hc.phone_normalized, hc.associated_deal_ids
        FROM identity_matches im
        JOIN hubspot_contacts hc
          ON instr(COALESCE(hc.associated_deal_ids, ''), im.source_id) > 0
        WHERE im.source_table = 'hubspot_deals'
          AND im.target_table = 'pike13_people'
          AND im.target_id = ?
        UNION
        SELECT DISTINCT hc.email_normalized, hc.phone_normalized, hc.associated_deal_ids
        FROM hubspot_deals d
        JOIN hubspot_contacts hc
          ON instr(COALESCE(hc.associated_deal_ids, ''), d.deal_id) > 0
        WHERE d.pike13_person_id = ?
        """,
        (person_id, person_id, person_id),
    ).fetchall()
    for row in rows:
        if row["email_normalized"]:
            emails.add(row["email_normalized"])
        if row["phone_normalized"]:
            phones.add(row["phone_normalized"])
        for deal_id in str(row["associated_deal_ids"] or "").split(","):
            deal_id = deal_id.strip()
            if deal_id:
                deal_ids.add(deal_id)
    return emails, phones, deal_ids


def hubspot_deal_names(conn, deal_ids):
    names = set()
    for deal_id in deal_ids:
        row = conn.execute("SELECT deal_name FROM hubspot_deals WHERE deal_id = ?", (deal_id,)).fetchone()
        if row and row["deal_name"]:
            name = deal_lead_name(row["deal_name"])
            if name:
                names.add(name)
    return names


def identity_keys_for_trial(conn, trial):
    emails, phones, deal_ids = hubspot_contact_keys(conn, trial["person_id"])
    if trial["email_normalized"]:
        emails.add(trial["email_normalized"])
    if trial["phone_normalized"]:
        phones.add(trial["phone_normalized"])
    search_names = set()
    if trial["full_name"]:
        search_names.add(clean_name(trial["full_name"]))
    search_names.update(hubspot_deal_names(conn, deal_ids))
    return emails, phones, deal_ids, search_names


def email_name_search_rows(conn, search_names):
    rows = []
    seen = set()
    for name in search_names:
        terms = name_terms(name)
        if len(terms) < 2:
            continue
        where = " AND ".join(
            [
                """
                LOWER(
                    COALESCE(subject, '') || ' ' ||
                    COALESCE(snippet, '') || ' ' ||
                    COALESCE(body, '') || ' ' ||
                    COALESCE(raw_text, '')
                ) LIKE ?
                """
                for _ in terms
            ]
        )
        for row in conn.execute(
            f"""
            SELECT 'email' AS channel, message_id AS communication_id, direction,
                   message_at AS event_at, school, subject AS outcome
            FROM school_email_messages
            WHERE date(message_at) IS NOT NULL
              AND {where}
            """,
            tuple(f"%{term}%" for term in terms),
        ).fetchall():
            key = ("email", row["communication_id"])
            if key not in seen:
                seen.add(key)
                rows.append(dict(row))
    return rows


def communication_rows(conn, emails, phones, search_names=None):
    rows = []
    for email in emails:
        rows.extend(
            dict(row)
            for row in conn.execute(
                """
                SELECT 'email' AS channel, communication_id, direction, event_at, school, subject AS outcome
                FROM vw_school_email_communications
                WHERE external_email_normalized = ?
                  AND date(event_at) IS NOT NULL
                """,
                (email,),
            ).fetchall()
        )
    for phone in phones:
        rows.extend(
            dict(row)
            for row in conn.execute(
                """
                SELECT channel, communication_id, direction, event_at, school, outcome
                FROM vw_dialpad_communications
                WHERE phone_normalized = ?
                  AND date(event_at) IS NOT NULL
                """,
                (phone,),
            ).fetchall()
        )
    rows.extend(email_name_search_rows(conn, search_names or set()))
    deduped = {}
    for row in rows:
        key = (row["channel"], row["communication_id"])
        deduped[key] = row
    return sorted(deduped.values(), key=lambda row: row.get("event_at") or "")


def outcome_for_trial(row):
    status = (row["status"] or "").lower()
    service = (row["service"] or "").lower()
    if row["no_show_flag"] or "no show" in status:
        return "no_show"
    if row["canceled_flag"] or "cancel" in status:
        return "canceled"
    if "complete" in status or "checked" in status:
        return "attended"
    if parse_dt(row["starts_at"]) and parse_dt(row["starts_at"]) > datetime.now():
        return "scheduled_future"
    if "trial" in service:
        return "trial_scheduled"
    return "unknown"


def identity_status(emails, phones, search_names):
    if emails or phones:
        return "direct_contact_keys"
    if search_names:
        return "name_search_only"
    return "insufficient_identity"


def summarize_trial(row, comms, emails=None, phones=None, search_names=None):
    emails = emails or set()
    phones = phones or set()
    search_names = search_names or set()
    trial_start = parse_dt(row["starts_at"])
    before = []
    after = []
    for comm in comms:
        event_at = parse_dt(comm.get("event_at"))
        if not trial_start or not event_at:
            continue
        if event_at < trial_start:
            before.append((event_at, comm))
        else:
            after.append((event_at, comm))
    last_before = before[-1] if before else None
    first_after = after[0] if after else None
    hours_before = hours_between(last_before[0], trial_start) if last_before else None
    hours_after = hours_between(trial_start, first_after[0]) if first_after else None
    outcome = outcome_for_trial(row)
    if outcome == "no_show" and not first_after:
        followup_status = "no_post_no_show_followup"
    elif outcome == "no_show" and hours_after is not None and hours_after > 72:
        followup_status = "late_post_no_show_followup"
    elif outcome == "no_show":
        followup_status = "post_no_show_followup_found"
    elif not last_before:
        if emails or phones:
            followup_status = "no_pre_trial_outreach"
        elif search_names:
            followup_status = "no_pre_trial_outreach_name_search_only"
        else:
            followup_status = "no_pre_trial_outreach_identity_limited"
    else:
        followup_status = "outreach_found"
    return {
        "trial_ref": opaque_ref("trial", row["visit_id"]),
        "person_ref": opaque_ref("person", row["person_id"]),
        "school": row["school"] or "unknown",
        "service_type": "trial" if "trial" in (row["service"] or "").lower() else "visit",
        "starts_at": row["starts_at"],
        "outcome": outcome,
        "pre_trial_outreach_found": bool(last_before),
        "last_pre_trial_channel": communication_label(last_before[1]) if last_before else "none",
        "last_pre_trial_hours_before": hours_before,
        "last_pre_trial_bucket": delay_bucket(hours_before),
        "post_trial_outreach_found": bool(first_after),
        "first_post_trial_channel": communication_label(first_after[1]) if first_after else "none",
        "first_post_trial_hours_after": hours_after,
        "first_post_trial_bucket": delay_bucket(hours_after),
        "followup_status": followup_status,
        "communication_count": len(comms),
        "identity_status": identity_status(emails, phones, search_names),
        "email_key_count": len(emails),
        "phone_key_count": len(phones),
        "name_search_used": bool(search_names),
    }


def build_trial_followup_report(conn, start_date, end_date, school="West U"):
    conn.row_factory = sqlite3.Row
    rows = []
    for trial in fetch_trials(conn, start_date, end_date, school):
        emails, phones, _, search_names = identity_keys_for_trial(conn, trial)
        comms = communication_rows(conn, emails, phones, search_names)
        rows.append(summarize_trial(trial, comms, emails, phones, search_names))
    counts = {}
    for row in rows:
        counts[row["followup_status"]] = counts.get(row["followup_status"], 0) + 1
    outcome_counts = {}
    for row in rows:
        outcome_counts[row["outcome"]] = outcome_counts.get(row["outcome"], 0) + 1
    identity_counts = {}
    for row in rows:
        identity_counts[row["identity_status"]] = identity_counts.get(row["identity_status"], 0) + 1
    return {
        "window_start": start_date,
        "window_end": end_date,
        "school": school,
        "summary": {
            "trial_rows": len(rows),
            "pre_trial_outreach_missing_rows": sum(1 for row in rows if not row["pre_trial_outreach_found"]),
            "post_trial_outreach_missing_rows": sum(
                1 for row in rows if row["outcome"] in {"no_show", "canceled", "attended"} and not row["post_trial_outreach_found"]
            ),
            "by_followup_status": dict(sorted(counts.items())),
            "by_outcome": dict(sorted(outcome_counts.items())),
            "by_identity_status": dict(sorted(identity_counts.items())),
        },
        "rows": rows,
    }


def render_trial_followup_markdown(report):
    summary = report["summary"]
    lines = [
        "# Trial Follow-Up Intelligence Report",
        "",
        f"Window: {report['window_start']} to {report['window_end']}",
        f"School: {report['school']}",
        "",
        "## Summary",
        "",
        f"- Trial rows: {summary['trial_rows']}",
        f"- Missing pre-trial outreach: {summary['pre_trial_outreach_missing_rows']}",
        f"- Missing post-trial/outcome outreach: {summary['post_trial_outreach_missing_rows']}",
        "",
        "## Follow-Up Status",
        "",
    ]
    for status, count in summary["by_followup_status"].items():
        lines.append(f"- {status}: {count}")
    lines.extend(["", "## Identity Coverage", ""])
    for status, count in summary.get("by_identity_status", {}).items():
        lines.append(f"- {status}: {count}")
    lines.extend(
        [
            "",
            "## Rows",
            "",
            "| Trial | Outcome | Start | Identity | Pre-Trial Contact | Last Before | Post-Trial Contact | First After | Status |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in report["rows"]:
        lines.append(
            "| {trial} | {outcome} | {start} | {identity} | {pre} | {before} | {post} | {after} | {status} |".format(
                trial=row["trial_ref"],
                outcome=row["outcome"],
                start=clean(row["starts_at"]),
                identity=row["identity_status"],
                pre="yes" if row["pre_trial_outreach_found"] else "no",
                before=f"{row['last_pre_trial_channel']} / {row['last_pre_trial_bucket']}",
                post="yes" if row["post_trial_outreach_found"] else "no",
                after=f"{row['first_post_trial_channel']} / {row['first_post_trial_bucket']}",
                status=row["followup_status"],
            )
        )
    lines.extend(
        [
            "",
            "_This report is sanitized: it excludes customer names, emails, phones, message bodies, notes, transcripts, raw page text, screenshots, and source URLs._",
            "",
        ]
    )
    return "\n".join(lines)


def clean(value):
    return str(value or "").replace("|", "\\|").replace("\n", " ").strip()


def report_to_json(report):
    return json.dumps(report, indent=2, sort_keys=True)
