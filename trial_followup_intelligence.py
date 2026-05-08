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
        """,
        (person_id,),
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


def communication_rows(conn, emails, phones):
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


def summarize_trial(row, comms):
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
        followup_status = "no_pre_trial_outreach"
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
    }


def build_trial_followup_report(conn, start_date, end_date, school="West U"):
    conn.row_factory = sqlite3.Row
    rows = []
    for trial in fetch_trials(conn, start_date, end_date, school):
        emails, phones, _ = hubspot_contact_keys(conn, trial["person_id"])
        comms = communication_rows(conn, emails, phones)
        rows.append(summarize_trial(trial, comms))
    counts = {}
    for row in rows:
        counts[row["followup_status"]] = counts.get(row["followup_status"], 0) + 1
    outcome_counts = {}
    for row in rows:
        outcome_counts[row["outcome"]] = outcome_counts.get(row["outcome"], 0) + 1
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
    lines.extend(
        [
            "",
            "## Rows",
            "",
            "| Trial | Outcome | Start | Pre-Trial Contact | Last Before | Post-Trial Contact | First After | Status |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in report["rows"]:
        lines.append(
            "| {trial} | {outcome} | {start} | {pre} | {before} | {post} | {after} | {status} |".format(
                trial=row["trial_ref"],
                outcome=row["outcome"],
                start=clean(row["starts_at"]),
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
