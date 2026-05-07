import hashlib
import json
from collections import Counter


GAP_CATEGORIES = (
    "ready_for_review",
    "missing_hubspot_contact",
    "missing_pike13_match",
    "missing_first_visit",
    "missing_conversion_signal",
    "missing_dialpad_match",
    "targeted_dialpad_not_wired",
    "excluded_stage",
)


EXCLUDED_STAGE_MARKERS = ("closed lost", "not a lead", "enrolled")


def lead_ref(deal_id):
    digest = hashlib.sha256(str(deal_id or "").encode("utf-8")).hexdigest()
    return f"lead_{digest[:10]}"


def is_excluded_stage(stage):
    lowered = (stage or "").strip().lower()
    return any(marker in lowered for marker in EXCLUDED_STAGE_MARKERS)


def boolish(value):
    return bool(value) and str(value).strip().lower() not in {"0", "false", "none", "null", ""}


def classify_gap(row):
    if boolish(row.get("excluded_stage_flag")):
        return "excluded_stage"
    if not boolish(row.get("trusted_contact_flag")):
        return "missing_hubspot_contact"
    if not boolish(row.get("pike13_match_flag")):
        return "missing_pike13_match"
    if not boolish(row.get("first_visit_flag")):
        return "missing_first_visit"
    if not boolish(row.get("conversion_signal_flag")):
        return "missing_conversion_signal"
    if not boolish(row.get("dialpad_match_flag")):
        if boolish(row.get("targeted_dialpad_found_flag")):
            return "targeted_dialpad_not_wired"
        return "missing_dialpad_match"
    return "ready_for_review"


def _safe_int(value):
    return int(value or 0)


def fetch_gap_rows(conn, school="", limit=500):
    params = {"school": school or "", "limit": limit}
    rows = conn.execute(
        """
        WITH deal_contacts AS (
            SELECT
                d.deal_id,
                c.contact_id,
                c.phone_normalized
            FROM hubspot_deals d
            JOIN hubspot_contacts c
              ON instr(COALESCE(c.associated_deal_ids, ''), d.deal_id) > 0
            WHERE COALESCE(
                    json_extract(
                        CASE WHEN json_valid(COALESCE(c.raw_json, '{}')) THEN c.raw_json ELSE '{}' END,
                        '$.trusted'
                    ),
                    0
                ) = 1
              AND LOWER(COALESCE(c.email_normalized, '')) NOT LIKE '%@schoolofrock.com'
        ),
        pike13_match AS (
            SELECT deal_id, pike13_person_id AS person_id
            FROM hubspot_deals
            WHERE COALESCE(pike13_person_id, '') != ''
            UNION
            SELECT im.source_id AS deal_id, im.target_id AS person_id
            FROM identity_matches im
            WHERE im.source_table = 'hubspot_deals'
              AND im.target_table = 'pike13_people'
            UNION
            SELECT dc.deal_id, im.target_id AS person_id
            FROM deal_contacts dc
            JOIN identity_matches im
              ON im.source_table = 'hubspot_contacts'
             AND im.source_id = dc.contact_id
             AND im.target_table = 'pike13_people'
        ),
        first_visits AS (
            SELECT
                person_id,
                COUNT(*) AS first_visit_rows,
                SUM(CASE
                    WHEN COALESCE(attendance_confirmed_flag, 0) = 1
                      OR COALESCE(checked_in_flag, 0) = 1
                      OR LOWER(COALESCE(status, '')) IN ('complete', 'completed', 'canceled', 'late cancel', 'no show')
                      OR LOWER(COALESCE(status, '')) LIKE '%cancel%'
                      OR LOWER(COALESCE(status, '')) LIKE '%show%'
                    THEN 1 ELSE 0 END) AS attendance_outcome_rows
            FROM pike13_visits
            WHERE COALESCE(first_visit_flag, 0) = 1
            GROUP BY person_id
        ),
        conversion_signals AS (
            SELECT
                person_id,
                COUNT(*) AS conversion_rows
            FROM pike13_plans_passes
            WHERE LOWER(COALESCE(name, '')) NOT LIKE '%trial%'
              AND LOWER(COALESCE(name, '')) NOT LIKE '%free%'
              AND (
                  COALESCE(starts_at, '') != ''
               OR COALESCE(next_invoice_at, '') != ''
               OR COALESCE(payer_name, '') != ''
              )
            GROUP BY person_id
        ),
        dialpad_direct AS (
            SELECT
                dc.deal_id,
                COUNT(*) AS communication_rows
            FROM deal_contacts dc
            JOIN vw_dialpad_communications comms
              ON comms.phone_normalized = dc.phone_normalized
            WHERE COALESCE(dc.phone_normalized, '') != ''
            GROUP BY dc.deal_id
        ),
        targeted AS (
            SELECT
                deal_id,
                SUM(CASE
                    WHEN outcome IN ('found_sms', 'found_call', 'found_voicemail', 'found_call_review')
                      OR COALESCE(found_sms_count, 0) > 0
                      OR COALESCE(found_voice_count, 0) > 0
                      OR COALESCE(found_call_review_count, 0) > 0
                    THEN 1 ELSE 0 END) AS targeted_found_rows
            FROM dialpad_target_searches
            GROUP BY deal_id
        )
        SELECT
            d.deal_id,
            COALESCE(d.school, '') AS school,
            COALESCE(d.stage, '') AS stage,
            CASE WHEN LOWER(COALESCE(d.stage, '')) LIKE '%closed lost%'
                   OR LOWER(COALESCE(d.stage, '')) LIKE '%not a lead%'
                   OR LOWER(COALESCE(d.stage, '')) LIKE '%enrolled%'
                 THEN 1 ELSE 0 END AS excluded_stage_flag,
            CASE WHEN COUNT(DISTINCT dc.contact_id) > 0 THEN 1 ELSE 0 END AS trusted_contact_flag,
            CASE WHEN COUNT(DISTINCT pm.person_id) > 0 THEN 1 ELSE 0 END AS pike13_match_flag,
            CASE WHEN COALESCE(SUM(fv.first_visit_rows), 0) > 0 THEN 1 ELSE 0 END AS first_visit_flag,
            CASE WHEN COALESCE(SUM(fv.attendance_outcome_rows), 0) > 0 THEN 1 ELSE 0 END AS attendance_outcome_flag,
            CASE WHEN COALESCE(SUM(cs.conversion_rows), 0) > 0 THEN 1 ELSE 0 END AS conversion_signal_flag,
            CASE WHEN COALESCE(MAX(dd.communication_rows), 0) > 0 THEN 1 ELSE 0 END AS dialpad_match_flag,
            CASE WHEN COALESCE(MAX(t.targeted_found_rows), 0) > 0 THEN 1 ELSE 0 END AS targeted_dialpad_found_flag,
            COUNT(DISTINCT dc.contact_id) AS trusted_contact_count,
            COUNT(DISTINCT pm.person_id) AS pike13_match_count,
            COALESCE(SUM(fv.first_visit_rows), 0) AS first_visit_rows,
            COALESCE(SUM(fv.attendance_outcome_rows), 0) AS attendance_outcome_rows,
            COALESCE(SUM(cs.conversion_rows), 0) AS conversion_signal_rows,
            COALESCE(MAX(dd.communication_rows), 0) AS dialpad_communication_rows,
            COALESCE(MAX(t.targeted_found_rows), 0) AS targeted_dialpad_found_rows
        FROM hubspot_deals d
        LEFT JOIN deal_contacts dc ON dc.deal_id = d.deal_id
        LEFT JOIN pike13_match pm ON pm.deal_id = d.deal_id
        LEFT JOIN first_visits fv ON fv.person_id = pm.person_id
        LEFT JOIN conversion_signals cs ON cs.person_id = pm.person_id
        LEFT JOIN dialpad_direct dd ON dd.deal_id = d.deal_id
        LEFT JOIN targeted t ON t.deal_id = d.deal_id
        WHERE (:school = '' OR COALESCE(d.school, '') = :school)
        GROUP BY d.deal_id
        ORDER BY
            excluded_stage_flag,
            CASE COALESCE(d.stage, '')
                WHEN 'Waiting On Us' THEN 0
                WHEN 'Scheduled Trial/Tour' THEN 1
                WHEN 'Trial/Tour Completed & Unconverted' THEN 2
                WHEN 'Contacted' THEN 3
                ELSE 4
            END,
            d.deal_id
        LIMIT :limit
        """,
        params,
    ).fetchall()
    result = []
    for index, row in enumerate(rows, start=1):
        data = dict(row)
        gap_category = classify_gap(data)
        result.append(
            {
                "row": index,
                "lead_ref": lead_ref(data.get("deal_id")),
                "school": data.get("school") or "unknown",
                "stage": data.get("stage") or "unknown",
                "gap_category": gap_category,
                "hubspot_contact_complete": boolish(data.get("trusted_contact_flag")),
                "pike13_match_found": boolish(data.get("pike13_match_flag")),
                "pike13_first_visit_found": boolish(data.get("first_visit_flag")),
                "attendance_outcome_found": boolish(data.get("attendance_outcome_flag")),
                "conversion_signal_found": boolish(data.get("conversion_signal_flag")),
                "dialpad_match_found": boolish(data.get("dialpad_match_flag")),
                "targeted_dialpad_found_not_wired": (
                    boolish(data.get("targeted_dialpad_found_flag"))
                    and not boolish(data.get("dialpad_match_flag"))
                ),
                "excluded_stage": boolish(data.get("excluded_stage_flag")),
                "trusted_contact_count": _safe_int(data.get("trusted_contact_count")),
                "pike13_match_count": _safe_int(data.get("pike13_match_count")),
                "first_visit_rows": _safe_int(data.get("first_visit_rows")),
                "attendance_outcome_rows": _safe_int(data.get("attendance_outcome_rows")),
                "conversion_signal_rows": _safe_int(data.get("conversion_signal_rows")),
                "dialpad_communication_rows": _safe_int(data.get("dialpad_communication_rows")),
                "targeted_dialpad_found_rows": _safe_int(data.get("targeted_dialpad_found_rows")),
            }
        )
    return result


def source_readiness(conn):
    return {
        "hubspot_deals": conn.execute("SELECT COUNT(*) FROM hubspot_deals").fetchone()[0],
        "hubspot_contacts": conn.execute("SELECT COUNT(*) FROM hubspot_contacts").fetchone()[0],
        "pike13_people": conn.execute("SELECT COUNT(*) FROM pike13_people").fetchone()[0],
        "pike13_first_visits": conn.execute(
            "SELECT COUNT(*) FROM pike13_visits WHERE COALESCE(first_visit_flag, 0) = 1"
        ).fetchone()[0],
        "pike13_plans_passes": conn.execute("SELECT COUNT(*) FROM pike13_plans_passes").fetchone()[0],
        "dialpad_communications": conn.execute("SELECT COUNT(*) FROM vw_dialpad_communications").fetchone()[0],
        "dialpad_target_searches": conn.execute("SELECT COUNT(*) FROM dialpad_target_searches").fetchone()[0],
    }


def summarize_gap_rows(rows, readiness=None):
    by_gap = Counter(row["gap_category"] for row in rows)
    by_school = Counter(row["school"] for row in rows)
    by_stage = Counter(row["stage"] for row in rows)
    return {
        "rows_reviewed": len(rows),
        "ready_for_review_rows": by_gap.get("ready_for_review", 0),
        "missing_pike13_match_rows": by_gap.get("missing_pike13_match", 0),
        "missing_dialpad_match_rows": by_gap.get("missing_dialpad_match", 0),
        "targeted_dialpad_not_wired_rows": by_gap.get("targeted_dialpad_not_wired", 0),
        "by_gap_category": {category: by_gap.get(category, 0) for category in GAP_CATEGORIES},
        "by_school": dict(sorted(by_school.items())),
        "by_stage": dict(sorted(by_stage.items())),
        "source_readiness": readiness or {},
    }


def build_gap_report(conn, school="", limit=500):
    rows = fetch_gap_rows(conn, school, limit)
    summary = summarize_gap_rows(rows, source_readiness(conn))
    return {"summary": summary, "rows": rows}


def render_gap_markdown(report, school=""):
    summary = report["summary"]
    rows = report["rows"]
    lines = [
        "# Lead Intelligence Gap Report",
        "",
        f"School filter: {school or 'all'}",
        "",
        "## Summary",
        "",
        f"- Rows reviewed: {summary['rows_reviewed']}",
        f"- Ready for review: {summary['ready_for_review_rows']}",
        f"- Missing Pike13 match: {summary['missing_pike13_match_rows']}",
        f"- Missing Dialpad match: {summary['missing_dialpad_match_rows']}",
        f"- Targeted Dialpad not wired: {summary['targeted_dialpad_not_wired_rows']}",
        "",
        "## Gap Categories",
        "",
    ]
    for category, count in summary["by_gap_category"].items():
        lines.append(f"- {category}: {count}")
    lines.extend(["", "## Source Readiness", ""])
    for source, count in summary["source_readiness"].items():
        lines.append(f"- {source}: {count}")
    lines.extend(
        [
            "",
            "## Rows",
            "",
            "| Lead | School | Stage | Gap | HubSpot Contact | Pike13 Match | First Visit | Attendance | Conversion | Dialpad | Targeted Evidence |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in rows:
        lines.append(
            "| {lead_ref} | {school} | {stage} | {gap} | {contact} | {pike13} | {first_visit} | {attendance} | {conversion} | {dialpad} | {targeted} |".format(
                lead_ref=row["lead_ref"],
                school=clean_cell(row["school"]),
                stage=clean_cell(row["stage"]),
                gap=row["gap_category"],
                contact=yes_no(row["hubspot_contact_complete"]),
                pike13=yes_no(row["pike13_match_found"]),
                first_visit=yes_no(row["pike13_first_visit_found"]),
                attendance=yes_no(row["attendance_outcome_found"]),
                conversion=yes_no(row["conversion_signal_found"]),
                dialpad=yes_no(row["dialpad_match_found"]),
                targeted=yes_no(row["targeted_dialpad_found_not_wired"]),
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


def clean_cell(value):
    return str(value or "").replace("|", "\\|").replace("\n", " ").strip()


def yes_no(value):
    return "yes" if value else "no"


def report_to_json(report):
    return json.dumps(report, indent=2, sort_keys=True)
