#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from source_completeness import (  # noqa: E402
    DEFAULT_PIKE13_LOOKAHEAD_DAYS,
    DEFAULT_WINDOW_DAYS,
    build_source_completeness_report,
)


DEFAULT_OUTPUT = "outputs/progress/lead_intelligence_status.md"
STATUS_LABELS = {
    "ready": "READY",
    "partial": "PARTIAL",
    "blocked": "BLOCKED",
    "stale": "STALE",
}


def status_label(status):
    return STATUS_LABELS.get((status or "").lower(), (status or "unknown").upper())


def coverage_text(coverage, field):
    item = (coverage or {}).get(field) or {}
    return f"{item.get('fill_rate', 0):.1f}% ({item.get('filled', 0)}/{item.get('total', 0)})"


def import_run_text(run):
    if not run:
        return "No import run recorded."
    status = status_label(run.get("status"))
    finished = run.get("finished_at") or "not finished"
    seen = run.get("rows_seen", 0)
    inserted = run.get("rows_inserted", 0)
    updated = run.get("rows_updated", 0)
    text = f"{status} at {finished}; seen {seen}, inserted {inserted}, updated {updated}."
    stale = run.get("stale_running_run")
    if stale:
        text += f" Ignored stale RUNNING run started {stale.get('started_at') or 'unknown'}."
    return text


def bullet_list(items):
    if not items:
        return "- None."
    return "\n".join(f"- {item}" for item in items)


def hubspot_summary(hubspot):
    coverage = hubspot.get("field_coverage", {})
    return [
        f"Rows: {hubspot.get('rows', 0)}",
        f"Deal ID: {coverage_text(coverage, 'deal_id')}",
        f"Stage: {coverage_text(coverage, 'stage')}",
        f"School: {coverage_text(coverage, 'school')}",
        f"Create date: {coverage_text(coverage, 'create_date')}",
        f"Source URL: {coverage_text(coverage, 'source_url')}",
        f"Trusted contacts: {hubspot.get('contact_quality', {}).get('trusted_rows', 0)}",
    ]


def dialpad_summary(dialpad):
    sms_coverage = dialpad.get("sms_field_coverage", {})
    voice_coverage = dialpad.get("voice_field_coverage", {})
    target_search = dialpad.get("target_search", {})
    route_discovery = dialpad.get("route_discovery", {})
    daily_intake = dialpad.get("daily_intake", {})
    return [
        f"SMS rows: {dialpad.get('sms_rows', 0)}",
        f"SMS timestamp coverage: {coverage_text(sms_coverage, 'message_at')}",
        f"SMS direction coverage: {coverage_text(sms_coverage, 'direction')}",
        f"Voice rows: {dialpad.get('voice_rows', 0)}",
        f"Voice timestamp coverage: {coverage_text(voice_coverage, 'event_at')}",
        f"Conversation History rows: {dialpad.get('conversation_history_rows', 0)}",
        f"Call-review transcript/audio access URLs: {dialpad.get('conversation_history_recording_or_transcript_url_rows', 0)}",
        f"Call-review rows: {dialpad.get('call_review_rows', 0)}",
        f"Call-review transcripts: {dialpad.get('call_review_transcript_rows', 0)}",
        f"Call-review recaps: {dialpad.get('call_review_recap_rows', 0)}",
        f"Call-review action-item rows: {dialpad.get('call_review_action_item_rows', 0)}",
        f"Daily intake tagged rows: {daily_intake.get('daily_intake_rows', 0)}",
        f"Dialpad communication rows in proof window: {daily_intake.get('communication_window_rows', 0)}",
        f"Daily inbound rows: {daily_intake.get('daily_inbound_rows', 0)}",
        f"Unmatched inbound rows: {daily_intake.get('unmatched_inbound_rows', 0)}",
        f"Possible leads not in HubSpot: {daily_intake.get('possible_lead_not_in_hubspot_rows', 0)}",
        f"Inbound rows without later outbound follow-up: {daily_intake.get('no_followup_rows', 0)}",
        f"Latest daily intake timestamp: {daily_intake.get('latest_daily_intake_at') or 'none'}",
        f"Discovery fallback required: {'yes' if daily_intake.get('discovery_fallback_required') else 'no'}",
        f"Route-map probes: {route_discovery.get('rows', 0)}",
        f"Route-map usable/partial/blocked: {route_discovery.get('usable_routes', 0)}/{route_discovery.get('partial_routes', 0)}/{route_discovery.get('blocked_routes', 0)}",
        f"Route-map SMS/voice/call-review routes: {route_discovery.get('sms_routes', 0)}/{route_discovery.get('voice_routes', 0)}/{route_discovery.get('call_review_routes', 0)}",
        f"Targeted lead-phone searches: {target_search.get('rows', 0)}",
        f"Targeted searches found: {target_search.get('targets_found', 0)}",
        f"Targeted searches not found/blocked: {target_search.get('targets_not_found', 0) + target_search.get('filter_not_supported_rows', 0) + target_search.get('ui_blocked_rows', 0) + target_search.get('auth_blocked_rows', 0)}",
        f"Future source timestamps: SMS {dialpad.get('future_sms_timestamp_rows', 0)}, voice {dialpad.get('future_voice_timestamp_rows', 0)}",
    ]


def pike13_summary(pike13):
    route_discovery = pike13.get("route_discovery", {})
    return [
        "Lesson Visits / Notes",
        f"Lesson visit rows: {pike13.get('lesson_visit_rows', 0)}",
        f"Completed notes: {pike13.get('completed_note_rows', 0)}",
        f"Missing notes: {pike13.get('missing_note_rows', 0)}",
        f"No-shows: {pike13.get('no_show_rows', 0)}",
        f"Canceled lessons: {pike13.get('canceled_rows', 0)}",
        f"Trial lesson rows: {pike13.get('trial_lesson_rows', 0)}",
        f"Latest lesson date: {pike13.get('latest_lesson_date') or 'none'}",
        f"Note score coverage: {pike13.get('note_score_coverage', {}).get('fill_rate', 0):.1f}% "
        f"({pike13.get('note_score_coverage', {}).get('filled', 0)}/{pike13.get('note_score_coverage', {}).get('total', 0)})",
        "Lead Outcomes",
        f"People rows: {pike13.get('people_rows', 0)}",
        f"Rich lead/outcome visit rows: {pike13.get('rich_visit_rows', pike13.get('visit_rows', 0))}",
        f"Plan/pass rows: {pike13.get('plan_pass_rows', 0)}",
        f"Window plus lookahead visits: {pike13.get('window_plus_lookahead_visit_rows', 0)}",
        f"Route fallback probes: {route_discovery.get('rows', 0)}",
        f"Route fallback usable/partial/blocked: {route_discovery.get('usable_routes', 0)}/{route_discovery.get('partial_routes', 0)}/{route_discovery.get('blocked_routes', 0)}",
    ]


def matching_summary(matching):
    match_types = matching.get("by_match_type") or []
    if match_types:
        type_text = ", ".join(f"{row.get('match_type')}: {row.get('rows')}" for row in match_types)
    else:
        type_text = "none"
    return [
        f"Identity matches: {matching.get('rows', 0)}",
        f"Matched HubSpot deals: {matching.get('matched_hubspot_deals', 0)}",
        f"Matched HubSpot contacts: {matching.get('matched_hubspot_contacts', 0)}",
        f"Match types: {type_text}",
    ]


def next_actions(report):
    sources = report.get("sources", {})
    actions = []
    dialpad = sources.get("dialpad", {})
    pike13 = sources.get("pike13", {})
    hubspot = sources.get("hubspot", {})
    first_value = report.get("first_value", {})
    if first_value.get("candidate_leads", 0) and first_value.get("candidate_leads_with_dialpad_comms", 0) == 0:
        target_search = dialpad.get("target_search", {})
        if target_search.get("rows", 0) == 0:
            actions.append("Run targeted Dialpad discovery for lead-attention candidates.")
        elif target_search.get("targets_found", 0) == 0:
            actions.append("Review targeted Dialpad discovery results; current candidate phone keys were not found or were blocked in Dialpad UI.")
        else:
            actions.append("Wire targeted Dialpad search results into lead-attention matching.")
    if (
        dialpad.get("conversation_history_recording_or_transcript_url_rows", 0) > 0
        and dialpad.get("call_review_transcript_rows", 0) == 0
        and dialpad.get("call_review_recap_rows", 0) == 0
    ):
        actions.append("Capture transcript, recap, and action-item text from Dialpad call-review URLs.")
    elif (
        dialpad.get("conversation_history_rows", 0) > 0
        and dialpad.get("conversation_history_recording_or_transcript_url_rows", 0) == 0
    ):
        actions.append("Fix Dialpad Conversation History call-review URL capture.")
    daily_intake = dialpad.get("daily_intake", {})
    if daily_intake.get("discovery_fallback_required"):
        actions.append("Review Dialpad route-discovery fallback before any DB upload/sync; the latest daily intake was not fully successful.")
    if daily_intake.get("unmatched_inbound_rows", 0):
        actions.append("Review the unmatched inbound report for missed calls, voicemails, inbound SMS, and possible leads not in HubSpot.")
    if (
        dialpad.get("status") == "ready"
        and first_value.get("candidate_leads_with_dialpad_comms", 0) > 0
        and daily_intake.get("daily_intake_rows", 0) == 0
    ):
        actions.append("Build daily Dialpad communications intake and an unmatched inbound action report so calls, texts, and voicemails without HubSpot leads are not missed.")
    if pike13.get("lesson_visit_rows", 0) and pike13.get("rich_visit_rows", pike13.get("visit_rows", 0)) == 0:
        actions.append("Use existing Pike13 lesson visits for note-quality/current-student insight, while still extracting rich trial/outcome/plan data.")
    elif pike13.get("visit_rows", 0) == 0:
        actions.append("Unblock Pike13 visits/outcomes so lead timelines can show trial attendance, no-shows, and enrollment outcomes.")
    if hubspot.get("status") != "ready":
        actions.append("Bring HubSpot lead-spine fields back to ready status before widening the proof window.")
    if not actions:
        actions.append("Widen the proof window from 7 days to 30 days and review matching quality.")
    return actions


def ai_readiness(report):
    sources = report.get("sources", {})
    hubspot_ready = sources.get("hubspot", {}).get("status") == "ready"
    dialpad_ready = sources.get("dialpad", {}).get("status") == "ready"
    pike13_ready = sources.get("pike13", {}).get("status") == "ready"
    lesson_visits = sources.get("pike13", {}).get("lesson_visit_rows", 0)
    matching_rows = report.get("matching", {}).get("rows", 0)
    call_review_urls = sources.get("dialpad", {}).get("conversation_history_recording_or_transcript_url_rows", 0)
    call_review_text = (
        sources.get("dialpad", {}).get("call_review_transcript_rows", 0)
        + sources.get("dialpad", {}).get("call_review_recap_rows", 0)
    )
    first_value = report.get("first_value", {})
    first_value_ready = first_value.get("report_ready", False)
    candidate_comms = first_value.get("candidate_leads_with_dialpad_comms", 0)
    pike13_visits = sources.get("pike13", {}).get("rich_visit_rows", sources.get("pike13", {}).get("visit_rows", 0))
    if call_review_text and candidate_comms:
        sentiment_status = "ready for limited proof"
    elif call_review_text:
        sentiment_status = "communication text ready; lead-candidate matching still not ready"
    elif call_review_urls:
        sentiment_status = "ready after call-review transcript ingestion"
    else:
        sentiment_status = "not ready"
    if pike13_ready:
        outcome_status = "ready"
    elif pike13_visits:
        outcome_status = "partial; Pike13 visit/outcome rows loaded but below readiness threshold"
    else:
        outcome_status = "blocked until Pike13 visits/outcomes load"
    return [
        f"Lead follow-up insights: {'ready for limited proof' if hubspot_ready and dialpad_ready and matching_rows and first_value_ready else 'not ready'}",
        f"Sentiment/coaching analysis: {sentiment_status}",
        f"Lesson-note quality/current-student operations: {'ready for existing notes data' if lesson_visits else 'not ready'}",
        f"Outcome attribution: {outcome_status}",
        "AI lead-management automation: not ready; needs reliable source completeness, outcome data, and human-reviewed recommendation quality first.",
    ]


def render_dashboard(report):
    window = report.get("window", {})
    sources = report.get("sources", {})
    hubspot = sources.get("hubspot", {})
    dialpad = sources.get("dialpad", {})
    pike13 = sources.get("pike13", {})
    matching = report.get("matching", {})
    first_value = report.get("first_value", {})
    blockers = []
    for name, source in (("HubSpot", hubspot), ("Dialpad", dialpad), ("Pike13", pike13)):
        for blocker in source.get("blockers") or []:
            blockers.append(f"{name}: {blocker}")

    lines = [
        "# Lead Intelligence Progress Dashboard",
        "",
        f"Overall status: **{status_label(report.get('overall_status'))}**",
        f"Proof window: **{window.get('start')}** through **{window.get('end')}** ({window.get('days')} days)",
        f"Pike13 lookahead: through **{window.get('pike13_lookahead_end')}** ({window.get('pike13_lookahead_days')} days)",
        "",
        "## Source Readiness",
        "",
        f"### HubSpot - {status_label(hubspot.get('status'))}",
        bullet_list(hubspot_summary(hubspot)),
        f"- Latest import: {import_run_text(hubspot.get('latest_import_run'))}",
        "",
        f"### Dialpad - {status_label(dialpad.get('status'))}",
        bullet_list(dialpad_summary(dialpad)),
        f"- Latest SMS import: {import_run_text(dialpad.get('latest_sms_import_run'))}",
        f"- Latest voice import: {import_run_text(dialpad.get('latest_voice_import_run'))}",
        f"- Latest daily intake import: {import_run_text(dialpad.get('latest_daily_intake_import_run'))}",
        f"- Latest call-review import: {import_run_text(dialpad.get('latest_call_review_import_run'))}",
        f"- Latest route-map import: {import_run_text(dialpad.get('latest_route_discovery_import_run'))}",
        f"- Latest target-search import: {import_run_text(dialpad.get('latest_target_search_import_run'))}",
        "",
        f"### Pike13 - {status_label(pike13.get('status'))}",
        bullet_list(pike13_summary(pike13)),
        f"- Latest import: {import_run_text(pike13.get('latest_import_run'))}",
        f"- Latest route fallback import: {import_run_text(pike13.get('latest_route_discovery_import_run'))}",
        "",
        f"### Matching - {status_label(matching.get('status'))}",
        bullet_list(matching_summary(matching)),
        "",
        f"## First Value Report - {status_label(first_value.get('status'))}",
        "",
        bullet_list(
            [
                f"Report ready: {'yes' if first_value.get('report_ready') else 'no'}",
                f"Call-review URL rows: {first_value.get('call_review_url_rows', 0)}",
                f"Call-review transcript rows: {first_value.get('call_review_transcript_rows', 0)}",
                f"Call-review recap rows: {first_value.get('call_review_recap_rows', 0)}",
                f"Matched HubSpot deals: {first_value.get('matched_hubspot_deals', 0)}",
                f"Matched HubSpot contacts: {first_value.get('matched_hubspot_contacts', 0)}",
                f"Lead-attention candidates: {first_value.get('candidate_leads', 0)}",
                f"Candidates with trusted phone: {first_value.get('candidate_leads_with_trusted_phone', 0)}",
                f"Candidates with matched Dialpad communications: {first_value.get('candidate_leads_with_dialpad_comms', 0)}",
            ]
        ),
        "",
        "## Blockers",
        "",
        bullet_list(blockers + [f"First value: {item}" for item in first_value.get("blockers", [])]),
        "",
        "## Next Actions",
        "",
        bullet_list(next_actions(report)),
        "",
        "## Future AI Readiness",
        "",
        bullet_list(ai_readiness(report)),
        "",
        "_This dashboard is intentionally count/status oriented. It does not include customer names, phone numbers, SMS bodies, transcripts, raw lesson notes, or call summaries._",
        "",
    ]
    return "\n".join(lines)


def build_dashboard(db_path, window_days, pike13_lookahead_days):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        report = build_source_completeness_report(conn, window_days, pike13_lookahead_days)
        conn.commit()
    finally:
        conn.close()
    return render_dashboard(report)


def main():
    parser = argparse.ArgumentParser(description="Generate a Markdown lead-intelligence progress dashboard.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--pike13-lookahead-days", type=int, default=DEFAULT_PIKE13_LOOKAHEAD_DAYS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--print", action="store_true", dest="print_output")
    args = parser.parse_args()

    markdown = build_dashboard(args.db, args.window_days, args.pike13_lookahead_days)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    if args.print_output:
        print(markdown)
    else:
        print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
