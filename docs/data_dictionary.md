# NotesReminder Data Dictionary

## Status Labels

- `production`: Active in `reminders.db` and used by current operational workflows.
- `shadow`: Used for lead-intelligence proof or future reporting, not yet production source of truth.
- `derived`: Rebuildable from raw/source tables.
- `legacy`: Preserved for compatibility; not the preferred long-term read path.
- `future`: Planned but not implemented.

## Sensitivity Labels

- `low`: Counts, IDs, dates, school labels, and operational metadata.
- `medium`: Names, emails, phone numbers, source URLs, internal notes, and staff/customer linkage.
- `high`: SMS/email bodies, call transcripts, lesson note text, recordings, raw page captures, and AI summaries derived from raw customer content.

Broad dashboards and reports should be sanitized by default even when raw content is stored in the main production database.

## Production Tables In `reminders.db`

| Object | Status | Purpose | Source | Primary Key | Natural Key | Freshness | Sensitive Fields |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `reminders` | production, legacy | Flat production lesson-note source used by daily notes email and S3 sync. | Pike13 scrape via `run_daily.py`/`noteschecker.py`. | `id`; `lesson_id` unique. | `school`, `pike13_lesson_id`; fallback lesson type/date/time/students. | Daily/manual production notes run. | `students`, `notes_text`, `note_score_explanation`. |
| `call_logs` | production | Imported call log rows for callback/missed-call reporting. | Dialpad CSV import. | implementation-defined row id. | call timestamp, external number, direction/category. | Manual/import cadence. | names, phone numbers. |
| `call_client_matches` | production | Match evidence between calls and Pike13 client records. | `import_call_data.py`. | implementation-defined row id. | `call_id`, client identifier, match value. | Rebuilt with call/client import. | phone numbers, match values. |
| `pike13_clients` | production | Pike13 client CSV records used for matching. | Pike13 client CSV import. | implementation-defined row id. | Pike13 client/person ID, email, phone. | Manual client export/import. | names, emails, phones. |
| `dialpad_calls` | production | Raw imported Dialpad calls. | Dialpad CSV import. | implementation-defined row id. | Dialpad call ID or timestamp/number. | Manual/import cadence. | names, phone numbers. |
| `dialpad_voicemails` | production | Imported Dialpad voicemail metadata/transcription. | Dialpad CSV import. | implementation-defined row id. | voicemail/call ID, timestamp, number. | Manual/import cadence. | voicemail transcription, names, phones. |
| `dialpad_recordings` | production | Imported Dialpad recording metadata. | Dialpad CSV import. | implementation-defined row id. | recording/call ID. | Manual/import cadence. | recording URLs, names, phones. |
| `dialpad_daily_stats` | production | Dialpad daily aggregate stats. | Dialpad CSV import. | implementation-defined row id. | date/user/school. | Manual/import cadence. | low. |
| `dialpad_user_stats` | production | Dialpad user aggregate stats. | Dialpad CSV import. | implementation-defined row id. | user/date/window. | Manual/import cadence. | staff identifiers. |
| `recording_downloads` | production/shadow | Local recording download index and status. | Recording download scripts. | call/recording identifier depending on script version. | call ID or recording URL. | Recording download runs. | file paths, recording URLs. |
| `recording_transcripts` | production/shadow | Transcript and analysis metadata for downloaded recordings. | Whisper/OpenAI/AWS transcription scripts. | call/recording identifier depending on script version. | call ID or recording URL. | Transcription runs. | transcript text, summaries, action items, sentiment/intent. |

## Shadow Lead-Intelligence Tables

These now live in the promoted single `reminders.db` alongside production notes data. They remain marked `shadow` until their downstream dashboards are accepted for management decisions.

| Object | Status | Purpose | Source | Primary Key | Natural Key | Freshness | Sensitive Fields |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `source_import_runs` | shadow | Import run metadata and status. | All extractors/importers. | `run_id`. | source, start/end, status. | Every import run. | errors may include source details. |
| `hubspot_deals` | shadow | HubSpot deal/lead records. | HubSpot extractor. | `deal_id`. | HubSpot deal ID. | Lead refresh cadence. | deal names, owners, notes, source URLs. |
| `hubspot_contacts` | shadow | HubSpot contact records. | HubSpot extractor. | `contact_id`. | HubSpot contact ID, normalized email/phone. | Lead refresh cadence. | names, emails, phones. |
| `hubspot_tasks` | shadow | HubSpot follow-up tasks. | HubSpot extractor. | `task_id`. | HubSpot task ID. | Lead refresh cadence. | task subjects/notes, owner. |
| `hubspot_activities` | shadow | HubSpot timeline activities. | HubSpot extractor. | `activity_id`. | HubSpot activity ID. | Lead refresh cadence. | activity details. |
| `dialpad_sms_threads` | shadow | Dialpad SMS thread metadata. | Dialpad browser extractor. | `thread_id`. | thread ID or normalized phone/school. | Daily rolling window. | names, phone numbers. |
| `dialpad_sms_messages` | shadow | Dialpad SMS messages. | Dialpad browser extractor. | `message_id`. | source message ID/thread/time. | Daily rolling window. | SMS body, names, phones. |
| `dialpad_voice_events` | shadow | Dialpad calls, missed calls, voicemails, recordings. | Dialpad browser extractor. | `event_id`. | event/call ID or source URL. | Daily rolling window. | transcripts, summaries, recording URLs, phones. |
| `dialpad_call_reviews` | shadow | Dialpad call-review transcript/recap/action-item evidence. | Dialpad call-review extractor. | call-review URL or call ID. | `call_review_url`, `call_id`. | Call-review ingestion cadence. | transcripts, recaps, action items. |
| `dialpad_target_searches` | shadow | Targeted Dialpad search diagnostics. | Dialpad discovery/extractor scripts. | implementation-defined row id. | run ID, deal/contact target. | Discovery runs. | target hashes/source diagnostics. |
| `dialpad_route_discoveries` | shadow | Dialpad route-capability diagnostics. | Dialpad discovery scripts. | implementation-defined row id. | run ID, route name. | Discovery runs. | source URLs, diagnostic text. |
| `source_route_discoveries` | shadow | Generic route/source diagnostics. | Discovery scripts. | implementation-defined row id. | source, route, run ID. | Discovery runs. | diagnostic text/source URLs. |
| `school_email_messages` | shadow | School mailbox inbound/outbound messages. | Gmail/school email extraction. | `message_id`. | message/thread ID, mailbox. | Email refresh cadence. | subject, body, raw text/json, emails. |
| `pike13_people` | shadow | Rich Pike13 person/client records. | Pike13 authenticated extractor. | `person_id`. | Pike13 person ID. | Pike13 lead/outcome refresh. | names, emails, phones, source URLs. |
| `pike13_visits` | shadow | Pike13 visits/trials/attendance/no-shows. | Pike13 authenticated extractor and notes data. | visit ID or implementation-defined ID. | person ID, start time, service. | Pike13 lead/outcome refresh. | names, attendance details. |
| `pike13_plans_passes` | shadow | Pike13 plans/passes/membership evidence. | Pike13 authenticated extractor. | plan/pass ID or implementation-defined ID. | person ID, plan/pass identifier, date range. | Pike13 lead/outcome refresh. | payer, membership/payment-adjacent details. |
| `identity_matches` | shadow | Cross-system identity match evidence. | Lead schema/resolution scripts. | implementation-defined row id. | source/target IDs and match type. | Rebuilt with identity work. | names, emails, phones, match evidence. |
| `persons` | shadow | Resolved real-person identity hub across HubSpot, Pike13, Dialpad, and school email. | deterministic identity resolver. | `person_id`. | exact email, exact phone, HubSpot/Pike13 IDs. | Person identity refresh. | names, emails, phones, source linkage. |
| `person_identities` | shadow | Source IDs, emails, and phones linked to `persons`. | deterministic identity resolver. | `identity_id`. | person + identity type/value + source row. | Person identity refresh. | names, emails, phones, match evidence. |
| `person_resolution_conflicts` | shadow | Duplicate/ambiguous identity groups needing review. | deterministic identity resolver. | `conflict_id`. | conflict type + person IDs. | Person identity refresh. | identity evidence. |
| `communication_ai_insights` | experimental/shadow | Human-review communication insights with model/prompt/run metadata and auditable source links. | Phase 19 insight generation scripts. | source table + source ID + model + prompt version. | source event ID, model, prompt version, insight run ID. | Insight runs. | summaries, sentiment, recommendations, evidence pointers, model output metadata. |
| `raw_captures` | shadow | Local-only index of raw extractor payload files for replay and parser regression checks. | authenticated extractors. | `capture_id`; SHA-256 hash. | Extractor runs; 90-day local retention. | high: raw source payload pointers and metadata. |

## Shadow Lead-Intelligence Views

| Object | Status | Purpose | Source Tables | Freshness | Sensitivity |
| --- | --- | --- | --- | --- | --- |
| `vw_dialpad_communications` | derived, shadow | Unified Dialpad SMS/voice event stream. | Dialpad SMS and voice tables. | Recreated by schema setup. | medium/high depending selected columns. |
| `vw_dialpad_daily_intake` | derived, shadow | Daily Dialpad intake with match/follow-up flags. | `vw_dialpad_communications`, HubSpot, Pike13. | Recreated by schema setup. | medium. |
| `vw_unmatched_dialpad_inbound` | derived, shadow | Inbound Dialpad rows without trusted HubSpot match. | `vw_dialpad_daily_intake`. | Recreated by schema setup. | medium. |
| `vw_school_email_communications` | derived, shadow | Unified school email communication stream. | `school_email_messages`. | Recreated by schema setup. | medium/high depending selected columns. |
| `vw_pike13_lesson_visits` | derived, shadow | Pike13 lesson visit view from notes/reminders. | `reminders`. | Recreated by schema setup. | medium. |
| `vw_lead_timeline` | derived, shadow | Cross-system lead timeline. | HubSpot, Dialpad, school email, Pike13. | Recreated by schema setup. | medium/high. |
| `vw_stale_leads` | derived, shadow | Leads needing follow-up based on task/touch recency. | HubSpot. | Recreated by schema setup. | medium. |
| `vw_unanswered_communications` | derived, shadow | Inbound SMS/missed calls/voicemails without later outbound follow-up. | Dialpad views. | Recreated by schema setup. | medium. |
| `vw_unanswered_messages` | derived, shadow | Inbound SMS without later outbound follow-up. | `vw_unanswered_communications`. | Recreated by schema setup. | medium. |
| `vw_no_show_followup` | derived, shadow | Pike13 no-shows with follow-up context. | Pike13 visits, people, HubSpot. | Recreated by schema setup. | medium. |
| `vw_lead_conversion_path` | derived, shadow | Lead-created to trial/enrollment path. | HubSpot, Pike13. | Recreated by schema setup. | medium. |
| `vw_person_journey` | derived, shadow | Chronological event stream by resolved `person_id`. | persons, HubSpot, Dialpad, school email, Pike13. | Recreated by schema setup. | medium/high. |

## Derived Reporting Schema Targets

These are rebuildable reporting tables/views. They should be additive until production reads are explicitly migrated.

| Object | Status | Purpose | Source | Primary Key / Natural Key | Sensitivity |
| --- | --- | --- | --- | --- | --- |
| `schools` | derived | School dimension and display labels. | `reminders` and source data. | school code/name. | low. |
| `instructors` | derived | Instructor dimension. | `reminders`. | school + instructor name. | low/medium. |
| `students` | derived | Student dimension. | `reminders`. | school + student name; future `person_id`. | medium. |
| `lessons` | derived | Normalized lesson facts. | `reminders`. | `lesson_id`, `pike13_lesson_id`. | medium. |
| `lesson_students` | derived | Lesson-to-student relationship, with nullable resolved `person_id` where exact identity is available. | `reminders`, `persons`. | lesson ID + student ID/name. | medium. |
| `lesson_notes` | derived | Note completion, note text, timestamps. | `reminders`. | lesson ID. | high. |
| `lesson_attendance` | derived | Attendance status by lesson. | `reminders`. | lesson ID. | medium. |
| `lesson_note_scores_history` | derived | Historical note-score changes. | note scoring pipeline. | lesson ID + score timestamp/hash. | high. |
| `vw_missing_notes_by_instructor` | future/derived | Missing-note counts by instructor/window. | lesson reporting tables. | school + instructor + window. | low/medium. |
| `vw_note_completion_rate` | future/derived | Note completion rate by school/instructor/window. | lesson reporting tables. | school + instructor + window. | low/medium. |
| `vw_missing_notes_by_school_day` | future/derived | Missing-note counts by school/date. | lesson reporting tables. | school + date. | low. |
| `vw_note_quality_league_table` | future/derived | School/instructor note-quality league table. | lesson notes/scores and reportable filter. | school + instructor + window. | low/medium. |
| `vw_callback_speed` | future/derived | Callback speed from missed calls/voicemails. | Dialpad tables/views. | school + event/window. | medium. |
| `vw_churn_candidates` | future/derived | Churn-risk candidates from lesson cadence rules. | lessons/attendance/communications. | person/student + risk window. | medium. |

## Future Identity, Journey, Raw Capture, And Financial Objects

| Object | Status | Purpose | Source | Primary Key / Natural Key | Sensitivity |
| --- | --- | --- | --- | --- | --- |
| `qb_customers` | future | QuickBooks customer records. | QuickBooks. | QB customer ID. | medium. |
| `qb_invoices` | future | QuickBooks invoices. | QuickBooks. | QB invoice ID. | medium/high financial. |
| `qb_payments` | future | QuickBooks payments. | QuickBooks. | QB payment ID. | medium/high financial. |
| `qb_subscriptions` | future | Subscription/plan lifecycle from QuickBooks. | QuickBooks. | QB subscription/customer/date. | medium/high financial. |

## Business Rule: Reportable Lessons

The current reportable-lesson rule is intentionally the same rule used by the production notes reminder workflow.

A lesson is excluded from note-quality league-table scoring when any of these are true:

- `lesson_type` contains `admin`.
- `lesson_type` contains `meeting`.
- `students` contains a comma, meaning the row is a multi-student/group lesson.
- `instructor_name` has no alphabetic character.
- `instructor_name` contains `admin`.
- `instructor_name` contains `trial`.
- `instructor_name` contains `rookies`.
- The instructor name is blank.

Current decision:

- Use this reportable-lesson filter for note-quality league tables.
- Do not include group lessons or multi-student lessons for now.
- Revisit later if a separate group-lesson league view becomes useful.

## Business Rule: Note-Quality League Score

For a selected date window, school, and optionally instructor:

1. Build the reportable lesson set.
2. Each reportable lesson with no completed/scored note contributes `0`.
3. Each reportable lesson with a completed scored note contributes `note_score / 10`.
4. Sum all components into `score_sum`.
5. Divide by total reportable lessons and multiply by 100:

```text
league_score = SUM(score_component) / reportable_lessons * 100
```

Output fields:

- rank
- school
- instructor, when ranking instructors
- reportable lessons
- lessons with notes
- missing notes
- completion rate
- score sum
- league score
- average note score among scored notes

Ranking:

- Sort by `league_score` descending.
- Use reportable lesson count descending as the first tie-breaker.
- Use instructor/school name ascending as the final deterministic tie-breaker.

Delivery decision:

- League tables live in dashboards/MCP for now.
- Do not add league tables to recurring emails yet.
- Revisit email delivery after the dashboard version is accepted.

## Data Ownership

- `reminders` and normalized lesson-note tables: production notes pipeline owner.
- Dialpad imported CSV tables: call import pipeline owner.
- Dialpad browser intake/call-review tables: lead intelligence intake owner until production merge.
- HubSpot tables: lead intelligence intake owner until production merge.
- Pike13 rich outcome tables: lead intelligence intake owner until production merge.
- School email tables: lead intelligence intake owner until production merge.
- Recording downloads/transcripts: recording pipeline owner.
- Raw captures: extractor owner, with retention and sensitivity rules defined before production use.
- QuickBooks tables: future financial pipeline owner.
- Dashboards, scorecards, and MCP views: reporting owner, sourced only from accepted tables/views for production decisions.
