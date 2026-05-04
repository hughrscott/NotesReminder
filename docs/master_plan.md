# Master Plan: Notes Continuity To Lead Intelligence

## Summary

For the next few days, run the existing Notes Reminder process manually with MFA using a persistent Pike13 browser profile. In parallel, continue the lead-intelligence work in phases. The end goal is a trusted MCP-backed operating system for the schools: reliable daily data intake, evidence-backed lead follow-up reports, management scorecards, and eventually LLM-assisted coaching and lead-management automation.

At the end of every phase, check in, review what actually happened, compare it against this plan, and revise the next phases if the evidence says we should.

## Phase 1: Keep Production Notes Running

- Use local/manual Notes Reminder runs while Pike13 MFA blocks GitHub Actions.
- Use one shared persistent Pike13 browser profile so one MFA session can cover both Pike13 accounts while the session remains valid.
- Run both schools through `scripts/run_notes_local_mfa.sh`, which creates local and S3 backups, runs the normal scrape/email/S3 sync, and keeps the existing daily email content unchanged.
- Keep GitHub Actions from running the broken Pike13 login path until there is a stable automation replacement.
- Do not use Gmail app passwords or automated mailbox access for MFA codes.

## Phase 2: Checkpoint Current Code

- Commit the existing work in logical chunks: Pike13 MFA/manual-run reliability, then Dialpad intake/unmatched inbound/dashboard/reporting.
- Leave unrelated `package.json` and `package-lock.json` untouched unless confirmed intentional.
- Do not commit `reminders.db`, browser profiles, screenshots, generated reports, backups, exports, or raw customer evidence.
- Push to GitHub after the code checkpoint.

## Phase 3: Finish Dialpad Daily Intake

- Keep Dialpad/HubSpot/Pike13 lead proof work in `outputs/lead_intelligence/lead_intelligence_working.db` until the production merge gate passes.
- Rebuild the lead working DB from the latest production `reminders.db` after each successful notes run so lesson-note evidence remains current.
- Use Dialpad Conversation History as the main intake route.
- Default daily refresh is a 2-day rolling window; proof/backfill starts at 7 days.
- Capture calls, missed calls, voicemails, recordings, call-review URLs, AI transcripts/recaps, and SMS where available.
- Preserve source timestamps, direction, participant phone/contact key, school/department, source URL, raw text, transcript/recap availability, and durable call-review/audio URLs.
- Generate the progress dashboard, lead-attention report, unmatched inbound report, and route discovery report if extraction fails.
- Acceptance: daily Dialpad intake finds both matched lead communications and unmatched inbound communications that may represent leads missing from HubSpot.

## Phase 4: Pike13 Lead Outcome Hardening

- Keep using existing `reminders` lesson visits for current-student operations and note-quality insight.
- Add richer Pike13 lead/outcome data: people, trials, visits, no-shows, cancellations, plans/passes, enrollment/churn/hold timestamps.
- Link Pike13 outcomes back to HubSpot leads using Pike13 person ID, email, phone, and school.
- Acceptance: lead timelines can show lead created, contacted, trial booked, trial attended/no-showed, enrolled/lost, and follow-up evidence.

## Phase 5: Controlled Backfill

- Widen only after the 7-day proof is trustworthy: 7 days, 30 days, April 1 2026 forward, then January 1 2025 forward.
- Before each widening, create local and S3 DB backups, run source completeness, and review dashboard blockers.
- Keep backfill in the lead working DB until production notes, lead reports, Dialpad intake, and Pike13 outcome readiness pass the merge gate.
- Acceptance: each wider window preserves source timestamps, matching quality, and report usefulness without corrupting the production notes workflow.

## Phase 6: First Business Value Reports And MCP Tools

- Promote local reports into MCP-backed workflows: stale leads, lead timeline, unmatched inbound, unanswered communications, no-show follow-up, and conversion path.
- Keep reports sanitized by default: counts, IDs, timestamps, statuses, risk reasons, and source links, not raw customer content.
- Acceptance: the system can answer, with evidence, which leads need attention today and why.

## Phase 7: LLM Insight Layer

- Once data quality is trusted, use LLMs on stored source text for sentiment, intent, urgency, call quality, follow-up quality, missed opportunity detection, and coaching recommendations.
- Store LLM outputs separately from raw source data with model/version/run metadata.
- Keep source evidence linked so every recommendation can be audited.
- Acceptance: recommendations are explainable and trace back to HubSpot, Dialpad, Pike13, or lesson-note evidence.

## Phase 8: Management Scorecards And AI Lead Management

- Build school and staff scorecards for first response time, stale leads, unanswered inbound, no-show recovery, trial-to-enrollment rate, follow-up consistency, and note quality.
- Add human-reviewed AI recommendations first.
- Only later consider automation such as drafted follow-ups, task creation, or lead-management actions.
- No autonomous customer-facing action without explicit approval and guardrails.

## Phase-End Check-In

- At the end of each phase, summarize what was completed, what failed or changed, and what the dashboard/report evidence says.
- Decide whether to proceed, repeat, narrow, widen, or revise the plan.
- Create a code/data checkpoint where appropriate.
- Treat this master plan as a living plan, not a contract to keep going if the data says to adjust.

## Assumptions And Defaults

- `reminders.db` remains the production source of truth and syncs to S3.
- `outputs/lead_intelligence/lead_intelligence_working.db` is the temporary local lead proof DB and is not committed or uploaded to the production S3 key.
- Lead intelligence must merge with production before production use because lesson notes are key performance indicators.
- Manual Pike13 MFA runs are the correct short-term answer.
- Browser profiles are local operational state and are not committed.
- Existing daily/weekly notes emails remain unchanged until there is a separate accepted plan to add new reporting.
- Dialpad is the immediate blocker for first lead value.
- Pike13 rich outcomes follow Dialpad daily intake.
- Raw customer content can live in the DB for analysis, but dashboards and reports stay sanitized by default.
