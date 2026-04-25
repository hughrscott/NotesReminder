# Authenticated Discovery Evidence Pack

Discovery window: January 1, 2025 forward
Refresh target: daily batch
Safety boundary: view/export only; no creates, edits, deletes, sends, or status updates.

## Current Baseline

- Local `reminders.db` has been synced from S3.
- Git checkpoint: `checkpoint-before-authenticated-discovery-20260425`
- S3 rollback copy: `s3://notesreminder-db/backups/reminders-before-authenticated-discovery-20260425.db`
- Lessons current through: `2026-04-18`
- Dialpad call logs current through: `2026-01-16`
- Recording transcripts current through: `2026-01-21`

## HubSpot Source Map

### Pages And Reports Inspected

| Area | URL / UI path | Evidence file | Notes |
| --- | --- | --- | --- |
| Reports dashboard | `https://app.hubspot.com/home-beta` | `docs/discovery/evidence/hubspot_20260425T191335Z.png` | Authenticated persistent profile confirmed; page title `Reports dashboard`. |
| School Dashboard Homepage | `https://app.hubspot.com/reports-dashboard/6841203/view/12432365` | `docs/discovery/evidence/hubspot_hubspot_dashboard_authenticated_20260425T191920Z.json` | Shows tasks due, overdue tasks, current deal stages, leads over time, PPC reports, closed-lost reason, and shortcut links. |
| Tasks | `https://app.hubspot.com/tasks/6841203/view/all` | `docs/discovery/evidence/hubspot_hubspot_tasks_all_20260425T192027Z.json` | Task object exists with views for All, Due today, Overdue, New Leads, Upcoming; current default view showed 0 rows due assigned-to filter. |
| Deals board | `https://app.hubspot.com/contacts/6841203/objects/0-3/views/all/board` | `docs/discovery/evidence/hubspot_hubspot_deals_board_all_20260425T192115Z.json` | Primary lead object appears to be Deals in `Lead Pipeline`; 6,430 deals visible. |
| Deal detail | HubSpot deal record `0-3/59434765469` | `docs/discovery/evidence/hubspot_hubspot_deal_detail_waiting_on_us_followup_needed_20260425T192231Z.json` | Representative `Waiting On Us` lead with follow-up needed, task history, email activity, stage movement, Bridge notes, contact association, and Pike13 link. |
| Contact detail | HubSpot contact record `0-1/217146040659` | `docs/discovery/evidence/hubspot_hubspot_contact_detail_associated_student_20260425T192357Z.json` | Representative student contact with SMS opt-in, lead status, contact owner, associated deal, school association, and Pike13 link. |
| Report drilldown/export | TBD | TBD | TBD |
| Report drilldown | Dashboard report detail route `/145896734` | `docs/discovery/evidence/hubspot_hubspot_current_deal_stages_past_4_weeks_drilldown_20260425T194328Z.json` | Report detail confirms filters, report summary, summarized/unsummarized tabs, and report count. |
| Report unsummarized dataset | Same report detail route | `docs/discovery/evidence/hubspot_hubspot_current_deal_stages_unsummarized_dataset_20260425T194433Z.json` | Unsummarized dataset for this chart is grouped report data, not row-level deal records. |
| Deals list export check | `https://app.hubspot.com/contacts/6841203/objects/0-3/views/all/list` | `docs/discovery/evidence/hubspot_hubspot_deals_list_all_for_export_20260425T195057Z.json` | List view exposes table rows and columns; Export control is visible but disabled in current session/view. |
| Tasks | TBD | TBD | TBD |

### Fields Available

| Object | Field label | Internal name if visible | Exportable? | Needed for v1? | Notes |
| --- | --- | --- | --- | --- | --- |
| Deal | Deal stage | TBD | TBD | Yes | Lead pipeline state |
| Deal | Create date | TBD | TBD | Yes | Initial load and lead age |
| Deal | School Name - Deal | TBD | TBD | Yes | School attribution |
| Deal | Follow Up Needed | TBD | TBD | Yes | Queue signal |
| Deal | Trial Date (Deal) | TBD | TBD | Yes | Trial scheduling and conversion path |
| Deal | Trial No Show | TBD | TBD | Yes | No-show outcome signal |
| Deal | Date Entered Scheduled Trial Stage | TBD | TBD | Yes | Funnel timing |
| Deal | Last Contacted | TBD | TBD | Yes | Follow-up age |
| Deal | Last Activity Date | TBD | TBD | Yes | Staleness |
| Deal | Area of Interest | TBD | TBD | Yes | Program interest |
| Deal | Instrument Type | TBD | TBD | Yes | Lead detail and matching context |
| Deal | Lead Source - Deal | TBD | TBD | Yes | Attribution |
| Deal | Marketing Source - Deal | TBD | TBD | Yes | Attribution |
| Contact | Phone / mobile | TBD | TBD | Yes | Dialpad matching |
| Contact | Email | TBD | TBD | Yes | Pike13 matching |
| Contact | SMS opt In | TBD | TBD | Yes | Determines SMS follow-up constraints |
| Contact | Contact owner / Contact Owner 2 | TBD | TBD | Yes | Accountability |
| Contact | School Lead Status | TBD | TBD | Yes | SoR Lead Manager state |
| Contact | Form Notes and Availability | TBD | TBD | Yes | Lead context |
| Task | Due date / status / owner | TBD | TBD | Yes | Accountability |
| Activity | Deal stage movement | TBD | TBD | Yes | Stage history is visible in activity timeline |
| Activity | Bridge notes | TBD | TBD | Yes | Bridge writes forced-stage reasons such as Trial Booked and Trial No-Show |

### Load And Refresh

- Earliest reliable load date: TBD
- Initial load approach: likely Deals + Contacts + Tasks + Activities from `2025-01-01`; browser table extraction is viable, but UI export is not yet confirmed because the Deals export button was disabled.
- Daily incremental approach: likely rescan deals/tasks/activities changed since prior run, or recent rolling window if browser-only export lacks modified-date filtering.
- Export/browser/API gaps: report unsummarized datasets may be aggregated rather than raw objects; Deals export appears disabled in inspected views; internal property names not yet captured.

### Matching Keys

- HubSpot deal ID: visible in URLs as object type `0-3`, record id such as `59434765469`.
- HubSpot contact ID: visible in URLs as object type `0-1`, record id such as `217146040659`.
- Phone/email: visible on contact and deal-associated contact records.
- School/location: visible in deal name, `School Name - Deal`, contact `School Lead Status`, and associated School object.
- Bridge/Pike13 IDs: direct Pike13 person link visible, e.g. `/people/15046380`; Bridge integration writes notes and SoR Lead Manager state.

## Dialpad Source Map

### Pages And Reports Inspected

| Area | URL / UI path | Evidence file | Notes |
| --- | --- | --- | --- |
| History - Messages | `https://dialpad.com/app/history/messages` | `docs/discovery/evidence/dialpad_dialpad_after_login_20260425T192839Z.json` | Authenticated; message list shows unread count, contacts/numbers, snippets, dates, and message counts. |
| History - All | `https://dialpad.com/app/history/all` | `docs/discovery/evidence/dialpad_dialpad_history_all_20260425T192936Z.json` | Mixed communication history shows SMS snippets plus call outcomes such as `Caller hung up` and `Voicemail`. |
| History - Missed | `https://dialpad.com/app/history/missed` | `docs/discovery/evidence/dialpad_dialpad_history_missed_20260425T193012Z.json` | Missed-call view visible; current list shows historical missed calls and missed-call-with-voicemail labels. |
| Voicemails | `https://dialpad.com/app/history/voicemails` | `docs/discovery/evidence/dialpad_dialpad_history_voicemails_20260425T193050Z.json` | Voicemail list exposes caller, duration, date, and full transcription text in UI. |
| Message thread detail | Dialpad feed URL with opaque contact/profile IDs | `docs/discovery/evidence/dialpad_dialpad_sms_thread_please_text_me_20260425T195334Z.json` | Thread detail exposes full SMS body, date, time, phone, unknown-contact status, contact profile pane, media/link sections, and Zendesk panel. |
| Recordings | TBD | TBD | Existing CSV/browser download pipeline exists; UI page still needs source-map capture. |
| Department filters | Left nav on Dialpad history pages | Same Dialpad captures | Departments `HEIGHTS` and `WESTU` visible; need verify whether filters affect exports/API/browser capture. |

### Fields Available

| Object | Field label | Exportable? | Needed for v1? | Notes |
| --- | --- | --- | --- | --- |
| SMS thread | Contact/name/phone | TBD | Yes | Visible in Messages and All views. |
| SMS thread | Snippet/body preview | TBD | Yes | Visible in list; full thread detail still needs capture. |
| SMS thread | Last message date | TBD | Yes | Visible in Messages and All views. |
| SMS thread | Unread/message count | TBD | Yes | Visible as count badges. |
| SMS message | Timestamp | TBD | Yes | Need thread detail capture. |
| SMS message | Direction | Browser UI TBD | Yes | Thread detail shows individual messages; direction may require DOM inspection or visual side/layout, not plain text alone. |
| SMS message | Full body | Browser UI | Yes | Captured in thread detail. |
| SMS message | Department/team | TBD | Yes | Departments visible; filter effect TBD. |
| Call | Disposition/hangup reason | TBD | Yes | `Caller hung up`, `Missed call`, `Missed call & voicemail`, and `Voicemail` are visible. |
| Voicemail | Duration/date/transcript | TBD | Yes | Visible in Voicemails UI. |

### Load And Refresh

- Earliest reliable load date: at least `2025-01-01` for call/voicemail list views; Messages UI shows older history but full reliable SMS depth is TBD.
- Initial load approach: use existing CSV import for calls/voicemails/recordings where possible; SMS can be browser-extracted from Messages/thread pages if export/API is unavailable.
- Daily incremental approach: likely rescan recent Messages/All pages and import new calls from CSV/export/API; exact export path still TBD.
- Export/browser/API gaps: message export capability not found in the inspected UI; delivery/read status, direction, and department filter behavior still need verification.

### Matching Keys

- Phone number: visible in list views and already normalized in current call import.
- Dialpad contact/thread ID: feed URL and list links contain opaque contact/profile IDs that can function as source identifiers if stable.
- Call ID / master call ID: available in existing CSV imports; not visible in list captures.
- Department/school: `HEIGHTS` and `WESTU` visible in left navigation.

## Pike13 Source Map

### Pages And Reports Inspected

| Area | URL / UI path | Evidence file | Notes |
| --- | --- | --- | --- |
| Pike13 login/interstitial | `https://westu-sor.pike13.com/two_factor/offer` | `docs/discovery/evidence/pike13-westu_pike13_westu_landing_20260425T193356Z.json` | Authenticated session hit 2FA offer; `Skip for Now` is visible, matching existing scraper behavior. |
| Client profile | `https://westu-sor.pike13.com/people/15046380` | `docs/discovery/evidence/pike13-westu_pike13_westu_person_from_hubspot_20260425T193456Z.json` | HubSpot direct Pike13 link resolved to same person; page shows no membership, upcoming trial, contact info, plans/passes, bills, visits, sent emails, and profile actions. |
| Trial lesson/event | `https://westu-sor.pike13.com/e/292297814` | `docs/discovery/evidence/pike13-westu_pike13_westu_trial_event_from_person_20260425T193549Z.json` | Event page shows Adult Band Trial, date/time, roster, attendance, unpaid/waiver flags, no-show state, instructor/service, and visit IDs. |
| Plans/passes | `https://westu-sor.pike13.com/people/15046380/balances` | `docs/discovery/evidence/pike13-westu_pike13_westu_person_plans_passes_20260425T193733Z.json` | Tabs for Active, Upcoming, Inactive; representative lead has none. |
| Visits/history | `https://westu-sor.pike13.com/people/15046380/visits` | `docs/discovery/evidence/pike13-westu_pike13_westu_person_past_visits_20260425T193812Z.json` | Complete visit history shows filters All, Complete, Unpaid, No Show, Incomplete and confirms trial visit outcome. |
| Insights | `https://westu-sor.pike13.com/desk/reports#/` | `docs/discovery/evidence/pike13-westu_pike13_westu_reports_menu_after_login_20260425T200845Z.json` | Shows operational KPIs for collected/refunded revenue, new clients, first visits, first memberships, last visits, expiring memberships, unconfirmed attendance, unpaid visits, no-shows, late cancellations. |
| Clients & Staff report catalog | Pike13 reports `Clients & Staff` | `docs/discovery/evidence/pike13-westu_pike13_westu_reports_clients_staff_20260425T200957Z.json` | Confirms report families: Clients, Client Passes & Plans, Enrollments, Pass and Plan Conversions, Schedule, Staff Member Schedule, Staff Members, with quick views. |
| The Heights public/auth check | `https://theheights-sor.pike13.com/offerings` / sign-in | `docs/discovery/evidence/pike13-heights_pike13_heights_landing_authenticated_20260425T195619Z.json` | The Heights profile was not authenticated during this pass; needs login for true second-school spot-check. |
| Client export | TBD | Existing CSV import code and future UI capture | Need verify export filters/columns in Pike13 Clients or Reports. |
| Tasks/follow-ups/leads | TBD | TBD | TBD |

### Fields Available

| Object | Field label | Exportable? | Needed for v1? | Notes |
| --- | --- | --- | --- | --- |
| Client | Client ID / person ID | Existing CSV and URL | Yes | Stable Pike13 key; URL person id observed as `15046380`. |
| Client | Customer ID | Existing CSV | Yes | Possible payment/account key |
| Client | Lead source / marketing source | Existing CSV | Yes | Attribution |
| Client | Email / phone | Existing CSV and UI | Yes | Deterministic match to HubSpot/Dialpad. |
| Client | Membership state | TBD | Yes | UI shows `No Membership`; export/report path TBD. |
| Trial | Booked/completed/no-show | UI, export TBD | Yes | Event and visit pages show `No Show` for representative lead. |
| Trial | Event ID / visit ID | UI | Yes | Event URL and visit links expose stable IDs. |
| Trial | Waiver/unpaid status | UI | Maybe | Useful operational context. |
| Plan/pass | Name/start/end/status | UI, export TBD | Yes | Plans & Passes has Active/Upcoming/Inactive tabs. |
| Task/follow-up | Due/status/owner | TBD | Maybe | If available |

### Load And Refresh

- Earliest reliable load date: current lesson scrape and client CSV cover `2025-01-01` forward; richer visits/plans export depth still TBD.
- Initial load approach: keep existing lesson scrape/client CSV import, add visits/trials/plans source once export/report path is verified.
- Daily incremental approach: likely daily scrape recent schedule/events plus refresh client export; plans/visits may require report/export or per-person pages for changed leads.
- Export/browser/API gaps: Client export columns, visits report, plans/pass report, and lead/task availability still need authenticated report-menu discovery.

### Matching Keys

- Pike13 Client/person ID: visible in URL, e.g. `15046380`.
- Customer ID: available in existing CSV; UI verification TBD.
- Phone/email/guardian email: visible on person page and existing CSV.
- Student/guardian name + school: visible on person/event pages.
- Trial dates: visible in HubSpot deal, Pike13 person dashboard, event page, and visit history.
- Event/visit IDs: visible in Pike13 URLs and event roster links.

## Unified Model Notes

Candidate tables/views:

- `people`
- `leads`
- `touchpoints`
- `tasks`
- `trial_events`
- `enrollment_outcomes`
- `source_import_runs`
- `vw_stale_leads`
- `vw_unanswered_messages`
- `vw_lead_followup_timing`
- `vw_lead_conversion_path`
- `vw_school_management_scorecard`

## Open Decisions

- Whether browser exports are reliable enough for each source.
- Whether daily refresh can be incremental by updated timestamp or must rescan recent windows.
- How far back Dialpad SMS can be loaded.
- Whether Bridge/Pike13 IDs exist in HubSpot and can be used as deterministic joins.
