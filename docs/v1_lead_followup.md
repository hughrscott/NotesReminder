# V1 Lead Follow-Up Timeline

V1 answers one management question: which leads are not being followed up properly, and what happened to them?

The implementation is additive. It keeps the existing notes, Dialpad call/transcript, Pike13 client, S3, and MCP architecture intact, then adds lead-follow-up tables, curated views, browser extraction scripts, and MCP tools.

## Database Setup

Create or refresh the V1 schema on the local SQLite database:

```bash
python3 lead_followup_schema.py --db reminders.db
```

The schema can be run repeatedly. It creates:

- `source_import_runs`
- `hubspot_deals`
- `hubspot_contacts`
- `hubspot_tasks`
- `hubspot_activities`
- `dialpad_sms_threads`
- `dialpad_sms_messages`
- `pike13_people`
- `pike13_visits`
- `pike13_plans_passes`
- `identity_matches`

It also creates these curated views:

- `vw_lead_timeline`
- `vw_stale_leads`
- `vw_unanswered_messages`
- `vw_no_show_followup`
- `vw_lead_conversion_path`

## Browser Extractors

All extractors use persistent Playwright profiles under `browser_profiles/`. First runs should be headed so Okta/MFA can be completed in the browser. Later runs can use `--headless` if the profile remains authenticated.

HubSpot visible deal/contact extraction:

```bash
python3 scripts/extract_hubspot_leads.py --db reminders.db --profile-dir browser_profiles/hubspot --limit 25 --detail-limit 10
```

Dialpad visible SMS thread/message extraction:

```bash
python3 scripts/extract_dialpad_sms.py --db reminders.db --profile-dir browser_profiles/dialpad --thread-limit 20
```

Dialpad visible voice history extraction:

```bash
python3 scripts/extract_dialpad_voice.py --db reminders.db --profile-dir browser_profiles/dialpad --views calls,missed,voicemails,recordings --limit-per-view 25
```

Pike13 linked person/outcome extraction from HubSpot-linked person IDs:

```bash
python3 scripts/extract_pike13_leads.py --db reminders.db --profile-dir browser_profiles/pike13-westu --base-url https://westu-sor.pike13.com --school "West U" --limit 25
```

Pike13 can also target explicit person URLs:

```bash
python3 scripts/extract_pike13_leads.py --db reminders.db --person-url https://westu-sor.pike13.com/people/15046380
```

Each extractor logs to `source_import_runs` with source, extractor, window, row counts, status, and errors.

## MCP Tools

The MCP server keeps the existing SQL tools and adds:

- `initialize_lead_followup_schema()`
- `stale_leads(school, days, limit)`
- `lead_timeline(search)`
- `unanswered_messages(school, days, limit)`
- `unanswered_communications(school, days, limit)`
- `no_show_followup(school, days, limit)`
- `lead_conversion_path(search)`

The communications views preserve full available message/transcript text in source tables for later LLM sentiment, intent, urgency, action-item, and outcome analysis. MCP tools should default to concise evidence, not full transcript dumps.

## Initial Load And Refresh Defaults

Default initial load window is January 1, 2025 forward where the source UI makes it practical.

Default refresh cadence is daily batch. Until reliable updated-date filters are proven in each UI, daily jobs should rescan recent rolling windows and rely on idempotent upserts.

## Commit Boundaries

Do commit reusable scripts, schema, tests, and sanitized documentation.

Do not commit:

- `browser_profiles/`
- `docs/discovery/evidence/`
- local SQLite databases
- raw screenshots or exports containing customer data
