# HANDOFF: SOR Pike13 Date-Bounded Reports → OS Spine

**Status:** working pipeline + verified cross-source join (built 2026-07-16).
**Where:** `~/projects/hughrscott/NotesReminder/` on the Linux box.
**DB:** `reminders.db` (SQLite, in that dir).

---

## WHAT IS VERIFIED WORKING (do NOT re-derive)

### 1. The date-bounded data path
The only Pike13 path that honors a date window is the **per-report detail view**, reached via an address-bar hash (NOT a query param on the API):

```
/desk/reports#/people/details?filters=(<FIELD>:!((btw:!('<YYYY-MM-DD>','<YYYY-MM-DD>'))))&sort=(col:last_membership_end,order:d)&hide=(minor:!t,empty:!t)
```

That fires: `GET /desk/api/v3/reports/clients/queries?auth_token=<tok>&subdomain=<sub>`

Response shape:
```json
{ "data": { "attributes": { "rows": [[...]], "total_count": N, "has_more": false, "fields": [{"name":"full_name","type":"string"}, ...] } } }
```
Row fields (in order): `full_name, email, phone, address, current_plans, person_id` (person_id = **8-digit numeric**, e.g. 15181220).

**The top-level Insights KPI dashboard has NO date control** — hard-coded windows only. Ignore it for date-bounded pulls.

### 2. Verified date-filter FIELD TOKENS (probed live)
| token | signal | Jul15–Aug11 rows |
|---|---|---|
| `last_membership_end` | memberships *ending* in window | 4 |
| `last_visit_date` | *last visit* in window | 43 |
| `first_visit_date` | *first visit* in window | 0 (valid; none in-window) |

### 3. INVALID as date filters (silently return the ENTIRE 5,154-client base regardless of window — DO NOT USE)
`created_at`, `client_created_at`, `expiry_date`, `membership_start`, `first_membership_start`, `last_membership_start`

### 4. The JOIN BRIDGE (critical — two different ID spaces)
- `/queries` `person_id` (8-digit numeric) matches `pike13_people.person_id` **directly** — no name-bridge needed *within* Pike13 tables.
- But `pike13_people.person_id` (numeric) ≠ the unified `persons.person_id` (which is `person_XXXX`).
- To reach Dialpad/HubSpot/School-Email you MUST bridge:
  `pike13_report_pulls.person_id` → `pike13_people.person_id` → `pike13_people.person_identity_id` (= `person_XXXX`) → `persons.person_id`
- Dialpad call tables do NOT have `person_id` directly: route through `dialpad_sms_threads.person_id` and `dialpad_voice_events.person_id`.
- The 23-char Pike13 Client hash on `/api/v2` paths is a THIRD id space (join via NAME) — separate concern.

### 5. MFA auth caveat
Auth is **flaky under rapid back-to-back sessions** (can stick at `/accounts/sign_in`). Use `pull_reports_batch` (authenticate ONCE, pull many fields) — never loop single `pull_report` calls. MFA code read from `huscott@schoolofrock.com` via `pike13_auto_auth.authenticate_pike13`.

---

## FILE INVENTORY (what each does)
- `pike13_report_puller.py` — `pull_report()` / `pull_reports_batch()`. The reusable puller. Auth + URL-hash nav + `/queries` intercept + parse.
- `run_retention_pull.py` — pulls the 3 verified fields in ONE session, upserts into `pike13_report_pulls` (PK `pull_id,person_id,field`), logs `source_import_runs`. **One MFA cycle.**
- `build_cross_source_views.py` — builds views `vw_pull_spine` + `vw_pull_cross_source` (Pike13 pull × unified hub × Dialpad/HubSpot/School-Email), with honest `os_status` flag.
- `verify_datefilter_url.py`, `probe_fields.py`, `probe_fields2.py`, `discover_report_fields.py` — discovery harness (one-offs, keep for reference).
- `models/pike13_fields_probed.json`, `pike13_fields_probed2.json`, `pike13_report_verify_*.json` — evidence.

---

## CURRENT DB STATE (as of this session)
- `pike13_report_pulls`: pull_id 1 (4 rows, old schema, `field_label` NULL) + pull_id 2 (47 rows: last_visit 43, membership_ending 3, first_visit 0). 46 distinct people in pull 2.
- `vw_pull_cross_source` status: `no_unified_identity` 29 · `pike13_only__no_cross_touch` 16 · `has_cross_source` 1.
- **Key gap:** only 17/46 pulled Pike13 clients reach the `persons` hub at all → the rest of the OS spine (Dialpad/HubSpot ingestion) is **partially populated in this snapshot**. A cross-source join today mostly surfaces empties = missing upstream ingestion, NOT a broken join.

---

## REMAINING WORK (pick up in a new session)
Priority order:

1. **[Blocker for real value] Diagnose why 29/46 active Pike13 clients aren't in `persons`.** Is it a missed identity-match pass? Run the match logic (email/phone normalization) on the pulled `person_id`s vs `persons`. Until `persons` is current, cross-source plays are tiny.
2. **Refresh the pull on a live window** (e.g. weekly cadence) and confirm accumulation across pull_ids. Consider a cron (Mon AM CT) using `run_retention_pull.py`.
3. **Snapshot the dashboard KPI cards** (hard-coded 7-day numbers) into the DB too, so the OS has both date-bounded detail AND headline metrics. Hint: the KPI cards are separate from `/api/insights/widgets/*` (those need a filter to return data).
4. **Turn `pike13_only__no_cross_touch` (16 names) into an export** for Vivian/GM — CSV or Telegram-ready. These are engaged Pike13 clients with zero other-channel footprint.
5. **Extend to Heights SOR** (`heights-sor` subdomain) — same code, different `--school`. Verify the field tokens hold there too.
6. **Persist these facts to memory** (the save looped this session — retry as a single replace on the SOR v13 memory entry; don't re-add as new entries or it overflows 2,200 chars).

---

## OPEN CAVEATS
- Memory NOT saved this session (tool looped). The above facts live only in this doc + the scripts until a retry lands.
- `first_visit_date` returns 0 in Jul–Aug windows; widen the window (e.g. trailing 90d) to capture trial→conversion.
- `membership_ending` count drifted 4→3 between pulls (a boundary date likely shifted) — expected for live data, not a bug.
