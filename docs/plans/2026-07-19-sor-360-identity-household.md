# SOR 360 Identity and Household Foundation Implementation Plan

> **For Hermes:** Execute this plan through the dedicated `sor-360-identity` Kanban board. Implementation cards use TDD; PM cards own architecture and quantitative gates. Do not merge ambiguous identities automatically.

**Goal:** Build a durable, household-aware identity spine that links Pike13, HubSpot, lessons, Dialpad, and school email without collapsing siblings, guardians, payers, or students who share contact details.

**Architecture:** Pike13 people are canonical person anchors. Shared email addresses, phone numbers, addresses, and guardian/payer records become many-to-many contact points and household evidence—not automatic person-union keys. Direct source identifiers remain authoritative; ambiguous candidates enter an explicit review queue with evidence and reason codes.

**Tech Stack:** Python 3.11, SQLite, existing `lead_followup_schema.py`, `notesreminder/lib/person_identity.py`, pytest, Hermes Kanban (`pm` + `backend` profiles).

---

## 1. Verified starting point

Measured against `/home/ubuntu/projects/hughrscott/NotesReminder/reminders.db` on 2026-07-19:

| Metric | Current |
|---|---:|
| Pike13 people | 1,067 |
| Pike13 people linked to canonical person | 992 |
| Pike13 people unlinked | 75 |
| HubSpot contacts | 1,214 |
| HubSpot contacts linked | 819 |
| HubSpot contacts unlinked | 395 |
| Canonical `persons` | 1,944 |
| `person_identities` rows | 4,945 |
| Open identity conflicts | 206 |
| `multiple_hubspot_contact` conflicts | 181 |
| `multiple_pike13_person` conflicts | 25 |
| Lesson-student rows | 34,085 |
| Lesson-student rows linked | 14,183 |
| Distinct lesson students unresolved | 689 |
| Unresolved students with one exact name+school Pike13 candidate | 211 |

Existing targeted regression suite:

```bash
python -m pytest \
  tests/test_person_identity.py \
  tests/test_person_journey.py \
  tests/test_school_email.py -q
# Verified: 12 passed
```

The existing foundation is real and should be evolved, not replaced:

- `lead_followup_schema.py:432-472` — `persons`, `person_identities`, `person_resolution_conflicts`
- `notesreminder/lib/person_identity.py:222-382` — deterministic resolver
- `scripts/refresh_person_identities.py` — refresh CLI
- `tests/test_person_identity.py` — current identity tests
- `tests/test_person_journey.py` — journey/privacy tests
- `source_completeness.py:201-369` — source-level match generation; internal-domain exclusion already replaces the old trusted flag

## 2. Problem statement

`refresh_person_identities()` currently unions source records on exact email or phone. This is safe for the same adult represented in multiple systems, but unsafe when:

- siblings share a guardian email or phone;
- a minor and payer share an account contact;
- multiple HubSpot contacts represent members of one household;
- a family reuses an address or contact point.

The 206 open conflicts are therefore architectural evidence, not merely dirty rows. Person equivalence and household association need separate graphs.

## 3. Non-negotiable resolution policy

1. **One Pike13 person ID anchors one canonical person.**
2. **Two Pike13 person IDs are never merged solely because they share email, phone, guardian, payer, or address.**
3. **Direct identifiers outrank inferred evidence.** A HubSpot deal carrying `pike13_person_id` may link directly.
4. **Shared contact details create household/contact-point edges, not person equivalence.**
5. **Exact name+school may generate a candidate; it does not override contradictory direct identifiers.**
6. **Fuzzy name alone never auto-merges.**
7. **Every unresolved record gets a machine-readable reason code.**
8. **Refresh is deterministic and idempotent.** Re-running with unchanged input preserves IDs and counts.
9. **Sensitive fields stay out of logs, task comments, and default journey output.**
10. **Production DB migration requires a backup, dry-run report, and human approval.**

## 4. Target data model

Keep `persons`, but migrate identity evidence into explicit entities:

### `households`

- `household_id TEXT PRIMARY KEY`
- `display_name TEXT`
- `school TEXT`
- `resolution_status TEXT`
- `created_at TEXT`
- `updated_at TEXT`

### `household_members`

- `household_id TEXT`
- `person_id TEXT`
- `role TEXT` — `student`, `guardian`, `payer`, `dependent`, `adult_member`, `unknown`
- `source_system TEXT`
- `source_id TEXT`
- `confidence REAL`
- `evidence_type TEXT`
- unique constraint on household/person/role/source evidence

### `contact_points`

- `contact_point_id TEXT PRIMARY KEY`
- `contact_type TEXT` — `email`, `phone`, `address`
- `normalized_value TEXT`
- `sensitivity TEXT`
- unique constraint on type/value

### `contact_point_links`

- `contact_point_id TEXT`
- `entity_type TEXT` — `person` or `household`
- `entity_id TEXT`
- `relationship TEXT` — `personal`, `guardian`, `payer`, `shared_household`, `unknown`
- `source_system TEXT`
- `source_table TEXT`
- `source_id TEXT`
- `confidence REAL`
- `evidence TEXT`

### `identity_resolution_candidates`

- source record, proposed person/household, rule, score, evidence JSON
- `status`: `pending`, `accepted`, `rejected`, `superseded`
- no candidate mutates canonical links until accepted by deterministic policy or human review

### `identity_resolution_runs`

- run ID, timestamp, input counts, output counts, rule version, conflict counts, coverage deltas, rollback/backup reference

Keep `person_resolution_conflicts` during migration for compatibility; deprecate only after downstream consumers move to candidate/review tables.

## 5. Implementation sequence

### Phase A — Baseline and decision record

**Files:**
- Create: `docs/identity/identity-baseline.md`
- Create: `docs/identity/identity-resolution-policy.md`
- Create: `scripts/identity_quality_report.py`
- Test: `tests/test_identity_quality_report.py`

**Work:**
1. Capture current counts, conflict samples, duplicate contact-point distributions, and consumer inventory.
2. Record the person-versus-household policy above as an ADR.
3. Add a deterministic quality report with JSON and privacy-safe Markdown output.
4. Prove the report contains counts/reason codes but no raw email, phone, message body, or customer name by default.

**Gate:** Baseline query output is reproducible from a DB copy; current 12 targeted tests remain green.

### Phase B — Golden fixture and schema migration

**Files:**
- Modify: `lead_followup_schema.py`
- Create: `tests/fixtures/identity_households.json`
- Create: `tests/test_household_identity_schema.py`
- Modify: `tests/test_person_identity.py`

**Fixture must include:**
- same person across Pike13 + HubSpot + Dialpad;
- two siblings sharing guardian email and phone;
- payer distinct from student;
- duplicate HubSpot contacts for one adult;
- two schools with same name but different people;
- contradictory direct Pike13 IDs;
- unique exact name+school lesson candidate;
- ambiguous same-name lesson candidate;
- internal `@schoolofrock.com` contact;
- missing email/phone.

**Gate:** Migration is additive and idempotent; existing DB opens without destructive rewrite; all old tests and new schema tests pass.

### Phase C — Person resolver refactor

**Files:**
- Modify: `notesreminder/lib/person_identity.py`
- Modify: `scripts/refresh_person_identities.py`
- Modify: `tests/test_person_identity.py`
- Create: `tests/test_household_identity_resolution.py`

**Work:**
1. Anchor canonical people on Pike13 person IDs and direct source identifiers.
2. Stop unioning distinct Pike13 people through shared email/phone.
3. Populate contact points and person/household links.
4. Preserve stable person IDs across reruns and non-conflicting data additions.
5. Emit candidates/conflicts with reason codes instead of guessing.

**Gate:**
- 1,067/1,067 Pike13 people receive canonical person IDs.
- zero golden-fixture sibling or payer/student false merges.
- direct HubSpot-deal-to-Pike13 links preserved.
- two identical refreshes produce identical canonical IDs and summary counts.

### Phase D — Household inference

**Files:**
- Create: `notesreminder/lib/household_identity.py`
- Modify: `notesreminder/lib/person_identity.py`
- Create: `tests/test_household_inference.py`

**Evidence priority:**
1. explicit Pike13 dependent/account-manager/guardian relationships;
2. direct payer/plan evidence;
3. shared guardian contact plus corroborating school/address evidence;
4. shared email/phone alone creates a candidate household edge, not a confirmed person merge.

**Gate:** Every household edge records source evidence and confidence; removing an input row and refreshing removes/supersedes the derived edge deterministically.

### Phase E — Lesson-student bridge

**Files:**
- Create: `notesreminder/lib/lesson_identity.py`
- Create: `scripts/backfill_lesson_student_identities.py`
- Create: `tests/test_lesson_identity.py`
- Modify: `build_reporting_schema.py` or the canonical migration location selected in the ADR

**Work:**
1. Link lesson `student_id` to canonical person using direct Pike13 mapping where available.
2. Admit unique exact normalized name+school candidates only under the documented rule.
3. Write `match_type`, confidence, evidence, run ID, and unresolved reason.
4. Never overwrite an existing higher-confidence manual/direct link.

**Gate:**
- all 34,085 lesson-student rows are classified as linked or unresolved-with-reason;
- the 211 currently measurable unique exact name+school candidates are evaluated;
- no ambiguous duplicate-name row auto-links;
- linked coverage cannot decrease without an explicit regression report.

### Phase F — Operations, integration, and review

**Files:**
- Modify: `run_retention_cron.py`
- Modify: `mcp_server.py` and/or `notesreminder/mcp/tools.py`
- Create: `docs/identity/identity-runbook.md`
- Create: `tests/test_identity_pipeline_integration.py`

**Work:**
1. Run identity refresh after source ingestion and before churn/report generation.
2. Add dry-run and `--db-copy` modes.
3. Surface quality metrics and conflict deltas through MCP/read-only reporting.
4. Document backup, migration, rollback, review, and weekly monitoring.
5. Run full regression suite on a copied database.

**Final gate:**
- targeted and full test counts reported exactly;
- privacy scan passes;
- no production DB mutation performed by an agent;
- PM produces approve/reject recommendation;
- code remains on the feature worktree for human review and merge.

## 6. Kanban graph

```text
K0 Baseline + dirty-tree safety audit [pm, initially BLOCKED]
  → K1 Identity/household ADR [pm]
    → K2 Golden fixture + quality harness [backend]
      → K3 Additive household/contact schema [backend]
        → K4 Refactor canonical person resolver [backend]
          → K5 Household inference [backend]
            → K6 Lesson-student bridge [backend]
              → K7 Conflict queue + quality report [backend]
                → G1 Quantitative data-quality gate [pm]
                  → K8 Pipeline/MCP/runbook integration [backend]
                    → G2 Final regression/privacy review [pm]
```

The graph is intentionally mostly sequential. All cards share one persistent feature worktree; parallel writes would trade a little speed for avoidable merge risk.

## 7. Hermes execution design

- Dedicated board: `sor-360-identity`
- Profiles: `pm` for architecture/gates; `backend` for code/data work
- Shared workspace: `/home/ubuntu/projects/hughrscott/NotesReminder/.worktrees/sor-360-identity`
- Feature branch: `feat/sor-360-identity`
- Skills pinned:
  - PM: `sor-data-pipeline`, `kanban-worker`
  - Backend: `sor-data-pipeline`, `test-driven-development`, `kanban-worker`
- Root card starts **blocked**, so creating the board cannot dispatch work before human review.
- Every card has an idempotency key.
- Coding cards complete only after commit + exact tests; they do not merge or mutate production.
- `G2` must block `review-required` if it recommends merging or applying the migration.

Bootstrapper:

```bash
python scripts/bootstrap_sor_360_identity_board.py
# dry run only

python scripts/bootstrap_sor_360_identity_board.py --apply
# creates worktree, board, and blocked dependency graph

hermes kanban --board sor-360-identity list
hermes kanban --board sor-360-identity unblock <K0_TASK_ID>
```

## 8. Definition of done

This foundation is done when:

- each Pike13 person has exactly one canonical person;
- households, guardians, payers, and shared contact points are represented without forced person merges;
- all source and lesson links carry rule/evidence/confidence/run provenance;
- unresolved records are visible and classified;
- resolver runs are deterministic, repeatable, and reversible;
- the full 360 timeline consumes canonical person/household IDs;
- regression, privacy, and production-migration gates are explicitly approved.
