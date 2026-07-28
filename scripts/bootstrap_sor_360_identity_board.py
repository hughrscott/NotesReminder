#!/usr/bin/env python3
"""Create the SOR 360 identity/household Hermes Kanban board.

Dry-run is the default. Use --apply to create the feature worktree, board, and
cards. The root card is converted immediately into a sticky human block; add
--start to unblock it after the whole graph has been created and inspected.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

BOARD = "sor-360-identity"
BOARD_NAME = "SOR 360 Identity"
REPO = Path("/home/ubuntu/projects/hughrscott/NotesReminder")
WORKTREE = REPO / ".worktrees" / BOARD
BRANCH = "feat/sor-360-identity"
PLAN_RELATIVE = Path("docs/plans/2026-07-19-sor-360-identity-household.md")
SOURCE_PLAN = REPO / PLAN_RELATIVE
PLAN = WORKTREE / PLAN_RELATIVE


@dataclass(frozen=True)
class Card:
    key: str
    title: str
    assignee: str
    priority: int
    parents: tuple[str, ...]
    body: str
    skills: tuple[str, ...]
    goal: bool = False
    blocked: bool = False


COMMON_GUARDS = """
## Operating constraints
- Read the parent handoff(s) and the implementation plan before working.
- Work only in the assigned persistent feature worktree.
- Never mutate `reminders.db`; use a temporary copy for all migrations/backfills.
- Never put raw names, emails, phones, message bodies, OAuth codes, or credentials in comments or completion metadata.
- Report exact test commands and exact pass/fail counts.
- Do not merge to `main` or deploy. Commit only to the feature branch.
- If acceptance criteria cannot be met without guessing identity, block with evidence rather than auto-linking.
""".strip()


CARDS = (
    Card(
        key="K0",
        title="K0: Baseline identity coverage and protect dirty worktree",
        assignee="pm",
        priority=110,
        parents=(),
        blocked=True,
        skills=("sor-data-pipeline",),
        body="""
## Mission
Establish a reproducible, privacy-safe baseline before any identity code changes. Inventory current identity consumers and confirm the feature worktree is isolated from the dirty main checkout.

## Required outputs
- `docs/identity/identity-baseline.md`
- inventory of every reader/writer of `persons`, `person_identities`, `identity_matches`, source `person_id` columns, and `lesson_students.person_id`
- baseline JSON/SQL with current coverage and conflict counts
- explicit confirmation that production `reminders.db` was not modified

## Verified starting numbers to reproduce
- Pike13 people 1,067; linked 992; unlinked 75
- HubSpot contacts 1,214; linked 819; unlinked 395
- canonical persons 1,944; person identities 4,945
- open conflicts 206: 181 multiple HubSpot contact, 25 multiple Pike13 person
- lesson-student rows 34,085; linked 14,183; unresolved distinct students 689
- 211 unresolved students have one exact name+school Pike13 candidate
- targeted suite: 12 passed

## Acceptance
Numbers are reproduced from a DB copy or discrepancies are explained. Sensitive values do not appear in the deliverable. Complete with machine-readable metrics and file paths.
""".strip(),
    ),
    Card(
        key="K1",
        title="K1: Approve person-vs-household identity ADR",
        assignee="pm",
        priority=105,
        parents=("K0",),
        skills=("sor-data-pipeline",),
        body="""
## Mission
Turn the plan's non-negotiable resolution policy into an implementation ADR after checking K0's source/consumer inventory.

## Required output
`docs/identity/identity-resolution-policy.md` covering:
- Pike13-person anchor rule
- direct-ID precedence
- person equivalence versus household association
- guardian/payer/dependent roles
- contact-point many-to-many semantics
- exact name+school candidate policy
- conflict/review reason codes
- stable-ID and idempotency guarantees
- compatibility/migration strategy for existing consumers

## Acceptance
The ADR explicitly forbids merging different Pike13 people solely through shared email/phone/address. Every rule has positive, negative, and ambiguous examples. No schema or implementation code is changed in this card.
""".strip(),
    ),
    Card(
        key="K2",
        title="K2: Build golden household identity fixture and quality harness",
        assignee="backend",
        priority=100,
        parents=("K1",),
        skills=("sor-data-pipeline", "test-driven-development"),
        body="""
## Mission
Create tests that fail under the current shared-email/shared-phone union behavior before changing production logic.

## Files
- create `tests/fixtures/identity_households.json`
- create `tests/test_household_identity_resolution.py`
- create `scripts/identity_quality_report.py`
- create `tests/test_identity_quality_report.py`

## Fixture cases
Same adult across sources; two siblings sharing guardian email+phone; payer distinct from student; duplicate HubSpot contacts for one adult; same name in two schools; contradictory Pike13 IDs; unique and ambiguous lesson-name candidates; internal School of Rock email; missing contact data.

## Acceptance
- demonstrate at least one expected RED test against current resolver
- quality report defaults to aggregate/reason-code output only
- privacy test rejects raw names/emails/phones/message bodies
- existing targeted suite remains 12/12 green before later implementation cards
- commit code and complete with commit SHA plus exact test counts; downstream cards, not this worker, review the commit
""".strip(),
    ),
    Card(
        key="K3",
        title="K3: Add household, contact-point, candidate, and run schema",
        assignee="backend",
        priority=95,
        parents=("K2",),
        skills=("sor-data-pipeline", "test-driven-development"),
        body="""
## Mission
Implement the additive schema approved in K1 without destructively rewriting existing identity tables.

## Files
- modify `lead_followup_schema.py`
- create `tests/test_household_identity_schema.py`

## Required tables
`households`, `household_members`, `contact_points`, `contact_point_links`, `identity_resolution_candidates`, and `identity_resolution_runs` with indexes, uniqueness constraints, evidence/provenance, and status fields defined by the ADR.

## Acceptance
Migration is idempotent; existing databases open safely; foreign keys/indexes are tested; old identity/journey tests and all K2 tests pass. Use a temporary DB only. Commit and report exact test counts.
""".strip(),
    ),
    Card(
        key="K4",
        title="K4: Refactor canonical person resolver around direct anchors",
        assignee="backend",
        priority=90,
        parents=("K3",),
        skills=("sor-data-pipeline", "test-driven-development"),
        goal=True,
        body="""
## Mission
Refactor `notesreminder/lib/person_identity.py` so shared contact details no longer collapse distinct people.

## Requirements
- each Pike13 person ID anchors exactly one canonical person
- direct HubSpot `pike13_person_id` links remain authoritative
- shared contact details populate contact-point/household evidence
- duplicate same-person source records may resolve only under ADR-approved evidence
- ambiguous records create candidates/conflicts with reason codes
- canonical IDs remain stable across identical reruns and non-conflicting additions
- update `scripts/refresh_person_identities.py` summary/dry-run behavior

## Acceptance
On a copied current DB, 1,067/1,067 Pike13 people receive canonical IDs. Golden siblings and payer/student pairs never merge. Two unchanged refreshes produce identical IDs and counts. All relevant tests pass exactly. Commit; do not merge or touch production DB.
""".strip(),
    ),
    Card(
        key="K5",
        title="K5: Infer households, guardians, payers, and dependents",
        assignee="backend",
        priority=85,
        parents=("K4",),
        skills=("sor-data-pipeline", "test-driven-development"),
        goal=True,
        body="""
## Mission
Implement household inference from explicit Pike13 client/account-manager/guardian/dependent and plan payer evidence.

## Files
- create `notesreminder/lib/household_identity.py`
- modify resolver orchestration as needed
- create `tests/test_household_inference.py`

## Evidence order
Explicit relationship > direct payer/plan evidence > guardian contact plus corroborating school/address > shared contact alone as candidate only.

## Acceptance
Every household/member edge stores role, source evidence, confidence, and run provenance. Shared email/phone alone never confirms person equivalence. Refresh deterministically removes or supersedes derived edges when evidence disappears. Tests cover siblings, payer/student, cross-school ambiguity, and duplicate contacts.
""".strip(),
    ),
    Card(
        key="K6",
        title="K6: Backfill lesson students with evidence and unresolved reasons",
        assignee="backend",
        priority=80,
        parents=("K5",),
        skills=("sor-data-pipeline", "test-driven-development"),
        goal=True,
        body="""
## Mission
Build a precision-first bridge from lesson students to canonical people.

## Files
- create `notesreminder/lib/lesson_identity.py`
- create `scripts/backfill_lesson_student_identities.py`
- create `tests/test_lesson_identity.py`
- add match provenance columns/tables in the canonical schema migration location

## Requirements
Use direct identifiers first. Evaluate unique exact normalized name+school only under K1 policy. Preserve higher-confidence/manual links. Classify every row as linked or unresolved with reason, match type, confidence, evidence, and run ID.

## Acceptance
All 34,085 current rows are classifiable on a DB copy; all 211 currently measurable unique candidates are evaluated; ambiguous duplicate-name rows never auto-link; linked coverage cannot decline silently. Report before/after counts without PII.
""".strip(),
    ),
    Card(
        key="K7",
        title="K7: Conflict review queue and identity quality report",
        assignee="backend",
        priority=75,
        parents=("K6",),
        skills=("sor-data-pipeline", "test-driven-development"),
        body="""
## Mission
Finish the operator-facing, privacy-safe review surface for unresolved identities and household candidates.

## Requirements
- JSON and Markdown quality reports
- counts by source, rule, confidence band, and unresolved reason
- candidate accept/reject/supersede workflow without destructive deletion
- resolution-run deltas and stable IDs
- read-only query path suitable for MCP exposure

## Acceptance
Reports reproduce K0 metrics plus post-change deltas, reveal zero raw PII by default, and make every old/new conflict accountable. Add tests for review-state transitions and repeat runs.
""".strip(),
    ),
    Card(
        key="G1",
        title="G1: Quantitative identity and household quality gate",
        assignee="pm",
        priority=70,
        parents=("K7",),
        skills=("sor-data-pipeline",),
        body="""
## Mission
Independently audit K2-K7 against K1 and the copied current database. Do not repair code in this card.

## Hard gates
- 1,067/1,067 Pike13 people have one canonical person each
- zero golden-fixture sibling or payer/student false merges
- direct HubSpot-to-Pike13 links preserved
- identical reruns produce identical IDs/counts
- every lesson-student row linked or unresolved-with-reason
- ambiguous duplicate-name lesson rows do not auto-link
- reports/logs/comments contain no raw PII
- exact targeted and full test results are recorded

## Outcome
Complete only with an explicit PASS and evidence. If any hard gate fails, block with the exact failing metric/test and recommend a new remediation card; do not wave it through.
""".strip(),
    ),
    Card(
        key="K8",
        title="K8: Integrate identity refresh into pipeline, MCP, and runbook",
        assignee="backend",
        priority=65,
        parents=("G1",),
        skills=("sor-data-pipeline", "test-driven-development"),
        goal=True,
        body="""
## Mission
Integrate the approved resolver after source ingestion and before retention/report generation without applying it to production.

## Files
- modify `run_retention_cron.py`
- modify `mcp_server.py` and/or `notesreminder/mcp/tools.py`
- create `docs/identity/identity-runbook.md`
- create `tests/test_identity_pipeline_integration.py`

## Requirements
Dry-run and DB-copy modes; backup/rollback instructions; quality metrics and conflict deltas exposed read-only; failure prevents downstream churn report generation; sensitive detail remains opt-in.

## Acceptance
Integration tests prove ordering and fail-closed behavior. Full suite runs on a copied DB. Commit with exact test results. No production DB mutation, merge, or deploy.
""".strip(),
    ),
    Card(
        key="G2",
        title="G2: Final regression, privacy, and production-readiness review",
        assignee="pm",
        priority=60,
        parents=("K8",),
        skills=("sor-data-pipeline",),
        body="""
## Mission
Perform final independent review of the feature worktree and produce a go/no-go recommendation for human merge and production migration.

## Review
Architecture compliance; migration idempotency/rollback; deterministic IDs; household false-merge protection; lesson evidence; downstream compatibility; exact full-suite results; privacy scan; copied-DB dry run; documentation completeness.

## Required output
`docs/identity/production-readiness-review.md` with PASS/FAIL by gate, exact commands/results, residual risks, and reversible production migration steps.

## Lifecycle
Do not merge or mutate production. Add a structured review handoff comment, then block with `review-required:` so Hugh can approve the branch and separately authorize production migration.
""".strip(),
    ),
)


def run(command: list[str], *, capture: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=REPO,
        check=True,
        text=True,
        capture_output=capture,
    )


def assert_profiles_exist(required: Iterable[str]) -> None:
    output = run(["hermes", "profile", "list"]).stdout
    missing = [name for name in required if not re.search(rf"(?m)^\s*[◆ ]?{re.escape(name)}\s", output)]
    if missing:
        raise RuntimeError(f"Missing Hermes profiles: {', '.join(missing)}")


def ensure_worktree() -> None:
    if not (WORKTREE / ".git").exists():
        WORKTREE.parent.mkdir(parents=True, exist_ok=True)
        branch_exists = subprocess.run(
            ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{BRANCH}"],
            cwd=REPO,
            check=False,
        ).returncode == 0
        if branch_exists:
            run(["git", "worktree", "add", str(WORKTREE), BRANCH])
        else:
            run(["git", "worktree", "add", "-b", BRANCH, str(WORKTREE), "main"])

    # The plan may be uncommitted in the source checkout when this bootstrapper
    # is first run. Put a real copy inside the workers' allowed workspace.
    PLAN.parent.mkdir(parents=True, exist_ok=True)
    PLAN.write_text(SOURCE_PLAN.read_text())


def board_exists() -> bool:
    output = run(["hermes", "kanban", "boards", "list"]).stdout
    return bool(re.search(rf"(?m)^\s*[●*]?\s*{re.escape(BOARD)}\s", output))


def ensure_board() -> None:
    if board_exists():
        return
    run([
        "hermes", "kanban", "boards", "create", BOARD,
        "--name", BOARD_NAME,
        "--description", "Household-aware identity foundation for the School of Rock 360 customer view",
        "--icon", "◎",
        "--color", "#2563eb",
        "--default-workdir", str(WORKTREE),
    ])


def create_card(card: Card, ids: dict[str, str]) -> str:
    body = (
        f"Implementation plan: {PLAN}\n"
        f"Shared feature worktree: {WORKTREE}\n\n"
        f"{card.body}\n\n{COMMON_GUARDS}"
    )
    command = [
        "hermes", "kanban", "--board", BOARD, "create", card.title,
        "--body", body,
        "--assignee", card.assignee,
        "--workspace", f"dir:{WORKTREE}",
        "--priority", str(card.priority),
        "--idempotency-key", f"{BOARD}:{card.key}",
        "--max-runtime", "3h",
        "--created-by", "sor-360-bootstrap",
    ]
    for parent in card.parents:
        command.extend(["--parent", ids[parent]])
    for skill in card.skills:
        command.extend(["--skill", skill])
    if card.goal:
        command.extend(["--goal", "--goal-max-turns", "15"])
    if card.blocked:
        command.extend(["--initial-status", "blocked"])
    command.append("--json")

    raw = run(command).stdout
    payload = json.loads(raw)
    task_id = payload.get("task_id") or payload.get("id")
    if task_id is None and isinstance(payload.get("task"), dict):
        task_id = payload["task"].get("id")
    if not task_id:
        raise RuntimeError(f"Could not parse task id for {card.key}: {raw}")
    return str(task_id)


def task_payload(task_id: str) -> dict:
    raw = run([
        "hermes", "kanban", "--board", BOARD, "show", task_id, "--json",
    ]).stdout
    return json.loads(raw)


def ensure_sticky_human_block(task_id: str) -> None:
    """Give K0 a real block event so recompute_ready cannot promote it.

    `--initial-status blocked` parks creation but does not emit the worker-style
    `blocked` event used by Hermes' sticky-block guard. Convert it immediately
    to a real needs-input block before creating child cards.
    """
    payload = task_payload(task_id)
    status = payload["task"]["status"]
    has_sticky_event = any(event.get("kind") == "blocked" for event in payload.get("events", []))
    if status == "blocked" and has_sticky_event:
        return
    if status == "blocked":
        run(["hermes", "kanban", "--board", BOARD, "unblock", task_id])
        status = "ready"
    if status not in {"ready", "running"}:
        raise RuntimeError(f"Cannot create sticky review gate from K0 status {status!r}")
    run([
        "hermes", "kanban", "--board", BOARD, "block", task_id,
        "Board created and awaiting Hugh's review/authorization before agentic execution starts.",
        "--kind", "needs_input",
    ])


def print_dry_run() -> None:
    print(f"Board: {BOARD} ({BOARD_NAME})")
    print(f"Repository: {REPO}")
    print(f"Worktree: {WORKTREE}")
    print(f"Branch: {BRANCH}")
    print("\nDependency graph:")
    for card in CARDS:
        parents = ", ".join(card.parents) or "root"
        status = "HUMAN BLOCK" if card.blocked else "dependency-gated"
        print(f"  {card.key:<2} [{card.assignee:<7}] <- {parents:<4} | {status:<16} | {card.title}")
    print("\nNo files, boards, cards, or git worktrees were changed.")
    print("Run with --apply to create the blocked board; add --start to unblock K0.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Create worktree, board, and cards")
    parser.add_argument("--start", action="store_true", help="Unblock K0 after creation (requires --apply)")
    args = parser.parse_args()

    if args.start and not args.apply:
        parser.error("--start requires --apply")
    if not args.apply:
        print_dry_run()
        return

    if not SOURCE_PLAN.exists():
        raise RuntimeError(f"Implementation plan not found: {SOURCE_PLAN}")
    assert_profiles_exist({card.assignee for card in CARDS})
    ensure_worktree()
    ensure_board()

    ids: dict[str, str] = {}
    for card in CARDS:
        ids[card.key] = create_card(card, ids)
        if card.key == "K0":
            ensure_sticky_human_block(ids[card.key])
        print(f"{card.key}: {ids[card.key]} — {card.title}")

    manifest = {
        "board": BOARD,
        "branch": BRANCH,
        "worktree": str(WORKTREE),
        "plan": str(PLAN),
        "tasks": ids,
    }
    manifest_path = Path.home() / ".hermes" / "kanban" / "boards" / BOARD / "bootstrap-manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(f"Manifest: {manifest_path}")

    if args.start:
        run(["hermes", "kanban", "--board", BOARD, "unblock", ids["K0"]])
        print(f"Started K0: {ids['K0']}")
    else:
        print(f"K0 remains blocked for review. Start with:")
        print(f"  hermes kanban --board {BOARD} unblock {ids['K0']}")


if __name__ == "__main__":
    main()
