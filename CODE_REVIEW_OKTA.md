# Code Review: Okta Cookie Extraction & MCP Server Integration

**Reviewer:** Automated code review subagent
**Commit reviewed:** `e213c67` ("Implement Okta cookie extraction, MCP server, and health checks (P0+P1)")
**Plan reference:** `OKTA_COOKIE_PLAN.md`
**Date:** 2026-07-09

---

## Summary Verdict

**Overall: CONDITIONAL PASS — usable, but fix 2 blocking bugs before relying on it unattended.**

The implementation follows the plan closely and preserves backward compatibility for the existing profile-dir/interactive-login and env-credential login paths. The cookie injection logic is structurally correct (injected before navigation, filtered for expiry). However there are real bugs that will bite in production: a shell portability bug in two `#!/bin/sh` scripts, a broken-by-refactor exception-handling regression in `run_daily.py`'s email-on-failure behavior, and a cookie-file security/permissions gap. None of these are showstoppers for a supervised first run, but they should be fixed before this runs unattended on cron.

---

## File-by-File Review

### 1. `notesreminder/lib/cookie_auth.py` — **PASS (minor issues)**

- `load_cookies`, `check_cookie_freshness`, `inject_cookies_into_context`, `inject_storage_into_page` all match the plan's design.
- **Correctness of freshness check:** `check_cookie_freshness` checks `extracted_at` age against `MAX_AGE_DAYS`, then scans `cookies[].expires` for the earliest ISO timestamp, returning `"expired"` if any cookie is already past expiry, else `"fresh"` with `soonest_expiry_days`. This is correct as designed and matches `inject_cookies_into_context`'s expiry filter (both use `datetime.fromisoformat` on the same field).
- **Bug (minor) — line 51/70/111:** `datetime.fromisoformat(extracted_at)` / `datetime.fromisoformat(expires)` assume the ISO string always includes a timezone offset (produced by `datetime.now(timezone.utc).isoformat()` upstream, which does). This is fine as long as the cookie file is always produced by `extract_pike13_cookies.py`; if someone hand-edits the JSON with a naive datetime string, `age_days = (datetime.now(timezone.utc) - extracted_dt).days` will raise `TypeError: can't subtract offset-naive and offset-aware datetimes` — this is only caught by the bare `except (ValueError, TypeError): pass` at line 61, so it fails safe (falls through to "fresh" if extraction-age check silently skips). Not a functional bug, but worth noting: a malformed file could silently mask an actually-stale cookie set.
- **Minor: `CookieExpiredError` (line 18) is defined but never raised anywhere in the codebase.** Dead code — either use it (e.g., raise it from `inject_cookies_into_context` for an already-expired-but-attempted injection) or remove it.
- **Security:** No cookie values are logged. Good. File permissions are not set/enforced here (see `extract_pike13_cookies.py` review below — that's the real gap).
- **Path resolution (line 9):** `DEFAULT_COOKIES_PATH` resolves relative to the *module's* directory (`notesreminder/lib/../../pike13_cookies.json` → repo root). This is correct assuming the package structure stays `notesreminder/lib/cookie_auth.py` at 2 levels below repo root. Confirmed correct today.
- `inject_cookies_into_context` builds `secure: c.get("secure", True)` (default `True`) whereas the *extractor* defaults `secure: c.get("secure", False)` (default `False`) — asymmetric defaults (see cross-file inconsistency note in extractor section below). Not a bug per se since real extracted cookies always carry an explicit `secure` value, but if a hand-crafted/partial cookie JSON omits the field, injection and extraction disagree on default security posture.

### 2. `scripts/extract_pike13_cookies.py` — **PASS (security gap)**

- Implementation matches the plan almost verbatim (plan's own code block was essentially copy-pasted, `time` import moved to top-level — improvement over the plan's inline `import time`).
- **Security issue (real, should fix):** The script writes `pike13_cookies.json` with plaintext session cookies via `output.write_text(...)` (line 142) using default file permissions (typically `0644`), meaning the file is world-readable on multi-user systems. This file is functionally equivalent to a live Pike13 (and potentially Okta) session token. **Recommend:** `os.chmod(output_path, 0o600)` immediately after writing, both here and wherever the file lands after SCP.
- The `refresh_pike13_cookies.sh` wrapper (see below) SCPs this file over the network — SCP-over-SSH is encrypted in transit, which is fine, but the destination file permissions on the Oracle server are not set either (see `install_mcp_server.sh`/systemd unit — no `umask` or explicit chmod post-copy).
- **Logic:** Authentication-detection loop (lines 41-63) is reasonable — checks URL for `/accounts/sign_in` or `/login`, polls up to 5 minutes for the user to complete Okta MFA in the visible (headed, `headless: False`) browser. This matches the plan.
- **Minor bug:** Lines 33/91 iterate `schools = ["westu-sor", "theheights-sor"]` but the `authenticated` flag and `break` (line 63) mean **only the first school's cookies are validated for auth**, then storage extraction (lines 90-102) navigates to *both* schools' `/schedule` regardless of `authenticated` status — if the first school authenticates but the second doesn't, storage for the second is silently captured pointing at a login/redirect page (harmless, just wasted work, not a security issue since it's the user's own already-open session).
- **Bug (edge case):** If `authenticated` is `False` for both schools (Okta MFA never completed within 5 min), the script still proceeds to dump whatever cookies exist (line 66-67, intentional per plan) and writes an output file. This is correct per plan intent ("dump whatever cookies we have anyway") but the resulting file could contain **no valid Pike13 session cookie at all**, and nothing downstream (`cookie_auth.py`) distinguishes "cookie file exists but has zero useful auth cookies" from "cookie file has valid auth cookies" — `check_cookie_freshness` would likely report `"fresh"` (no `expires` fields to fail on) even though the cookies don't actually authenticate. The only real signal is the runtime check in `noteschecker.py` (`is_authenticated()` after injection) — that safety net does exist, so this is not exploitable, just a slightly confusing operator experience (extraction script reports success/writes a file even on auth failure).
- No hard-coded secrets. Good.

### 3. `notesreminder/mcp/tools.py` — **PASS**

- **Async lock:** `PIKE13_SCRAPE_LOCK = asyncio.Lock()` at module scope, used via `async with PIKE13_SCRAPE_LOCK:` in both `pike13_scrape_lessons` and `pike13_import_and_update_db`. This correctly serializes concurrent scrape calls within a single running event loop (i.e., within one MCP server process) — which is the only concurrency model that matters here (FastMCP runs a single asyncio loop). **Correct and sufficient** for this use case; no race condition between the two Pike13 tools since they share the same module-level lock object.
- **Tool signatures:** All three tools (`pike13_scrape_lessons`, `pike13_cookie_status`, `pike13_import_and_update_db`) use plain typed keyword args with defaults (`str`, `int`) — compatible with FastMCP's automatic JSON-schema tool generation. No issues.
- **Date defaulting logic (lines 34-39, duplicated in 91-96):** if `start_date` is empty, compute it from `end_date` (or `now()`) minus `limit_days`. Correct, though the duplication between `pike13_scrape_lessons` and `pike13_import_and_update_db` is a DRY violation — a shared helper (`_resolve_date_range(start_date, end_date, limit_days)`) would reduce copy-paste risk. Not a bug, just a maintainability note.
- **`pike13_import_and_update_db` (line 110-118):** Opens its own SQLite connection with `sqlite3.connect(db_path)` and calls `update_reminders_from_dataframe(conn, df, school)` — correctly using the refactored function. **Bug (minor, real):** the DB connection here has no S3 sync — this tool updates the *local* `reminders.db` only. If the MCP server's `REMINDERS_DB_PATH` differs from the S3-synced copy used by `run_daily.py`/cron, this could silently update a stale local copy without syncing back to S3, causing the cron job and the MCP tool to diverge. Not addressed in the plan or code — worth flagging to Hugh as an operational gotcha, not a code bug per se.
- **Error handling:** No try/except around `scrape_lessons()` calls in either tool — if scraping throws (e.g., `CookieAuthError`, `ValueError` for missing PIKE13_USER/PASS, or any Playwright exception), it propagates as an uncaught exception through the MCP tool call. FastMCP will surface this as a tool-call error to the client, which is acceptable behavior for an MCP tool (the caller sees the failure) but there's no user-friendly JSON error envelope like the one in `pike13_cookie_status`'s try/except (lines 61-74). **Recommend:** wrap the scrape calls in try/except and return a structured `{"status": "error", "error": str(e)}` for parity with `pike13_cookie_status`, since MCP clients often display raw tracebacks unhelpfully.
- **Import-inside-function pattern** (`from noteschecker import scrape_lessons` inside the `async with` block, `from run_daily import update_reminders_from_dataframe` inside the function body) — this avoids circular imports (mcp_server.py → tools.py → noteschecker.py / run_daily.py, while run_daily.py imports from noteschecker.py too) but does mean `run_daily.py`'s module-level code (argparse setup, `load_dotenv()`, etc.) executes on first call. Confirmed `run_daily.py`'s module-level code has no side effects beyond `load_dotenv()` and constant definitions — safe.

### 4. `scripts/check_cookie_health.py` — **PASS**

- Straightforward exit-code contract: `2` = missing, `1` = expired, `0` = fresh or soon-to-expire (with warning printed to stdout). Matches typical cron health-check conventions (nonzero degrees of severity).
- Only calls `check_cookie_freshness()` — no cookie values printed. Good, no secret leakage.
- Minor: relies on `result.get("soonest_expiry_days", 999)` defaulting to a nonsense sentinel (999) when the key is absent (e.g., when status is `"fresh"` but there was no expiry data at all, i.e., session cookies with no `expires`). This is intentional and safe (999 > 3, so it won't trigger the warning branch).

### 5. `scripts/refresh_pike13_cookies.sh` — **FAIL (shebang/set bug)**

- **Bug (real, will break on most Linux systems):** Line 1 shebang is `#!/bin/sh`, but line 2 is `set -euo pipefail`. `pipefail` is a bash/ksh extension, **not** POSIX and **not supported by `dash`**, which is `/bin/sh` on Debian/Ubuntu (including the Oracle Cloud server this is meant to run scripts on/from, and Hugh's Mac if he uses the system `/bin/sh` rather than bash). Running this script with `sh scripts/refresh_pike13_cookies.sh` (or executing it directly if `/bin/sh` → dash) will raise:
  ```
  scripts/refresh_pike13_cookies.sh: 2: set: Illegal option -o pipefail
  ```
  and abort immediately (since `set -e` is also in effect and the `set` command itself fails). **This script currently cannot run on stock Debian/Ubuntu `/bin/sh`.** It will work if invoked explicitly as `bash scripts/refresh_pike13_cookies.sh`, or on macOS if `/bin/sh` happens to be bash-compatible (it isn't by default either — macOS `/bin/sh` is also a POSIX-mode shell without `pipefail` support in some versions).
  **Fix:** change shebang to `#!/bin/bash` (matches actual usage), or drop `pipefail` and rely on `set -eu` only.
- Logic otherwise matches the plan: extract → conditionally SCP to `$ORACLE_HOST` → SSH restart the systemd unit. Correct sequencing.
- **Security:** SCP destination path is hardcoded (`/home/ubuntu/projects/hughrscott/NotesReminder/pike13_cookies.json`) — fine for this single-server deployment, but no explicit `chmod 600` is applied post-copy on the remote side either locally or via the ssh command. Minor gap — recommend adding `ssh "ubuntu@${ORACLE_HOST}" "chmod 600 /home/ubuntu/.../pike13_cookies.json"` after the scp.
- No secrets embedded in the script itself. Good.

### 6. `scripts/run_mcp_server.sh` — **FAIL (same shebang/set bug)**

- Same `#!/bin/sh` + `set -euo pipefail` incompatibility as above (lines 1-2). This is the script referenced directly by the **systemd unit's `ExecStart`** (`deploy/notesreminder-mcp.service` line 10) — if systemd invokes it via its shebang (which it will, since `ExecStart` calls the file directly with the executable bit, triggering `execve` → kernel reads shebang), and if the target system's `/bin/sh` is dash (true for stock Ubuntu, which Oracle Cloud Ubuntu images are), **the MCP server systemd service will fail to start** with the same `Illegal option -o pipefail` error.
  This is the most operationally serious bug in this review — it can prevent the MCP server from ever starting on a fresh Oracle Ubuntu box. **Must verify** (`readlink -f /bin/sh` on the target Oracle server) before relying on the systemd unit; if `/bin/sh -> dash`, fix the shebang to `#!/bin/bash`.
- Otherwise correct: sources `.env` if present, prefers a local venv, execs `mcp_server.py "$@"` (correctly forwards `--transport sse --host ... --port ...` args from the systemd unit).

### 7. `deploy/notesreminder-mcp.service` — **PASS**

- Standard systemd unit, `Type=simple`, restarts on failure with a 10s backoff — reasonable for a long-running server.
- Correctly sets `REMINDERS_DB_PATH` and `PIKE13_COOKIES_PATH` as absolute paths via `Environment=`. Good — avoids relying on `.env` file being present/loaded under systemd (though `run_mcp_server.sh` will also try to source `.env`, so both mechanisms are present — no conflict, systemd `Environment=` takes precedence since it's exported into the shell that then execs the script, and `set -a; . ./.env; set +a` won't override already-exported vars unless `.env` has unset guards — actually it **will** override, since sourcing a file that does `VAR=value` unconditionally reassigns regardless of prior export. This means if `.env` also defines `REMINDERS_DB_PATH` or `PIKE13_COOKIES_PATH`, it will silently override the systemd-set ones. Minor but worth flagging: keep `.env` on the server free of these two vars, or accept that `.env` wins.)
- No secrets embedded directly in the unit file (good — Okta/Pike13 credentials would ideally live in `.env` or a systemd `EnvironmentFile=`, not here). This unit doesn't need Pike13 creds at all since cookie auth bypasses the login-form path when cookies are present. Consistent with design.
- Runs as `User=ubuntu`, not root. Correct principle of least privilege.

### 8. `deploy/install_mcp_server.sh` — **PASS**

- Simple, does what it says: copies unit file, `daemon-reload`, `enable`, `start`. No shebang/pipefail issue here since it contains no `set -o pipefail` (just `set -euo pipefail` — wait, checking again: line 2 is `set -euo pipefail` and shebang is `#!/bin/sh` — **same bug as above**, technically present here too.**FAIL for the same reason** — under dash this script will also abort at line 2. Low real-world impact since it's a one-time manual install step Hugh would likely run with `bash deploy/install_mcp_server.sh` explicitly, but worth fixing for consistency.

### 9. `noteschecker.py` — **PASS (cookie injection logic is correct)**

This is the most safety-critical file. Detailed trace of the injection logic:

- **Injection happens BEFORE navigation** — confirmed correct. Sequence at lines 199-230 (new code):
  1. `cookie_payload = load_cookies()` (may raise `CookieAuthError`, caught)
  2. `check_cookie_freshness(cookie_payload)` — gate on `"fresh"` status
  3. `inject_cookies_into_context(context, cookie_payload)` — **cookies added to context BEFORE any `page.goto()` call in this branch** ✅ matches Playwright's requirement that `context.add_cookies()` happen before the request that needs them (line 208, then `page.goto(schedule_home_url)` at line 214). This is correct and matches the plan's explicit instruction ("Must be called BEFORE navigating").
  4. Post-injection, navigates to `schedule_home_url` directly (skipping the login page entirely) and calls `is_authenticated()` to verify.
  5. If not authenticated, resets `cookie_auth_attempted = False` so the code falls through to the pre-existing profile_dir/login-form paths (line 232).
- **Backward compatibility — confirmed correct via 3 scenarios:**
  - **No cookie file present:** `load_cookies()` raises `CookieAuthError` at line 203 (`FileNotFoundError`-derived message), caught at line 227-229, `cookie_auth_attempted` stays `False`. Falls through to line 232 unchanged — **identical behavior to pre-cookie-injection code.** ✅
  - **`profile_dir` is passed (interactive local Mac usage):** The `if not profile_dir:` guard at line 201 means cookie injection is **entirely skipped** when a profile dir is supplied — preserves the existing Mac-based persistent-profile workflow exactly as before. ✅
  - **Cookies present but stale (`status != "fresh"`):** Falls through the `else` branch (line 224-226), `cookie_auth_attempted` remains `False`, proceeds to existing login logic. ✅
- **Bug (real, minor–moderate): `inject_storage_into_page` is imported (line 17) but never called anywhere in `noteschecker.py`.** The plan (and `cookie_auth.py`) explicitly built this function to restore `localStorage` items alongside cookies, since "some apps store auth tokens there" (plan §A1 step 5). It is dead code in the actual injection flow — if Pike13's Okta-issued session genuinely depends on `localStorage` values (which the extractor script goes out of its way to capture per-school), skipping this call could mean cookie-only injection **silently fails to fully authenticate** in cases where cookies alone aren't sufficient, even though `is_authenticated()` would correctly detect that failure and fall back to form login. Net effect: not unsafe, but potentially reduces the success rate of cookie auth if Pike13/Okta relies on local storage tokens. **Recommend:** call `inject_storage_into_page(page, cookie_payload, school=school_subdomain)` after cookie injection and before/after the initial `page.goto(schedule_home_url)` (per the plan's helper design, it expects the page to already be on the Pike13 domain, so it should be called *after* the `page.goto`, not before).
- **`is_authenticated()` check quality (lines 115-130, unmodified, reused correctly):** Robust multi-signal check (URL markers, body text markers for 2FA/sign-in, presence of "Schedule" link). Reused as-is for cookie-injection verification — appropriate reuse, no duplication.
- **Post-scrape cookie health check (lines 530-544):** Only runs `if cookie_auth_attempted:` — correct, avoids writing spurious cookie-expiry alerts when cookie auth wasn't used (e.g., form-login or profile-dir paths). Writes an alert file at `notesreminder/outputs/cookie_alert.txt` (`os.path.dirname(__file__)` here resolves to the **repo root**, since `noteschecker.py` lives at the repo root, not inside `notesreminder/`) — so the actual path is `<repo_root>/outputs/cookie_alert.txt`. This is fine, just noting the path resolves correctly relative to this file's real location (confirmed: `noteschecker.py` is at repo root, so `os.path.dirname(__file__)` → repo root → `outputs/cookie_alert.txt`). No existing consumer of this file was found elsewhere in the repo (not tailed by `check_cookie_health.py` or any cron script reviewed) — the alert is written but nothing currently reads `outputs/cookie_alert.txt`. Not a bug, but an incomplete feature: the plan doesn't specify a consumer either, so this may be intentional groundwork for a future notification step.
- **Existing behavior preserved:** The non-cookie login block (profile_dir and form-login paths) is byte-for-byte the same logic as before, just re-indented one level deeper inside the new `if not cookie_auth_attempted or not await is_authenticated():` guard. Confirmed via diff — no logic was altered, only wrapped. ✅
- **No secrets logged:** cookie count/status/expiry summaries are printed when `verbose=True`, but cookie *values* are never printed. Good.

### 10. `mcp_server.py` — **PASS**

- `from notesreminder.mcp.tools import register_pike13_tools` + `register_pike13_tools(mcp)` at module level (after all other tool definitions) — correctly registers the 3 new tools alongside the existing 20+ SQL/reporting tools on the same `FastMCP` instance.
- **SSE transport support (new `__main__` block):** `argparse` added for `--transport {stdio,sse}`, `--host`, `--port`, defaulting to `stdio` (i.e., **default behavior for anyone invoking `python mcp_server.py` with no args is unchanged** — backward compatible). ✅ matches `deploy/notesreminder-mcp.service`'s explicit `--transport sse --host 127.0.0.1 --port 8090` invocation.
- **Security note:** SSE transport binds to `127.0.0.1` by default in both the argparse default and the systemd unit's explicit flag — not exposed on the network interface, good. No auth/token gating on the SSE endpoint itself is visible in this diff (FastMCP's SSE transport typically has no built-in auth) — acceptable given it's bound to loopback only, but worth a note if this ever needs to be exposed beyond localhost (e.g., through an SSH tunnel or reverse proxy) — would need an auth layer added at that point.
- No functional regressions to the 20+ pre-existing tools — diff only adds an import and 14 lines at the end of the file.

### 11. `run_daily.py` — **CONDITIONAL PASS (behavior regression found)**

- **Refactor correctness:** `update_reminders_from_dataframe(conn, df, school, verbose, skip_note_scoring, note_score_model, note_score_version, return_completed_lessons)` correctly extracts the per-row DB upsert loop that was previously inline in `main()`. All internal references to `school_subdomain` → `school`, `args.verbose` → `verbose`, `args.note_score_model` → `note_score_model`, etc. were consistently renamed via the diff. Confirmed no leftover `args.` references inside the extracted function body.
- **Regression (real, behavior change): the original per-row `conn = sqlite3.connect(DB_PATH)` / `conn.close()` pattern (opening and closing a fresh connection for *every row* in the old code) was correctly removed and replaced with a single connection passed in and reused across all rows** — this is actually a **quality improvement** (avoids N connection round-trips), not a regression, and functionally equivalent as long as no code between rows depended on connection isolation (none does — confirmed no transaction-per-row semantics existed before, `conn.commit()` was called after every insert/update in the original too, preserved in the refactor at the same relative position).
- **Real regression — exception handling changed silently:** In the original inline code, if `score_note_quality()` raised `FatalScoringError` **mid-loop** (line ~798-816 of the *old* code, now inside `update_reminders_from_dataframe`), the original handler did: `conn.close()`, sent a delay-notice email, then `raise SystemExit(...)`. In the refactored `update_reminders_from_dataframe` (lines 798-806 new), this was replaced with:
  ```python
  except FatalScoringError:
      raise
  ```
  i.e., the function now just re-raises the bare exception with **no email side-effect inside the function** — which is *intentional* by design (email-sending was correctly moved up to `main()`'s new outer `try/except FatalScoringError as exc:` block at lines ~1015-1030, which does call `send_delay_notice(...)` after catching). **This refactor is actually correct** — the email-on-failure behavior is preserved, just relocated from inside the row loop to a single outer handler in `main()`. Verified: `main()`'s new wrapping code (visible in the diff, lines "conn = sqlite3.connect(DB_PATH) / try: result = update_reminders_from_dataframe(...) / except FatalScoringError as exc: ... send_delay_notice(...)") correctly reproduces the original email-then-exit behavior.
  **However, one behavior difference remains:** in the **original** code, a `FatalScoringError` raised on row N would have already `conn.commit()`-ed rows 1..N-1 individually (each row committed before moving to the next) — so partial progress up to the failing row was persisted. In the **refactored** code this is *still true* (each row still calls `conn.commit()` immediately after its own insert/update, unchanged), so **no regression** here either. Both versions commit incrementally per-row and both correctly send the delay-notice + exit on fatal scoring errors. This is a genuine PASS after closer inspection — flagged initially as suspicious but verified correct on full trace.
- **`pike13_import_and_update_db` (MCP tool) always calls with `skip_note_scoring` defaulting to `True`** (the new function's default), meaning **MCP-triggered scrapes never run LLM note scoring**, unlike `run_daily.py`'s cron path which scores notes by default (`skip_note_scoring` only True if `--skip-note-scoring` flag passed). This is called out in the docstring ("default True for MCP use") and appears to be an intentional design choice (avoid slow/costly OpenAI calls when Hugh invokes the tool ad-hoc via chat) — not a bug, but worth confirming with Hugh that this is the desired default, since it means notes fetched via the MCP tool won't have quality scores unless he later runs `run_daily.py` normally over the same date range.
- **No import errors found.** `sqlite3`, `pd`, all pre-existing imports remain valid; `update_reminders_from_dataframe` is importable from `run_daily` by `notesreminder/mcp/tools.py` without triggering unwanted side effects (confirmed `run_daily.py`'s module-level code is just constants + `load_dotenv()`, no argparse execution or network calls at import time).

### 12. `.env.example` — **PASS**

- Adds `OKTA_USERNAME`, `OKTA_PASSWORD`, `PIKE13_COOKIES_PATH`, `PIKE13_COOKIE_MAX_AGE_DAYS` with empty/default placeholder values — no real secrets committed. ✅
- **Minor inconsistency:** `OKTA_USERNAME`/`OKTA_PASSWORD` are added here per the plan, but **neither `cookie_auth.py` nor `noteschecker.py` nor `mcp/tools.py` actually reads these two env vars anywhere** (confirmed via repo-wide grep — only `scripts/extract_school_emails.py`, a pre-existing unrelated file, reads `OKTA_USERNAME`/`OKTA_PASSWORD`/`SOR_OKTA_USERNAME`/`SOR_OKTA_PASSWORD`). The new cookie-injection code path **does not use Okta credentials at all** — by design, it relies entirely on pre-extracted cookies, never performing a live Okta login itself. So adding these vars to `.env.example` under a comment "used by cookie injection path" is **misleading/incorrect documentation** — they're not used by the new code; they're only used by the pre-existing, unrelated `extract_school_emails.py` script. Recommend fixing the comment to avoid confusing Hugh into thinking he must set Okta credentials for the new cookie-based flow (he doesn't — he only needs to run the Mac-side extraction script once, which uses his already-authenticated browser, not credentials).

### 13. `.gitignore` — **PASS**

- Adds `pike13_cookies.json` — correctly prevents the sensitive cookie file from ever being committed. ✅ Correctly placed alongside other sensitive/generated-file entries (`browser_profiles/`, `dialpad_profile/`).
- **Verify:** confirmed the file is not already tracked in git history prior to this commit (no `pike13_cookies.json` appears in the repo's git objects reviewed) — no accidental prior commit of the secret file.

---

## Bugs Found (Summary Table)

| # | Severity | File | Location | Issue |
|---|----------|------|----------|-------|
| 1 | **High (blocking)** | `scripts/run_mcp_server.sh` | line 1-2 | `#!/bin/sh` + `set -o pipefail` — dash doesn't support `pipefail`; **systemd service may fail to start** on stock Ubuntu. Must verify `/bin/sh` target on Oracle server or change shebang to `#!/bin/bash`. |
| 2 | Medium | `scripts/refresh_pike13_cookies.sh` | line 1-2 | Same `sh`+`pipefail` incompatibility; script may fail when run with `sh` instead of explicit `bash`. |
| 3 | Medium | `deploy/install_mcp_server.sh` | line 1-2 | Same `sh`+`pipefail` incompatibility. |
| 4 | Medium | `scripts/extract_pike13_cookies.py` | line 142 | Cookie JSON written with default file permissions (world-readable); should `chmod 0o600` after write — file contains live session credentials. |
| 5 | Low | `noteschecker.py` | line 17 (import), never called | `inject_storage_into_page` imported but never invoked — localStorage-based auth tokens (if any) are never restored, potentially reducing cookie-auth success rate for storage-dependent Okta sessions. |
| 6 | Low | `notesreminder/lib/cookie_auth.py` | line 18 | `CookieExpiredError` defined but unused/dead code. |
| 7 | Low | `.env.example` | new lines after 15 | `OKTA_USERNAME`/`OKTA_PASSWORD` comment claims they're "used by cookie injection path" but no new code reads them — misleading documentation. |
| 8 | Low | `mcp/tools.py` | `pike13_scrape_lessons`, `pike13_import_and_update_db` | No try/except around `scrape_lessons()` calls — uncaught exceptions surface as raw tracebacks to MCP clients instead of structured JSON errors (inconsistent with `pike13_cookie_status`'s error handling). |
| 9 | Low (informational) | `notesreminder-mcp.service` + `run_mcp_server.sh` | — | `.env` sourcing in the shell script can silently override the systemd-set `REMINDERS_DB_PATH`/`PIKE13_COOKIES_PATH` env vars if `.env` also defines them on the server — operational gotcha, not a bug per se. |
| 10 | Low (informational) | `mcp/tools.py` `pike13_import_and_update_db` | line 110-118 | Updates local `reminders.db` only, no S3 sync — may diverge from the cron-maintained, S3-synced copy if `REMINDERS_DB_PATH` differs between the MCP server and `run_daily.py`. |

---

## Backward Compatibility Assessment

**Confirmed backward compatible.** Traced all 3 pre-existing invocation modes:

1. **No cookies, no profile_dir, PIKE13_USER/PASSWORD set (old CI/cron default):** `load_cookies()` raises `CookieAuthError` → caught → falls through unchanged to form-login. Identical to pre-change behavior.
2. **`profile_dir` passed (Hugh's local Mac persistent-profile workflow):** Cookie injection block is skipped entirely via `if not profile_dir:` guard. Identical to pre-change behavior.
3. **`interactive_login=True` (headed MFA wait):** Untouched by this change; only reachable inside the `if not cookie_auth_attempted or not await is_authenticated():` block, same as before.

`run_daily.py`'s CLI args, `mcp_server.py`'s default `stdio` transport, and all 20+ pre-existing MCP tools are unmodified in behavior.

---

## Recommendation

**Ready for Hugh to use in a supervised capacity (run the MCP server and cron job with someone watching the first few runs), but NOT yet safe for fully unattended/cron-only operation until:**

1. **Fix the shell shebang/pipefail bug (#1, #2, #3)** — verify `/bin/sh` on the Oracle server, or switch shebangs to `#!/bin/bash` in all 3 affected scripts. This could silently prevent the MCP systemd service from ever starting.
2. **Add `chmod 0o600` to the cookie file after extraction and after SCP transfer (#4)** — trivial fix, meaningful security improvement for a file that is a live session credential.
3. Consider wiring up `inject_storage_into_page` (#5) if cookie-only auth proves unreliable in practice — currently dead code that the plan explicitly called for.
4. Fix the `.env.example` comment (#7) so Hugh doesn't waste time hunting for Okta credentials he doesn't need to set for this flow.

None of the remaining low-severity items block a first supervised run. The core cookie-injection logic in `noteschecker.py` and `cookie_auth.py` — the highest-risk surface area — is correct: cookies are injected before navigation, expired cookies are filtered, freshness gating works, and every pre-existing login path is preserved unchanged when cookies are absent or the injection fails.
