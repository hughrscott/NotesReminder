#!/usr/bin/env python3
"""Backfill orchestrator — runs per-source, per-month, with DeepSeek-pro verification.

Design:
- Serial execution (the warm Okta profile dir is shared; concurrent use clashes).
- Each (source, school, month-chunk) is one unit of work.
- After each chunk, a before/after SQL check is sent to DeepSeek-pro (via the
  local litellm proxy, direct HTTP — no tool-call wrapper) which returns
  PASS/FAIL with a one-line reason.
- Resume-safe: a progress JSON records every chunk's status; re-running skips
  already-verified chunks.
- Per-chunk timeout so a hung extractor can't stall the whole run.
- Final Telegram summary on completion.

Usage:
  python backfill_orchestrator.py            # run full plan Jan17->today
  python backfill_orchestrator.py --source pike13   # one source only
  python backfill_orchestrator.py --dry        # print plan, no execution
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent
DB = REPO / "reminders.db"
LOG_DIR = REPO / "logs" / "backfill_chunks"
LOG_DIR.mkdir(parents=True, exist_ok=True)
PROFILE = REPO / "browser_profiles" / "sor_shared"
VENV_PY = Path.home() / ".hermes" / "env" / "bin" / "python"
PY = str(VENV_PY if VENV_PY.exists() else "python3")
PROGRESS = REPO / "backfill_progress.json"
LOG = REPO / "backfill_run.log"

SCHOOLS = ["westu-sor", "theheights-sor"]

# DeepSeek-pro via local litellm proxy (direct HTTP, avoids Vertex tool-call bug)
DEEPSEEK_URL = "http://localhost:4000/v1/chat/completions"
DEEPSEEK_MODEL = "deepseek-v4-pro"

# Per-chunk wall-clock timeout (seconds). A hung scraper won't stall the queue.
CHUNK_TIMEOUT_S = int(os.environ.get("BACKFILL_CHUNK_TIMEOUT_S", "1500"))


def log(msg: str):
    line = f"{datetime.now().isoformat(timespec='seconds')}  {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def load_progress() -> dict:
    if PROGRESS.exists():
        try:
            return json.loads(PROGRESS.read_text())
        except Exception:
            pass
    return {"chunks": {}}


def save_progress(p: dict):
    PROGRESS.write_text(json.dumps(p, indent=2, default=str))


def month_chunks(start: date, end: date):
    """Yield (chunk_start, chunk_end) one calendar month at a time, inclusive."""
    y, m = start.year, start.month
    cur = date(y, m, 1)
    while cur <= end:
        # last day of this month
        if m == 12:
            nxt = date(y + 1, 1, 1)
        else:
            nxt = date(y, m + 1, 1)
        last = nxt - timedelta(days=1)
        cs = max(cur, start)
        ce = min(last, end)
        yield cs, ce
        cur = nxt
        y, m = cur.year, cur.month


# --- Source definitions -----------------------------------------------------
# Each job: builds a command list and a verify SQL (params: start, end).
def jobs_for(source: str, start: str, end: str, school: str | None) -> list[dict]:
    prof = str(PROFILE)
    if source == "pike13":
        return [{
            "source": "pike13", "school": school,
            "cmd": [PY, "run_daily.py", "--school", school,
                    "--start-date", start, "--end-date", end,
                    "--pike13-profile-dir", prof, "--no-email",
                    "--skip-note-scoring", "--skip-s3-sync",
                    "--db-path", str(DB)],
            "verify_sql": (
                "SELECT COUNT(*) FROM lessons WHERE lesson_date BETWEEN ? AND ? "
                "AND school_id IS NOT NULL"
            ),
            "verify_params": [start, end],
            "verify_table": "lessons",
        }]
    if source == "dialpad_sms":
        return [{
            "source": "dialpad_sms", "school": None,
            "cmd": [PY, "scripts/extract_dialpad_sms.py", "--db", str(DB),
                    "--profile-dir", prof, "--start-date", start, "--headless"],
            "verify_sql": (
                "SELECT COUNT(*) FROM dialpad_sms_messages "
                "WHERE substr(message_at,1,10) BETWEEN ? AND ?"
            ),
            "verify_params": [start, end],
            "verify_table": "dialpad_sms_messages",
        }]
    if source == "dialpad_voice":
        return [{
            "source": "dialpad_voice", "school": None,
            "cmd": [PY, "scripts/extract_dialpad_daily_intake.py", "--db", str(DB),
                    "--profile-dir", prof, "--school", school or "westu-sor",
                    "--window-days", "40", "--headless"],
            "verify_sql": (
                "SELECT COUNT(*) FROM dialpad_voice_events "
                "WHERE substr(event_at,1,10) BETWEEN ? AND ?"
            ),
            "verify_params": [start, end],
            "verify_table": "dialpad_voice_events",
        }]
    if source == "hubspot":
        return [{
            "source": "hubspot", "school": None,
            "cmd": [PY, "scripts/run_date_window_lead_load.py", "--db", str(DB),
                    "--hubspot-profile-dir", prof, "--start-date", start,
                    "--end-date", end, "--headless"],
            "verify_sql": (
                "SELECT COUNT(*) FROM hubspot_deals "
                "WHERE COALESCE(last_activity_date,'') BETWEEN ? AND ?"
            ),
            "verify_params": [start, end],
            "verify_table": "hubspot_deals",
        }]
    if source == "school_email":
        return [{
            "source": "school_email", "school": None,
            "cmd": [PY, "school_email.py", "--db", str(DB),
                    "--profile-dir", prof, "--start-date", start, "--headless"],
            "verify_sql": (
                "SELECT COUNT(*) FROM school_email_messages "
                "WHERE substr(message_at,1,10) BETWEEN ? AND ?"
            ),
            "verify_params": [start, end],
            "verify_table": "school_email_messages",
        }]
    raise ValueError(f"unknown source {source}")


ALL_SOURCES = ["pike13", "dialpad_sms", "dialpad_voice", "hubspot", "school_email"]


def count_rows(sql: str, params: list) -> int:
    c = sqlite3.connect(str(DB))
    try:
        r = c.execute(sql, params).fetchone()
        return r[0] if r else 0
    finally:
        c.close()


def deepseek_verify(source: str, school: str, start: str, end: str,
                    before: int, after: int, max_before: str, max_after: str) -> dict:
    prompt = f"""You are verifying a data-backfill chunk for a School of Rock CRM DB.

Source: {source}{(' school='+school) if school else ''}
Backfill window: {start} .. {end}
Verify table row count for that window:
  BEFORE chunk: {before} rows
  AFTER  chunk: {after} rows
Table MAX(date) overall:
  BEFORE: {max_before}
  AFTER:  {max_after}

Rules:
- This is a BACKFILL: the chunk is only a SUCCESS if it ADDED new data for the window.
- PASS only if AFTER > BEFORE (the chunk inserted new rows for the window).
- FAIL if AFTER == BEFORE AND BEFORE == 0 (window still empty, nothing added).
- FAIL if AFTER == BEFORE AND BEFORE > 0 (no new rows added — chunk did no work; do NOT pass on stale pre-existing data).
- The extractor return code is already checked separately; a non-zero rc is an automatic FAIL.

Reply with exactly one line:
VERDICT: PASS|FAIL — <one-line reason>"""
    try:
        req = urllib.request.Request(
            DEEPSEEK_URL,
            data=json.dumps({
                "model": DEEPSEEK_MODEL,
                "messages": [{"role": "user", "content": prompt}],
            }).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=90) as r:
            d = json.load(r)
        text = d["choices"][0]["message"]["content"].strip()
    except Exception as e:  # noqa: BLE001
        text = f"VERDICT: UNKNOWN — deepseek call failed: {e}"
    verdict = "PASS" if text.upper().startswith("VERDICT: PASS") else (
        "FAIL" if text.upper().startswith("VERDICT: FAIL") else "UNKNOWN")
    return {"raw": text, "verdict": verdict}


def run_chunk(job: dict, start: str, end: str) -> dict:
    key = f"{job['source']}:{job.get('school') or '-'}:{start}"
    before = count_rows(job["verify_sql"], job["verify_params"])
    max_before = None
    log(f"[START] {key}  cmd={' '.join(job['cmd'])}")
    t0 = time.time()
    # Stream child output to a per-chunk log file (NEVER capture_output=True —
    # a long extractor emits >64KB, fills the pipe, and deadlocks the parent).
    chunk_log = LOG_DIR / f"chunk_{key.replace(':','_')}.log"
    try:
        with open(chunk_log, "w") as lf:
            cp = subprocess.run(job["cmd"], cwd=str(REPO), timeout=CHUNK_TIMEOUT_S,
                                stdout=lf, stderr=subprocess.STDOUT)
        rc = cp.returncode
        log(f"[DONE ] {key} rc={rc} in {int(time.time()-t0)}s  (log: {chunk_log.name})")
        if rc != 0:
            # surface the last lines of the chunk log
            try:
                with open(chunk_log) as lf:
                    lines = lf.read().splitlines()[-25:]
                log(f"        tail: {' | '.join(lines)}")
            except Exception:
                pass
    except subprocess.TimeoutExpired:
        rc = -9
        tail = f"TIMEOUT after {CHUNK_TIMEOUT_S}s"
        log(f"[KILL ] {key} {tail}")
    after = count_rows(job["verify_sql"], job["verify_params"])
    # overall max for context
    try:
        c = sqlite3.connect(str(DB)); cur = c.execute(
            f"SELECT MAX({_date_col(job['verify_table'])}) FROM {job['verify_table']}")
        max_after = cur.fetchone()[0]; c.close()
    except Exception:
        max_after = None
    # PRIMARY GATE: a non-zero extractor return code means the chunk failed,
    # regardless of what pre-existing data is in the table. Never mark PASS.
    if rc != 0:
        log(f"[VERIFY] {key} rc={rc} -> FAIL (extractor error; added 0 new rows)")
        return {"key": key, "rc": rc, "before": before, "after": after,
                "verdict": "FAIL",
                "deepseek": f"extractor rc={rc} (non-zero) -> FAIL",
                "end": end, "finished_at": datetime.now().isoformat(timespec="seconds")}
    dv = deepseek_verify(job["source"], job.get("school"), start, end,
                         before, after, max_before, max_after)
    log(f"[VERIFY] {key} before={before} after={after} -> {dv['verdict']} | {dv['raw']}")
    return {"key": key, "rc": rc, "before": before, "after": after,
            "verdict": dv["verdict"], "deepseek": dv["raw"],
            "end": end, "finished_at": datetime.now().isoformat(timespec="seconds")}


def _date_col(table: str) -> str:
    return {
        "lessons": "lesson_date",
        "dialpad_sms_messages": "message_at",
        "dialpad_voice_events": "event_at",
        "hubspot_deals": "last_activity_date",
        "school_email_messages": "message_at",
    }.get(table, "rowid")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", help="limit to one source")
    ap.add_argument("--school", help="limit pike13 to one school")
    ap.add_argument("--dry", action="store_true", help="print plan only")
    ap.add_argument("--start", default="2026-01-17", help="global start YYYY-MM-DD")
    ap.add_argument("--end", default=date.today().isoformat(), help="global end YYYY-MM-DD")
    args = ap.parse_args()

    sources = [args.source] if args.source else ALL_SOURCES
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    # Build plan
    plan = []
    for src in sources:
        for (cs, ce) in month_chunks(start, end):
            s, e = cs.isoformat(), ce.isoformat()
            for sch in (SCHOOLS if src == "pike13" else [args.school]):
                if src != "pike13" and args.school:
                    sch = args.school
                elif src != "pike13":
                    sch = None
                plan.append((src, sch, s, e))

    log(f"PLAN: {len(plan)} chunks across sources={sources} {start}..{end}")
    if args.dry:
        for src, sch, s, e in plan:
            print(f"  {src:14s} {sch or '-':14s} {s}..{e}")
        return

    progress = load_progress()
    done = 0
    passed = 0
    failed = 0
    for (src, sch, s, e) in plan:
        jobs = jobs_for(src, s, e, sch)
        for job in jobs:
            key = f"{job['source']}:{job.get('school') or '-'}:{s}"
            prev = progress["chunks"].get(key)
            if prev and prev.get("verdict") == "PASS":
                log(f"[SKIP ] {key} already PASS")
                done += 1; passed += 1
                continue
            res = run_chunk(job, s, e)
            progress["chunks"][key] = res
            save_progress(progress)
            done += 1
            if res["verdict"] == "PASS":
                passed += 1
            else:
                failed += 1

    log(f"COMPLETE: {done} chunks, {passed} PASS, {failed} FAIL")
    # Telegram summary
    try:
        import importlib
        sys.path.insert(0, str(REPO))
        from okta_auth.config import get_config
        from telegram import Bot
        c = get_config()
        fails = [k for k, v in progress["chunks"].items() if v.get("verdict") != "PASS"]
        msg = (f"📊 BACKFILL COMPLETE\n"
               f"Chunks: {done} | PASS: {passed} | FAIL: {failed}\n"
               f"Window: {start}..{end}\n")
        if fails:
            msg += "FAILURES:\n" + "\n".join(f"  • {k}: {progress['chunks'][k].get('deepseek','')[:80]}" for k in fails[:15])
        else:
            msg += "All chunks verified by DeepSeek-pro. 🎉"
        async def _s():
            await Bot(c["TELEGRAM_BOT_TOKEN"]).send_message(chat_id=8520226556, text=msg)
        import asyncio
        asyncio.run(_s())
    except Exception as e:  # noqa: BLE001
        log(f"telegram summary failed: {e}")


if __name__ == "__main__":
    main()
