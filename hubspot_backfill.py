#!/usr/bin/env python3
"""HubSpot backfill - SYNC Playwright (matches extract_hubspot_leads.py API),
on its OWN warm profile copy (browser_profiles/hubspot) so it runs CONCURRENTLY
with the Pike13 backfill (browser_profiles/sor_shared). NO API access - scraping only.

Auth (consensus: Deepseek v4 pro + Gemini 3.5): navigate to app.hubspot.com/, check the
FINAL url after redirects. If app.hubspot.com/... (no login/okta) -> warm session valid,
SKIP the push. Only if redirected to login/Okta, run email->SSO->password+push fallback.

Scrape: the REAL deals board is the PORTAL-SPECIFIC URL
https://app.hubspot.com/sales/6841203/deals (portal 6841203 from dashboard URL) ->
redirects to /contacts/6841203/objects/0-3/views/all/list and shows the deal table.
Reached via in-page location.href nav (preserves warm session; raw goto breaks SPA routing).
We delegate scraping to extract_hubspot_leads.capture_visible_deal_rows (sync)."""
from __future__ import annotations
import sys, time, os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))
from dotenv import load_dotenv
load_dotenv(Path.home() / ".hermes" / "SOR" / ".sorenv")
from playwright.sync_api import sync_playwright

HUBSPOT_EMAIL = "huscott@schoolofrock.com"
HUBSPOT_PROFILE = Path("browser_profiles/hubspot")
HUBSPOT_DEALS_URL = "https://app.hubspot.com/sales/6841203/deals"

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def sso_handshake(page):
    page.goto("https://app.hubspot.com/", wait_until="domcontentloaded")
    page.wait_for_timeout(6000)
    uu = page.url.lower()
    if "hubspot.com" in uu and "login" not in uu and "okta" not in uu:
        log("WARM SESSION VALID -> %s (skip push)" % page.url); return True
    log("warm session not valid (url=%s); running email->SSO->push fallback" % page.url)
    page.goto("https://app.hubspot.com/login", wait_until="domcontentloaded")
    page.wait_for_timeout(5000)
    try:
        ab = page.locator("button:has-text('Accept All')").first
        if ab.count() and ab.is_visible(): ab.click()
    except Exception: pass
    page.wait_for_timeout(1000)
    uname = page.locator("#username")
    try:
        uname.wait_for(state="visible", timeout=8000)
    except Exception:
        if "okta.com" in page.url.lower():
            return _okta_password_push(page)
        return False
    uname.fill(HUBSPOT_EMAIL)
    page.wait_for_timeout(1000)
    page.locator("button:has-text('Continue')").first.click()
    page.wait_for_timeout(4000)
    sso = page.locator("button:has-text('Sign in with SSO'), a:has-text('Sign in with SSO')").first
    if sso.count():
        sso.click(); log("clicked Sign in with SSO -> Okta")
    t0 = time.time()
    while time.time() - t0 < 30:
        page.wait_for_timeout(3000)
        uu = page.url.lower()
        if "hubspot.com" in uu and "login" not in uu:
            log("AUTO-AUTH LANDED -> %s" % page.url); return True
        if "okta.com" in uu:
            log("on Okta password page"); return _okta_password_push(page)
    log("TIMEOUT waiting for SSO result"); return False

def _okta_password_push(page):
    from okta_auth.telegram_bot import _notify
    try:
        pw = page.locator("input[type=password]").first
        if pw.count() and not pw.input_value():
            pw.fill(os.environ.get("OKTA_PASSWORD", ""))
    except Exception as e:
        log("pw fill err: %s" % e)
    for sel in ["button:has-text('Send push')", "button:has-text('Verify')", "button:has-text('Sign in')", "input[type=submit]"]:
        el = page.locator(sel).first
        if el.count() and el.is_visible():
            log("clicking Okta control: %s" % sel); el.click(); break
    _notify("📲 HubSpot/Okta push - approve on your iPhone to continue the HubSpot backfill")
    t0 = time.time()
    while time.time() - t0 < 360:
        page.wait_for_timeout(4000)
        uu = page.url.lower()
        if "hubspot.com" in uu and "login" not in uu:
            log("APPROVED -> %s" % page.url); return True
    log("TIMEOUT waiting for Okta approval"); return False

def goto_deals_board(page):
    """Navigate to the portal-specific deals board via in-page nav (preserves session)."""
    if "login" in page.url.lower():
        return False
    try:
        page.evaluate("(u) => { window.location.href = u; }", HUBSPOT_DEALS_URL)
    except Exception as e:
        log("deals nav evaluate err: %s" % e)
    page.wait_for_timeout(7000)
    body = page.locator("body").inner_text()
    if "login" in page.url.lower():
        log("deals nav redirected to login; session lost"); return False
    if any(t in body.lower() for t in ["lead pipeline", "deal stage", "pipeline"]):
        log("deals board loaded: %s" % page.url); return True
    log("deals board url=%s but no deal tokens (len=%d)" % (page.url, len(body)))
    return False

def main():
    import sqlite3
    from extract_hubspot_leads import (
        ensure_lead_followup_schema, start_import_run, finish_import_run,
        capture_visible_deal_rows, filter_deal_rows_by_school,
        upsert_deal, parse_deal_text, merge_deal_rows, upsert_contact_from_text,
        wait_until_ready, write_raw_capture,
    )
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            str(HUBSPOT_PROFILE), headless=True, viewport={"width": 1440, "height": 1000})
        page = context.pages[0] if context.pages else context.new_page()
        if not sso_handshake(page):
            log("SSO handshake failed; aborting"); context.close(); return
        if not goto_deals_board(page):
            log("could not reach deals board; aborting"); context.close(); return
        conn = sqlite3.connect(str(Path("reminders.db").resolve()))
        ensure_lead_followup_schema(conn)
        run_id = start_import_run(conn, "hubspot", "hubspot_backfill.py", None, None, {"url": page.url})
        conn.commit()
        # capture_visible_deal_rows: SYNC Playwright, returns 3-tuples (deal_id, link, spine_row)
        visible = capture_visible_deal_rows(page, 200)
        rows = filter_deal_rows_by_school(visible, None)
        log("captured %d visible deal rows, %d after filter (url=%s)" % (len(visible), len(rows), page.url))
        written = 0
        for i, (deal_id, link, spine) in enumerate(rows):
            upsert_deal(conn, spine); written += 1
            if i < 25:
                try:
                    dp = context.new_page()
                    dp.goto(link["href"], wait_until="domcontentloaded", timeout=60000)
                    wait_until_ready(dp)
                    txt = dp.locator("body").inner_text(timeout=30000)
                    write_raw_capture(conn, source="hubspot", capture_type="hubspot_deal_text",
                                      content=txt, source_url=dp.url,
                                      metadata={"deal_id": deal_id}, import_run_id=run_id,
                                      extension="txt", label=f"deal-{deal_id}")
                    detail = parse_deal_text(deal_id, dp.url, txt)
                    upsert_deal(conn, merge_deal_rows(spine, detail)); written += 1
                    written += upsert_contact_from_text(conn, deal_id, dp.url, txt)
                    dp.close()
                except Exception as e:
                    log("detail err %s: %s" % (deal_id, e))
        finish_import_run(conn, run_id, "success", len(rows), written, 0)
        conn.commit(); conn.close()
        log("DONE: wrote %d hubspot rows" % written)
        context.close()

if __name__ == "__main__":
    main()
