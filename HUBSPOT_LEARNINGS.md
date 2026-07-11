# HubSpot Backfill — Learnings (VERIFIED WORKING 2026-07-11)

## HARD CONSTRAINT — NO API ACCESS (Hugh, explicit, non-negotiable)
- Hugh does NOT have and will NEVER have HubSpot API access. SCRAPING/BROWSER ONLY.
- Do NOT propose API keys, Private App tokens, OAuth, or fetch() to api.hubspot.com.
  (api.hubspot.com -> 401 needs token; app.hubspot.com/api-crm/... -> 404.)
- The ONLY sanctioned path: browser scraping with the warm Okta session on browser_profiles/hubspot.

## Flow (user-confirmed)
`hubspot.com/login` -> email `huscott@schoolofrock.com` -> "Sign in with SSO" -> Okta SAML -> HubSpot.
The Okta "HubSpot" tile is a bookmark; it NEVER fires a real push. Real flow is email-initiated.

## Verified working config
- `hubspot_backfill.py` = **SYNC Playwright** (matches `extract_hubspot_leads.py` API).
  NOTE: `capture_visible_deal_rows(page, limit)` is SYNC (no `await`) -> backfill MUST be sync.
- Auth: navigate to `app.hubspot.com/`, check FINAL url. If `app.hubspot.com/...` (no login/okta)
  -> WARM SESSION VALID -> SKIP push. Fallback only if redirected to Okta (password+push+Telegram).
- Deals board URL (PORTAL-SPECIFIC, found via probe): `https://app.hubspot.com/sales/6841203/deals`
  (portal 6841203 from dashboard URL) -> redirects to `/contacts/6841203/objects/0-3/views/all/list`,
  shows the deal table. Reached via `page.evaluate("location.href = URL")` (in-page nav preserves session).
  GENERIC URLs (/deals-board, /deals, /pipeline, /contacts/6841203/deal) 404 or redirect to login.
- Tables written: `hubspot_deals` (50 rows) + `hubspot_contacts` (1233 rows). NOT `hubspot_lead_followup`.

## Concurrency
- HubSpot uses its OWN profile copy `browser_profiles/hubspot` (copied from sor_shared).
- Pike13 uses `browser_profiles/sor_shared`. Both run CONCURRENTLY, no SingletonLock clash.
- Serial is NOT required.

## Result (2026-07-11 run)
WROTE 50 hubspot_deals + 1233 hubspot_contacts. Zero push, zero API. Confirmed.
