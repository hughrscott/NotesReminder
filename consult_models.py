#!/usr/bin/env python3
"""Consult Deepseek v4 pro (via local proxy) and Gemini 3.5 (OpenRouter) on the
HubSpot auth architecture decision, and print both raw opinions."""
import os, json, urllib.request

# load keys from ~/.hermes/.env (read-only)
env_path = os.path.expanduser("~/.hermes/.env")
vals = {}
if os.path.exists(env_path):
    for line in open(env_path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line and not line.startswith("DISABLED"):
            k, v = line.split("=", 1)
            vals[k.strip()] = v.strip()

PROMPT = """You are advising on the auth architecture for a headless (no-GUI) Linux server that scrapes HubSpot data nightly.

CONTEXT:
- Auth flow: hubspot.com/login -> enter email huscott@schoolofrock.com -> click "Sign in with SSO" -> Okta SAML -> HubSpot.
- The ONLY enrolled MFA factor is Okta Verify PUSH to the user's iPhone (no SMS, no TOTP). Approving requires the user to physically tap their phone.
- We maintain a persistent Chromium profile `sor_shared` that holds a WARM Okta session.
- Findings from testing:
  (1) With the warm Okta session, HubSpot SSO SOMETIMES auto-authenticates with NO push and NO password (desirable). But INTERMITTENTLY the warm session auto-redirects PAST the login page, so the email field (`#username`) never appears and the script cannot tell whether it is already authenticated.
  (2) The HubSpot data we need lives at app.hubspot.com/ (CRM home, which stays authenticated). Navigating directly to app.hubspot.com/contacts FORCE-REDIRECTS to login even from an authenticated session.
  (3) The data extractor uses synchronous Playwright.
  (4) Running the HubSpot backfill and the Pike13 backfill concurrently on `sor_shared` causes a Chrome SingletonLock conflict, so they must be SERIALIZED.

We need the HubSpot auth to be DETERMINISTIC and reliable. Three candidate approaches:

(a) "Trust the warm session": Detect 'already on HubSpot / auto-redirected' and proceed without a push. Always try to skip the push; only fall back to a push if we truly land on the Okta password page. Handle the #username-absent-but-maybe-authed case.

(b) "Always force a fresh login": Every run, fill the Okta password and send a push, ping the user on Telegram, and wait for iPhone approval. 100% reliable and deterministic, but requires the user to approve a push on every run (they are OK with that via a Telegram ping).

(c) "Unified session manager": Refactor into one module that pre-warms Okta ONCE, then drives each service (Pike13, HubSpot, Dialpad, Gmail) from that single warm session, serializing access with a lock. More upfront work but centralizes auth.

Question: Which approach (or combination) do you recommend, and why? Weigh: (1) reliability/determinism, (2) user friction (iPhone push taps), (3) maintainability of headless server code. Be concrete and concise. If you recommend (a), specify exactly how to robustly detect the authenticated/auto-redirected state without a push."""

def call_openai_compat(url, api_key, model, prompt, timeout=120):
    body = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a pragmatic senior engineer. Answer concisely and concretely."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.3,
    }).encode()
    req = urllib.request.Request(url, data=body, headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer " + api_key,
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.load(r)
    return d["choices"][0]["message"]["content"]

print("=" * 70)
print("DEEPSEEK v4 PRO (local proxy)")
print("=" * 70)
try:
    ds = call_openai_compat(
        "http://localhost:4000/v1/chat/completions",
        vals.get("LITELLM_MASTER_KEY", "sk-1234"),  # proxy may not need a real key
        "deepseek-v4-pro", PROMPT)
    print(ds)
except Exception as e:
    print("DEEPSEEK ERROR:", repr(e))

print()
print("=" * 70)
print("GEMINI 3.5 PRO (OpenRouter)")
print("=" * 70)
gem_ok = False
for slug in ["google/gemini-3.5-pro", "gemini-3.5-pro", "google/gemini-3.5-flash"]:
    try:
        gm = call_openai_compat(
            "https://openrouter.ai/api/v1/chat/completions",
            vals.get("OPENROUTER_API_KEY", ""), slug, PROMPT)
        print(f"[model: {slug}]")
        print(gm)
        gem_ok = True
        break
    except Exception as e:
        print(f"GEMINI slug {slug} failed: {e}")
if not gem_ok:
    print("All Gemini OpenRouter slugs failed.")
