#!/usr/bin/env python3
"""
pike13_mapper.py — Full Pike13 application mapper (School of Rock).

Authenticates as staff, then crawls the app capturing BOTH layers:
  (a) UI surface:   nav menus, links, buttons, forms, data fields
  (b) network/API:  every request + response (esp. JSON endpoints behind each page)

Output:
  models/pike13_app_map.json          final structured catalog
  models/pike13_app_map_progress.json incremental (resumable)

PRIVACY: only STRUCTURE is stored (field names, endpoint paths, param shapes,
top-level JSON keys). No student/parent record VALUES are persisted.

Run:
  python3 pike13_mapper.py [--school westu-sor] [--max-pages 60]
"""
import asyncio
import sys
import json
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone

sys.path.insert(0, "/home/ubuntu/projects/hughrscott/NotesReminder")
import pike13_auto_auth
from playwright.async_api import async_playwright

ROOT = Path("/home/ubuntu/projects/hughrscott/NotesReminder")
MODELS = ROOT / "models"
OUT = MODELS / "pike13_app_map.json"
PROG = MODELS / "pike13_app_map_progress.json"

APP_HOST_RE = re.compile(r"https://[\w-]+\.pike13\.com")

SEEN = set()
ENDPOINTS = {}
UI = {"nav_menus": [], "links": [], "buttons": [], "forms": [], "fields": []}
PAGES_VISITED = []


def norm(u):
    p = urlparse(u)
    q = parse_qs(p.query)
    return p.path + "?" + "&".join(sorted(q.keys()))


def snapshot_state():
    return {
        "endpoints": {
            k: {
                "method": v["method"],
                "path": v["path"],
                "count": v["count"],
                "content_types": sorted(v["content_types"]),
                "top_keys": sorted(v["top_keys"]),
                "examples": v["examples"][:2],
            }
            for k, v in ENDPOINTS.items()
        },
        "ui": UI,
        "pages_visited": PAGES_VISITED,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def save_progress():
    PROG.write_text(json.dumps(snapshot_state(), indent=2, default=list))


async def on_response(resp):
    try:
        url = resp.url
        if not APP_HOST_RE.search(url):
            return
        method = resp.request.method
        ct = resp.headers.get("content-type", "")
        is_json = ("json" in ct) or "/api/" in url or "graphql" in url.lower()
        if not is_json:
            return
        key = f"{method} {norm(url)}"
        top = []
        try:
            body = await resp.body()
            if body and "json" in ct:
                try:
                    d = json.loads(body[:200000])
                    if isinstance(d, dict):
                        top = list(d.keys())[:40]
                    elif isinstance(d, list) and d and isinstance(d[0], dict):
                        top = list(d[0].keys())[:40]
                except Exception:
                    pass
        except Exception:
            pass
        e = ENDPOINTS.get(
            key,
            {
                "method": method,
                "path": norm(url),
                "count": 0,
                "content_types": set(),
                "top_keys": set(),
                "examples": [],
            },
        )
        e["count"] += 1
        e["content_types"].add(ct)
        for k in top:
            e["top_keys"].add(k)
        if len(e["examples"]) < 2:
            e["examples"].append(url)
        ENDPOINTS[key] = e
    except Exception:
        pass


async def collect_ui(page):
    links = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => ({text: (e.innerText||'').trim().slice(0,80), href: e.href}))",
    )
    for l in links:
        if APP_HOST_RE.search(l.get("href", "")) and l.get("text"):
            UI["links"].append(l)
    btns = await page.eval_on_selector_all(
        "button",
        "els => els.map(e => (e.innerText||e.getAttribute('aria-label')||'').trim().slice(0,80))",
    )
    UI["buttons"].extend([b for b in btns if b])
    fields = await page.eval_on_selector_all(
        "input,select,textarea",
        "els => els.map(e => ({tag:e.tagName, type:e.getAttribute('type'), name:e.getAttribute('name'), id:e.id, placeholder:e.getAttribute('placeholder')}))",
    )
    UI["fields"].extend(
        [f for f in fields if f.get("name") or f.get("id") or f.get("placeholder")]
    )


async def crawl(playwright, school, max_pages):
    context = await pike13_auto_auth.authenticate_pike13(
        playwright, school_subdomain=school, headless=True, verbose=False
    )
    page = context.pages[0]
    page.on("response", on_response)
    base = f"https://{school}.pike13.com"

    # Land on the desk; tolerate routing differences.
    try:
        await page.goto(base + "/desk", wait_until="networkidle", timeout=60000)
    except Exception:
        await page.goto(base + "/people", wait_until="networkidle", timeout=60000)

    # Discover nav links from common containers.
    nav_links = await page.eval_on_selector_all(
        "nav a[href], header a[href], [class*='sidebar'] a[href], [class*='menu'] a[href]",
        "els => els.map(e => e.href)",
    )
    seeds = list({u for u in nav_links if APP_HOST_RE.search(u)})
    # Known important routes as a floor.
    for r in [
        "/people",
        "/reports",
        "/desk/reports",
        "/schedule",
        "/clients",
        "/plans",
        "/billing",
        "/settings",
        "/dashboard",
    ]:
        seeds.append(base + r)
    seeds = [u for u in seeds if norm(u) not in SEEN][:max_pages]

    print(f"[map] {len(seeds)} seed routes, crawling...")
    for i, url in enumerate(seeds):
        n = norm(url)
        if n in SEEN:
            continue
        SEEN.add(n)
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await collect_ui(page)
            PAGES_VISITED.append(url)
        except Exception as ex:
            PAGES_VISITED.append({"url": url, "error": str(ex)[:160]})
        if (i + 1) % 5 == 0:
            save_progress()
            print(
                f"  [map] {i+1}/{len(seeds)} pages, {len(ENDPOINTS)} endpoints, "
                f"{len(UI['links'])} links, {len(UI['buttons'])} buttons"
            )

    save_progress()
    OUT.write_text(json.dumps(snapshot_state(), indent=2, default=list))
    print(
        f"\nDONE. {len(ENDPOINTS)} endpoints, {len(UI['links'])} links, "
        f"{len(UI['buttons'])} buttons -> {OUT}"
    )
    await context.close()


if __name__ == "__main__":
    school = sys.argv[sys.argv.index("--school") + 1] if "--school" in sys.argv else "westu-sor"
    mp = 60
    if "--max-pages" in sys.argv:
        mp = int(sys.argv[sys.argv.index("--max-pages") + 1])

    async def main():
        async with async_playwright() as pw:
            await crawl(pw, school, mp)

    asyncio.run(main())
