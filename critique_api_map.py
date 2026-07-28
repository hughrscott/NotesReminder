#!/usr/bin/env python3
"""
critique_api_map.py — 3-model-family review of the Pike13 API map.

Calls DEEPSEEK (api.deepseek.com) and GEMINI (generativelanguage.googleapis.com)
in parallel, asks each the SAME gap-analysis question about the Pike13 API map,
saves both critiques to models/api_map_critiques.json. Hermes reconciles after.

Keys are read from ~/.hermes/.env (DEEPSEEK_API_KEY, GEMINI_API_KEY).
No keys are printed or logged.
"""
import os
import sys
import json
import requests
from pathlib import Path

# Load .env (minimal parser, names that matter)
env_path = Path.home() / ".hermes" / ".env"
env = {}
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")

DEEPSEEK_KEY = env.get("DEEPSEEK_API_KEY", "")
GEMINI_KEY = env.get("GEMINI_API_KEY", "")

MAP_PATH = Path.home() / "projects/hughrscott/NotesReminder/models/pike13_app_map.json"
OUT = Path.home() / "projects/hughrscott/NotesReminder/models/api_map_critiques.json"

MAP_TEXT = MAP_PATH.read_text() if MAP_PATH.exists() else "MAP NOT FOUND"

PROMPT = """You are a senior API/data-engineering reviewer. A team is building a "School of Rock operating system" that scrapes Pike13 (a gym/music-school SaaS) because the franchise owner has no usable API access. They auto-discovered the app's internal JSON API by crawling as a logged-in staff user. Your job: find GAPS and RISKS in their discovery and advise what to verify next.

The discovered API surface (structure only, no record values):

{map}

Return STRICT JSON only, no prose outside the JSON:
{{
  "completeness_gaps": ["missing endpoints/areas they should probe next"],
  "risk_flags": ["fragility/blocking/bot-detection risks in relying on these internal endpoints"],
  "verify_first": ["the 3 highest-value endpoints to verify are stable+accessible before building on them"],
  "official_api_advice": ["assessment of developer.pike13.com / whether an official API would replace crawling"],
  "top_recommendation": "one sentence on the single best next move"
}}"""

SYSTEM = "You are a precise API/data-engineering reviewer. Respond with valid JSON only."


def call_deepseek():
    if not DEEPSEEK_KEY:
        return {"error": "DEEPSEEK_API_KEY not found"}
    url = "https://api.deepseek.com/chat/completions"
    body = {
        "model": "deepseek-v4-pro",
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": PROMPT.format(map=MAP_TEXT[:8000])},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.2,
    }
    r = requests.post(url, json=body, headers={"Authorization": f"Bearer {DEEPSEEK_KEY}"}, timeout=180)
    r.raise_for_status()
    return json.loads(r.json()["choices"][0]["message"]["content"])


def call_gemini():
    if not GEMINI_KEY:
        return {"error": "GEMINI_API_KEY not found"}
    # Use gemini-3.5-flash (current, available on generateContent) via Generative Language API
    model = "gemini-3.5-flash"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_KEY}"
    body = {
        "systemInstruction": {"parts": [{"text": SYSTEM}]},
        "contents": [{"parts": [{"text": PROMPT.format(map=MAP_TEXT[:8000])}]}],
        "generationConfig": {"responseMimeType": "application/json", "temperature": 0.2},
    }
    r = requests.post(url, json=body, timeout=180)
    r.raise_for_status()
    data = r.json()
    text = data["candidates"][0]["content"]["parts"][0]["text"]
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    try:
        return json.loads(text)
    except Exception:
        # Retry once asking for raw JSON only
        body["contents"][0]["parts"][0]["text"] = (
            "Return ONLY minified JSON, no markdown, no commentary: "
            + PROMPT.format(map=MAP_TEXT[:8000])
        )
        r2 = requests.post(url, json=body, timeout=180)
        t2 = r2.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        if t2.startswith("```"):
            t2 = t2.split("```")[1]
        return json.loads(t2)


def safe(fn, name):
    try:
        print(f"  [{name}] calling...")
        return fn()
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:300]}"}


if __name__ == "__main__":
    print("Running 3-model-family critique (DeepSeek + Gemini; Hermes reconciles):")
    results = {}
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=2) as ex:
        f_d = ex.submit(safe, call_deepseek, "deepseek")
        f_g = ex.submit(safe, call_gemini, "gemini")
        results["deepseek-v4-pro"] = f_d.result()
        results["gemini-3.5-flash"] = f_g.result()

    OUT.write_text(json.dumps(results, indent=2))
    print(f"\nSaved critiques -> {OUT}")
    for k, v in results.items():
        if "error" in v:
            print(f"  {k}: ERROR -> {v['error']}")
        else:
            print(f"  {k}: OK | top_recommendation: {v.get('top_recommendation','(none)')[:160]}")
