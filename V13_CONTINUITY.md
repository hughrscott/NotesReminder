# SOR Churn v13 — Continuity Notes (handoff for tomorrow)

**Date:** 2026-07-16 (evening)
**Status:** PAUSED by Hugh. Do NOT run the nightly cron (it was removed). Resume fresh in the morning.

---

## WHAT WE SET OUT TO DO
Wire the v13 cancellation-risk model (trained on Pike13 ground-truth leavers) into the
retention report pipeline, and schedule the first combined run for 10pm CT to Vivian + Hugh only.

## WHAT'S DONE (committed to git)
1. **Pulled 4 Pike13 CSVs** from huscott@schoolofrock.com via IMAP (himalaya `[accounts.sor]`,
   imap.gmail.com). Saved to `/home/ubuntu/pike13_csvs/`.
   - 2 × `Person_Plan ... Plans Details` CSVs (1,704 plan rows w/ real End Date + Canceled?)
   - 1 × `Clients` roster (969 clients)
   - 1 × `HeightsLastPlanDate` (redundant)
2. **Trained v13 on real ground truth.** `churn_model_v13_build.py` → 735 labeled rows
   (386 Pike13 leavers + 349 local active negatives).
   - CV AUC **0.816 ± 0.038**, holdout AUC **0.862** (honest — earlier 1.000 was a leakage bug).
   - Artifacts: `models/v13_labeled_dataset.csv`, `v13_coefficients.csv`, `v13_model.pkl`.
   - **Important:** `v13_model.pkl` now holds a BUNDLE `{model, scaler, features}` (scaler added
     so scoring is correct). Re-run build script if you need to regenerate.
3. **Extracted shared feature module** `v13_features.py` (used by both training + scoring).
4. **Built `score_v13.py`** → writes `models/v13_risk_scores.csv` (student_id, name, school, v13_risk, v13_tier + raw features).
5. **Wired v13 into `retention_intelligence.py`:**
   - `main()` now merges `v13_risk`/`v13_tier` onto each profile from `v13_risk_scores.csv`.
   - v13 cancellation-risk line added to the ACTIONABLE student block.
   - v13 fields added to the JSON output (`retention_intelligence.json`).
   - v12 (v11_risk_scores.csv) remains the PRIMARY tiering; v13 is an overlay.

## THE BLOCKER WE HIT (unresolved)
**Student count is wildly wrong.** You flagged it: "we don't have 600 students, we have about
half of that across both schools" (~300 enrolled).

Findings from DB inspection tonight:
- `students` table = **1,524 rows** spanning 18 months (Jan 2025–Jul 2026). This is a HISTORICAL
  roster, not current enrollment.
- Lesson activity by window: 14d=344, 30d=436, 60d=572, 90d=622 students.
- Pike13 `pike13_people` = 1,067 clients but **764 are "Free Trial Lesson"** + camps/workshops.
  Filtering to non-trial enrollment leaves only ~45 named.
- **The Pike13 export is also STALE:** only **8 clients** have a plan with End Date >= today and
  not Canceled. So "active membership" from the export = 8 → unusable.
- You confirmed the DB tables are **stale and polluted for both schools** (memory note 2026-07-13).

**My 90-day scoring cutoff (622) was the wrong denominator** — it scored trials, camp completers,
and already-churned students as a live churn population.

## DECISION NEEDED TOMORROW (left open)
We discussed two scoring-universe options:
- **A)** Lesson in last 30d AND not a Pike13 trial/camp state (~closest to your ~300).
- **B)** True "active membership" only — but the export can't support this (only 8 active plans).

You chose **B**, then I found B is impossible from this data. Then you said (last message, mid-turn):
**"you should count anyone with a leaving date, even if it's in the future, as a leaver."**

That last instruction is the key unresolved steer — it reframes the LABEL definition, not just the
scoring universe. Need to decide together how to apply it:
- Does "leaving date in the future" come from the Pike13 plan End Date (only 8 have future dates)?
- Or from the local lesson gap (no future lesson scheduled ⇒ implicitly leaving)?
- And does this replace, or supplement, the v13 churn label we already trained?

**Do NOT regenerate v13_risk_scores.csv or re-run the report until this is settled.**

## OPEN QUESTIONS / KNOWN GAPS
- v13 comms features are weak (cancellation/frustration keywords ~0 — sparse phone→student match).
  Lesson/note features carry the model. Worth widening comms match later.
- The pipeline still reads `v11_risk_scores.csv` for the PRIMARY risk. v11 is also likely trained
  on the polluted/large roster. May need re-examination.
- Cron: the nightly retention cron was REMOVED. The old `SOR Churn Report` job (620e9bcdabe4) is
  still PAUSED — leave it paused.

## KEY FILE PATHS
- Project: `/home/ubuntu/projects/hughrscott/NotesReminder/`
- DB: `reminders.db` (NOT sor_data.db)
- v13 build: `churn_model_v13_build.py`
- v13 features (shared): `v13_features.py`
- v13 scoring: `score_v13.py`
- Report pipeline: `retention_intelligence.py`
- Cron runner: `run_retention_cron.py` (emails vscott+huscott — was going to be pointed at Vivian+Hugh only)
- Email CSVs: `/home/ubuntu/pike13_csvs/`
- v13 artifacts: `models/v13_model.pkl`, `v13_risk_scores.csv`, `v13_coefficients.csv`, `v13_labeled_dataset.csv`

## RECOMMENDED MORNING PLAN
1. Resolve the "leaving date = leaver" definition (above).
2. Re-define the scoring universe to match true enrollment (~300, not 622).
3. Re-run `score_v13.py` with corrected universe → verify count ≈ 300.
4. Dry-run `retention_intelligence.py`, SHOW the report in chat (your policy) before any send.
5. Only THEN schedule the cron — to Vivian + Hugh only (not vscott/huscott broadcast) until reviewed.
