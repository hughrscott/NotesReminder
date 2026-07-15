# SOR Churn Model v10 — Build Plan

## Goal
Build a churn prediction model with genuine predictive power (not seasonal pattern matching). Target: features with correct causal signs, useful for operational triage.

## Current State
- v9 model: 12 features, 4 correct signs, 0.991 test AUC (inflated by seasonal confound)
- 20,868 lessons, 18 months (Jan 2025–Jul 2026), 682 labeled students (285 churned, 397 active)
- 21-day observation gap, Logistic Regression with L2 (C=0.05)

## Feature Set (13 features)

### Keep (5 — already correct signs)
1. **`avg_note_score`** — mean lesson note quality (1-10). Coefficient ~-1.84. Strongest signal.
2. **`teacher_consistency`** — % lessons with most frequent instructor. Small but directionally correct.
3. **`has_communication`** — binary: any parent comm data on file. Engaged parents = lower risk.
4. **`positive_hits`** — count of positive phrases in parent communications.
5. **`makeup_x_missed`** — interaction term distinguishing summer-break credit use from pre-quit burn.

### New (7 — self-referenced, no seasonal confound)
6. **`days_since_last_lesson`** — recency. Core RFM feature, #1 churn predictor in literature.
7. **`freq_trend_3mo`** — (lessons in last 30d) / (lessons in prior 30d). Self-referenced decline.
8. **`lesson_spacing_std`** — standard deviation of days between lessons. Irregularity = disengagement.
9. **`membership_days`** — days since first lesson. Tenure baseline for survival analysis.
10. **`total_lessons_lifetime`** — lifetime lesson count. Investment proxy.
11. **`instructor_changes`** — count of distinct instructors in 60d window. Disruption signal.
12. **`makeup_stockpile`** — estimated unused makeup credits: max(0, expected_lessons_90d - actual_lessons_90d). Early warning — accumulation precedes burn.
13. **`days_since_penultimate`** — gap between last and second-to-last lesson. Gap acceleration.

### Dropped (6 — wrong signs or artifacts)
- `attendance_ratio` (14d/45d) — replaced by `freq_trend_3mo`
- `lessons_vs_baseline` — seasonal baseline doesn't work (event counts ≠ individual attendance)
- `note_completion_rate` — admin artifact (bulk-completes notes for churned students)
- `total_cancel_hits` — phrase ambiguity ("cancel Tuesday" ≠ cancel membership)
- `total_concern_hits` — same ambiguity
- `makeup_ratio` (standalone) — replaced by stockpile (accumulation) tracking

## Implementation Steps

### Phase 1: Build new features (30 min)
1. Add `compute_features_v10()` to `churn_model.py` alongside existing `compute_features()`.
2. New features computed from `lessons_df` only (no external APIs needed):
   - `days_since_last_lesson`: ref_date - max(lesson_date)
   - `freq_trend_3mo`: lessons_last_30d / max(lessons_prior_30d, 1)
   - `lesson_spacing_std`: np.std of consecutive lesson date diffs (min 3 lessons)
   - `membership_days`: ref_date - min(lesson_date)
   - `total_lessons_lifetime`: len(lessons_df)
   - `instructor_changes`: nunique(instructor_id) in 60d window
   - `makeup_stockpile`: max(0, (90/median_spacing) - lessons_last_90d)
   - `days_since_penultimate`: gap between two most recent lessons
3. Merge existing sentiment features from `comm_sentiment.csv`.

### Phase 2: Train and evaluate (15 min)
1. Same training pipeline as v9: 21-day gap, 80/20 stratified split, 5-fold CV.
2. Logistic Regression with L2, sweep C values [0.01, 0.05, 0.1, 0.5] via GridSearchCV.
3. Evaluate: coefficient sanity check (≥70% correct signs), test AUC, precision/recall on churned class.
4. Flag threshold: ≥30% predicted probability.

### Phase 3: Hardening (15 min)
1. Check for collinearity: VIF > 5 = flag for review.
2. Check for leakage: any feature computed from data AFTER ref_date?
3. Sanity check: can each feature's sign be explained causally?
4. Stability: do coefficients hold sign across CV folds?

## Success Criteria
- ≥8/13 features (62%) with correct coefficient signs
- `avg_note_score` retains negative sign
- `freq_trend_3mo` has correct negative sign (declining = risk)
- `days_since_last_lesson` has correct positive sign (longer gap = risk)
- Test AUC > 0.85 (honest, not inflated)
- Flagged rate: 15-25% of active students (useful triage, not alarm fatigue)

## Risks
- `freq_trend_3mo` may still capture seasonality if student's own prior month was summer break
- `membership_days` may covary with churn window definition
- Sparse students (<3 lessons) get default values for spacing features — noise
