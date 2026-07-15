# SOR Churn Model — Complete Feature Audit

## Data Available
| Source | Records | Coverage | Freshness |
|--------|:------:|:---:|---|
| `lessons` | 20,868 | 2 schools, 18 months | Through Jul 13, 2026 |
| `lesson_notes` | 20,868 | 100% of lessons | Through Jul 13, 2026 |
| `pike13_visits` | 1,082 | 5% of lessons | First-visit only (1,066/1,082) |
| Voicemails | 1,446 | 534 matched to students (37%) | Stale: ends Jan 2026 |
| SMS | 10,813 | 5,203 matched (48%) | Current through Jun 2026 |
| Emails | 417 inbound | 43 matched via bridge | Current |
| Call reviews | 302 | 0 matched | Broken bridge |

---

## Feature Inventory — All 12 Current Features

### Behavioral (from lessons)

| # | Feature | What it measures | Status | Verdict |
|---|---------|-----------------|:---:|---|
| 1 | `attendance_ratio` | Recent 14d ÷ prior 45d. Decline detector. | ✗ +0.19 | **KEEP but fix:** Core RFM frequency-trend feature. Broken because churned students attend MORE before quitting (1.30× seasonal baseline). Fix: use student's OWN 3-month average as denominator, not global prior window. |
| 2 | `lessons_vs_baseline` | Student lpw ÷ school×month avg | ✗ +0.59 | **DROP:** Seasonal baseline approach is fundamentally wrong — event counts ≠ individual attendance. Replaced by within-student trend (#1 fix). |
| 3 | `teacher_consistency` | % lessons with most frequent instructor | ⚠️ flip-flops | **KEEP:** Instructor changes signal disruption. Small effect but directionally correct when not confounded. |
| 4 | `avg_note_score` | Mean note quality (1-10 scale) | ✅ −1.83 | **STRONGEST FEATURE.** Instructors give worse notes to departing students. The one genuinely predictive signal. |
| 5 | `note_completion_rate` | % lessons with completed notes | ✗ +0.21 | **DROP:** Admin bulk-completes notes for churned students, inflating their scores. Not a genuine behavioral signal. |

### Credit Accumulation / Burn

| # | Feature | What it measures | Status | Verdict |
|---|---------|-----------------|:---:|---|
| 6 | `makeup_ratio` | % makeup sessions in window | ✗ flip-flopped | **KEEP as interaction only:** Alone it's ambiguous (summer break vs burning credits). Needs partner feature. |
| 7 | `missed_in_window` | Expected − actual lessons in 60d | ✗ −0.91 | **REPLACE with idle_days:** This is really measuring "summer break vs school year." Replace with `days_since_last_lesson` (recency — the #1 churn predictor in RFM literature). |
| 8 | `makeup_x_missed` | Interaction: makeup × missed | ✅ −0.03 | **KEEP:** Correctly identifies "high makeup + high missed = summer break (safe)." Small but directionally correct. |

### Communication Sentiment

| # | Feature | What it measures | Status | Verdict |
|---|---------|-----------------|:---:|---|
| 9 | `has_communication` | Any comm data on file | ✅ −0.27 | **KEEP:** Having parent contact info = engaged parent = lower risk. Stable signal. |
| 10 | `total_cancel_hits` | Phrase "cancel" in comms | ✗ −0.09 | **DROP standalone:** "Cancel Tuesday's lesson" ≠ "cancel membership." Too ambiguous. Useful only if we can distinguish context. |
| 11 | `total_concern_hits` | Cancel + complaint phrases | ✗ −0.06 | **DROP:** Same ambiguity as cancel_hits. Combined phrase bag has no discriminative power. |
| 12 | `positive_hits` | "Love", "great", "thank you" | ✅ −0.05 | **KEEP:** Positive parent language = engagement. Weak but directionally correct. |

---

## NEW Features to Add (from research + domain knowledge)

### Recency (RFM — the #1 churn predictor)

| # | Feature | Source | Rationale |
|---|---------|--------|-----------|
| 13 | `days_since_last_lesson` | `lessons` | **Recency** — core RFM feature. Long gaps = disengagement. Will replace `missed_in_window`. |
| 14 | `days_since_penultimate` | `lessons` | Gap between last and second-to-last lesson. Accelerating gaps = trouble. |
| 15 | `gap_vs_average` | Derived | (days_since_last / avg_spacing) − 1. How many "missed" lessons? Your original insight. |

### Frequency Trend (within-student, self-referenced)

| # | Feature | Source | Rationale |
|---|---------|--------|-----------|
| 16 | `freq_trend_3mo` | `lessons` | (lessons last 30d ÷ lessons month prior) — self-referenced decline. No seasonal confound. |
| 17 | `freq_trend_6mo` | `lessons` | Longer-term decline. Catches slow faders. |

### Lesson Cadence / Consistency

| # | Feature | Source | Rationale |
|---|---------|--------|-----------|
| 18 | `lesson_spacing_std` | `lessons` | Std dev of days between lessons. Irregular schedule = disengagement. |
| 19 | `cadence_change` | `lessons` | (recent median spacing) ÷ (lifetime median spacing). Switching from weekly to biweekly. |

### Membership / Tenure

| # | Feature | Source | Rationale |
|---|---------|--------|-----------|
| 20 | `membership_days` | `lessons` | Days since first lesson. Newer members have higher churn risk (survival analysis baseline). |
| 21 | `total_lessons_lifetime` | `lessons` | More lessons = more invested = lower risk. |

### Instructor / Engagement

| # | Feature | Source | Rationale |
|---|---------|--------|-----------|
| 22 | `instructor_changes` | `lessons` | Count of distinct instructors in window. More changes = disruption. |
| 23 | `unique_instructors` | `lessons` | Different from consistency — raw count, not ratio. |

### Credit Health (your domain insight)

| # | Feature | Source | Rationale |
|---|---------|--------|-----------|
| 24 | `makeup_stockpile` | Derived | Estimated unused makeup credits: (expected − attended) over 90d. Accumulating credits without using them = early warning. Distinct from `makeup_ratio` which measures USAGE. |
| 25 | `makeup_urgency` | Derived | makeup_ratio ÷ makeup_stockpile. High urgency = burning through stockpile = imminent churn. |

---

## Recommended Feature Set (v10)

**KEEP (5):**
1. `avg_note_score` — strongest signal, instructor quality drops before churn
2. `teacher_consistency` — instructor stability matters
3. `has_communication` — engaged parents = lower risk
4. `positive_hits` — positive parent language
5. `makeup_x_missed` — credit burn context

**REPLACE (1):**
6. `missed_in_window` → `days_since_last_lesson` — recency is #1 churn predictor

**ADD (7):**
7. `freq_trend_3mo` — self-referenced decline (no seasonal confound)
8. `lesson_spacing_std` — schedule regularity
9. `membership_days` — tenure (newer = higher risk)
10. `total_lessons_lifetime` — investment
11. `instructor_changes` — disruption count
12. `makeup_stockpile` — accumulating unused credits (early warning)
13. `days_since_last_lesson` — recency

**DROP (6):**
- `attendance_ratio` — replaced by `freq_trend_3mo`
- `lessons_vs_baseline` — seasonal baseline doesn't work for this data
- `note_completion_rate` — admin artifact, not behavioral
- `total_cancel_hits` — too ambiguous
- `total_concern_hits` — too ambiguous
- `makeup_ratio` — standalone replaced by stockpile + urgency

That takes us from 12 features (4 correct signs) to 13 features, all with clear causal interpretations.
