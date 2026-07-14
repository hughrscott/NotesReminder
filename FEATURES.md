# SOR Churn Model — Feature Dictionary (v9)

All features are computed per student within a 60-day window ending at `ref_date`.
For churned students: ref_date = last_lesson - 21 days (observation gap).
For active students: ref_date = today.

## Behavioral Features

| # | Feature | Type | Expected Sign | Status | Source | Description |
|---|---------|------|:---:|:---:|--------|-------------|
| 1 | `attendance_ratio` | trend | − | ✗ Wrong (+0.19) | `lessons` | Recent 14d / prior 45d. <1 = declining. Confounded by summer slump. |
| 2 | `lessons_vs_baseline` | ratio | − | ✗ Wrong (+0.38) | `lessons` + `seasonal_baselines.pkl` | Student's weekly attendance ÷ school×month historical average. Churned students attend 1.30× baseline before quitting. |
| 3 | `teacher_consistency` | ratio | − | ✗ Wrong (+0.08) | `lessons.instructor_id` | % of lessons with most common instructor. Flips seasonally. |
| 4 | `avg_note_score` | mean | − | ✅ Correct **(-1.83)** | `lesson_notes` | Average note quality (1-10 scale). Strongest signal in model. |
| 5 | `note_completion_rate` | ratio | − | ✗ Wrong (+0.31) | `lesson_notes` | % of lessons with completed notes. Admin cleanup inflates churned scores. |

## Credit Accumulation / Burn Features

| # | Feature | Type | Expected Sign | Status | Source | Description |
|---|---------|------|:---:|:---:|--------|-------------|
| 6 | `makeup_ratio` | ratio | + | ✅ Correct **(+0.05)** | `lessons.lesson_type` LIKE '%MAKE%UP%' | % of lessons that are makeup sessions. Late signal — credit burn before quitting. |
| 7 | `missed_in_window` | count | + | ✗ Wrong (-0.91) | Derived from lesson spacing | Expected − actual lessons in 60d window. Summer-break students have high values — confound. |
| 8 | `makeup_x_missed` | interaction | − | ✅ Correct (-0.01) | makeup_ratio × missed_in_window | High both = summer break (safe). Low missed + high makeup = burn before quit (risk). |

## Communication Sentiment Features (via `comm_sentiment.py`)

Sources: Voicemails (1,446 transcripts), SMS (10,737 via threads), Emails (417 inbound), Call Reviews (302 — unmatched).

| # | Feature | Type | Expected Sign | Status | Source | Description |
|---|---------|------|:---:|:---:|--------|-------------|
| 9 | `has_communication` | binary | 0 | ✅ Correct **(-0.30)** | All comm channels | Sentinel: 1 if any communication data on file. Having comms = engaged parent = lower risk. |
| 10 | `total_cancel_hits` | count | + | ✗ Wrong (-0.03) | Phrase mining | "cancel", "quit", "stop" etc. Ambiguous — "cancel Tuesday's lesson" ≠ "cancel membership". |
| 11 | `total_concern_hits` | count | + | ✗ Wrong (-0.02) | Phrase mining | Cancel + dissatisfaction + scheduling + financial. Same ambiguity as cancel_hits. |
| 12 | `positive_hits` | count | − | ✗ Wrong (+0.04) | Phrase mining | "love", "great", "thank", "enjoy" etc. Engaged parents use positive language — but also communicate more when quitting. |

## Infrastructure

| Component | File | Purpose |
|-----------|------|---------|
| Seasonal baselines | `seasonal_baselines.py` → `models/seasonal_baselines.pkl` | School×month avg lessons/week. West U: 0.85 (Jul) to 1.45 (Dec). |
| Sentiment pipeline | `comm_sentiment.py` → `models/comm_sentiment.csv` | VADER + phrase mining across all comm channels. Uses `pike13_clients` bridge. |
| Client bridge | `comm_sentiment.py::build_client_bridge()` | Maps parent phones/emails → students via `pike13_clients`. |
| Heuristic report | `churn_watch.py` → `models/churn_watch.csv` | Action-focused GM report with 3 tiers. |
| Backfill tracker | `BACKFILL.md` | List of scraper fixes and data gaps to address. |

## Current Model Performance (v9, Logistic Regression)

- 734 labeled (276 churned, 458 active), 21-day observation gap
- 80/20 stratified train/test split, 5-fold CV on training set
- Test AUC: 0.995 (held-out), CV AUC: 0.986
- 77/458 active flagged (>30% risk)
- 4/12 features have correct coefficient signs

## Known Issues

1. **Seasonal confound**: Active students measured in July (summer slump). Churned students from all months. Model can't distinguish "summer break" from "disengagement."
2. **68% comm coverage ceiling**: Limited by 61% of Pike13 people lacking phone/email. Cannot improve without data backfill.
3. **Call reviews unmatched**: 302 review transcripts with 0 student matches. Bridge format mismatch.
4. **Voicemail staleness**: 1,446 transcripts end Jan 2026. Missing Feb–Jul 2026.
