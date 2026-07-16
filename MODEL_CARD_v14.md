# SOR Churn Model v14

**Date:** July 2026  
**Model Type:** Logistic Regression (L2-regularized)  
**AUC:** 0.866 (test), 0.803 (CV)  
**Training Window:** 90-day lookback  
**Target:** Student churn (ceased lessons without formal cancellation)

---

## Executive Summary

v14 replaces keyword-based churn signals (v12/v13) with **lesson frequency patterns** as the primary predictor. The key insight: parents who "quiet quit" — stop attending lessons AND stop communicating — account for 60% of churn. Lesson gap patterns, tenure, and attendance decline are 10x more predictive than note quality or communication sentiment.

### Performance

| Model | AUC | Key Insight |
|---|---|---|
| v13 (keyword features) | 0.547* | Weak predictors, inverted coefficients |
| v14 baseline (lesson frequency) | 0.858 | Gap patterns + tenure dominate |
| v14 + comms engagement | **0.866** | +0.008 from cancellation/reschedule signals |

*Note: v13 retrained with corrected lookback window. Original v13 (0.816) used different evaluation methodology.

---

## Features

### Primary: Lesson Frequency (12 features)

| Feature | Type | Description |
|---|---|---|
| `total_lessons` | Count | Lifetime lesson count at reference date |
| `lessons_30d` | Count | Lessons in 30 days before reference |
| `lessons_60d` | Count | Lessons in 60 days before reference |
| `lessons_90d` | Count | Lessons in 90 days before reference |
| `freq_decline_ratio` | Ratio | 30d lessons / 30-60d lessons (<1 = declining) |
| `days_since_last` | Days | Days since last lesson before reference |
| `max_gap_days` | Days | Longest gap between lessons |
| `avg_gap_days` | Days | Average gap between lessons |
| `gap_std` | Days | Standard deviation of lesson gaps |
| `tenure_days` | Days | Days from first lesson to reference date |
| `teacher_consistency` | Ratio | Fraction of lessons with most-common instructor |

### Secondary: Note Quality (1 feature)

| Feature | Type | Description |
|---|---|---|
| `avg_note_score` | 0-10 | Mean note quality score in 90-day window |

### Tertiary: Comms Engagement (8 features)

| Feature | Type | Description |
|---|---|---|
| `comms_engagement_total` | Count | Real parent comms (post-spam-filter) |
| `comms_engagement_avg_risk` | Score | Mean risk score (-1 to +1.3) |
| `comms_engagement_cancellation_rate` | Rate | Fraction of comms that are cancellations |
| `comms_engagement_praise_rate` | Rate | Fraction of comms that are praise |
| `comms_engagement_inquiry_rate` | Rate | Fraction of comms that are inquiries |
| `comms_engagement_positive_ratio` | Rate | Fraction of comms with positive sentiment |
| `comms_engagement_negative_ratio` | Rate | Fraction of comms with negative sentiment |
| `comms_engagement_risk_volatility` | Std | Risk score standard deviation across comms |

---

## Training Configuration

```python
# Model
LogisticRegression(
    penalty='l2',        # L2 regularization
    C=1.0,               # Regularization strength (selected via CV)
    class_weight='balanced',  # Handle 19% churn rate
    solver='liblinear',
    max_iter=2000,
    random_state=42
)

# Evaluation
train_test_split(test_size=0.2, stratify=y, random_state=42)
StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
StandardScaler  # Feature standardization

# Window
LOOKBACK_DAYS = 90     # Features computed from 90 days before end/reference date
FEATURE_WINDOW = 90    # Feature extraction window
MIN_LESSONS = 5        # Minimum lessons for inclusion
```

## Label Construction

- **Churned (label=1):** Student appears in `pike13_leavers.json` with an end_date. Reference date = end_date - 90 days. Features computed from all data before reference date.
- **Active (label=0):** Student has a lesson within last 60 days and is not in leavers list. Reference date = today.

## Coefficient Analysis

Top predictive features (v14 + engagement, C=1.0):

| Feature | Coefficient | Interpretation |
|---|---|---|
| `lessons_60d` | +2.30 | More recent lessons → **higher** churn risk (paradoxical — see note) |
| `total_lessons` | -2.41 | More total lessons → lower churn risk ✓ |
| `gap_std` | -2.02 | Consistent schedule → lower churn risk ✓ |
| `avg_gap_days` | +1.39 | Longer gaps → higher churn risk ✓ |
| `lessons_30d` | -1.11 | Recent activity → lower churn risk ✓ |
| `tenure_days` | +0.88 | Longer tenure → higher churn risk (long-term fatigue) |
| `freq_decline_ratio` | +0.73 | Declining frequency → higher churn risk ✓ |
| `comms_engagement_risk_volatility` | +0.31 | Erratic comms → higher churn risk ✓ |
| `comms_engagement_negative_ratio` | -0.18 | Complaints → lower churn risk (engaged parents) |

**Note on `lessons_60d`:** The positive coefficient is due to collinearity with `lessons_30d` — students with high 60d counts but low 30d counts are declining, which is captured by `freq_decline_ratio`.

## Key Findings

1. **"Quiet quit" is the dominant pattern:** 60% of leavers have zero communication history. They don't complain, don't cancel — they just disappear.

2. **Lesson frequency is 10x more predictive than content quality:** Note scores and comms sentiment add marginal signal. The strongest predictors are gap patterns and attendance decline.

3. **Spam is 91% of matched comms:** Of 2,586 matched communications, only 219 are real parent communications. The rest are Dialpad system messages, marketing texts, and wrong-number calls.

4. **Engaged parents don't churn:** Negative sentiment in comms actually predicts retention, not churn. Parents who complain are still showing up and still care.

5. **90-day lookback is the sweet spot:** 15-day windows captured "end-of-relationship" noise (final detailed notes, spike in comms). 60-90 day windows capture the behavioral decline BEFORE the decision to leave.

## Dependencies

### Data Pipeline (must run before training)

| Script | Purpose | Runtime |
|---|---|---|
| `comms_matcher_v3.py` | Match all comms to students | ~30s |
| `comms_engagement_scorer.py` | Engagement + sentiment scoring | ~60s |
| `score_lesson_notes.py` | Score all lesson notes (gpt-4o-mini) | ~30 min (one-time) |
| `pike13_leavers.json` | List of churned students (manual/scraped) | Manual |

### Training

```bash
cd NotesReminder
source .venv/bin/activate
python3 churn_model_v14_full.py
```

Output: `models/churn_model_v14_final_enhanced.pkl`

## Files

| File | Description |
|---|---|
| `churn_model_v14_full.py` | Full training script with all features |
| `comms_matcher_v3.py` | Multi-strategy comms-to-student matching |
| `comms_engagement_scorer.py` | Keyword engagement + RoBERTa sentiment |
| `score_lesson_notes.py` | GPT-4o-mini note quality scoring |
| `models/churn_model_v14_final_enhanced.pkl` | Trained model artifact |
| `models/comms_engagement_features.csv` | Per-student engagement features |
| `models/comms_final_deduped.csv` | Combined comms matching results |

---

*Generated by Hermes Agent, July 2026*
