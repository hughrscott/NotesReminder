#!/usr/bin/env python3
"""churn_model_v12.py — Keyword-based sentiment + frequency features added to v11.

Builds on v11's 6-feature baseline. Adds:
  Keyword sentiment: frustration, urgency, cancellation, scheduling_stress, financial, positive
  Frequency: comm_count (extended), comm_spike_7d, comm_spike_30d, comm_trend, days_since_last_comm
  Interaction: sentiment × frequency, sentiment × spike

VADER and DistilBERT both struggled with short parent messages. Keyword categories
are more interpretable and map directly to the 7 churn archetypes.
"""

import sqlite3, re, json, pickle, warnings
from pathlib import Path
from datetime import date, timedelta
from collections import defaultdict
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

DB_PATH = Path(__file__).parent / "reminders.db"
MODELS_DIR = Path(__file__).parent / "models"
MATCHES_PATH = MODELS_DIR / "comms_name_matches.json"
TODAY = date.today()

# ═══════════════════════════════════════════════════════════
# 1. KEYWORD SENTIMENT CATEGORIES
# ═══════════════════════════════════════════════════════════

SENTIMENT_CATEGORIES = {
    "frustration": {
        "keywords": [
            "not happy", "disappointed", "frustrated", "unacceptable", "ridiculous",
            "this is ridiculous", "fed up", "never again", "upset", "problem",
            "issue with", "complaint", "not satisfied", "waste of time", "waste of money",
            "not working", "doesn't work", "broken"
        ],
        "weight": -0.8,
        "archetype": "Communication Red Flags",
    },
    "urgency": {
        "keywords": [
            "asap", "urgent", "emergency", "need to speak", "call me back",
            "call me as soon", "right away", "immediately", "critical"
        ],
        "weight": -0.4,
        "archetype": "Escalation",
    },
    "cancellation": {
        "keywords": [
            "cancel", "stop lessons", "stopping lessons", "not coming back",
            "last lesson", "final lesson", "quit", "quitting", "drop out",
            "dropping out", "no longer", "discontinue", "end lessons",
            "not continue", "pulling him out", "pulling her out", "pulling them out",
            "won't be attending anymore", "not going to continue"
        ],
        "weight": -0.9,
        "archetype": "Disengagement",
    },
    "scheduling_stress": {
        "keywords": [
            "completely forgot", "so sorry", "my apologies", "double booked",
            "conflict", "schedule conflict", "can't make", "running late",
            "forgot about", "missed", "overslept", "traffic", "stuck in traffic",
            "won't be able to make", "have to miss", "need to reschedule",
            "need to move", "change the time", "different time", "another day"
        ],
        "weight": -0.3,
        "archetype": "Schedule Conflict",
    },
    "financial": {
        "keywords": [
            "bill", "billing", "charged", "charge", "credit card", "payment",
            "too expensive", "can't afford", "cost", "price", "pricing",
            "invoice", "overdue", "past due", "late fee", "how much",
            "discount", "scholarship", "financial", "money"
        ],
        "weight": -0.6,
        "archetype": "Financial Stress",
    },
    "positive": {
        "keywords": [
            "love", "great", "amazing", "thank you so much", "excited",
            "fantastic", "wonderful", "awesome", "incredible", "best",
            "so happy", "thrilled", "loves it", "really enjoys", "having a blast",
            "doing great", "making progress", "improving", "can't wait",
            "looking forward", "so proud", "really good", "excellent"
        ],
        "weight": 0.6,
        "archetype": "Retention Signal",
    },
}


def categorize_transcript(text):
    """Classify a transcript into sentiment categories. Returns dict of category → count."""
    text_lower = text.lower()
    scores = {}
    for category, config in SENTIMENT_CATEGORIES.items():
        hits = 0
        for kw in config["keywords"]:
            if kw in text_lower:
                hits += 1
        scores[category] = hits
    return scores


# ═══════════════════════════════════════════════════════════
# 2. LOAD COMMUNICATION DATA
# ═══════════════════════════════════════════════════════════

def load_comms_with_students():
    """Load all communications matched to students via phone or name."""
    con = sqlite3.connect(str(DB_PATH))
    matches = json.load(open(MATCHES_PATH))

    # Phone → student from name matches
    phone_to_student = {}
    for key, info in matches["matches"].items():
        if not key.startswith("call_") and "@" not in key and not key.startswith("sms_"):
            phone_to_student[key] = info["student"]

    # Phone-matched from identity_matches + pike13_people
    phone_matched = {}
    ppl = pd.read_sql_query("""
        SELECT full_name, phone, phone_normalized FROM pike13_people
        WHERE phone IS NOT NULL OR phone_normalized IS NOT NULL
    """, con)
    for _, r in ppl.iterrows():
        name = str(r["full_name"]).strip()
        for col in ["phone", "phone_normalized"]:
            p = str(r.get(col, "") or "")
            if p and name:
                phone_matched[re.sub(r"\D", "", p)] = name

    all_phones = {**phone_matched, **phone_to_student}
    print(f"  Phone→student mappings: {len(all_phones)}")

    # ── Voicemails ──
    student_comms = defaultdict(list)
    vms = pd.read_sql_query("""
        SELECT external_number, transcription_text, date as created_at
        FROM dialpad_voicemails WHERE transcription_text IS NOT NULL
    """, con)
    for _, row in vms.iterrows():
        phone = re.sub(r"\D", "", str(row["external_number"]))
        student = all_phones.get(phone)
        if not student:
            continue
        txt = str(row["transcription_text"])[:2000]
        dt = str(row.get("created_at", ""))
        cats = categorize_transcript(txt)
        student_comms[student].append({
            "source": "voicemail", "text": txt, "date": dt[:10] if dt else "",
            "categories": cats,
        })

    # ── Call reviews ──
    call_students = {}
    for key, info in matches["matches"].items():
        if key.startswith("call_"):
            call_students[key.replace("call_", "")] = info["student"]

    reviews = pd.read_sql_query("""
        SELECT call_id, transcript_text, recap_text, event_at
        FROM dialpad_call_reviews WHERE transcript_text IS NOT NULL OR recap_text IS NOT NULL
    """, con)
    for _, row in reviews.iterrows():
        cid = str(row["call_id"])
        student = call_students.get(cid)
        if not student:
            continue
        txt = (str(row.get("transcript_text", "") or "") + " " +
               str(row.get("recap_text", "") or ""))[:2000]
        dt = str(row.get("event_at", ""))
        cats = categorize_transcript(txt)
        student_comms[student].append({
            "source": "call_review", "text": txt, "date": dt[:10] if dt else "",
            "categories": cats,
        })

    # ── SMS via thread phone ──
    sms_threads = pd.read_sql_query(
        "SELECT thread_id, phone FROM dialpad_sms_threads WHERE phone IS NOT NULL", con)
    thread_phone = {}
    for _, r in sms_threads.iterrows():
        phone = re.sub(r"\D", "", str(r["phone"]))
        if phone:
            thread_phone[str(r["thread_id"])] = phone

    sms = pd.read_sql_query("""
        SELECT thread_id, body, message_at FROM dialpad_sms_messages
        WHERE body IS NOT NULL AND body != ''
    """, con)
    for _, row in sms.iterrows():
        phone = thread_phone.get(str(row["thread_id"]))
        student = all_phones.get(phone, "") if phone else ""
        if not student:
            continue
        txt = str(row["body"])[:500]
        dt = str(row.get("message_at", ""))
        cats = categorize_transcript(txt)
        student_comms[student].append({
            "source": "sms", "text": txt, "date": dt[:10] if dt else "",
            "categories": cats,
        })

    con.close()
    total = sum(len(v) for v in student_comms.values())
    print(f"  Total comms loaded: {total} across {len(student_comms)} students")
    return student_comms


# ═══════════════════════════════════════════════════════════
# 3. FEATURE ENGINEERING
# ═══════════════════════════════════════════════════════════

def build_features(student_comms, lookback_days=180):
    """Compute per-student sentiment + frequency features."""
    rows = []
    cutoff = (TODAY - timedelta(days=lookback_days)).isoformat()

    for student, comms in student_comms.items():
        if not comms:
            continue

        # Sort by date, filter to lookback
        dated = [c for c in comms if c["date"]]
        dated.sort(key=lambda x: x["date"])
        recent = [c for c in dated if c["date"] >= cutoff]
        all_c = recent if recent else dated
        if not all_c:
            continue

        n = len(all_c)

        # ── Keyword sentiment scores ──
        total_weighted = 0.0
        cat_counts = defaultdict(int)
        for c in all_c:
            for cat, config in SENTIMENT_CATEGORIES.items():
                hits = c["categories"].get(cat, 0)
                if hits > 0:
                    cat_counts[cat] += hits
                    total_weighted += config["weight"] * hits

        avg_sentiment = total_weighted / max(n, 1)

        frustration_hits = cat_counts.get("frustration", 0)
        urgency_hits = cat_counts.get("urgency", 0)
        cancellation_hits = cat_counts.get("cancellation", 0)
        scheduling_hits = cat_counts.get("scheduling_stress", 0)
        financial_hits = cat_counts.get("financial", 0)
        positive_hits = cat_counts.get("positive", 0)

        # Presence features (binary: does this student have ANY hits?)
        has_frustration = 1 if frustration_hits > 0 else 0
        has_cancellation = 1 if cancellation_hits > 0 else 0
        has_financial = 1 if financial_hits > 0 else 0
        has_positive = 1 if positive_hits > 0 else 0

        # ── Frequency features ──
        # Days since last communication
        last_date_str = all_c[-1]["date"]
        try:
            days_since_last = (TODAY - date.fromisoformat(last_date_str)).days
        except:
            days_since_last = 999

        # Spike: comms in last 7d / comms in last 30d
        d7 = (TODAY - timedelta(days=7)).isoformat()
        d14 = (TODAY - timedelta(days=14)).isoformat()
        d30 = (TODAY - timedelta(days=30)).isoformat()

        n_7d = sum(1 for c in all_c if c["date"] >= d7)
        n_14d = sum(1 for c in all_c if c["date"] >= d14)
        n_30d = sum(1 for c in all_c if c["date"] >= d30)

        # Spike ratio: recent vs baseline (avoid div/0)
        baseline_7d = max((n - n_7d) / max((lookback_days - 7) / 7, 1), 0.01)
        spike_7d = n_7d / baseline_7d if baseline_7d > 0 else 1.0

        baseline_30d = max((n - n_30d) / max((lookback_days - 30) / 30, 1), 0.01)
        spike_30d = n_30d / baseline_30d if baseline_30d > 0 else 1.0

        # Communication trend: are comms accelerating or decelerating?
        comm_trend = 0.0
        if len(dated) >= 3:
            try:
                dts = [date.fromisoformat(c["date"]) for c in dated if c["date"]]
                if len(dts) >= 3:
                    xs = np.arange(len(dts))
                    # Count comms per month (rolling)
                    ys = []
                    for i in range(len(dts)):
                        window_start = dts[i] - timedelta(days=60)
                        count = sum(1 for d in dts[max(0, i - 20):i + 1]
                                    if d >= window_start)
                        ys.append(count)
                    if np.std(xs) > 0:
                        comm_trend = float(np.polyfit(xs[-10:], ys[-10:], 1)[0])
            except:
                pass

        # Longest gap
        longest_gap = 0
        if len(dated) >= 2:
            gaps = []
            for i in range(1, len(dated)):
                try:
                    d1 = date.fromisoformat(dated[i - 1]["date"])
                    d2 = date.fromisoformat(dated[i]["date"])
                    gaps.append((d2 - d1).days)
                except:
                    pass
            longest_gap = max(gaps) if gaps else 0

        # Source count
        sources = len(set(c["source"] for c in all_c))

        row = {
            "student": student,
            "comm_count_v2": n,
            "comm_sources_v2": sources,
            "days_since_last_comm": days_since_last,
            "comm_spike_7d": round(spike_7d, 3),
            "comm_spike_30d": round(spike_30d, 3),
            "comm_trend": round(comm_trend, 4),
            "longest_comm_gap": longest_gap,
            "avg_keyword_sentiment": round(avg_sentiment, 4),
            "frustration_hits": frustration_hits,
            "cancellation_hits": cancellation_hits,
            "financial_hits": financial_hits,
            "scheduling_hits": scheduling_hits,
            "positive_hits": positive_hits,
            "has_frustration": has_frustration,
            "has_cancellation": has_cancellation,
            "has_financial": has_financial,
            "has_positive": has_positive,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    print(f"  Features built: {len(df)} students × {len(df.columns)} columns")
    return df


# ═══════════════════════════════════════════════════════════
# 4. MERGE WITH v11 AND TRAIN v12
# ═══════════════════════════════════════════════════════════

def train_v12(comm_features):
    """Merge comm features with v11 features and train logistic regression."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import cross_val_score, StratifiedKFold
    from sklearn.metrics import roc_auc_score, accuracy_score, classification_report

    # Load labeled data from v11
    labeled_path = MODELS_DIR / "labeled_students.csv"
    if not labeled_path.exists():
        print("  ERROR: labeled_students.csv not found. Run churn_model_v11.py first.")
        return

    labeled = pd.read_csv(labeled_path)
    print(f"\n  Labeled students: {len(labeled)} ({labeled['churned'].sum()} churned)")

    # Merge with comm features
    merged = labeled.merge(comm_features, left_on="student_name", right_on="student", how="left")

    # Fill missing comm features with 0 (no comms = zero signal)
    comm_cols = [c for c in comm_features.columns if c != "student"]
    for col in comm_cols:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0)

    # Drop students with no student_name
    merged = merged.dropna(subset=["student_name"])

    # ── Feature selection ──
    # v11 baseline features
    v11_features = [
        "avg_note_score", "membership_days", "total_lessons_lifetime",
        "teacher_consistency", "has_communication", "communication_count"
    ]

    # v12 new: only keyword features that proved directionally correct
    v12_new = [
        "has_cancellation", "has_positive", "has_frustration",
        "days_since_last_comm",
    ]

    # Keep only features that exist in the data and have variance
    all_features = []
    for f in v11_features + v12_new:
        if f in merged.columns:
            vals = merged[f].dropna()
            if vals.std() > 0.001:
                all_features.append(f)
            else:
                print(f"  Dropping {f} — zero variance")

    print(f"\n  Features: {len(all_features)} total ({len(v11_features)} v11 + {len(all_features)-len(v11_features)} new)")

    X = merged[all_features].copy()
    y = merged["churned"].values

    # Impute remaining NaNs with median
    for col in X.columns:
        if X[col].isna().any():
            X[col] = X[col].fillna(X[col].median())

    # Scale
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    X_scaled = pd.DataFrame(X_scaled, columns=all_features)

    # Train logistic regression with L2 regularization
    model = LogisticRegression(
        penalty=None,  # Use l1_ratio=0 via default solver
        solver="lbfgs",
        max_iter=2000,
        class_weight="balanced",
        random_state=42,
    )

    # Fit
    model.fit(X_scaled, y)

    # Cross-validation
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_auc = cross_val_score(model, X_scaled, y, cv=cv, scoring="roc_auc")
    cv_acc = cross_val_score(model, X_scaled, y, cv=cv, scoring="accuracy")

    # In-sample metrics
    y_pred = model.predict(X_scaled)
    y_prob = model.predict_proba(X_scaled)[:, 1]
    auc = roc_auc_score(y, y_prob)
    acc = accuracy_score(y, y_pred)

    # ── Feature importance (coefficients) ──
    coef_df = pd.DataFrame({
        "feature": all_features,
        "coefficient": model.coef_[0],
        "abs_coef": np.abs(model.coef_[0]),
    }).sort_values("abs_coef", ascending=False)

    # ── Sign validation ──
    expected_signs = {
        "avg_note_score": -1,        # higher score → less churn
        "membership_days": -1,       # longer member → less churn
        "total_lessons_lifetime": -1,  # more lessons → less churn
        "teacher_consistency": -1,   # consistent teacher → less churn
        "has_communication": -1,     # parent communicates → less churn (engaged)
        "communication_count": -1,   # more comms → less churn
        "comm_count_v2": -1,         # more comms → less churn
        "avg_keyword_sentiment": -1, # positive sentiment → less churn
        "has_frustration": 1,        # frustration → more churn
        "has_cancellation": 1,       # cancel language → more churn
        "has_financial": 1,          # financial stress → more churn
        "has_positive": -1,          # positive language → less churn
        "comm_spike_7d": 1,          # sudden spike → concern → more churn
        "comm_spike_30d": 1,         # sustained spike → concern
        "days_since_last_comm": 1,   # silence → more churn
        "longest_comm_gap": 1,       # long gap → more churn
        "scheduling_hits": 1,        # scheduling stress → more churn
        "frustration_hits": 1,       # more frustration → more churn
        "cancellation_hits": 1,      # more cancel mentions → more churn
        "financial_hits": 1,         # more financial mentions → more churn
        "positive_hits": -1,         # more positive → less churn
    }

    wrong_signs = []
    correct_signs = 0
    for _, row in coef_df.iterrows():
        feat = row["feature"]
        coef = row["coefficient"]
        expected = expected_signs.get(feat)
        if expected is None:
            continue
        if (coef > 0 and expected < 0) or (coef < 0 and expected > 0):
            wrong_signs.append(f"{feat} ({coef:+.3f}, expected {expected:+d})")
        else:
            correct_signs += 1

    # ── Report ──
    print(f"\n{'=' * 60}")
    print(f"v12 MODEL RESULTS")
    print(f"{'=' * 60}")
    print(f"  Students: {len(y)} ({y.sum()} churned)")
    print(f"  Features: {len(all_features)}")
    print(f"  CV AUC:     {cv_auc.mean():.3f} ± {cv_auc.std():.3f}")
    print(f"  CV Accuracy:{cv_acc.mean():.3f} ± {cv_acc.std():.3f}")
    print(f"  Train AUC:  {auc:.3f}")
    print(f"  Train Acc:  {acc:.3f}")

    print(f"\n  Sign coverage:")
    features_with_expected = sum(1 for f in all_features if f in expected_signs)
    print(f"    {correct_signs}/{features_with_expected} correct signs")

    if wrong_signs:
        print(f"\n  ⚠️  WRONG SIGNS ({len(wrong_signs)}):")
        for ws in wrong_signs:
            print(f"    ❌ {ws}")
    else:
        print(f"\n  ✅ ALL SIGNS CORRECT")

    # Top coefficients
    print(f"\n  Top 10 features (by abs coefficient):")
    for _, row in coef_df.head(10).iterrows():
        expected = expected_signs.get(row["feature"])
        sign_marker = ""
        if expected is not None:
            actual_sign = 1 if row["coefficient"] > 0 else -1
            sign_marker = "✅" if actual_sign == expected else "❌"
        print(f"    {sign_marker} {row['feature']:30s} {row['coefficient']:+.4f}")

    # ── Save model ──
    MODELS_DIR.mkdir(exist_ok=True)
    model_data = {
        "model": model,
        "scaler": scaler,
        "features": all_features,
        "coefficients": dict(zip(all_features, model.coef_[0])),
        "intercept": model.intercept_[0],
        "cv_auc": float(cv_auc.mean()),
        "metrics": {"auc": auc, "accuracy": acc},
    }
    pickle.dump(model_data, open(MODELS_DIR / "v12_model.pkl", "wb"))
    coef_df.to_csv(MODELS_DIR / "v12_coefficients.csv", index=False)
    print(f"\n  Model saved: models/v12_model.pkl")

    return model_data, merged


# ═══════════════════════════════════════════════════════════
# 5. MAIN
# ═══════════════════════════════════════════════════════════

def main():
    print("churn_model_v12.py — Keyword Sentiment + Logistic Regression")
    print()

    # Step 1: Load comms
    print("1. Loading communications...")
    student_comms = load_comms_with_students()

    # Step 2: Build features
    print("\n2. Building sentiment + frequency features...")
    features = build_features(student_comms)

    # Show distribution of key features
    print("\n   Feature distributions:")
    for col in ["has_frustration", "has_cancellation", "has_financial", "has_positive",
                "comm_spike_7d", "comm_trend", "avg_keyword_sentiment"]:
        if col in features.columns:
            vals = features[col].dropna()
            if vals.nunique() <= 3:
                print(f"     {col}: {vals.value_counts().to_dict()}")
            else:
                pct = features[col].describe()
                print(f"     {col}: mean={pct['mean']:.3f}, p50={pct['50%']:.3f}, p95={vals.quantile(0.95):.3f}")

    # Show students with cancellation hits
    cancels = features[features["has_cancellation"] == 1]
    if len(cancels) > 0:
        print(f"\n   {len(cancels)} students with cancellation language:")
        for _, r in cancels.iterrows():
            print(f"     {r['student']}: {r['cancellation_hits']} hits")

    # Step 3: Train v12
    print("\n3. Training v12 logistic regression...")
    model_data, merged = train_v12(features)

    # Step 4: Save features for future use
    features.to_csv(MODELS_DIR / "comm_features_v12.csv", index=False)
    print(f"\n4. Features saved: models/comm_features_v12.csv")


if __name__ == "__main__":
    main()
