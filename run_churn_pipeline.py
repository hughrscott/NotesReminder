#!/usr/bin/env python3
"""
run_churn_pipeline.py — Regenerate all churn model features after a database refresh.

Run after: Pike13 scrape, HubSpot sync, Dialpad sync
Produces: comms matching, engagement scoring, churn model training

Usage:
    python3 run_churn_pipeline.py                    # Full pipeline
    python3 run_churn_pipeline.py --skip-training    # Features only, no model training
    python3 run_churn_pipeline.py --force            # Force re-run even if outputs exist
"""
import subprocess, sys, json, pickle
from pathlib import Path
from datetime import datetime

MODELS_DIR = Path(__file__).parent / "models"
SCRIPTS = {
    "comms_matching": "comms_matcher_v3.py",
    "comms_engagement": "comms_engagement_scorer.py",
    "churn_training": "churn_model_v14_full.py",
}
OUTPUTS = {
    "comms_matching": "comms_match_results.json",
    "comms_engagement": "comms_engagement_features.csv",
    "churn_training": "churn_model_v14_final_enhanced.pkl",
}


def run_step(name, script, output, force=False):
    output_path = MODELS_DIR / output
    if output_path.exists() and not force:
        mtime = datetime.fromtimestamp(output_path.stat().st_mtime)
        age_hours = (datetime.now() - mtime).total_seconds() / 3600
        if age_hours < 24:
            print(f"  [{name}] Skipping — output exists ({age_hours:.1f}h old). Use --force to re-run.")
            return True
        print(f"  [{name}] Output is {age_hours:.1f}h old, re-running...")
    
    print(f"  [{name}] Running {script}...")
    result = subprocess.run(
        [sys.executable, script],
        cwd=str(Path(__file__).parent),
        capture_output=True, text=True, timeout=3600
    )
    if result.returncode != 0:
        print(f"  [{name}] FAILED:\n{result.stderr[-500:]}")
        return False
    
    # Verify output
    if not output_path.exists():
        print(f"  [{name}] Output not found: {output_path}")
        return False
    
    # Quick validation
    if name == "comms_matching":
        with open(output_path) as f:
            data = json.load(f)
        total_matched = sum(data[ch]["matched"] for ch in ["email", "sms", "voicemail"])
        print(f"  [{name}] ✓ {total_matched} comms matched")
    elif name == "comms_engagement":
        import csv
        with open(output_path) as f:
            rows = list(csv.DictReader(f))
        print(f"  [{name}] ✓ {len(rows)} students scored")
    elif name == "churn_training":
        with open(output_path, "rb") as f:
            model = pickle.load(f)
        print(f"  [{name}] ✓ AUC={model['test_auc']:.3f}, {len(model['features'])} features")
    
    return True


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Churn model feature pipeline")
    parser.add_argument("--skip-training", action="store_true", help="Skip model training")
    parser.add_argument("--force", action="store_true", help="Force re-run all steps")
    parser.add_argument("--step", choices=list(SCRIPTS.keys()), help="Run only one step")
    args = parser.parse_args()
    
    print("=" * 60)
    print("Churn Pipeline — Feature Regeneration")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)
    
    steps = [args.step] if args.step else list(SCRIPTS.keys())
    if args.skip_training and "churn_training" in steps:
        steps.remove("churn_training")
    
    results = {}
    for step in steps:
        if step not in SCRIPTS:
            continue
        ok = run_step(step, SCRIPTS[step], OUTPUTS[step], force=args.force)
        results[step] = ok
        if not ok:
            print(f"\n  Pipeline stopped at '{step}' — fix errors and re-run.")
            sys.exit(1)
    
    print("\n" + "=" * 60)
    print("Pipeline complete:")
    for step, ok in results.items():
        print(f"  {'✓' if ok else '✗'} {step}")
    print("=" * 60)


if __name__ == "__main__":
    main()
