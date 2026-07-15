#!/usr/bin/env python3
"""Cron wrapper: run retention pipeline + email reports to vscott + huscott.

Called by cron job. Generates retention intelligence reports for both schools,
then emails the text reports directly via Hostinger SMTP.
"""
import sys, os, smtplib, tomllib, subprocess
from pathlib import Path
from datetime import date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

PROJECT_DIR = Path(__file__).parent
MODELS_DIR = PROJECT_DIR / "models"
VENV_PYTHON = "/home/ubuntu/.hermes/env/bin/python3"

def get_hermes_password():
    """Extract hermes@hughrscott.com password from Himalaya config."""
    with open("/home/ubuntu/.config/himalaya/config.toml", "rb") as f:
        config = tomllib.load(f)
    for acc in config.get("accounts", {}).values():
        cmd = acc.get("backend", {}).get("auth", {}).get("cmd", "")
        if "echo " in cmd:
            return cmd.split("echo ")[-1].strip().strip('"').strip("'")
    raise RuntimeError("Could not extract Hermes password from himalaya config")

def run_pipeline():
    """Run retention_intelligence.py and return paths to report files."""
    os.chdir(str(PROJECT_DIR))
    result = subprocess.run(
        [VENV_PYTHON, "retention_intelligence.py"],
        capture_output=True, text=True, timeout=180
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"Pipeline error: {result.stderr}", file=sys.stderr)
    
    reports = sorted(MODELS_DIR.glob("retention_intel_*.txt"))
    return reports

def email_reports(reports):
    """Send retention reports via Hostinger SMTP."""
    if not reports:
        print("No reports to send")
        return
    
    pwd = get_hermes_password()
    today = date.today().strftime("%B %d, %Y")
    
    # Build email body with reports inline
    body_parts = [f"School of Rock Retention Intelligence — {today}", "", ""]
    
    for rp in reports:
        school = rp.stem.replace("retention_intel_", "").replace("_", " ")
        content = open(rp).read()
        body_parts.append("─" * 72)
        body_parts.append(f"  {school}")
        body_parts.append("─" * 72)
        body_parts.append(content)
        body_parts.append("")
    
    body = "\n".join(body_parts)
    
    msg = MIMEMultipart()
    msg["From"] = "hermes@hughrscott.com"
    msg["To"] = "vscott@schoolofrock.com, huscott@schoolofrock.com"
    msg["Subject"] = f"🎸 Retention Report — {today}"
    msg.attach(MIMEText(body, "plain"))
    
    server = smtplib.SMTP_SSL("smtp.hostinger.com", 465, timeout=15)
    server.login("hermes@hughrscott.com", pwd)
    server.sendmail(
        "hermes@hughrscott.com",
        ["vscott@schoolofrock.com", "huscott@schoolofrock.com"],
        msg.as_string()
    )
    server.quit()
    print(f"Emailed {len(reports)} reports to vscott+huscott")

def main():
    print("=== Retention Pipeline Cron ===")
    reports = run_pipeline()
    email_reports(reports)
    print("Done.")

if __name__ == "__main__":
    main()
