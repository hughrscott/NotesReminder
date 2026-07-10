#!/usr/bin/env python3
"""Send final summary email to Hugh about the SOR automation work."""
import os
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Load credentials
env = {}
for line in open('/home/ubuntu/.hermes/SOR/.sorenv'):
    line = line.strip()
    if not line or line.startswith('#'):
        continue
    if '=' in line:
        k, v = line.split('=', 1)
        v = v.strip().strip('"').strip("'")
        env[k] = v

smtp_host = "smtp.mail.me.com"
smtp_port = 587
sender = env.get("SENDER_EMAIL", "hughrscott@mac.com")
password = env.get("SENDER_PASSWORD", "")

msg = MIMEMultipart("alternative")
msg["Subject"] = "School of Rock — Pike13 Automation Complete: Backfill Done, Daily Cron Set, Phase 2 Proposal Ready"
msg["From"] = sender
msg["To"] = "hughrscott@mac.com"
msg["Cc"] = "hugh.scott@gmail.com"

plain = """School of Rock — Pike13 Automation Summary

WHAT WAS DONE:
1. Pike13 Auto-MFA: Built and tested an automated authentication system that handles
   the full login flow — username/password, MFA challenge, reading verification codes
   from the SOR email via IMAP, entering the code, and establishing a staff session.
   No human intervention needed. This was the key breakthrough.

2. Backfill: Scraped lesson notes for July 6-9 for both West U and The Heights.
   8 summary emails sent to you and Vivian with notes completed, missing notes,
   and quality scores for each day/school.
   Database synced to S3.

3. Daily Cron: System crontab set for 9pm CT (2am UTC) Monday-Saturday.
   Runs the full pipeline: S3 download -> auto-MFA login -> scrape lessons ->
   read notes -> score with GPT-4o-mini -> update DB -> send email summaries
   to hughrscott@mac.com and vivian@schoolofrock.com -> S3 upload.
   Starting tomorrow, you and Vivian will get daily summary emails automatically.

4. Automation Proposal: A comprehensive 746-line analysis has been written
   (AUTOMATION_PROPOSAL.md) covering how to automate HubSpot, Dialpad, Gmail,
   and additional Pike13 data. It's committed to GitHub.

5. GitHub: All code changes committed and pushed to hughrscott/NotesReminder.

BACKFILL RESULTS:
West U: Jul 6 (16/22 notes), Jul 7 (21/29), Jul 8 (21/27), Jul 9 (10/29)
The Heights: Jul 6 (23/29), Jul 7 (11/17), Jul 8 (9/22), Jul 9 (5/25)

WHAT'S NEXT (Phase 2):
The automation proposal outlines a phased approach:
- Phase 1 (immediate): Integrate Pike13 lead extraction into the daily flow
- Phase 2 (weeks 2-3): Build auto-auth for HubSpot (email MFA, same pattern)
  and Dialpad (needs manual discovery of auth flow)
- Phase 3 (weeks 3-4): Unified orchestrator, health monitoring, alerting

The Pike13 auto-MFA pattern (IMAP email code reading) should work for HubSpot.
Dialpad may use Google SSO which requires a different approach.

CREDENTIALS NEEDED FOR PHASE 2:
- HubSpot login credentials (email + password)
- Dialpad login credentials (email + password)
These need to be added to /home/ubuntu/.hermes/SOR/.sorenv

The database will be updated every day with all Pike13 lesson notes data.
Once Phase 2 is implemented, it will also include HubSpot, Dialpad, and Gmail data.

All code is at: https://github.com/hughrscott/NotesReminder
Proposal is at: AUTOMATION_PROPOSAL.md in the repo.
"""

html = f"""<html><body style="font-family:Arial,sans-serif;font-size:14px;color:#222;">
<h2>School of Rock — Pike13 Automation Summary</h2>

<h3>What Was Done:</h3>
<ol>
<li><strong>Pike13 Auto-MFA</strong> — Built and tested automated authentication: login → MFA challenge → read code from SOR email via IMAP → enter code → staff session. No human intervention needed.</li>
<li><strong>Backfill</strong> — Scraped lesson notes for July 6-9 for both schools. 8 summary emails sent to you and Vivian. DB synced to S3.</li>
<li><strong>Daily Cron</strong> — Set for 9pm CT Mon-Sat. Full pipeline: scrape → score → email → S3 sync. You and Vivian get daily summaries automatically.</li>
<li><strong>Automation Proposal</strong> — 746-line analysis for HubSpot/Dialpad/Gmail automation (AUTOMATION_PROPOSAL.md on GitHub).</li>
<li><strong>GitHub</strong> — All changes committed and pushed.</li>
</ol>

<h3>Backfill Results:</h3>
<table border="1" cellpadding="5" style="border-collapse:collapse;">
<tr><th>School</th><th>Date</th><th>Notes</th><th>Missing</th><th>Total</th></tr>
<tr><td>West U</td><td>Jul 6</td><td>16</td><td>6</td><td>22</td></tr>
<tr><td>West U</td><td>Jul 7</td><td>21</td><td>8</td><td>29</td></tr>
<tr><td>West U</td><td>Jul 8</td><td>21</td><td>6</td><td>27</td></tr>
<tr><td>West U</td><td>Jul 9</td><td>10</td><td>19</td><td>29</td></tr>
<tr><td>The Heights</td><td>Jul 6</td><td>23</td><td>6</td><td>29</td></tr>
<tr><td>The Heights</td><td>Jul 7</td><td>11</td><td>6</td><td>17</td></tr>
<tr><td>The Heights</td><td>Jul 8</td><td>9</td><td>13</td><td>22</td></tr>
<tr><td>The Heights</td><td>Jul 9</td><td>5</td><td>20</td><td>25</td></tr>
</table>

<h3>What's Next (Phase 2):</h3>
<ul>
<li><strong>Phase 1</strong> — Integrate Pike13 lead extraction into daily flow</li>
<li><strong>Phase 2</strong> — Auto-auth for HubSpot (email MFA) and Dialpad (needs discovery)</li>
<li><strong>Phase 3</strong> — Unified orchestrator, health monitoring, alerting</li>
</ul>

<p><em>The database will be updated every day with all Pike13 lesson notes data.
Once Phase 2 is implemented, it will also include HubSpot, Dialpad, and Gmail data.</em></p>

<p>Code: <a href="https://github.com/hughrscott/NotesReminder">github.com/hughrscott/NotesReminder</a><br>
Proposal: AUTOMATION_PROPOSAL.md in the repo</p>
</body></html>"""

msg.attach(MIMEText(plain, "plain"))
msg.attach(MIMEText(html, "html"))

recipients = ["hughrscott@mac.com", "hugh.scott@gmail.com"]

try:
    smtp = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
    smtp.starttls()
    smtp.login(sender, password)
    smtp.sendmail(sender, recipients, msg.as_string())
    smtp.quit()
    print("Summary email sent!")
except Exception as e:
    print(f"Email error: {e}")