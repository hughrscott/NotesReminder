# NotesReminder

## Required Environment Variables
Set these before running any scripts (e.g., in a local `.env` file that `dotenv` loads or via `export` in your shell):

| Variable | Purpose |
| --- | --- |
| `PIKE13_USER` | Pike13 login email used by the Playwright scraper. |
| `PIKE13_PASSWORD` | Pike13 login password. |
| `AWS_ACCESS_KEY_ID` | AWS credential for syncing `reminders.db` to S3. |
| `AWS_SECRET_ACCESS_KEY` | Matching AWS secret. |
| `AWS_DEFAULT_REGION` | Region for the S3 client (e.g., `us-east-1`). |

Optional: update `run_daily.py` if you need different SMTP credentials or recipient emails.

## CLI Usage
Run the help command anytime to see the full synopsis:

```bash
python run_daily.py --help
```

Key flags:

| Flag | Description |
| --- | --- |
| `--school` | Pike13 subdomain (default `westu-sor`). |
| `--start-date`, `--end-date` | Date range in `YYYY-MM-DD`. Defaults to the past 7 days if omitted. |
| `--init-db` | Rebuilds `reminders.db` using `init_db.py` and uploads it to S3. |
| `--verbose` | Enables detailed logging (Playwright progress, AWS sync info). |
| `--summary` | Controls email content: `none` (missing only), `notes`, `missing`, or `both`. |
| `--to` | Required list of primary recipients for the summary email. |
| `--cc` | Optional list of CC email addresses. |

## What the Project Does
This repo automates “missing lesson notes” reminders for School of Rock locations:

1. `run_daily.py` is the main entry point. It:
   - Downloads `reminders.db` from S3 (or initializes it with `--init-db`).
   - Runs the Playwright scraper (`noteschecker.py`) for the requested school/date window to capture lesson details, note status, attendance, and room locations.
   - Updates the SQLite database with the latest scrape results.
   - Filters single-student lessons without notes for the selected dates and emails a grouped HTML/plain-text report through the configured SMTP server.
   - Re-uploads the refreshed database to S3 so subsequent runs stay in sync.
   - Optional flags: `--verbose` for detailed logging, `--summary` (`notes`, `missing`, or `both`) for CLI summaries, and `--init-db` to rebuild/upload an empty database.

2. `noteschecker.py` performs the Pike13 scrape using Playwright, taking screenshots/trace files for debugging and writing a CSV per run.

3. `init_db.py` (or `notesreminder.py`) creates the SQLite schema when you need a fresh `reminders.db`.

4. `instructormapping.py` provides instructor contact details if you extend the workflow to notify teachers directly.

## Running a Report
Install dependencies once:

```bash
pip install -r requirements.txt
playwright install
```

Example command (quiet mode):

```bash
python run_daily.py --school westu-sor --start-date 2025-06-18 --end-date 2025-06-18 \
  --to huscott@schoolofrock.com vscott@schoolofrock.com
```

Add `--summary both` for CLI summaries or `--verbose` for scrape-level logs. After a successful run you’ll receive an email report and see the database synced back to S3.
