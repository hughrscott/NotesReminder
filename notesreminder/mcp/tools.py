"""MCP tool definitions for the NotesReminder server."""
import asyncio
import json
import os
import sqlite3
from datetime import datetime

from notesreminder.lib.cookie_auth import check_cookie_freshness, load_cookies


PIKE13_SCRAPE_LOCK = asyncio.Lock()  # prevent concurrent scrapes


def register_pike13_tools(mcp):
    """Register Pike13 scraping tools on the MCP server."""

    @mcp.tool()
    async def pike13_scrape_lessons(
        school: str = "westu-sor",
        start_date: str = "",
        end_date: str = "",
        limit_days: int = 7,
    ) -> str:
        """Scrape Pike13 lesson data for a school/date range. Uses injected Okta cookies for auth.

        Args:
            school: Pike13 subdomain (westu-sor or theheights-sor)
            start_date: Start date YYYY-MM-DD (defaults to limit_days ago)
            end_date: End date YYYY-MM-DD (defaults to today)
            limit_days: If start_date is empty, scrape this many days back from now
        """
        from datetime import timedelta

        if not start_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
            start_dt = end_dt - timedelta(days=limit_days)
            start_date = start_dt.strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        async with PIKE13_SCRAPE_LOCK:
            from noteschecker import scrape_lessons
            try:
                df = await scrape_lessons(
                    school_subdomain=school,
                    start_date=start_date,
                    end_date=end_date,
                    verbose=True,
                )
            except Exception as e:
                return json.dumps({
                    "status": "error",
                    "error": str(e),
                }, indent=2)
        return json.dumps({
            "status": "success",
            "school": school,
            "start_date": start_date,
            "end_date": end_date,
            "lessons_scraped": len(df),
            "columns": list(df.columns),
        }, default=str, indent=2)

    @mcp.tool()
    async def pike13_cookie_status() -> str:
        """Check the health of stored Pike13 auth cookies. Returns freshness, expiry, and cookie count."""
        try:
            payload = load_cookies()
            freshness = check_cookie_freshness(payload)
            result = {
                "cookies_available": True,
                "cookie_count": payload.get("cookie_count", 0),
                "extracted_at": payload.get("extracted_at"),
                "freshness": freshness,
            }
        except Exception as e:
            result = {
                "cookies_available": False,
                "error": str(e),
            }
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def pike13_import_and_update_db(
        school: str = "westu-sor",
        start_date: str = "",
        end_date: str = "",
        limit_days: int = 7,
    ) -> str:
        """Scrape Pike13 lessons and update reminders.db with the results. Combines scrape + DB update.

        This is the primary tool for keeping the notes database current from Pike13.
        After scraping, it updates the reminders table and runs reporting schema sync.
        """
        from datetime import timedelta

        if not start_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
            start_dt = end_dt - timedelta(days=limit_days)
            start_date = start_dt.strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        async with PIKE13_SCRAPE_LOCK:
            from noteschecker import scrape_lessons
            try:
                df = await scrape_lessons(
                    school_subdomain=school,
                    start_date=start_date,
                    end_date=end_date,
                    verbose=True,
                )
            except Exception as e:
                return json.dumps({
                    "status": "error",
                    "error": str(e),
                }, indent=2)

        if df.empty:
            return json.dumps({"status": "no_data", "school": school, "lessons": 0}, indent=2)

        # Import the refactored DB update function from run_daily
        from run_daily import update_reminders_from_dataframe

        db_path = os.getenv("REMINDERS_DB_PATH", "reminders.db")
        conn = sqlite3.connect(db_path)
        try:
            result = update_reminders_from_dataframe(conn, df, school)
        finally:
            conn.close()

        return json.dumps({
            "status": "success",
            "school": school,
            "start_date": start_date,
            "end_date": end_date,
            "lessons_scraped": len(df),
            **result,
        }, default=str, indent=2)
