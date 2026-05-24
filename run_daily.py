"""Compatibility shim for the production daily notes entry point."""

from notesreminder.orchestration.run_daily import *  # noqa: F401,F403
from notesreminder.orchestration.run_daily import main


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
