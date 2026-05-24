"""Compatibility shim for the NotesReminder MCP server."""

import sys

from notesreminder.mcp import server as _server


if __name__ == "__main__":
    _server.mcp.run()
else:
    sys.modules[__name__] = _server
