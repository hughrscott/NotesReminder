#!/usr/bin/env python3
"""MCP server exposing the SOR reminders.db as queryable tools."""
import sqlite3
import json
import sys
from pathlib import Path

DB_PATH = "/home/ubuntu/projects/hughrscott/NotesReminder/reminders.db"

# Minimal MCP server using stdio (no external deps beyond stdlib)
# Implements the JSON-RPC 2.0 protocol over stdin/stdout

def handle_request(req: dict) -> dict:
    method = req.get("method", "")
    req_id = req.get("id")
    params = req.get("params", {})

    if method == "initialize":
        return {
            "jsonrpc": "2.0", "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "sor-db", "version": "1.0.0"}
            }
        }

    if method == "notifications/initialized":
        return None  # no response for notifications

    if method == "tools/list":
        return {
            "jsonrpc": "2.0", "id": req_id,
            "result": {
                "tools": [
                    {
                        "name": "query",
                        "description": "Run a read-only SQL query against the SOR database. Returns results as JSON. Tables: lessons, pike13_people, hubspot_contacts, hubspot_deals, dialpad_calls, dialpad_sms_messages, dialpad_voicemails, dialpad_call_reviews, dialpad_voice_events, school_email_messages, lesson_notes, identity_matches, and more.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "sql": {
                                    "type": "string",
                                    "description": "SQL SELECT query to execute (read-only)"
                                }
                            },
                            "required": ["sql"]
                        }
                    },
                    {
                        "name": "tables",
                        "description": "List all tables in the database with their column names.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {}
                        }
                    },
                    {
                        "name": "schema",
                        "description": "Get the schema (CREATE TABLE statement) for a specific table.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "table": {
                                    "type": "string",
                                    "description": "Table name to describe"
                                }
                            },
                            "required": ["table"]
                        }
                    }
                ]
            }
        }

    if method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})

        if tool_name == "tables":
            conn = sqlite3.connect(DB_PATH)
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
            result_text = "Tables:\n" + "\n".join(f"  {r[0]}" for r in rows)
            conn.close()

        elif tool_name == "schema":
            table = arguments.get("table", "")
            conn = sqlite3.connect(DB_PATH)
            try:
                schema = conn.execute(
                    f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                    [table]
                ).fetchone()
                cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
                result_text = f"CREATE statement:\n{schema[0] if schema else 'not found'}\n\nColumns:\n"
                result_text += "\n".join(f"  {c[1]} ({c[2]})" for c in cols)
                result_text += f"\n\nRow count: {conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]}"
            except Exception as e:
                result_text = f"Error: {e}"
            conn.close()

        elif tool_name == "query":
            sql = arguments.get("sql", "")
            # Safety: only allow SELECT
            cleaned = sql.strip()
            if not cleaned.upper().startswith("SELECT"):
                result_text = "Error: Only SELECT queries are allowed"
            else:
                conn = sqlite3.connect(DB_PATH)
                try:
                    cur = conn.execute(cleaned)
                    cols = [d[0] for d in cur.description] if cur.description else []
                    rows = cur.fetchall()[:200]  # limit rows
                    # Format as readable text
                    if not rows:
                        result_text = "Query returned 0 rows"
                    else:
                        lines = ["  " + " | ".join(cols)]
                        lines.append("  " + "-" * 40)
                        for row in rows:
                            lines.append("  " + " | ".join(str(v)[:60] for v in row))
                        result_text = f"{len(rows)} rows:\n" + "\n".join(lines)
                        if len(rows) == 200:
                            result_text += "\n  (truncated at 200 rows)"
                except Exception as e:
                    result_text = f"SQL Error: {e}"
                conn.close()
        else:
            result_text = f"Unknown tool: {tool_name}"

        return {
            "jsonrpc": "2.0", "id": req_id,
            "result": {
                "content": [{"type": "text", "text": result_text}]
            }
        }

    # Unknown method
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Unknown method: {method}"}}


def main():
    """MCP stdio loop: read JSON-RPC messages from stdin, write responses to stdout."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            resp = handle_request(req)
            if resp is not None:
                sys.stdout.write(json.dumps(resp) + "\n")
                sys.stdout.flush()
        except json.JSONDecodeError:
            pass
        except Exception as e:
            err = {"jsonrpc": "2.0", "id": None, "error": {"code": -32603, "message": str(e)}}
            sys.stdout.write(json.dumps(err) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
