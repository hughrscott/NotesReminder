#!/bin/sh
set -euo pipefail

echo "Installing NotesReminder MCP server as systemd service..."

sudo cp deploy/notesreminder-mcp.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable notesreminder-mcp
sudo systemctl start notesreminder-mcp

echo "Service installed. Check status with:"
echo "  sudo systemctl status notesreminder-mcp"
echo "  sudo journalctl -u notesreminder-mcp -f"
