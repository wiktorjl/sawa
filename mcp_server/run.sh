#!/bin/bash
# Portable MCP server launcher - resolves venv path relative to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
exec "$PROJECT_ROOT/.venv/bin/python" -m mcp_server.server
