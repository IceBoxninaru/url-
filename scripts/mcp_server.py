#!/usr/bin/env python3
"""Minimal stdio MCP server for URL archive resources."""
from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request
from typing import Any

BASE_URL = os.environ.get("URL_ARCHIVE_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
PROTOCOL_VERSION = "2024-11-05"

TOOLS = [
    {
        "name": "list_ai_search_urls",
        "description": "List AI-search/search-only URL resources. Defaults to today's resources in the Django app timezone when date/from/to are omitted.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "description": "YYYY-MM-DD local date to fetch."},
                "from": {"type": "string", "description": "YYYY-MM-DD start date, inclusive."},
                "to": {"type": "string", "description": "YYYY-MM-DD end date, inclusive."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                "query": {"type": "string"},
            },
        },
    },
    {
        "name": "list_search_only_urls",
        "description": "Alias for list_ai_search_urls.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "date": {"type": "string"},
                "from": {"type": "string"},
                "to": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                "query": {"type": "string"},
            },
        },
    },
    {
        "name": "recent_saved_urls",
        "description": "List recently saved non-search-only URL resources.",
        "inputSchema": {"type": "object", "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 20}}},
    },
    {
        "name": "search_urls",
        "description": "Search saved URL resources by keyword.",
        "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 20}}, "required": ["query"]},
    },
    {
        "name": "url_detail",
        "description": "Get URL resource detail by resource id.",
        "inputSchema": {"type": "object", "properties": {"resource_id": {"type": "integer"}}, "required": ["resource_id"]},
    },
    {
        "name": "url_media",
        "description": "Get media assets for a URL resource by resource id.",
        "inputSchema": {"type": "object", "properties": {"resource_id": {"type": "integer"}}, "required": ["resource_id"]},
    },
]


def http_get(path: str, params: dict[str, Any] | None = None) -> Any:
    query = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v not in (None, "")})
    url = f"{BASE_URL}{path}"
    if query:
        url = f"{url}?{query}"
    with urllib.request.urlopen(url, timeout=20) as response:
        body = response.read().decode("utf-8")
        return json.loads(body)


def tool_result(payload: Any) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=False, indent=2)}]}


def call_tool(name: str, args: dict[str, Any]) -> dict[str, Any]:
    args = args or {}
    if name in {"list_ai_search_urls", "list_search_only_urls"}:
        params = {k: args.get(k) for k in ("date", "from", "to", "limit", "query")}
        return tool_result(http_get("/api/resources/ai-search/", params))
    if name == "recent_saved_urls":
        return tool_result(http_get("/api/resources/recent/", {"limit": args.get("limit")}))
    if name == "search_urls":
        return tool_result(http_get("/api/resources/search/", {"query": args.get("query", ""), "limit": args.get("limit")}))
    if name == "url_detail":
        return tool_result(http_get(f"/api/resources/{int(args['resource_id'])}/"))
    if name == "url_media":
        detail = http_get(f"/api/resources/{int(args['resource_id'])}/")
        snapshot = detail.get("latest_snapshot") or {}
        return tool_result({"resource_id": args["resource_id"], "media_assets": snapshot.get("media_assets", []), "screenshot_asset": snapshot.get("screenshot_asset")})
    raise ValueError(f"Unknown tool: {name}")


def respond(message_id: Any, result: Any = None, error: Any = None) -> None:
    payload: dict[str, Any] = {"jsonrpc": "2.0", "id": message_id}
    if error is not None:
        payload["error"] = {"code": -32000, "message": str(error)}
    else:
        payload["result"] = result
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def handle(request: dict[str, Any]) -> None:
    method = request.get("method")
    message_id = request.get("id")
    params = request.get("params") or {}
    try:
        if method == "initialize":
            respond(message_id, {"protocolVersion": PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": "url-archive", "version": "0.1.0"}})
        elif method == "tools/list":
            respond(message_id, {"tools": TOOLS})
        elif method == "tools/call":
            respond(message_id, call_tool(params.get("name", ""), params.get("arguments") or {}))
        elif message_id is not None:
            respond(message_id, {})
    except Exception as exc:
        respond(message_id, error=exc)


def main() -> None:
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            handle(json.loads(line))
        except Exception as exc:
            respond(None, error=exc)


if __name__ == "__main__":
    main()
