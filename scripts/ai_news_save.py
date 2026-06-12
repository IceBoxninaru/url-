#!/usr/bin/env python3
"""Save curated AI-news URLs through the Django/Postgres app only.

Input JSON can be either a list of items or an object with an ``items`` list.
Each item must contain ``url`` and may contain ``title``, ``note``, ``tags``,
``source``, and ``search_query``.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from _config import DEFAULT_AI_NEWS_SAVE_MAX_ITEMS
from _django import ensure_docker_postgres_db, setup_django

setup_django()

from django.db import connection, transaction  # noqa: E402

from resources.models import Resource  # noqa: E402
from resources.services import enqueue_capture_job, normalize_url  # noqa: E402
from tags.models import Tag  # noqa: E402


DEFAULT_SAVE_REASON = "AI search"
DEFAULT_TAGS = ["AI"]
PROTECTED_HOSTS = {
    "help.openai.com",
}


def load_items(path: str) -> list[dict[str, Any]]:
    if path == "-":
        raw = sys.stdin.read().lstrip("\ufeff")
    else:
        raw = Path(path).read_text(encoding="utf-8-sig")
    payload = json.loads(raw)
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict) and isinstance(payload.get("items"), list):
        items = payload["items"]
    else:
        raise ValueError("items JSON must be a list or an object with an items list")
    normalized_items: list[dict[str, Any]] = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"item {index} is not an object")
        normalized_items.append(item)
    return normalized_items


def validate_item(item: dict[str, Any], *, reject_protected_hosts: bool) -> tuple[str, str, list[str]]:
    raw_url = str(item.get("url") or "").strip()
    if not raw_url:
        raise ValueError("missing url")
    parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"unsupported url: {raw_url}")
    host = parsed.netloc.lower().split("@")[-1].split(":")[0]
    if reject_protected_hosts and host in PROTECTED_HOSTS:
        raise ValueError(f"protected_host:{host}")

    title = str(item.get("title") or "").strip()
    if not title:
        title = host or raw_url
    tags = item.get("tags") or DEFAULT_TAGS
    if not isinstance(tags, list):
        raise ValueError("tags must be a list")
    clean_tags = []
    for tag in tags:
        tag_name = str(tag).strip()
        if tag_name and tag_name not in clean_tags:
            clean_tags.append(tag_name[:40])
    return raw_url, title[:255], clean_tags or DEFAULT_TAGS


def build_note(item: dict[str, Any]) -> str:
    note = str(item.get("note") or "").strip()
    source = str(item.get("source") or "").strip()
    search_query = str(item.get("search_query") or "").strip()
    parts = []
    if note:
        parts.append(note)
    if source:
        parts.append(f"source: {source}")
    if search_query:
        parts.append(f"search_query: {search_query}")
    return "\n".join(parts)


def save_items(items: list[dict[str, Any]], *, max_items: int, reject_protected_hosts: bool, dry_run: bool) -> dict[str, Any]:
    ensure_docker_postgres_db(connection)
    saved: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    if len(items) > max_items:
        rejected.extend(
            {
                "index": index,
                "url": item.get("url"),
                "reason": "over_daily_limit",
            }
            for index, item in enumerate(items[max_items:], start=max_items + 1)
        )
        items = items[:max_items]

    with transaction.atomic():
        for index, item in enumerate(items, start=1):
            try:
                raw_url, title, tags = validate_item(item, reject_protected_hosts=reject_protected_hosts)
            except ValueError as exc:
                rejected.append({"index": index, "url": item.get("url"), "reason": str(exc)})
                continue

            normalized = normalize_url(raw_url)
            existing = Resource.objects.filter(normalized_url=normalized).first()
            if existing:
                skipped.append({"index": index, "url": raw_url, "reason": "duplicate", "id": existing.id})
                continue

            if dry_run:
                saved.append({"index": index, "url": raw_url, "title": title, "dry_run": True})
                continue

            resource = Resource.objects.create(
                original_url=raw_url,
                normalized_url=normalized,
                domain=urlparse(normalized).netloc,
                title_manual=title,
                note=build_note(item),
                save_reason=str(item.get("save_reason") or DEFAULT_SAVE_REASON)[:40],
                search_only=True,
                capture_images=bool(item.get("capture_images", True)),
                capture_videos=bool(item.get("capture_videos", False)),
            )
            for tag_name in tags:
                tag, _ = Tag.objects.get_or_create(name=tag_name)
                resource.tags.add(tag)
            job = enqueue_capture_job(resource)
            saved.append({"index": index, "id": resource.id, "url": raw_url, "title": title, "job_id": job.id})

    return {
        "vendor": connection.vendor,
        "host": connection.settings_dict.get("HOST"),
        "saved": saved,
        "skipped": skipped,
        "rejected": rejected,
        "ok_to_verify_ids": [item["id"] for item in saved if "id" in item],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Save curated AI-news URLs.")
    parser.add_argument("--items-json", required=True, help="Path to JSON file, or '-' for stdin.")
    parser.add_argument("--max-items", type=int, default=DEFAULT_AI_NEWS_SAVE_MAX_ITEMS)
    parser.add_argument("--allow-protected-hosts", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        items = load_items(args.items_json)
        result = save_items(
            items,
            max_items=args.max_items,
            reject_protected_hosts=not args.allow_protected_hosts,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 2

    result["ok"] = not result["rejected"]
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
