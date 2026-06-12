#!/usr/bin/env python3
"""Strict verifier for daily AI-news URL saves."""
from __future__ import annotations

import argparse
import datetime as dt
import json
from typing import Any

from _config import DEFAULT_AI_NEWS_VERIFY_EXPECTED_MAX, DEFAULT_AI_NEWS_VERIFY_EXPECTED_MIN
from _django import ensure_docker_postgres_db, setup_django

setup_django()

from django.db import connection  # noqa: E402
from django.utils import timezone  # noqa: E402

from jobs.models import CaptureJob, JobStatus, JobType  # noqa: E402
from resources.models import LinkStatus, Resource, ResourceStatus  # noqa: E402


BAD_RESOURCE_STATUSES = {ResourceStatus.FETCH_FAILED, ResourceStatus.MAYBE_DELETED, ResourceStatus.GONE}
BAD_LINK_STATUSES = {LinkStatus.MAYBE_DELETED, LinkStatus.GONE, LinkStatus.ERROR}
BAD_JOB_STATUSES = {JobStatus.FAILED, JobStatus.RETRY_WAIT, JobStatus.RUNNING}


def local_day_bounds(target_date: dt.date) -> tuple[dt.datetime, dt.datetime]:
    tz = timezone.get_current_timezone()
    start = timezone.make_aware(dt.datetime.combine(target_date, dt.time.min), tz)
    end = timezone.make_aware(dt.datetime.combine(target_date + dt.timedelta(days=1), dt.time.min), tz)
    return start, end


def parse_ids(raw_ids: str) -> list[int]:
    ids: list[int] = []
    for part in raw_ids.replace(",", " ").split():
        ids.append(int(part))
    return ids


def queryset_for_args(args: argparse.Namespace):
    queryset = Resource.objects.with_related().filter(search_only=True)
    if args.ids:
        return queryset.filter(id__in=parse_ids(args.ids)).order_by("id")
    target_date = dt.date.fromisoformat(args.date)
    start, end = local_day_bounds(target_date)
    return queryset.filter(created_at__gte=start, created_at__lt=end).order_by("id")


def resource_payload(resource: Resource) -> dict[str, Any]:
    snapshot = resource.latest_snapshot
    jobs = list(
        CaptureJob.objects.filter(resource_id=resource.id)
        .order_by("id")
        .values("id", "resource_id", "job_type", "status", "attempt_count", "error_message")
    )
    failures: list[str] = []
    warnings: list[str] = []

    if resource.current_status in BAD_RESOURCE_STATUSES:
        failures.append(f"resource_status:{resource.current_status}")
    if resource.link_status in BAD_LINK_STATUSES:
        failures.append(f"link_status:{resource.link_status}")
    if snapshot is None:
        failures.append("missing_snapshot")
    else:
        if snapshot.http_status != 200:
            failures.append(f"http_status:{snapshot.http_status}")
        if snapshot.error_message:
            failures.append(f"snapshot_error:{snapshot.error_message[:160]}")
        if not (snapshot.extracted_text or snapshot.raw_html_path or snapshot.screenshot_full_path):
            failures.append("empty_snapshot")
        if not snapshot.ai_summary and not snapshot.ai_translation:
            warnings.append("missing_ai_enrichment")

    capture_jobs = [job for job in jobs if job["job_type"] == JobType.CAPTURE]
    if any(job["status"] in BAD_JOB_STATUSES for job in capture_jobs):
        failures.append("bad_capture_job_status")
    ai_jobs = [job for job in jobs if job["job_type"] == JobType.AI_ENRICH]
    if any(job["status"] in BAD_JOB_STATUSES for job in ai_jobs):
        warnings.append("ai_job_not_clean")

    return {
        "id": resource.id,
        "title": resource.title_manual,
        "url": resource.original_url,
        "status": resource.current_status,
        "link_status": resource.link_status,
        "snapshot_id": snapshot.id if snapshot else None,
        "http_status": snapshot.http_status if snapshot else None,
        "error": snapshot.error_message if snapshot else None,
        "jobs": jobs,
        "failures": failures,
        "warnings": warnings,
        "ok": not failures,
    }


def verify(args: argparse.Namespace) -> dict[str, Any]:
    ensure_docker_postgres_db(connection)
    resources = list(queryset_for_args(args))
    items = [resource_payload(resource) for resource in resources]
    failures = [item for item in items if not item["ok"]]
    warnings = [item for item in items if item["warnings"]]
    ok_count = len([item for item in items if item["ok"]])
    expected_min = args.expected_min
    expected_max = args.expected_max
    count_failure = None
    if expected_min is not None and len(items) < expected_min:
        count_failure = f"count_below_min:{len(items)}<{expected_min}"
    if expected_max is not None and len(items) > expected_max:
        count_failure = f"count_above_max:{len(items)}>{expected_max}"
    return {
        "vendor": connection.vendor,
        "host": connection.settings_dict.get("HOST"),
        "date": args.date,
        "count": len(items),
        "ok_count": ok_count,
        "failure_count": len(failures),
        "warning_count": len(warnings),
        "count_failure": count_failure,
        "items": items,
        "ok": not failures and count_failure is None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify daily AI-news URL saves.")
    parser.add_argument("--date", default=timezone.localdate().isoformat())
    parser.add_argument("--ids", default="", help="Comma or space separated resource ids. Overrides --date.")
    parser.add_argument("--expected-min", type=int, default=DEFAULT_AI_NEWS_VERIFY_EXPECTED_MIN)
    parser.add_argument("--expected-max", type=int, default=DEFAULT_AI_NEWS_VERIFY_EXPECTED_MAX)
    args = parser.parse_args()

    try:
        result = verify(args)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 4


if __name__ == "__main__":
    raise SystemExit(main())
