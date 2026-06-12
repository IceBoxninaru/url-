from __future__ import annotations

from datetime import timedelta

import httpx
from django.conf import settings
from django.utils import timezone

from resources.models import LinkStatus, Resource

from .content import extract_metadata, extract_text_from_html
from .types import LinkCheckResult


DELETE_MARKERS = [
    "deleted",
    "removed",
    "not found",
    "unavailable",
    "このページはご利用いただけません",
    "削除",
    "見つかりません",
    "404",
]


def detect_deleted_like(text: str, title: str, http_status: int | None) -> bool:
    if http_status in {404, 410}:
        return True
    combined = f"{title}\n{text}".lower()
    return any(marker in combined for marker in DELETE_MARKERS)


def should_refresh_link_check(resource: Resource, *, force: bool = False) -> bool:
    if force:
        return True
    if resource.last_link_check_at is None or resource.link_status == LinkStatus.UNCHECKED:
        return True
    refresh_after = timedelta(seconds=settings.LINK_CHECK_CACHE_SECONDS)
    return timezone.now() - resource.last_link_check_at >= refresh_after


def perform_link_check(url: str) -> LinkCheckResult:
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=settings.LINK_CHECK_HTTP_TIMEOUT,
            headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
        ) as client:
            response = client.get(url)
        html = response.text or ""
        title = ""
        extracted_text = ""
        content_type = response.headers.get("content-type", "")
        if "text/html" in content_type or not content_type:
            metadata = extract_metadata(html) if html else {}
            title = metadata.get("page_title", "")
            extracted_text = extract_text_from_html(html, str(response.url)) if html else ""
        deleted_like = detect_deleted_like(extracted_text, title, response.status_code)

        if response.status_code in {404, 410}:
            status = LinkStatus.GONE
        elif deleted_like:
            status = LinkStatus.MAYBE_DELETED
        elif response.status_code >= 400:
            status = LinkStatus.ERROR
        else:
            status = LinkStatus.ACTIVE

        return LinkCheckResult(
            status=status,
            http_status=response.status_code,
            checked_url=str(response.url),
        )
    except Exception as exc:
        return LinkCheckResult(status=LinkStatus.ERROR, error_message=str(exc))


def check_resource_link_status(resource: Resource, *, force: bool = False) -> Resource:
    if not should_refresh_link_check(resource, force=force):
        return resource

    source_url = resource.normalized_url or resource.original_url
    result = perform_link_check(source_url)
    resource.link_status = result.status
    resource.last_link_check_at = timezone.now()
    resource.last_link_check_http_status = result.http_status
    resource.last_link_check_error = result.error_message
    resource.save(
        update_fields=[
            "link_status",
            "last_link_check_at",
            "last_link_check_http_status",
            "last_link_check_error",
        ]
    )
    return resource
