from __future__ import annotations

from datetime import timedelta

from django.conf import settings
from django.utils import timezone

from resources.models import LinkStatus, Resource


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
