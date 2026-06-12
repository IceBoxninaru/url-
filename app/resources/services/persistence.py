from __future__ import annotations

import hashlib
import json

from django.conf import settings
from django.db.models import Max

from resources.models import Resource
from snapshots.models import Snapshot

from .media_discovery import guess_image_extension, guess_video_extension
from .storage import move_storage_file, write_storage_file
from .types import CaptureResult

def next_snapshot_no(resource: Resource) -> int:
    latest = resource.snapshots.aggregate(max_no=Max("snapshot_no"))["max_no"] or 0
    return latest + 1


def persist_snapshot(resource: Resource, result: CaptureResult) -> Snapshot:
    snapshot_no = next_snapshot_no(resource)
    prefix = f"snapshot_{snapshot_no:04d}"
    html_path = ""
    text_path = ""
    screenshot_path = ""
    image_assets: list[dict] = []
    video_assets: list[dict] = []

    if result.html:
        html_path = write_storage_file(
            settings.HTML_STORAGE_ROOT,
            resource.id,
            f"{prefix}.html",
            result.html,
        )
    if result.extracted_text:
        text_path = write_storage_file(
            settings.TEXT_STORAGE_ROOT,
            resource.id,
            f"{prefix}.txt",
            result.extracted_text,
        )
    json_path = write_storage_file(
        settings.JSON_STORAGE_ROOT,
        resource.id,
        f"{prefix}.json",
        json.dumps(result.response_payload, ensure_ascii=False, indent=2),
    )
    if result.screenshot_bytes:
        screenshot_path = write_storage_file(
            settings.SCREENSHOT_STORAGE_ROOT,
            resource.id,
            f"{prefix}_full.png",
            result.screenshot_bytes,
            binary=True,
        )
    for index, image in enumerate(result.captured_images, start=1):
        extension = guess_image_extension(image.source_url, image.content_type)
        image_path = write_storage_file(
            settings.IMAGE_STORAGE_ROOT,
            resource.id,
            f"{prefix}_img_{index:02d}{extension}",
            image.content,
            binary=True,
        )
        image_assets.append(
            {
                "source_url": image.source_url,
                "path": image_path,
                "content_type": image.content_type,
                "size_bytes": len(image.content),
            }
        )
    for index, video in enumerate(result.captured_videos, start=1):
        extension = guess_video_extension(video.source_url, video.content_type)
        video_path = move_storage_file(
            settings.VIDEO_STORAGE_ROOT,
            resource.id,
            f"{prefix}_vid_{index:02d}{extension}",
            video.temp_path,
        )
        video_assets.append(
            {
                "source_url": video.source_url,
                "path": video_path,
                "content_type": video.content_type,
                "size_bytes": video.size_bytes,
                **video.metadata,
            }
        )

    metadata = result.metadata
    payload_basis = result.extracted_text or result.html or result.error_message
    content_hash = hashlib.sha256(payload_basis.encode("utf-8")).hexdigest() if payload_basis else ""
    return Snapshot.objects.create(
        resource=resource,
        snapshot_no=snapshot_no,
        fetch_url=result.fetch_url,
        fetch_method=result.fetch_method,
        http_status=result.http_status,
        page_title=metadata.get("page_title", ""),
        site_name=metadata.get("site_name", ""),
        author=metadata.get("author", ""),
        published_at=metadata.get("published_at"),
        og_title=metadata.get("og_title", ""),
        og_description=metadata.get("og_description", ""),
        og_image_url=metadata.get("og_image_url", ""),
        extracted_text=result.extracted_text,
        content_hash=content_hash,
        image_assets=image_assets,
        video_assets=video_assets,
        raw_html_path=html_path,
        raw_text_path=text_path,
        raw_json_path=json_path,
        screenshot_full_path=screenshot_path,
        screenshot_taken_at=result.screenshot_taken_at,
        page_height=result.page_height,
        viewport_width=result.viewport_width,
        viewport_height=result.viewport_height,
        is_deleted_like=result.deleted_like,
        error_message=result.error_message,
    )

