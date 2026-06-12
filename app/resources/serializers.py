from __future__ import annotations

from resources.models import Resource
from resources.services import get_capture_files, get_snapshot_screenshot_file
from tags.models import Tag

def isoformat_or_none(value) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def truncate_text(value: str, *, limit: int = 1000) -> str:
    text = (value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit].rstrip()}..."


def storage_url_for_path(path: str) -> str:
    if not path:
        return ""
    if path.startswith(("http://", "https://", "/")):
        return path
    return f"/{path}"


def serialize_tag(tag: Tag) -> dict:
    return {
        "id": tag.id,
        "name": tag.name,
        "color": tag.color,
    }


def serialize_media_asset(asset: dict, media_type: str) -> dict:
    path = str(asset.get("path", "")).strip()
    return {
        **asset,
        "media_type": media_type,
        "path": path,
        "url": storage_url_for_path(path),
    }


def serialize_snapshot(snapshot, *, include_text: bool = False, include_media: bool = False) -> dict | None:
    if snapshot is None:
        return None

    image_files, video_files = get_capture_files(snapshot)
    screenshot_file = get_snapshot_screenshot_file(snapshot)
    image_assets = [serialize_media_asset(asset, "image") for asset in image_files]
    video_assets = [serialize_media_asset(asset, "video") for asset in video_files]
    screenshot_path = screenshot_file["path"] if screenshot_file else ""
    payload = {
        "id": snapshot.id,
        "snapshot_no": snapshot.snapshot_no,
        "fetch_url": snapshot.fetch_url,
        "fetch_method": snapshot.fetch_method,
        "http_status": snapshot.http_status,
        "fetched_at": isoformat_or_none(snapshot.fetched_at),
        "page_title": snapshot.page_title,
        "site_name": snapshot.site_name,
        "author": snapshot.author,
        "published_at": isoformat_or_none(snapshot.published_at),
        "summary": snapshot.ai_summary,
        "translation": snapshot.ai_translation if include_text else truncate_text(snapshot.ai_translation),
        "category": snapshot.ai_category,
        "image_count": len(image_assets),
        "video_count": len(video_assets),
        "screenshot_path": screenshot_path,
        "screenshot_url": storage_url_for_path(screenshot_path),
        "screenshot_missing": bool(snapshot.screenshot_full_path and not screenshot_file),
    }
    if include_media:
        payload.update(
            {
                "image_assets": image_assets,
                "video_assets": video_assets,
                "media_assets": [*image_assets, *video_assets],
                "screenshot_asset": serialize_media_asset(screenshot_file, "screenshot") if screenshot_file else None,
            }
        )
    if include_text:
        payload["extracted_text"] = snapshot.extracted_text
    else:
        payload["text_excerpt"] = truncate_text(snapshot.extracted_text)
    return payload


def serialize_resource(resource: Resource, *, detail: bool = False) -> dict:
    payload = {
        "id": resource.id,
        "title": resource.display_title,
        "url": resource.original_url,
        "normalized_url": resource.normalized_url,
        "domain": resource.domain,
        "favorite": resource.favorite,
        "search_only": resource.search_only,
        "interest_feedback": resource.interest_feedback,
        "interest_feedback_label": resource.get_interest_feedback_display(),
        "interest_labels": resource.interest_labels or [],
        "interest_label_names": resource.interest_label_names,
        "save_reason": resource.save_reason,
        "save_reason_label": resource.get_save_reason_display(),
        "next_action": resource.next_action,
        "review_state": resource.review_state,
        "review_state_label": resource.get_review_state_display(),
        "current_status": resource.current_status,
        "current_status_label": resource.get_current_status_display(),
        "link_status": resource.link_status,
        "link_status_label": resource.get_link_status_display(),
        "tags": [serialize_tag(tag) for tag in resource.tags.all()],
        "summary": resource.latest_summary,
        "translation": resource.latest_translation if detail else truncate_text(resource.latest_translation),
        "detail_path": resource.get_absolute_url(),
        "created_at": isoformat_or_none(resource.created_at),
        "updated_at": isoformat_or_none(resource.updated_at),
        "latest_snapshot": serialize_snapshot(resource.latest_snapshot, include_text=detail, include_media=detail),
    }
    if detail:
        payload.update(
            {
                "note": resource.note,
                "recheck_at": isoformat_or_none(resource.recheck_at),
                "capture_images": resource.capture_images,
                "capture_videos": resource.capture_videos,
                "last_link_check_at": isoformat_or_none(resource.last_link_check_at),
                "last_link_check_http_status": resource.last_link_check_http_status,
                "last_link_check_error": resource.last_link_check_error,
            }
        )
    return payload


def _payload_value(payload: dict, *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return str(value)
    return ""


def serialize_ai_search_resource(resource: Resource) -> dict:
    snapshot = resource.latest_snapshot
    ai_payload = snapshot.ai_payload if snapshot and isinstance(snapshot.ai_payload, dict) else {}
    summary = resource.latest_summary
    translation = resource.latest_translation
    source = _payload_value(ai_payload, "source", "ai_search_source")
    search_query = _payload_value(ai_payload, "search_query", "query", "ai_search_query")
    title = resource.display_title
    sendable_text = f"**{title}**\n<{resource.original_url}>"
    if summary:
        sendable_text += f"\n概要: {truncate_text(summary, limit=220)}"
    return {
        "resource_id": resource.id,
        "title": title,
        "url": resource.original_url,
        "summary": summary,
        "translation": translation,
        "created_at": isoformat_or_none(resource.created_at),
        "source": source,
        "search_query": search_query,
        "sendable_url": resource.original_url,
        "sendable_text": sendable_text,
    }

