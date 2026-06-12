from __future__ import annotations

import hashlib
import html
import json
import logging
import mimetypes
import os
import re
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup
from django.conf import settings
from django.db import transaction
from django.db.models import Max
from django.utils import timezone

from jobs.models import CaptureJob
from resources.models import Resource, ResourceStatus
from snapshots.models import FetchMethod, Snapshot

from .types import (
    AIResult,
    CapturedImage,
    CapturedVideo,
    CaptureResult,
    DownloadedMediaCandidate,
    DownloadedVideoAssets,
    LinkCheckResult,
    MediaProbeResult,
)
from .storage import (
    build_resource_directory,
    build_storage_asset_path,
    filter_existing_snapshot_assets,
    get_capture_files,
    get_snapshot_screenshot_file,
    move_storage_file,
    resolve_asset_file_path,
    resolve_storage_file_path,
    write_storage_file,
)
from .snapshots import build_snapshot_diff_context, build_snapshot_diff_items, get_previous_snapshot
from .urls import normalize_url
from .link_checks import (
    DELETE_MARKERS,
    check_resource_link_status,
    detect_deleted_like,
    perform_link_check,
    should_refresh_link_check,
)
from .jobs import enqueue_ai_job, enqueue_capture_job, status_from_snapshot
from .translation import (
    TRANSLATION_ENDPOINT,
    TRANSLATION_MAX_CHUNK_CHARS,
    TRANSLATION_MAX_SOURCE_CHARS,
    build_translation_source_text,
    is_probably_japanese_text,
    normalize_ai_text,
    split_translation_chunks,
    translate_text_chunk_to_japanese,
    translate_text_to_japanese,
)
from .ai_heuristics import STOP_WORDS, infer_category, similar_resource_ids, suggest_tags
from .local_llm import (
    extract_local_llm_content,
    local_llm_chat,
    local_llm_chat_completion_url,
    local_llm_models,
    normalize_local_llm_tags,
    parse_local_llm_json,
    truncate_ai_output,
)
from .artifacts import delete_resource_with_artifacts
from .content import extract_metadata, extract_text_from_html
from .media_discovery import (
    AUDIO_EXTENSIONS,
    ENCODED_MEDIA_URL_PATTERN,
    MEDIA_TEXT_SCAN_MAX_CHARS,
    RAW_MEDIA_URL_PATTERN,
    VIDEO_EXTENSIONS,
    build_media_candidate,
    classify_media_candidate_kind,
    collect_image_urls,
    collect_playwright_image_urls,
    collect_playwright_media_candidates,
    collect_playwright_video_urls,
    collect_video_candidate_details,
    collect_video_urls,
    decode_media_text_url,
    dedupe_urls,
    explain_media_candidate_skip,
    extract_media_candidate_urls_from_text,
    filter_image_candidate_urls,
    filter_video_candidate_urls,
    get_playwright_media_scope,
    get_playwright_profile_path,
    get_playwright_storage_state_path,
    guess_image_extension,
    guess_video_extension,
    html_unescape_and_clean_url,
    is_instagram_domain,
    is_observed_media_request,
    is_observed_media_response,
    is_observed_video_response,
    is_probable_audio_url,
    is_probable_media_url,
    is_probable_video_url,
    is_relevant_image_candidate,
    is_relevant_video_candidate,
    is_scoped_social_capture_domain,
    is_x_domain,
    is_x_hls_playlist_url,
    is_x_master_playlist_url,
    is_x_progressive_video_url,
    matches_configured_domain,
    merge_media_candidates,
    normalize_media_candidate_url,
    resolve_storage_state_path,
    score_instagram_video_candidate,
    score_video_candidate,
    score_x_video_candidate,
    should_scan_media_response_body,
    should_skip_image_url,
    should_skip_video_url,
    supports_video_capture,
)
from .ai_pipeline import (
    LOCAL_LLM_CATEGORIES,
    LOCAL_LLM_PROVIDERS,
    build_ai_source_text,
    build_local_llm_messages,
    run_ai_pipeline,
    run_local_llm_pipeline,
)

from .media_downloads import (
    MediaProbe,
    append_instagram_skip_log,
    build_instagram_audio_exploration_candidates,
    build_instagram_media_candidates,
    cleanup_capture_result,
    create_temp_download_path,
    delete_temp_file,
    download_image_assets,
    download_instagram_candidate,
    download_instagram_candidate_without_probe,
    download_instagram_video_assets,
    download_video_assets,
    durations_match,
    get_ffmpeg_executable,
    get_ffprobe_executable,
    merge_instagram_streams,
    pick_instagram_merge_pair,
    remux_x_hls_to_mp4,
    score_downloaded_media_candidate,
    summarize_instagram_video_result,
    validate_downloaded_video_file,
)
logger = logging.getLogger(__name__)


def should_use_playwright(resource: Resource, http_result: CaptureResult) -> bool:
    domain = resource.domain.lower()
    if matches_configured_domain(domain, settings.CAPTURE_JS_FALLBACK_DOMAINS):
        return True
    if http_result.error_message:
        return True
    if http_result.http_status and http_result.http_status >= 400:
        return True
    if len(http_result.extracted_text.strip()) < 240:
        return True
    return False


def fetch_with_http(
    url: str,
    *,
    capture_images: bool = True,
    capture_videos: bool = False,
    page_domain: str = "",
) -> CaptureResult:
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=settings.CAPTURE_HTTP_TIMEOUT,
            headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
        ) as client:
            response = client.get(url)
        html = response.text or ""
        resolved_domain = page_domain or urlparse(str(response.url)).netloc
        metadata = extract_metadata(html) if html else {}
        extracted_text = extract_text_from_html(html, str(response.url)) if html else ""
        captured_images = (
            download_image_assets(str(response.url), html, page_domain=resolved_domain)
            if capture_images and html
            else []
        )
        video_download = (
            download_video_assets(str(response.url), html, page_domain=resolved_domain)
            if capture_videos and html
            else DownloadedVideoAssets()
        )
        captured_videos = video_download.assets
        deleted_like = detect_deleted_like(
            extracted_text,
            metadata.get("page_title", ""),
            response.status_code,
        )
        response_payload = {
            "url": str(response.url),
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "capture_settings": {
                "images": capture_images,
                "videos": capture_videos,
            },
        }
        if capture_videos:
            response_payload["video_capture"] = {
                "candidate_urls": video_download.candidate_urls,
                "candidate_details": video_download.candidate_details,
                "attempts": video_download.attempts,
                "skip_logs": video_download.skip_logs,
                "summary": video_download.summary,
                "extraction_status": video_download.extraction_status,
                "extraction_strategy": video_download.extraction_strategy,
                "failure_reason": video_download.failure_reason,
                "selected_asset": video_download.selected_asset,
                "ffmpeg_available": bool(get_ffmpeg_executable()),
                "ffprobe_available": bool(get_ffprobe_executable()),
                "ffprobe_path": get_ffprobe_executable() or "",
            }
        return CaptureResult(
            fetch_url=str(response.url),
            fetch_method=FetchMethod.HTTP,
            http_status=response.status_code,
            html=html,
            extracted_text=extracted_text,
            metadata=metadata,
            response_payload=response_payload,
            captured_images=captured_images,
            captured_videos=captured_videos,
            deleted_like=deleted_like,
            error_message="" if response.status_code < 400 else f"HTTP {response.status_code}",
        )
    except Exception as exc:  # pragma: no cover
        return CaptureResult(fetch_url=url, fetch_method=FetchMethod.HTTP, error_message=str(exc))


def fetch_with_playwright(
    url: str,
    *,
    capture_images: bool = True,
    capture_videos: bool = False,
    page_domain: str = "",
) -> CaptureResult:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # pragma: no cover
        return CaptureResult(fetch_url=url, fetch_method=FetchMethod.PLAYWRIGHT, error_message=str(exc))

    try:
        with sync_playwright() as playwright:
            storage_state_path = get_playwright_storage_state_path(page_domain)
            profile_path = None if storage_state_path is not None else get_playwright_profile_path(page_domain)
            capture_domain = page_domain or urlparse(url).netloc
            viewport_defaults = {
                "width": settings.CAPTURE_VIEWPORT_WIDTH,
                "height": settings.CAPTURE_VIEWPORT_HEIGHT,
            }
            browser = None
            if profile_path is not None:
                context = playwright.chromium.launch_persistent_context(
                    str(profile_path),
                    headless=True,
                    viewport=viewport_defaults,
                )
            else:
                browser = playwright.chromium.launch(headless=True)
                context_kwargs = {"viewport": viewport_defaults}
                if storage_state_path is not None:
                    context_kwargs["storage_state"] = str(storage_state_path)
                context = browser.new_context(**context_kwargs)
            try:
                page = context.new_page()
                observed_video_urls: list[str] = []
                observed_media_requests: list[dict] = []
                observed_media_responses: list[dict] = []
                response_payload: dict = {
                    "storage_state_used": bool(storage_state_path),
                    "storage_state_path": str(storage_state_path) if storage_state_path else "",
                    "profile_path_used": str(profile_path) if profile_path else "",
                    "capture_settings": {
                        "images": capture_images,
                        "videos": capture_videos,
                    },
                }

                def remember_media_request(request):
                    raw_url = getattr(request, "url", "")
                    try:
                        resource_type = request.resource_type
                    except Exception:
                        resource_type = ""
                    if not is_observed_media_request(raw_url, capture_domain, resource_type=resource_type):
                        return
                    entry = {
                        "url": raw_url,
                        "resource_type": resource_type,
                        "method": getattr(request, "method", ""),
                    }
                    if entry not in observed_media_requests:
                        observed_media_requests.append(entry)

                def remember_media_response(response):
                    try:
                        request = response.request
                        resource_type = request.resource_type
                    except Exception:
                        resource_type = ""
                    raw_url = getattr(response, "url", "")
                    try:
                        content_type = response.headers.get("content-type", "")
                        content_length = response.headers.get("content-length", "")
                    except Exception:
                        content_type = ""
                        content_length = ""
                    response_status = getattr(response, "status", None)
                    if is_observed_media_response(
                        raw_url,
                        capture_domain,
                        content_type=content_type,
                        resource_type=resource_type,
                    ):
                        entry = {
                            "url": raw_url,
                            "resource_type": resource_type,
                            "content_type": content_type,
                            "content_length": content_length,
                            "response_status": response_status,
                        }
                        if entry not in observed_media_responses:
                            observed_media_responses.append(entry)
                        if classify_media_candidate_kind(
                            raw_url,
                            content_type=content_type,
                            resource_type=resource_type,
                        ) != "audio" and raw_url not in observed_video_urls:
                            observed_video_urls.append(raw_url)

                    if not capture_videos or not should_scan_media_response_body(
                        raw_url,
                        capture_domain,
                        content_type=content_type,
                        content_length=content_length,
                    ):
                        return
                    try:
                        response_text = response.text()
                    except Exception:
                        return
                    for candidate_url in extract_media_candidate_urls_from_text(
                        response_text,
                        raw_url,
                        page_domain=capture_domain,
                        include_audio=True,
                    ):
                        candidate_entry = {
                            "url": candidate_url,
                            "resource_type": "response_body",
                            "content_type": "",
                            "content_length": "",
                            "response_status": response_status,
                            "source_response_url": raw_url,
                        }
                        if candidate_entry not in observed_media_responses:
                            observed_media_responses.append(candidate_entry)
                        if (
                            classify_media_candidate_kind(candidate_url, resource_type="response_body") != "audio"
                            and candidate_url not in observed_video_urls
                        ):
                            observed_video_urls.append(candidate_url)

                page.on("request", remember_media_request)
                page.on("response", remember_media_response)
                response = page.goto(url, wait_until="domcontentloaded", timeout=settings.CAPTURE_PLAYWRIGHT_TIMEOUT_MS)
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass
                if capture_videos:
                    try:
                        page.locator("video").evaluate_all(
                            """
                            (elements) => {
                                elements.forEach((el) => {
                                    try {
                                        el.muted = true;
                                        el.preload = "auto";
                                        el.playsInline = true;
                                        const playResult = el.play && el.play();
                                        if (playResult && typeof playResult.catch === "function") {
                                            playResult.catch(() => {});
                                        }
                                    } catch (error) {}
                                });
                            }
                            """
                        )
                        page.wait_for_timeout(1500)
                    except Exception:
                        pass
                page.wait_for_timeout(3000)
                playwright_image_urls = collect_playwright_image_urls(page, page_domain=page_domain)
                playwright_media_candidates = collect_playwright_media_candidates(
                    page,
                    page_domain=page_domain,
                    response_entries=observed_media_responses,
                    request_entries=observed_media_requests,
                )
                playwright_video_urls = [
                    candidate["url"]
                    for candidate in playwright_media_candidates
                    if candidate.get("media_kind") != "audio"
                ] or collect_playwright_video_urls(
                    page,
                    page_domain=page_domain,
                    response_urls=observed_video_urls,
                )
                html = page.content()
                screenshot = page.screenshot(full_page=True, type="png")
                page_height = page.evaluate("() => document.documentElement.scrollHeight")
                viewport = page.viewport_size or viewport_defaults
                page_title = page.title()
                response_payload["observed_video_urls"] = observed_video_urls
                response_payload["observed_media_requests"] = observed_media_requests
                response_payload["observed_media_responses"] = observed_media_responses
            finally:
                context.close()
                if browser is not None:
                    browser.close()

        metadata = extract_metadata(html)
        if page_title and not metadata.get("page_title"):
            metadata["page_title"] = page_title
        extracted_text = extract_text_from_html(html, url) if html else ""
        final_url = response.url if response else url
        resolved_domain = page_domain or urlparse(final_url).netloc
        captured_images = (
            download_image_assets(final_url, html, extra_urls=playwright_image_urls, page_domain=resolved_domain)
            if capture_images and html
            else []
        )
        video_download = (
            download_video_assets(
                final_url,
                html,
                extra_urls=playwright_video_urls,
                page_domain=resolved_domain,
                extra_candidates=playwright_media_candidates,
            )
            if capture_videos and html
            else DownloadedVideoAssets()
        )
        captured_videos = video_download.assets
        status_code = response.status if response else None
        deleted_like = detect_deleted_like(extracted_text, metadata.get("page_title", ""), status_code)
        if capture_videos:
            response_payload["video_capture"] = {
                "observed_urls": observed_video_urls,
                "observed_media_requests": observed_media_requests,
                "observed_media_responses": observed_media_responses,
                "collected_urls": playwright_video_urls,
                "candidate_details": video_download.candidate_details,
                "candidate_urls": video_download.candidate_urls,
                "attempts": video_download.attempts,
                "skip_logs": video_download.skip_logs,
                "summary": video_download.summary,
                "extraction_status": video_download.extraction_status,
                "extraction_strategy": video_download.extraction_strategy,
                "failure_reason": video_download.failure_reason,
                "selected_asset": video_download.selected_asset,
                "ffmpeg_available": bool(get_ffmpeg_executable()),
                "ffprobe_available": bool(get_ffprobe_executable()),
                "ffprobe_path": get_ffprobe_executable() or "",
            }
        return CaptureResult(
            fetch_url=final_url,
            fetch_method=FetchMethod.PLAYWRIGHT,
            http_status=status_code,
            html=html,
            extracted_text=extracted_text,
            metadata=metadata,
            response_payload={
                "url": final_url,
                "status_code": status_code,
                "viewport": viewport,
                **response_payload,
            },
            screenshot_bytes=screenshot,
            screenshot_taken_at=timezone.now(),
            page_height=page_height,
            viewport_width=viewport["width"],
            viewport_height=viewport["height"],
            captured_images=captured_images,
            captured_videos=captured_videos,
            deleted_like=deleted_like,
            error_message="" if not status_code or status_code < 400 else f"HTTP {status_code}",
        )
    except Exception as exc:  # pragma: no cover
        return CaptureResult(fetch_url=url, fetch_method=FetchMethod.PLAYWRIGHT, error_message=str(exc))


def choose_capture_result(resource: Resource) -> CaptureResult:
    source_url = resource.normalized_url or resource.original_url
    capture_images = resource.capture_images
    supports_videos = supports_video_capture(resource.domain)
    requested_video_capture = resource.capture_videos and not resource.search_only
    if not resource.capture_videos and supports_videos:
        logger.warning(
            "Video capture is disabled by resource preference; skipping video download. "
            "resource_id=%s domain=%s url=%s",
            resource.pk,
            resource.domain,
            source_url,
        )
    capture_videos = requested_video_capture and supports_videos
    force_playwright = matches_configured_domain(resource.domain, settings.CAPTURE_JS_FALLBACK_DOMAINS)
    http_result = fetch_with_http(
        source_url,
        capture_images=capture_images,
        capture_videos=capture_videos and not force_playwright,
        page_domain=resource.domain,
    )
    if force_playwright or should_use_playwright(resource, http_result):
        playwright_result = fetch_with_playwright(
            source_url,
            capture_images=capture_images,
            capture_videos=capture_videos,
            page_domain=resource.domain,
        )
        if playwright_result.is_success:
            cleanup_capture_result(http_result)
            return playwright_result
        if http_result.html or http_result.http_status:
            cleanup_capture_result(playwright_result)
            return http_result
        return playwright_result
    return http_result


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


def execute_capture_job(job: CaptureJob) -> Snapshot:
    result = choose_capture_result(job.resource)
    try:
        with transaction.atomic():
            resource = Resource.objects.select_for_update().get(pk=job.resource_id)
            snapshot = persist_snapshot(resource, result)
            if result.fetch_url and result.fetch_url != resource.normalized_url and not snapshot.error_message:
                resource.normalized_url = normalize_url(result.fetch_url)
                resource.update_domain_from_url()
            resource.latest_snapshot = snapshot
            resource.current_status = status_from_snapshot(snapshot)
            resource.save(update_fields=["normalized_url", "domain", "latest_snapshot", "current_status", "updated_at"])
        if resource.current_status == ResourceStatus.FETCH_FAILED:
            raise RuntimeError(snapshot.error_message or "Capture failed.")
        if snapshot.is_success:
            enqueue_ai_job(snapshot.resource, snapshot)
        return snapshot
    finally:
        cleanup_capture_result(result)


def execute_ai_job(job: CaptureJob) -> Snapshot:
    snapshot = job.snapshot or job.resource.latest_snapshot
    if snapshot is None:
        raise ValueError("No snapshot available for AI enrichment.")
    ai_result = run_ai_pipeline(snapshot)
    snapshot.ai_summary = ai_result.summary
    snapshot.ai_translation = ai_result.translation
    snapshot.ai_category = ai_result.category
    snapshot.ai_payload = ai_result.payload
    snapshot.save(update_fields=["ai_summary", "ai_translation", "ai_category", "ai_payload"])
    return snapshot
