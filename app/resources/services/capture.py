from __future__ import annotations

import logging
from urllib.parse import urlparse

import httpx
from django.conf import settings
from django.utils import timezone

from resources.models import Resource
from snapshots.models import FetchMethod

from .content import extract_metadata, extract_text_from_html
from .link_checks import detect_deleted_like
from .media_discovery import (
    classify_media_candidate_kind,
    collect_playwright_image_urls,
    collect_playwright_media_candidates,
    collect_playwright_video_urls,
    extract_media_candidate_urls_from_text,
    get_playwright_profile_path,
    get_playwright_storage_state_path,
    is_observed_media_request,
    is_observed_media_response,
    matches_configured_domain,
    should_scan_media_response_body,
    supports_video_capture,
)
from .media_downloads import (
    cleanup_capture_result,
    download_image_assets,
    download_video_assets,
    get_ffmpeg_executable,
    get_ffprobe_executable,
)
from .types import CaptureResult, DownloadedVideoAssets

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

