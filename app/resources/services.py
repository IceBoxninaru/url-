from __future__ import annotations

import hashlib
import html
import json
import mimetypes
import os
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import httpx
import trafilatura
from bs4 import BeautifulSoup
from django.conf import settings
from django.db import transaction
from django.db.models import Max
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from jobs.models import CaptureJob, JobStatus, JobType
from resources.models import LinkStatus, Resource, ResourceStatus
from snapshots.models import FetchMethod, Snapshot

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

STOP_WORDS = {
    "the",
    "this",
    "that",
    "with",
    "from",
    "have",
    "your",
    "about",
    "into",
    "https",
    "http",
    "www",
    "com",
    "net",
    "org",
}

VIDEO_EXTENSIONS = {".mp4", ".mov", ".webm", ".m4v"}


@dataclass
class CaptureResult:
    fetch_url: str
    fetch_method: str
    http_status: int | None = None
    html: str = ""
    extracted_text: str = ""
    metadata: dict = field(default_factory=dict)
    response_payload: dict = field(default_factory=dict)
    screenshot_bytes: bytes | None = None
    screenshot_taken_at: datetime | None = None
    page_height: int | None = None
    viewport_width: int | None = None
    viewport_height: int | None = None
    captured_images: list["CapturedImage"] = field(default_factory=list)
    captured_videos: list["CapturedVideo"] = field(default_factory=list)
    error_message: str = ""
    deleted_like: bool = False

    @property
    def is_success(self) -> bool:
        return bool(self.html or self.extracted_text) and not self.error_message and (
            self.http_status is None or self.http_status < 400
        )


@dataclass
class AIResult:
    summary: str
    category: str
    payload: dict


@dataclass
class LinkCheckResult:
    status: str
    http_status: int | None = None
    checked_url: str = ""
    error_message: str = ""


@dataclass
class CapturedImage:
    source_url: str
    content: bytes
    content_type: str = ""


@dataclass
class CapturedVideo:
    source_url: str
    temp_path: Path
    size_bytes: int
    content_type: str = ""


def normalize_url(raw_url: str) -> str:
    candidate = raw_url.strip()
    if not candidate:
        raise ValueError("URL is required.")
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Only http(s) URLs are supported.")

    netloc = parsed.netloc.lower()
    if parsed.port:
        is_default_port = (parsed.scheme == "http" and parsed.port == 80) or (
            parsed.scheme == "https" and parsed.port == 443
        )
        if is_default_port and parsed.hostname:
            netloc = parsed.hostname.lower()

    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/") or "/"

    query_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower().startswith("utm_"):
            continue
        query_pairs.append((key, value))

    return urlunparse(
        (
            parsed.scheme.lower(),
            netloc,
            path,
            "",
            urlencode(query_pairs, doseq=True),
            "",
        )
    )


def build_resource_directory(root: Path, resource_id: int) -> Path:
    return root / f"resource_{resource_id:04d}"


def write_storage_file(root: Path, resource_id: int, filename: str, content, binary: bool = False) -> str:
    resource_dir = build_resource_directory(root, resource_id)
    resource_dir.mkdir(parents=True, exist_ok=True)
    target = resource_dir / filename
    if binary:
        target.write_bytes(content)
    else:
        target.write_text(content, encoding="utf-8")
    return target.relative_to(settings.ROOT_DIR).as_posix()


def move_storage_file(root: Path, resource_id: int, filename: str, source_path: Path) -> str:
    resource_dir = build_resource_directory(root, resource_id)
    resource_dir.mkdir(parents=True, exist_ok=True)
    target = resource_dir / filename
    shutil.move(str(source_path), target)
    return target.relative_to(settings.ROOT_DIR).as_posix()


def collect_image_urls(html: str, source_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[str] = []

    def push(raw_url: str | None):
        if not raw_url:
            return
        raw_url = html_unescape_and_clean_url(raw_url)
        if not raw_url or raw_url.startswith("data:"):
            return
        absolute = urljoin(source_url, raw_url)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            return
        if should_skip_image_url(absolute):
            return
        if absolute not in candidates:
            candidates.append(absolute)

    for meta in soup.find_all("meta"):
        key = meta.get("property") or meta.get("name")
        if key in {"og:image", "twitter:image"}:
            push(meta.get("content"))

    for image in soup.find_all("img"):
        push(image.get("src"))
        push(image.get("data-src"))
        if image.get("srcset"):
            first_candidate = image["srcset"].split(",")[0].strip().split(" ")[0]
            push(first_candidate)

    return candidates


def collect_video_urls(html: str, source_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[str] = []

    def push(raw_url: str | None):
        if not raw_url:
            return
        raw_url = html_unescape_and_clean_url(raw_url)
        if not raw_url:
            return
        absolute = urljoin(source_url, raw_url)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            return
        if should_skip_video_url(absolute):
            return
        if absolute not in candidates:
            candidates.append(absolute)

    for meta in soup.find_all("meta"):
        key = (meta.get("property") or meta.get("name") or "").lower()
        if key in {
            "og:video",
            "og:video:url",
            "og:video:secure_url",
            "twitter:player:stream",
        }:
            push(meta.get("content"))

    for video in soup.find_all("video"):
        push(video.get("src"))
        for source in video.find_all("source"):
            push(source.get("src"))

    return candidates


def html_unescape_and_clean_url(raw_url: str) -> str:
    cleaned = html.unescape(raw_url).strip()
    cleaned = re.sub(r'["\')\];,]+$', "", cleaned)
    return cleaned


def should_skip_image_url(image_url: str) -> bool:
    lowered = image_url.lower()
    parsed = urlparse(lowered)
    if lowered.startswith("data:"):
        return True
    if "profile_images" in parsed.path or "profile_banners" in parsed.path:
        return True
    if "/emoji/" in parsed.path:
        return True
    if parsed.netloc.startswith("abs.twimg.com") and parsed.path.endswith("/og/image.png"):
        return True
    return False


def should_skip_video_url(video_url: str) -> bool:
    lowered = video_url.lower()
    parsed = urlparse(lowered)
    if lowered.startswith(("data:", "blob:")):
        return True
    if parsed.scheme not in {"http", "https"}:
        return True
    return False


def is_probable_video_url(video_url: str, content_type: str = "") -> bool:
    normalized_type = content_type.split(";")[0].strip().lower()
    if normalized_type.startswith("video/"):
        return True
    suffix = Path(urlparse(video_url).path).suffix.lower()
    return suffix in VIDEO_EXTENSIONS


def matches_configured_domain(domain: str, allowed_domains: list[str]) -> bool:
    normalized = domain.lower().strip()
    return any(
        normalized == allowed or normalized.endswith(f".{allowed}")
        for allowed in allowed_domains
    )


def supports_video_capture(domain: str) -> bool:
    return matches_configured_domain(domain, settings.CAPTURE_VIDEO_DOMAINS)


def collect_playwright_image_urls(page) -> list[str]:
    candidates: list[str] = []
    try:
        image_entries = page.locator("img").evaluate_all(
            """
            (elements) =>
                elements.map((el) => ({
                    src: el.currentSrc || el.src || "",
                    width: el.naturalWidth || 0,
                    height: el.naturalHeight || 0,
                    alt: el.alt || "",
                }))
            """
        )
    except Exception:
        return candidates

    for entry in image_entries:
        raw_url = html_unescape_and_clean_url(entry.get("src", ""))
        if not raw_url:
            continue
        if should_skip_image_url(raw_url):
            continue
        width = entry.get("width") or 0
        height = entry.get("height") or 0
        parsed = urlparse(raw_url)
        if "pbs.twimg.com/media/" in raw_url:
            if raw_url not in candidates:
                candidates.append(raw_url)
            continue
        if width < 120 or height < 120:
            continue
        if parsed.scheme in {"http", "https"} and raw_url not in candidates:
            candidates.append(raw_url)

    return candidates


def collect_playwright_video_urls(page, response_urls: list[str] | None = None) -> list[str]:
    candidates: list[str] = []
    try:
        video_entries = page.locator("video").evaluate_all(
            """
            (elements) =>
                elements.map((el) => ({
                    currentSrc: el.currentSrc || "",
                    src: el.src || "",
                    sources: Array.from(el.querySelectorAll("source")).map((source) => source.src || ""),
                }))
            """
        )
    except Exception:
        video_entries = []

    for entry in video_entries:
        for raw_url in [entry.get("currentSrc", ""), entry.get("src", ""), *entry.get("sources", [])]:
            normalized = html_unescape_and_clean_url(raw_url)
            if not normalized or should_skip_video_url(normalized):
                continue
            if normalized not in candidates:
                candidates.append(normalized)

    for raw_url in response_urls or []:
        normalized = html_unescape_and_clean_url(raw_url)
        if not normalized or should_skip_video_url(normalized):
            continue
        if normalized not in candidates:
            candidates.append(normalized)

    return candidates


def guess_image_extension(source_url: str, content_type: str) -> str:
    normalized_type = content_type.split(";")[0].strip().lower()
    extension = mimetypes.guess_extension(normalized_type) if normalized_type else ""
    if extension == ".jpe":
        extension = ".jpg"
    if extension:
        return extension

    suffix = Path(urlparse(source_url).path).suffix.lower()
    if suffix:
        return suffix
    return ".img"


def guess_video_extension(source_url: str, content_type: str) -> str:
    normalized_type = content_type.split(";")[0].strip().lower()
    extension = mimetypes.guess_extension(normalized_type) if normalized_type else ""
    if extension == ".qt":
        extension = ".mov"
    if extension:
        return extension

    suffix = Path(urlparse(source_url).path).suffix.lower()
    if suffix in VIDEO_EXTENSIONS:
        return suffix
    return ".mp4"


def create_temp_download_path(extension: str) -> Path:
    suffix = extension if extension.startswith(".") else f".{extension}"
    fd, raw_path = tempfile.mkstemp(prefix="url-archive-", suffix=suffix)
    os.close(fd)
    return Path(raw_path)


def delete_temp_file(path: Path | None) -> None:
    if not path:
        return
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def cleanup_capture_result(result: CaptureResult) -> None:
    for video in result.captured_videos:
        delete_temp_file(video.temp_path)


def download_image_assets(source_url: str, html: str, extra_urls: list[str] | None = None) -> list[CapturedImage]:
    image_urls = collect_image_urls(html, source_url)
    for extra_url in extra_urls or []:
        normalized = html_unescape_and_clean_url(extra_url)
        if not normalized:
            continue
        absolute = urljoin(source_url, normalized)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        if should_skip_image_url(absolute):
            continue
        if absolute not in image_urls:
            image_urls.append(absolute)
    if not image_urls:
        return []

    captured: list[CapturedImage] = []
    with httpx.Client(
        follow_redirects=True,
        timeout=settings.CAPTURE_HTTP_TIMEOUT,
        headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
    ) as client:
        for image_url in image_urls:
            if len(captured) >= settings.CAPTURE_MAX_IMAGES:
                break
            try:
                with client.stream("GET", image_url) as response:
                    if response.status_code >= 400:
                        continue
                    content_type = response.headers.get("content-type", "")
                    if not content_type.lower().startswith("image/"):
                        continue

                    buffer = bytearray()
                    too_large = False
                    for chunk in response.iter_bytes():
                        buffer.extend(chunk)
                        if len(buffer) > settings.CAPTURE_MAX_IMAGE_BYTES:
                            too_large = True
                            break
                    if too_large or not buffer:
                        continue

                    captured.append(
                        CapturedImage(
                            source_url=str(response.url),
                            content=bytes(buffer),
                            content_type=content_type,
                        )
                    )
            except Exception:
                continue
    return captured


def download_video_assets(source_url: str, html: str, extra_urls: list[str] | None = None) -> list[CapturedVideo]:
    video_urls = collect_video_urls(html, source_url)
    for extra_url in extra_urls or []:
        normalized = html_unescape_and_clean_url(extra_url)
        if not normalized:
            continue
        absolute = urljoin(source_url, normalized)
        if should_skip_video_url(absolute):
            continue
        if absolute not in video_urls:
            video_urls.append(absolute)
    if not video_urls:
        return []

    captured: list[CapturedVideo] = []
    with httpx.Client(
        follow_redirects=True,
        timeout=settings.CAPTURE_HTTP_TIMEOUT,
        headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
    ) as client:
        for video_url in video_urls:
            if len(captured) >= settings.CAPTURE_MAX_VIDEOS:
                break
            temp_path: Path | None = None
            try:
                with client.stream("GET", video_url) as response:
                    if response.status_code >= 400:
                        continue
                    content_type = response.headers.get("content-type", "")
                    if not is_probable_video_url(str(response.url), content_type):
                        continue

                    temp_path = create_temp_download_path(guess_video_extension(str(response.url), content_type))
                    size_bytes = 0
                    too_large = False
                    with temp_path.open("wb") as handle:
                        for chunk in response.iter_bytes():
                            if not chunk:
                                continue
                            size_bytes += len(chunk)
                            if size_bytes > settings.CAPTURE_MAX_VIDEO_BYTES:
                                too_large = True
                                break
                            handle.write(chunk)
                    if too_large or size_bytes == 0:
                        delete_temp_file(temp_path)
                        continue

                    captured.append(
                        CapturedVideo(
                            source_url=str(response.url),
                            temp_path=temp_path,
                            size_bytes=size_bytes,
                            content_type=content_type,
                        )
                    )
            except Exception:
                delete_temp_file(temp_path)
                continue
    return captured


def extract_text_from_html(html: str, source_url: str) -> str:
    extracted = trafilatura.extract(
        html,
        url=source_url,
        include_comments=False,
        include_images=False,
        include_formatting=False,
        favor_precision=True,
    )
    if extracted:
        return extracted.strip()

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    return re.sub(r"\n{3,}", "\n\n", text)


def extract_metadata(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    def meta_value(*keys: str) -> str:
        for key in keys:
            tag = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
            if tag and tag.get("content"):
                return tag["content"].strip()
        return ""

    published_raw = meta_value(
        "article:published_time",
        "og:published_time",
        "published_time",
        "datePublished",
    )
    published_at = parse_datetime(published_raw) if published_raw else None
    return {
        "page_title": (soup.title.string.strip() if soup.title and soup.title.string else ""),
        "site_name": meta_value("og:site_name", "application-name"),
        "author": meta_value("author", "article:author"),
        "published_at": published_at,
        "og_title": meta_value("og:title"),
        "og_description": meta_value("og:description", "description"),
        "og_image_url": meta_value("og:image"),
    }


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


def fetch_with_http(url: str, *, capture_videos: bool = False) -> CaptureResult:
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=settings.CAPTURE_HTTP_TIMEOUT,
            headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
        ) as client:
            response = client.get(url)
        html = response.text or ""
        metadata = extract_metadata(html) if html else {}
        extracted_text = extract_text_from_html(html, str(response.url)) if html else ""
        captured_images = download_image_assets(str(response.url), html) if html else []
        captured_videos = download_video_assets(str(response.url), html) if capture_videos and html else []
        deleted_like = detect_deleted_like(
            extracted_text,
            metadata.get("page_title", ""),
            response.status_code,
        )
        return CaptureResult(
            fetch_url=str(response.url),
            fetch_method=FetchMethod.HTTP,
            http_status=response.status_code,
            html=html,
            extracted_text=extracted_text,
            metadata=metadata,
            response_payload={
                "url": str(response.url),
                "status_code": response.status_code,
                "headers": dict(response.headers),
            },
            captured_images=captured_images,
            captured_videos=captured_videos,
            deleted_like=deleted_like,
            error_message="" if response.status_code < 400 else f"HTTP {response.status_code}",
        )
    except Exception as exc:  # pragma: no cover
        return CaptureResult(fetch_url=url, fetch_method=FetchMethod.HTTP, error_message=str(exc))


def fetch_with_playwright(url: str, *, capture_videos: bool = False) -> CaptureResult:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # pragma: no cover
        return CaptureResult(fetch_url=url, fetch_method=FetchMethod.PLAYWRIGHT, error_message=str(exc))

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(
                viewport={
                    "width": settings.CAPTURE_VIEWPORT_WIDTH,
                    "height": settings.CAPTURE_VIEWPORT_HEIGHT,
                }
            )
            observed_video_urls: list[str] = []

            def remember_video_response(response):
                try:
                    request = response.request
                    resource_type = request.resource_type
                except Exception:
                    resource_type = ""
                raw_url = getattr(response, "url", "")
                if should_skip_video_url(raw_url):
                    return
                if resource_type == "media" or is_probable_video_url(raw_url):
                    if raw_url not in observed_video_urls:
                        observed_video_urls.append(raw_url)

            page.on("response", remember_video_response)
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
            playwright_image_urls = collect_playwright_image_urls(page)
            playwright_video_urls = collect_playwright_video_urls(page, response_urls=observed_video_urls)
            html = page.content()
            screenshot = page.screenshot(full_page=True, type="png")
            page_height = page.evaluate("() => document.documentElement.scrollHeight")
            viewport = page.viewport_size or {
                "width": settings.CAPTURE_VIEWPORT_WIDTH,
                "height": settings.CAPTURE_VIEWPORT_HEIGHT,
            }
            page_title = page.title()
            browser.close()

        metadata = extract_metadata(html)
        if page_title and not metadata.get("page_title"):
            metadata["page_title"] = page_title
        extracted_text = extract_text_from_html(html, url) if html else ""
        final_url = response.url if response else url
        captured_images = download_image_assets(final_url, html, extra_urls=playwright_image_urls) if html else []
        captured_videos = (
            download_video_assets(final_url, html, extra_urls=playwright_video_urls)
            if capture_videos and html
            else []
        )
        status_code = response.status if response else None
        deleted_like = detect_deleted_like(extracted_text, metadata.get("page_title", ""), status_code)
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
    capture_videos = supports_video_capture(resource.domain)
    force_playwright = matches_configured_domain(resource.domain, settings.CAPTURE_JS_FALLBACK_DOMAINS)
    http_result = fetch_with_http(source_url, capture_videos=capture_videos and not force_playwright)
    if force_playwright or should_use_playwright(resource, http_result):
        playwright_result = fetch_with_playwright(source_url, capture_videos=capture_videos)
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


def status_from_snapshot(snapshot: Snapshot) -> str:
    if snapshot.http_status in {404, 410}:
        return ResourceStatus.GONE
    if snapshot.is_deleted_like:
        return ResourceStatus.MAYBE_DELETED
    if snapshot.error_message or (snapshot.http_status and snapshot.http_status >= 400):
        return ResourceStatus.FETCH_FAILED
    return ResourceStatus.ACTIVE


def enqueue_capture_job(resource: Resource, priority: int = 100) -> CaptureJob:
    return CaptureJob.objects.create(
        owner=resource.owner,
        resource=resource,
        job_type=JobType.CAPTURE,
        status=JobStatus.QUEUED,
        priority=priority,
        scheduled_at=timezone.now(),
    )


def enqueue_ai_job(resource: Resource, snapshot: Snapshot, priority: int = 50) -> CaptureJob:
    return CaptureJob.objects.create(
        owner=resource.owner,
        resource=resource,
        snapshot=snapshot,
        job_type=JobType.AI_ENRICH,
        status=JobStatus.QUEUED,
        priority=priority,
        scheduled_at=timezone.now(),
    )


def summarize_text(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized:
        return ""
    limit = settings.AI_SUMMARY_MAX_CHARS
    if len(normalized) <= limit:
        return normalized
    truncated = normalized[: limit - 1].rstrip(" ,.;:")
    return f"{truncated}…"


def infer_category(snapshot: Snapshot) -> str:
    combined = f"{snapshot.page_title} {snapshot.site_name} {snapshot.og_description} {snapshot.extracted_text[:1200]}".lower()
    category_rules = {
        "social": ["tweet", "post", "instagram", "thread", "social"],
        "shopping": ["cart", "price", "shop", "buy", "product"],
        "documentation": ["docs", "reference", "api", "guide"],
        "news": ["news", "breaking", "press", "report"],
        "video": ["video", "watch", "stream", "episode"],
    }
    for category, markers in category_rules.items():
        if any(marker in combined for marker in markers):
            return category
    return "general"


def suggest_tags(snapshot: Snapshot) -> list[str]:
    source = f"{snapshot.page_title} {snapshot.og_description} {snapshot.site_name} {snapshot.extracted_text[:1000]}".lower()
    tokens = re.findall(r"[a-z0-9][a-z0-9_-]{2,}", source)
    ranked: list[str] = []
    for token in tokens:
        if token in STOP_WORDS:
            continue
        if token not in ranked:
            ranked.append(token)
        if len(ranked) == 5:
            break
    category = infer_category(snapshot)
    if category != "general" and category not in ranked:
        ranked.insert(0, category)
    return ranked[:5]


def similar_resource_ids(resource: Resource) -> list[int]:
    queryset = Resource.objects.exclude(pk=resource.pk)
    if resource.domain:
        queryset = queryset.filter(domain=resource.domain)
    return list(queryset.order_by("-updated_at").values_list("id", flat=True)[:5])


def run_ai_pipeline(snapshot: Snapshot) -> AIResult:
    provider = settings.AI_PROVIDER.lower()
    if provider == "noop":
        return AIResult(summary="", category="", payload={"provider": "noop", "tag_candidates": [], "similar_resource_ids": []})

    summary = summarize_text(snapshot.extracted_text or snapshot.og_description or snapshot.page_title)
    category = infer_category(snapshot)
    payload = {
        "provider": provider,
        "tag_candidates": suggest_tags(snapshot),
        "similar_resource_ids": similar_resource_ids(snapshot.resource),
    }
    return AIResult(summary=summary, category=category, payload=payload)


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
    snapshot.ai_category = ai_result.category
    snapshot.ai_payload = ai_result.payload
    snapshot.save(update_fields=["ai_summary", "ai_category", "ai_payload"])
    return snapshot


def delete_resource_with_artifacts(resource: Resource) -> None:
    resource_id = resource.id
    resource.delete()
    for root in (
        settings.HTML_STORAGE_ROOT,
        settings.TEXT_STORAGE_ROOT,
        settings.JSON_STORAGE_ROOT,
        settings.SCREENSHOT_STORAGE_ROOT,
        settings.IMAGE_STORAGE_ROOT,
        settings.VIDEO_STORAGE_ROOT,
    ):
        target = build_resource_directory(root, resource_id)
        if target.exists():
            target = target.resolve()
            root_path = Path(root).resolve()
            if root_path in target.parents:
                shutil.rmtree(target)
