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

logger = logging.getLogger(__name__)

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
AUDIO_EXTENSIONS = {".aac", ".m4a", ".mp3", ".ogg", ".oga", ".wav"}
TRANSLATION_MAX_SOURCE_CHARS = 1600
TRANSLATION_MAX_CHUNK_CHARS = 400
TRANSLATION_ENDPOINT = "https://translate.googleapis.com/translate_a/single"


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
    metadata: dict = field(default_factory=dict)


@dataclass
class DownloadedVideoAssets:
    assets: list["CapturedVideo"] = field(default_factory=list)
    candidate_urls: list[str] = field(default_factory=list)
    candidate_details: list[dict] = field(default_factory=list)
    attempts: list[dict] = field(default_factory=list)
    skip_logs: list[dict] = field(default_factory=list)
    summary: dict = field(default_factory=dict)
    extraction_status: str = "not_attempted"
    extraction_strategy: str = ""
    failure_reason: str = ""
    selected_asset: dict = field(default_factory=dict)


@dataclass
class MediaProbeResult:
    has_video: bool = False
    has_audio: bool = False
    duration_sec: float | None = None
    video_streams: int = 0
    audio_streams: int = 0
    format_name: str = ""
    probe_tool: str = ""
    failure_reason: str = ""
    raw: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "has_video": self.has_video,
            "has_audio": self.has_audio,
            "duration_sec": self.duration_sec,
            "video_streams": self.video_streams,
            "audio_streams": self.audio_streams,
            "format_name": self.format_name,
            "probe_tool": self.probe_tool,
            "failure_reason": self.failure_reason,
        }


@dataclass
class DownloadedMediaCandidate:
    candidate: dict
    source_url: str
    temp_path: Path
    size_bytes: int
    content_type: str = ""
    probe: MediaProbeResult = field(default_factory=MediaProbeResult)


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


def resolve_storage_file_path(raw_path: str) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = settings.ROOT_DIR / candidate
    return candidate


def filter_existing_snapshot_assets(assets: list[dict] | None) -> list[dict]:
    existing_assets: list[dict] = []
    for asset in assets or []:
        path = str(asset.get("path", "")).strip()
        if not path:
            continue
        file_path = resolve_storage_file_path(path)
        if file_path.exists() and file_path.is_file():
            existing_assets.append(asset)
    return existing_assets


def get_capture_files(snapshot: Snapshot | None) -> tuple[list[dict], list[dict]]:
    if snapshot is None:
        return [], []
    return (
        filter_existing_snapshot_assets(snapshot.image_assets),
        filter_existing_snapshot_assets(snapshot.video_assets),
    )


def sync_capture_flags(resource: Resource) -> tuple[list[dict], list[dict]]:
    image_files, video_files = get_capture_files(resource.latest_snapshot)
    resource.capture_images = bool(image_files)
    resource.capture_videos = bool(video_files)
    resource.save(update_fields=["capture_images", "capture_videos", "updated_at"])
    return image_files, video_files


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


def is_x_domain(domain: str) -> bool:
    return matches_configured_domain(domain, ["x.com", "twitter.com"])


def is_instagram_domain(domain: str) -> bool:
    return matches_configured_domain(domain, ["instagram.com"])


def is_scoped_social_capture_domain(domain: str) -> bool:
    return is_x_domain(domain) or is_instagram_domain(domain)


def resolve_storage_state_path(raw_path: str) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = settings.ROOT_DIR / candidate
    return candidate


def get_playwright_storage_state_path(page_domain: str) -> Path | None:
    if not is_x_domain(page_domain):
        return None
    raw_path = getattr(settings, "CAPTURE_X_STORAGE_STATE_PATH", "").strip()
    if not raw_path:
        return None
    candidate = resolve_storage_state_path(raw_path)
    if not candidate.exists() or not candidate.is_file():
        return None
    return candidate


def normalize_media_candidate_url(raw_url: str, source_url: str) -> str:
    cleaned = html_unescape_and_clean_url(raw_url)
    if not cleaned or cleaned.startswith(("data:", "blob:")):
        return ""
    absolute = urljoin(source_url, cleaned)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return ""
    query_pairs = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in {"bytestart", "byteend", "range"}
    ]
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            "",
            urlencode(query_pairs, doseq=True),
            "",
        )
    )


def is_relevant_image_candidate(image_url: str, page_domain: str) -> bool:
    parsed = urlparse(image_url.lower())
    if is_x_domain(page_domain):
        if not parsed.netloc.endswith(("twimg.com", "twitter.com", "x.com")):
            return False
        return any(
            marker in parsed.path
            for marker in ("/media/", "/ext_tw_video_thumb/", "/amplify_video_thumb/")
        )
    if is_instagram_domain(page_domain):
        if "cdninstagram.com" in parsed.netloc or parsed.netloc.endswith("fbcdn.net"):
            return not parsed.path.startswith("/rsrc.php")
        return False
    return True


def is_relevant_video_candidate(video_url: str, page_domain: str) -> bool:
    parsed = urlparse(video_url.lower())
    if is_x_domain(page_domain):
        if not parsed.netloc.endswith("video.twimg.com"):
            return False
        return is_x_hls_playlist_url(video_url) or is_x_progressive_video_url(video_url)
    if not is_probable_video_url(video_url):
        return False
    if is_instagram_domain(page_domain):
        return (
            ("cdninstagram.com" in parsed.netloc or parsed.netloc.endswith("fbcdn.net"))
            and parsed.path.endswith(".mp4")
        )
    return True


def is_probable_audio_url(audio_url: str, content_type: str = "") -> bool:
    normalized_type = content_type.split(";")[0].strip().lower()
    if normalized_type.startswith("audio/"):
        return True
    suffix = Path(urlparse(audio_url).path).suffix.lower()
    return suffix in AUDIO_EXTENSIONS


def is_probable_media_url(media_url: str, content_type: str = "") -> bool:
    return is_probable_video_url(media_url, content_type) or is_probable_audio_url(media_url, content_type)


def score_video_candidate(video_url: str) -> int:
    match = re.search(r"/(\d+)x(\d+)/", urlparse(video_url).path)
    if not match:
        return 0
    return int(match.group(1)) * int(match.group(2))


def is_x_hls_playlist_url(video_url: str) -> bool:
    parsed = urlparse(video_url.lower())
    return parsed.netloc.endswith("video.twimg.com") and parsed.path.endswith(".m3u8") and "/pl/" in parsed.path


def is_x_master_playlist_url(video_url: str) -> bool:
    if not is_x_hls_playlist_url(video_url):
        return False
    parsed = urlparse(video_url.lower())
    return "variant_version=" in parsed.query or "/pl/avc1/" not in parsed.path


def is_x_progressive_video_url(video_url: str) -> bool:
    parsed = urlparse(video_url.lower())
    if not (parsed.netloc.endswith("video.twimg.com") and parsed.path.endswith(".mp4")):
        return False
    if "/aud/" in parsed.path:
        return False
    if "/0/0/" in parsed.path:
        return False
    return "/vid/" in parsed.path


def score_x_video_candidate(video_url: str) -> int:
    if is_x_master_playlist_url(video_url):
        return 3_000_000_000 + score_video_candidate(video_url)
    if is_x_hls_playlist_url(video_url):
        return 2_000_000_000 + score_video_candidate(video_url)
    if is_x_progressive_video_url(video_url):
        return 1_000_000_000 + score_video_candidate(video_url)
    return 0


def score_instagram_video_candidate(video_url: str) -> int:
    parsed = urlparse(video_url.lower())
    score = score_video_candidate(video_url)
    if parsed.path.endswith(".mp4"):
        score += 10_000_000
    if "cdninstagram.com" in parsed.netloc:
        score += 1_000_000
    return score


def dedupe_urls(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def filter_image_candidate_urls(urls: list[str], page_domain: str) -> list[str]:
    filtered = [url for url in dedupe_urls(urls) if not should_skip_image_url(url)]
    if is_scoped_social_capture_domain(page_domain):
        filtered = [url for url in filtered if is_relevant_image_candidate(url, page_domain)]
    return filtered


def filter_video_candidate_urls(urls: list[str], page_domain: str) -> list[str]:
    filtered = [url for url in dedupe_urls(urls) if not should_skip_video_url(url)]
    filtered = [url for url in filtered if is_relevant_video_candidate(url, page_domain)]
    if is_x_domain(page_domain):
        return sorted(filtered, key=score_x_video_candidate, reverse=True)
    if is_instagram_domain(page_domain):
        return sorted(filtered, key=score_instagram_video_candidate, reverse=True)
    return filtered


def get_playwright_media_scope(page, page_domain: str):
    selectors: list[str] = []
    if is_x_domain(page_domain):
        selectors = ["article[data-testid='tweet']"]
    elif is_instagram_domain(page_domain):
        selectors = ["main article"]
    for selector in selectors:
        try:
            locator = page.locator(selector)
            if locator.count():
                return locator.first
        except Exception:
            continue
    return None


def collect_image_urls(html: str, source_url: str, page_domain: str = "") -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[str] = []

    def push(raw_url: str | None):
        if not raw_url:
            return
        absolute = normalize_media_candidate_url(raw_url, source_url)
        if not absolute:
            return
        if absolute not in candidates:
            candidates.append(absolute)

    for meta in soup.find_all("meta"):
        key = meta.get("property") or meta.get("name")
        if key in {"og:image", "twitter:image"}:
            push(meta.get("content"))

    for video in soup.find_all("video"):
        push(video.get("poster"))

    if not is_scoped_social_capture_domain(page_domain):
        for image in soup.find_all("img"):
            push(image.get("src"))
            push(image.get("data-src"))
            if image.get("srcset"):
                first_candidate = image["srcset"].split(",")[0].strip().split(" ")[0]
                push(first_candidate)

    return filter_image_candidate_urls(candidates, page_domain)


def collect_video_urls(html: str, source_url: str, page_domain: str = "") -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[str] = []

    def push(raw_url: str | None):
        if not raw_url:
            return
        absolute = normalize_media_candidate_url(raw_url, source_url)
        if not absolute:
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

    return filter_video_candidate_urls(candidates, page_domain)


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


def is_observed_video_response(
    video_url: str,
    page_domain: str,
    *,
    content_type: str = "",
    resource_type: str = "",
) -> bool:
    if should_skip_video_url(video_url):
        return False
    if is_x_domain(page_domain):
        return is_x_hls_playlist_url(video_url) or is_x_progressive_video_url(video_url)
    if resource_type == "media":
        return True
    normalized_type = content_type.split(";")[0].strip().lower()
    if normalized_type in {"application/x-mpegurl", "application/vnd.apple.mpegurl"}:
        return True
    return is_probable_video_url(video_url, content_type)


def is_observed_media_response(
    media_url: str,
    page_domain: str,
    *,
    content_type: str = "",
    resource_type: str = "",
) -> bool:
    if should_skip_video_url(media_url):
        return False
    if is_x_domain(page_domain):
        return is_observed_video_response(
            media_url,
            page_domain,
            content_type=content_type,
            resource_type=resource_type,
        )
    if resource_type == "media":
        return True
    normalized_type = content_type.split(";")[0].strip().lower()
    if normalized_type in {"application/x-mpegurl", "application/vnd.apple.mpegurl"}:
        return True
    return is_probable_media_url(media_url, content_type)


def is_observed_media_request(media_url: str, page_domain: str, *, resource_type: str = "") -> bool:
    if should_skip_video_url(media_url):
        return False
    if is_x_domain(page_domain):
        return is_observed_video_response(media_url, page_domain, resource_type=resource_type)
    if resource_type == "media":
        return True
    return is_probable_media_url(media_url)


def classify_media_candidate_kind(media_url: str, *, content_type: str = "", resource_type: str = "") -> str:
    normalized_type = content_type.split(";")[0].strip().lower()
    if normalized_type.startswith("audio/"):
        return "audio"
    if normalized_type.startswith("video/"):
        return "video"
    if normalized_type in {"application/x-mpegurl", "application/vnd.apple.mpegurl"}:
        return "video"
    if resource_type == "media" and is_probable_audio_url(media_url):
        return "audio"
    if is_probable_video_url(media_url):
        return "video"
    if is_probable_audio_url(media_url):
        return "audio"
    return "unknown"


def explain_media_candidate_skip(
    raw_url: str,
    source_url: str,
    *,
    page_domain: str,
    content_type: str = "",
    resource_type: str = "",
) -> str:
    cleaned = html_unescape_and_clean_url(raw_url)
    if not cleaned:
        return "empty_url"
    if cleaned.startswith(("data:", "blob:")):
        return "unsupported_inline_url"
    normalized = normalize_media_candidate_url(raw_url, source_url)
    if not normalized:
        return "normalize_failed"
    if should_skip_video_url(normalized):
        return "unsupported_url_scheme"
    if not is_scoped_social_capture_domain(page_domain):
        return ""
    if is_relevant_video_candidate(normalized, page_domain):
        return ""
    if is_instagram_domain(page_domain):
        if resource_type == "media":
            return ""
        if is_probable_audio_url(normalized, content_type):
            return ""
        return "not_instagram_media_candidate"
    return "not_social_media_candidate"


def build_media_candidate(
    raw_url: str,
    source_url: str,
    *,
    source: str,
    page_domain: str,
    content_type: str = "",
    content_length: str = "",
    resource_type: str = "",
    response_status: int | None = None,
) -> dict | None:
    skip_reason = explain_media_candidate_skip(
        raw_url,
        source_url,
        page_domain=page_domain,
        content_type=content_type,
        resource_type=resource_type,
    )
    if skip_reason:
        return None
    normalized = normalize_media_candidate_url(raw_url, source_url)
    if is_scoped_social_capture_domain(page_domain) and not is_relevant_video_candidate(normalized, page_domain):
        is_instagram_audio_candidate = is_instagram_domain(page_domain) and (
            resource_type == "media" or is_probable_audio_url(normalized, content_type)
        )
        if not is_instagram_audio_candidate:
            return None
    return {
        "url": normalized,
        "source": source,
        "media_kind": classify_media_candidate_kind(
            normalized,
            content_type=content_type,
            resource_type=resource_type,
        ),
        "content_type": content_type,
        "content_length": content_length,
        "resource_type": resource_type,
        "response_status": response_status,
    }


def merge_media_candidates(candidates: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for candidate in candidates:
        url = candidate.get("url", "")
        if not url:
            continue
        current = merged.get(url)
        if current is None:
            merged[url] = {
                **candidate,
                "sources": [candidate.get("source")] if candidate.get("source") else [],
            }
            continue
        source = candidate.get("source")
        if source and source not in current["sources"]:
            current["sources"].append(source)
        for key in ("content_type", "content_length", "resource_type", "response_status"):
            if not current.get(key) and candidate.get(key):
                current[key] = candidate[key]
        if current.get("media_kind") in {"", "unknown"} and candidate.get("media_kind") not in {"", "unknown"}:
            current["media_kind"] = candidate["media_kind"]
    return list(merged.values())


def matches_configured_domain(domain: str, allowed_domains: list[str]) -> bool:
    normalized = domain.lower().strip()
    return any(
        normalized == allowed or normalized.endswith(f".{allowed}")
        for allowed in allowed_domains
    )


def supports_video_capture(domain: str) -> bool:
    return matches_configured_domain(domain, settings.CAPTURE_VIDEO_DOMAINS)


def collect_playwright_image_urls(page, page_domain: str = "") -> list[str]:
    candidates: list[str] = []
    scope = get_playwright_media_scope(page, page_domain)
    if scope is not None:
        image_locator = scope.locator("img")
        video_locator = scope.locator("video")
    elif is_scoped_social_capture_domain(page_domain):
        return candidates
    else:
        image_locator = page.locator("img")
        video_locator = page.locator("video")
    try:
        image_entries = image_locator.evaluate_all(
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
        image_entries = []

    try:
        poster_entries = video_locator.evaluate_all(
            """
            (elements) =>
                elements.map((el) => ({
                    poster: el.poster || "",
                }))
            """
        )
    except Exception:
        poster_entries = []

    for entry in image_entries:
        raw_url = normalize_media_candidate_url(entry.get("src", ""), page.url)
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

    for entry in poster_entries:
        poster_url = normalize_media_candidate_url(entry.get("poster", ""), page.url)
        if poster_url and poster_url not in candidates:
            candidates.append(poster_url)

    return filter_image_candidate_urls(candidates, page_domain)


def collect_playwright_video_urls(page, page_domain: str = "", response_urls: list[str] | None = None) -> list[str]:
    candidates: list[str] = []
    scope = get_playwright_media_scope(page, page_domain)
    if scope is not None:
        video_locator = scope.locator("video")
    elif is_scoped_social_capture_domain(page_domain):
        video_locator = None
    else:
        video_locator = page.locator("video")
    try:
        video_entries = video_locator.evaluate_all(
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
            normalized = normalize_media_candidate_url(raw_url, page.url)
            if not normalized or should_skip_video_url(normalized):
                continue
            if normalized not in candidates:
                candidates.append(normalized)

    for raw_url in response_urls or []:
        normalized = normalize_media_candidate_url(raw_url, page.url)
        if not normalized or should_skip_video_url(normalized):
            continue
        if normalized not in candidates:
            candidates.append(normalized)

    return filter_video_candidate_urls(candidates, page_domain)


def collect_playwright_media_candidates(
    page,
    page_domain: str = "",
    response_entries: list[dict] | None = None,
    request_entries: list[dict] | None = None,
) -> list[dict]:
    candidates: list[dict] = []
    scope = get_playwright_media_scope(page, page_domain)
    if scope is not None:
        video_locator = scope.locator("video")
    elif is_scoped_social_capture_domain(page_domain):
        video_locator = None
    else:
        video_locator = page.locator("video")
    try:
        video_entries = video_locator.evaluate_all(
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
            candidate = build_media_candidate(
                raw_url,
                page.url,
                source="dom_video",
                page_domain=page_domain,
            )
            if candidate is not None:
                candidates.append(candidate)

    for entry in request_entries or []:
        candidate = build_media_candidate(
            entry.get("url", ""),
            page.url,
            source="network_request",
            page_domain=page_domain,
            resource_type=entry.get("resource_type", ""),
        )
        if candidate is not None:
            candidates.append(candidate)

    for entry in response_entries or []:
        candidate = build_media_candidate(
            entry.get("url", ""),
            page.url,
            source="network_response",
            page_domain=page_domain,
            content_type=entry.get("content_type", ""),
            content_length=entry.get("content_length", ""),
            resource_type=entry.get("resource_type", ""),
            response_status=entry.get("response_status"),
        )
        if candidate is not None:
            candidates.append(candidate)

    return merge_media_candidates(candidates)


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


def get_ffmpeg_executable() -> str | None:
    try:
        import imageio_ffmpeg
    except Exception:
        return None
    try:
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return None


def get_ffprobe_executable() -> str | None:
    configured = getattr(settings, "CAPTURE_FFPROBE_PATH", "").strip()
    if configured:
        candidate = Path(configured)
        if not candidate.is_absolute():
            candidate = settings.ROOT_DIR / candidate
        if candidate.exists() and candidate.is_file():
            return str(candidate)
    ffprobe = shutil.which("ffprobe")
    if ffprobe:
        return ffprobe
    ffmpeg = get_ffmpeg_executable()
    if not ffmpeg:
        return None
    probe_name = "ffprobe.exe" if ffmpeg.lower().endswith(".exe") else "ffprobe"
    sibling = Path(ffmpeg).with_name(probe_name)
    if sibling.exists():
        return str(sibling)
    return None


class MediaProbe:
    @staticmethod
    def probe_file(path: Path) -> MediaProbeResult:
        ffprobe = get_ffprobe_executable()
        if not ffprobe:
            return MediaProbeResult(failure_reason="ffprobe_unavailable")

        command = [
            ffprobe,
            "-v",
            "error",
            "-show_streams",
            "-show_format",
            "-print_format",
            "json",
            str(path),
        ]
        try:
            result = subprocess.run(command, capture_output=True, text=True)
        except Exception as exc:
            return MediaProbeResult(probe_tool="ffprobe", failure_reason=str(exc))
        if result.returncode != 0:
            return MediaProbeResult(
                probe_tool="ffprobe",
                failure_reason=result.stderr.strip() or f"ffprobe_exit_{result.returncode}",
            )
        try:
            payload = json.loads(result.stdout or "{}")
        except json.JSONDecodeError as exc:
            return MediaProbeResult(probe_tool="ffprobe", failure_reason=f"ffprobe_json_error:{exc}")

        streams = payload.get("streams", []) or []
        video_streams = sum(1 for stream in streams if stream.get("codec_type") == "video")
        audio_streams = sum(1 for stream in streams if stream.get("codec_type") == "audio")
        duration_raw = (payload.get("format") or {}).get("duration")
        duration_sec: float | None = None
        if duration_raw not in {None, ""}:
            try:
                duration_sec = round(float(duration_raw), 3)
            except (TypeError, ValueError):
                duration_sec = None
        return MediaProbeResult(
            has_video=video_streams > 0,
            has_audio=audio_streams > 0,
            duration_sec=duration_sec,
            video_streams=video_streams,
            audio_streams=audio_streams,
            format_name=(payload.get("format") or {}).get("format_name", ""),
            probe_tool="ffprobe",
            raw=payload,
        )


def remux_x_hls_to_mp4(video_url: str, max_size_bytes: int) -> tuple[CapturedVideo | None, dict]:
    attempt = {
        "candidate_url": video_url,
        "final_url": "",
        "mode": "x_hls_ffmpeg",
        "result": "skipped",
        "reason": "",
        "response_status": None,
        "content_type": "",
        "content_length": "",
        "output_size_bytes": 0,
    }
    ffmpeg = get_ffmpeg_executable()
    if not ffmpeg:
        attempt["reason"] = "ffmpeg_unavailable"
        return None, attempt

    temp_path: Path | None = None
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=settings.CAPTURE_HTTP_TIMEOUT,
            headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
        ) as client:
            response = client.get(video_url)
        attempt["final_url"] = str(response.url)
        attempt["response_status"] = response.status_code
        attempt["content_type"] = response.headers.get("content-type", "")
        attempt["content_length"] = response.headers.get("content-length", "")
        if response.status_code >= 400:
            attempt["reason"] = f"http_{response.status_code}"
            return None, attempt

        temp_path = create_temp_download_path(".mp4")
        command = [
            ffmpeg,
            "-y",
            "-loglevel",
            "error",
            "-protocol_whitelist",
            "file,http,https,tcp,tls,crypto",
            "-user_agent",
            settings.CAPTURE_HTTP_USER_AGENT,
            "-i",
            video_url,
            "-movflags",
            "+faststart",
            "-c",
            "copy",
            str(temp_path),
        ]
        result = subprocess.run(command, capture_output=True)
        if result.returncode != 0:
            attempt["result"] = "error"
            attempt["reason"] = result.stderr.decode("utf-8", "ignore")[:500] or f"ffmpeg_exit_{result.returncode}"
            delete_temp_file(temp_path)
            return None, attempt

        size_bytes = temp_path.stat().st_size if temp_path.exists() else 0
        attempt["output_size_bytes"] = size_bytes
        if size_bytes == 0:
            attempt["reason"] = "empty_output"
            delete_temp_file(temp_path)
            return None, attempt
        if size_bytes > max_size_bytes:
            attempt["reason"] = "too_large"
            delete_temp_file(temp_path)
            return None, attempt

        attempt["result"] = "saved"
        return (
            CapturedVideo(
                source_url=video_url,
                temp_path=temp_path,
                size_bytes=size_bytes,
                content_type="video/mp4",
            ),
            attempt,
        )
    except Exception as exc:
        attempt["result"] = "error"
        attempt["reason"] = str(exc)
        delete_temp_file(temp_path)
        return None, attempt


def download_image_assets(
    source_url: str,
    html: str,
    extra_urls: list[str] | None = None,
    page_domain: str = "",
) -> list[CapturedImage]:
    image_urls = collect_image_urls(html, source_url, page_domain=page_domain)
    for extra_url in extra_urls or []:
        normalized = normalize_media_candidate_url(extra_url, source_url)
        if not normalized:
            continue
        if normalized not in image_urls:
            image_urls.append(normalized)
    image_urls = filter_image_candidate_urls(image_urls, page_domain)
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


def append_instagram_skip_log(skip_logs: list[dict], *, phase: str, url: str, reason: str, detail: str = "") -> None:
    skip_logs.append(
        {
            "phase": phase,
            "url": url,
            "reason": reason,
            "detail": detail,
        }
    )


def build_instagram_media_candidates(
    source_url: str,
    html: str,
    extra_urls: list[str] | None = None,
    extra_candidates: list[dict] | None = None,
    page_domain: str = "",
) -> tuple[list[dict], list[dict]]:
    candidates: list[dict] = []
    skip_logs: list[dict] = []
    for video_url in collect_video_urls(html, source_url, page_domain=page_domain):
        candidate = build_media_candidate(
            video_url,
            source_url,
            source="html",
            page_domain=page_domain,
        )
        if candidate is not None:
            candidates.append(candidate)
        else:
            append_instagram_skip_log(
                skip_logs,
                phase="candidate_build",
                url=video_url,
                reason=explain_media_candidate_skip(video_url, source_url, page_domain=page_domain),
            )
    for extra_url in extra_urls or []:
        candidate = build_media_candidate(
            extra_url,
            source_url,
            source="extra_url",
            page_domain=page_domain,
        )
        if candidate is not None:
            candidates.append(candidate)
        else:
            append_instagram_skip_log(
                skip_logs,
                phase="candidate_build",
                url=extra_url,
                reason=explain_media_candidate_skip(extra_url, source_url, page_domain=page_domain),
            )
    for extra_candidate in extra_candidates or []:
        candidate = build_media_candidate(
            extra_candidate.get("url", ""),
            source_url,
            source=extra_candidate.get("source", "extra_candidate"),
            page_domain=page_domain,
            content_type=extra_candidate.get("content_type", ""),
            content_length=extra_candidate.get("content_length", ""),
            resource_type=extra_candidate.get("resource_type", ""),
            response_status=extra_candidate.get("response_status"),
        )
        if candidate is not None:
            candidates.append(candidate)
        else:
            append_instagram_skip_log(
                skip_logs,
                phase="candidate_build",
                url=extra_candidate.get("url", ""),
                reason=explain_media_candidate_skip(
                    extra_candidate.get("url", ""),
                    source_url,
                    page_domain=page_domain,
                    content_type=extra_candidate.get("content_type", ""),
                    resource_type=extra_candidate.get("resource_type", ""),
                ),
            )
    return merge_media_candidates(candidates), skip_logs


def build_instagram_audio_exploration_candidates(candidate_details: list[dict]) -> list[dict]:
    exploration: list[dict] = []
    seen: set[str] = set()
    for candidate in candidate_details:
        url = candidate.get("url", "")
        if not url or url in seen:
            continue
        media_kind = candidate.get("media_kind", "unknown")
        if media_kind == "audio":
            exploration.append(candidate)
            seen.add(url)
            continue
        if media_kind == "unknown" and (
            candidate.get("resource_type") == "media" or "network_response" in candidate.get("sources", [])
        ):
            exploration.append(candidate)
            seen.add(url)
    return exploration


def durations_match(video_duration: float | None, audio_duration: float | None, tolerance_sec: float = 1.5) -> bool:
    if video_duration is None or audio_duration is None:
        return True
    return abs(video_duration - audio_duration) <= tolerance_sec


def summarize_instagram_video_result(
    source_url: str,
    result: DownloadedVideoAssets,
    candidate_details: list[dict],
    downloaded_candidates: list[DownloadedMediaCandidate],
) -> dict:
    summary = {
        "source_url": source_url,
        "candidate_count": len(candidate_details),
        "attempt_count": len(result.attempts),
        "skip_count": len(result.skip_logs),
        "downloaded_count": len(downloaded_candidates),
        "complete_av_count": sum(1 for candidate in downloaded_candidates if candidate.probe.has_video and candidate.probe.has_audio),
        "video_only_count": sum(1 for candidate in downloaded_candidates if candidate.probe.has_video and not candidate.probe.has_audio),
        "audio_only_count": sum(1 for candidate in downloaded_candidates if candidate.probe.has_audio and not candidate.probe.has_video),
        "probe_failure_count": sum(1 for candidate in downloaded_candidates if candidate.probe.failure_reason),
        "selected_asset_count": len(result.assets),
        "extraction_status": result.extraction_status,
        "extraction_strategy": result.extraction_strategy,
        "failure_reason": result.failure_reason,
    }
    result.summary = summary
    logger.info(
        "instagram extraction summary url=%s status=%s strategy=%s candidates=%s downloaded=%s complete=%s video_only=%s audio_only=%s skips=%s failure=%s",
        source_url,
        summary["extraction_status"],
        summary["extraction_strategy"],
        summary["candidate_count"],
        summary["downloaded_count"],
        summary["complete_av_count"],
        summary["video_only_count"],
        summary["audio_only_count"],
        summary["skip_count"],
        summary["failure_reason"],
    )
    return summary


def score_downloaded_media_candidate(candidate: DownloadedMediaCandidate) -> tuple:
    return (
        1 if candidate.probe.has_video and candidate.probe.has_audio else 0,
        1 if "network_response" in candidate.candidate.get("sources", []) else 0,
        candidate.probe.duration_sec or 0,
        candidate.size_bytes,
    )


def download_instagram_candidate(client: httpx.Client, candidate: dict) -> tuple[DownloadedMediaCandidate | None, dict]:
    temp_path: Path | None = None
    attempt = {
        "candidate_url": candidate.get("url", ""),
        "candidate_sources": candidate.get("sources", []),
        "candidate_media_kind": candidate.get("media_kind", "unknown"),
        "final_url": "",
        "mode": "instagram_direct",
        "result": "skipped",
        "reason": "",
        "response_status": candidate.get("response_status"),
        "content_type": candidate.get("content_type", ""),
        "content_length": candidate.get("content_length", ""),
        "output_size_bytes": 0,
        "has_video": False,
        "has_audio": False,
        "duration_sec": None,
        "extraction_status": "failed",
        "extraction_strategy": "instagram_direct",
    }
    try:
        with client.stream("GET", candidate["url"]) as response:
            attempt["final_url"] = str(response.url)
            attempt["response_status"] = response.status_code
            attempt["content_type"] = response.headers.get("content-type", "")
            attempt["content_length"] = response.headers.get("content-length", "")
            if response.status_code >= 400:
                attempt["reason"] = f"http_{response.status_code}"
                return None, attempt
            content_type = response.headers.get("content-type", "")
            if not is_probable_media_url(str(response.url), content_type):
                attempt["reason"] = "not_media"
                return None, attempt

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
                attempt["reason"] = "too_large" if too_large else "empty_output"
                delete_temp_file(temp_path)
                return None, attempt

            probe = MediaProbe.probe_file(temp_path)
            attempt["output_size_bytes"] = size_bytes
            attempt["probe"] = probe.to_dict()
            attempt["has_video"] = probe.has_video
            attempt["has_audio"] = probe.has_audio
            attempt["duration_sec"] = probe.duration_sec
            if probe.failure_reason:
                attempt["result"] = "error"
                attempt["reason"] = probe.failure_reason
                attempt["extraction_status"] = "failed"
            elif probe.has_video and probe.has_audio:
                attempt["result"] = "saved"
                attempt["extraction_status"] = "success"
            elif probe.has_video:
                attempt["result"] = "saved"
                attempt["reason"] = "missing_audio_stream"
                attempt["extraction_status"] = "partial"
            elif probe.has_audio:
                attempt["result"] = "saved"
                attempt["reason"] = "missing_video_stream"
                attempt["extraction_status"] = "partial"
            else:
                attempt["result"] = "error"
                attempt["reason"] = "missing_media_streams"
                attempt["extraction_status"] = "failed"
            return (
                DownloadedMediaCandidate(
                    candidate=candidate,
                    source_url=str(response.url),
                    temp_path=temp_path,
                    size_bytes=size_bytes,
                    content_type=content_type,
                    probe=probe,
                ),
                attempt,
            )
    except Exception as exc:
        attempt["result"] = "error"
        attempt["reason"] = str(exc) or "download_exception"
        delete_temp_file(temp_path)
        return None, attempt


def pick_instagram_merge_pair(
    video_candidates: list[DownloadedMediaCandidate],
    audio_candidates: list[DownloadedMediaCandidate],
) -> tuple[tuple[DownloadedMediaCandidate, DownloadedMediaCandidate] | None, list[dict]]:
    ranked_pairs: list[tuple[tuple, DownloadedMediaCandidate, DownloadedMediaCandidate]] = []
    skip_logs: list[dict] = []
    for video_candidate in video_candidates:
        for audio_candidate in audio_candidates:
            video_duration = video_candidate.probe.duration_sec
            audio_duration = audio_candidate.probe.duration_sec
            if not durations_match(video_duration, audio_duration):
                append_instagram_skip_log(
                    skip_logs,
                    phase="pair_selection",
                    url=video_candidate.source_url,
                    reason="duration_mismatch",
                    detail=f"audio={audio_candidate.source_url} video_duration={video_duration} audio_duration={audio_duration}",
                )
                continue
            duration_delta = abs((video_duration or 0) - (audio_duration or 0))
            ranked_pairs.append(
                (
                    (
                        -(duration_delta if video_duration and audio_duration else 0),
                        video_candidate.size_bytes + audio_candidate.size_bytes,
                    ),
                    video_candidate,
                    audio_candidate,
                )
            )
    if not ranked_pairs:
        return None, skip_logs
    _, best_video, best_audio = max(ranked_pairs, key=lambda item: item[0])
    return (best_video, best_audio), skip_logs


def merge_instagram_streams(
    video_candidate: DownloadedMediaCandidate,
    audio_candidate: DownloadedMediaCandidate,
) -> tuple[CapturedVideo | None, dict]:
    attempt = {
        "candidate_url": video_candidate.candidate.get("url", ""),
        "audio_candidate_url": audio_candidate.candidate.get("url", ""),
        "candidate_sources": video_candidate.candidate.get("sources", []),
        "audio_candidate_sources": audio_candidate.candidate.get("sources", []),
        "mode": "instagram_ffmpeg_mux",
        "result": "skipped",
        "reason": "",
        "output_size_bytes": 0,
        "has_video": False,
        "has_audio": False,
        "duration_sec": None,
        "extraction_status": "failed",
        "extraction_strategy": "instagram_mux_ffmpeg",
    }
    ffmpeg = get_ffmpeg_executable()
    if not ffmpeg:
        attempt["reason"] = "ffmpeg_unavailable"
        return None, attempt

    merged_path: Path | None = None
    try:
        fallback_commands = [
            (
                "copy",
                [
                    ffmpeg,
                    "-y",
                    "-loglevel",
                    "error",
                    "-i",
                    str(video_candidate.temp_path),
                    "-i",
                    str(audio_candidate.temp_path),
                    "-map",
                    "0:v:0",
                    "-map",
                    "1:a:0",
                    "-c",
                    "copy",
                    "-shortest",
                ],
            ),
            (
                "aac_fallback",
                [
                    ffmpeg,
                    "-y",
                    "-loglevel",
                    "error",
                    "-i",
                    str(video_candidate.temp_path),
                    "-i",
                    str(audio_candidate.temp_path),
                    "-map",
                    "0:v:0",
                    "-map",
                    "1:a:0",
                    "-c:v",
                    "copy",
                    "-c:a",
                    "aac",
                    "-b:a",
                    "192k",
                    "-shortest",
                ],
            ),
        ]
        fallback_errors: list[dict] = []
        for mux_mode, base_command in fallback_commands:
            merged_path = create_temp_download_path(".mp4")
            command = [*base_command, str(merged_path)]
            result = subprocess.run(command, capture_output=True, text=True)
            if result.returncode == 0:
                attempt["mux_mode"] = mux_mode
                break
            fallback_errors.append(
                {
                    "mux_mode": mux_mode,
                    "reason": result.stderr.strip() or f"ffmpeg_exit_{result.returncode}",
                }
            )
            delete_temp_file(merged_path)
            merged_path = None
        if merged_path is None:
            attempt["fallback_errors"] = fallback_errors
            attempt["reason"] = fallback_errors[-1]["reason"] if fallback_errors else "ffmpeg_mux_failed"
            return None, attempt

        size_bytes = merged_path.stat().st_size if merged_path.exists() else 0
        if size_bytes == 0:
            attempt["reason"] = "empty_output"
            delete_temp_file(merged_path)
            return None, attempt
        probe = MediaProbe.probe_file(merged_path)
        attempt["output_size_bytes"] = size_bytes
        attempt["probe"] = probe.to_dict()
        attempt["has_video"] = probe.has_video
        attempt["has_audio"] = probe.has_audio
        attempt["duration_sec"] = probe.duration_sec
        if not (probe.has_video and probe.has_audio):
            attempt["reason"] = probe.failure_reason or "merged_asset_missing_av"
            delete_temp_file(merged_path)
            return None, attempt
        if not durations_match(video_candidate.probe.duration_sec, probe.duration_sec):
            attempt["reason"] = "merged_duration_mismatch"
            delete_temp_file(merged_path)
            return None, attempt

        attempt["result"] = "saved"
        attempt["extraction_status"] = "success"
        return (
            CapturedVideo(
                source_url=video_candidate.source_url,
                temp_path=merged_path,
                size_bytes=size_bytes,
                content_type="video/mp4",
                metadata={
                    "has_video": True,
                    "has_audio": True,
                    "duration_sec": probe.duration_sec,
                    "extraction_strategy": "instagram_mux_ffmpeg",
                    "failure_reason": "",
                    "probe": probe.to_dict(),
                    "audio_source_url": audio_candidate.source_url,
                    "mux_mode": attempt.get("mux_mode", "copy"),
                },
            ),
            attempt,
        )
    except Exception as exc:
        attempt["result"] = "error"
        attempt["reason"] = str(exc) or "ffmpeg_exception"
        delete_temp_file(merged_path)
        return None, attempt


def download_instagram_video_assets(
    source_url: str,
    html: str,
    extra_urls: list[str] | None = None,
    extra_candidates: list[dict] | None = None,
    page_domain: str = "",
) -> DownloadedVideoAssets:
    candidate_details, skip_logs = build_instagram_media_candidates(
        source_url,
        html,
        extra_urls=extra_urls,
        extra_candidates=extra_candidates,
        page_domain=page_domain,
    )
    result = DownloadedVideoAssets(
        candidate_urls=[candidate["url"] for candidate in candidate_details],
        candidate_details=candidate_details,
        skip_logs=skip_logs,
    )
    if not candidate_details:
        result.extraction_status = "failed"
        result.extraction_strategy = "instagram_candidates"
        result.failure_reason = "no_media_candidates"
        summarize_instagram_video_result(source_url, result, candidate_details, [])
        return result
    ffprobe = get_ffprobe_executable()
    if not ffprobe:
        append_instagram_skip_log(
            result.skip_logs,
            phase="probe_setup",
            url=source_url,
            reason="ffprobe_required",
        )
        result.extraction_status = "failed"
        result.extraction_strategy = "instagram_probe"
        result.failure_reason = "ffprobe_required"
        summarize_instagram_video_result(source_url, result, candidate_details, [])
        return result

    logger.info("instagram media candidates collected url=%s count=%s", source_url, len(candidate_details))
    downloaded_candidates: list[DownloadedMediaCandidate] = []
    primary_candidates = [candidate for candidate in candidate_details if candidate.get("media_kind") != "audio"]
    deferred_audio_candidates = [candidate for candidate in candidate_details if candidate.get("media_kind") == "audio"]
    for candidate in deferred_audio_candidates:
        append_instagram_skip_log(
            result.skip_logs,
            phase="primary_phase",
            url=candidate.get("url", ""),
            reason="deferred_to_audio_phase",
        )
    with httpx.Client(
        follow_redirects=True,
        timeout=settings.CAPTURE_HTTP_TIMEOUT,
        headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
    ) as client:
        for candidate in primary_candidates:
            downloaded_candidate, attempt = download_instagram_candidate(client, candidate)
            result.attempts.append(attempt)
            logger.info(
                "instagram media attempt url=%s candidate=%s status=%s reason=%s has_video=%s has_audio=%s",
                source_url,
                attempt["candidate_url"],
                attempt["extraction_status"],
                attempt["reason"],
                attempt["has_video"],
                attempt["has_audio"],
            )
            if downloaded_candidate is not None:
                downloaded_candidates.append(downloaded_candidate)

    complete_candidates = [
        candidate
        for candidate in downloaded_candidates
        if candidate.probe.has_video and candidate.probe.has_audio
    ]
    if complete_candidates:
        selected = max(complete_candidates, key=score_downloaded_media_candidate)
        result.assets.append(
            CapturedVideo(
                source_url=selected.source_url,
                temp_path=selected.temp_path,
                size_bytes=selected.size_bytes,
                content_type=selected.content_type,
                metadata={
                    "has_video": True,
                    "has_audio": True,
                    "duration_sec": selected.probe.duration_sec,
                    "extraction_strategy": "instagram_direct",
                    "failure_reason": "",
                    "probe": selected.probe.to_dict(),
                },
            )
        )
        result.extraction_status = "success"
        result.extraction_strategy = "instagram_direct"
        result.selected_asset = result.assets[0].metadata
        for candidate in downloaded_candidates:
            if candidate is not selected:
                delete_temp_file(candidate.temp_path)
        summarize_instagram_video_result(source_url, result, candidate_details, downloaded_candidates)
        return result

    video_only_candidates = [
        candidate
        for candidate in downloaded_candidates
        if candidate.probe.has_video and not candidate.probe.has_audio
    ]
    audio_phase_candidates = build_instagram_audio_exploration_candidates(candidate_details)
    tried_urls = {candidate.candidate.get("url", "") for candidate in downloaded_candidates}
    if audio_phase_candidates:
        logger.info("instagram audio exploration phase url=%s candidates=%s", source_url, len(audio_phase_candidates))
    with httpx.Client(
        follow_redirects=True,
        timeout=settings.CAPTURE_HTTP_TIMEOUT,
        headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
    ) as client:
        for candidate in audio_phase_candidates:
            if candidate.get("url", "") in tried_urls:
                append_instagram_skip_log(
                    result.skip_logs,
                    phase="audio_phase",
                    url=candidate.get("url", ""),
                    reason="already_attempted_in_primary_phase",
                )
                continue
            downloaded_candidate, attempt = download_instagram_candidate(client, candidate)
            attempt["mode"] = "instagram_audio_explore"
            result.attempts.append(attempt)
            logger.info(
                "instagram audio exploration attempt url=%s candidate=%s status=%s reason=%s has_video=%s has_audio=%s",
                source_url,
                attempt["candidate_url"],
                attempt["extraction_status"],
                attempt["reason"],
                attempt["has_video"],
                attempt["has_audio"],
            )
            if downloaded_candidate is not None:
                downloaded_candidates.append(downloaded_candidate)
                tried_urls.add(downloaded_candidate.candidate.get("url", ""))

    complete_candidates = [
        candidate
        for candidate in downloaded_candidates
        if candidate.probe.has_video and candidate.probe.has_audio
    ]
    if complete_candidates:
        selected = max(complete_candidates, key=score_downloaded_media_candidate)
        result.assets.append(
            CapturedVideo(
                source_url=selected.source_url,
                temp_path=selected.temp_path,
                size_bytes=selected.size_bytes,
                content_type=selected.content_type,
                metadata={
                    "has_video": True,
                    "has_audio": True,
                    "duration_sec": selected.probe.duration_sec,
                    "extraction_strategy": "instagram_direct",
                    "failure_reason": "",
                    "probe": selected.probe.to_dict(),
                },
            )
        )
        result.extraction_status = "success"
        result.extraction_strategy = "instagram_direct"
        result.selected_asset = result.assets[0].metadata
        for candidate in downloaded_candidates:
            if candidate is not selected:
                delete_temp_file(candidate.temp_path)
        summarize_instagram_video_result(source_url, result, candidate_details, downloaded_candidates)
        return result

    video_only_candidates = [
        candidate
        for candidate in downloaded_candidates
        if candidate.probe.has_video and not candidate.probe.has_audio
    ]
    audio_only_candidates = [
        candidate
        for candidate in downloaded_candidates
        if candidate.probe.has_audio and not candidate.probe.has_video
    ]
    if video_only_candidates and audio_only_candidates:
        pair, pair_skip_logs = pick_instagram_merge_pair(video_only_candidates, audio_only_candidates)
        result.skip_logs.extend(pair_skip_logs)
        if pair is not None:
            merged_video, merge_attempt = merge_instagram_streams(*pair)
            result.attempts.append(merge_attempt)
            if merged_video is not None:
                result.assets.append(merged_video)
                result.extraction_status = "success"
                result.extraction_strategy = "instagram_mux_ffmpeg"
                result.selected_asset = merged_video.metadata
                for candidate in downloaded_candidates:
                    delete_temp_file(candidate.temp_path)
                summarize_instagram_video_result(source_url, result, candidate_details, downloaded_candidates)
                return result
            append_instagram_skip_log(
                result.skip_logs,
                phase="mux",
                url=pair[0].source_url,
                reason=merge_attempt.get("reason", "mux_failed"),
                detail=f"audio={pair[1].source_url}",
            )

    for candidate in downloaded_candidates:
        delete_temp_file(candidate.temp_path)
    result.extraction_strategy = "instagram_probe"
    if video_only_candidates:
        result.extraction_status = "partial"
        result.failure_reason = "video_only_candidate"
    else:
        result.extraction_status = "failed"
        result.failure_reason = "no_av_candidate"
    summarize_instagram_video_result(source_url, result, candidate_details, downloaded_candidates)
    return result


def download_video_assets(
    source_url: str,
    html: str,
    extra_urls: list[str] | None = None,
    page_domain: str = "",
    extra_candidates: list[dict] | None = None,
) -> DownloadedVideoAssets:
    if is_instagram_domain(page_domain):
        return download_instagram_video_assets(
            source_url,
            html,
            extra_urls=extra_urls,
            extra_candidates=extra_candidates,
            page_domain=page_domain,
        )

    video_urls = collect_video_urls(html, source_url, page_domain=page_domain)
    for extra_url in extra_urls or []:
        normalized = normalize_media_candidate_url(extra_url, source_url)
        if not normalized:
            continue
        if normalized not in video_urls:
            video_urls.append(normalized)
    video_urls = filter_video_candidate_urls(video_urls, page_domain)
    result = DownloadedVideoAssets(candidate_urls=list(video_urls))
    if not video_urls:
        return result

    with httpx.Client(
        follow_redirects=True,
        timeout=settings.CAPTURE_HTTP_TIMEOUT,
        headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
    ) as client:
        for video_url in video_urls:
            if len(result.assets) >= settings.CAPTURE_MAX_VIDEOS:
                break
            if is_x_domain(page_domain) and is_x_hls_playlist_url(video_url):
                captured_video, attempt = remux_x_hls_to_mp4(video_url, settings.CAPTURE_MAX_VIDEO_BYTES)
                result.attempts.append(attempt)
                if captured_video is not None:
                    result.assets.append(captured_video)
                    if is_scoped_social_capture_domain(page_domain):
                        break
                continue
            temp_path: Path | None = None
            attempt = {
                "candidate_url": video_url,
                "final_url": "",
                "mode": "direct",
                "result": "skipped",
                "reason": "",
                "response_status": None,
                "content_type": "",
                "content_length": "",
                "output_size_bytes": 0,
            }
            try:
                with client.stream("GET", video_url) as response:
                    attempt["final_url"] = str(response.url)
                    attempt["response_status"] = response.status_code
                    attempt["content_type"] = response.headers.get("content-type", "")
                    attempt["content_length"] = response.headers.get("content-length", "")
                    if response.status_code >= 400:
                        attempt["reason"] = f"http_{response.status_code}"
                        result.attempts.append(attempt)
                        continue
                    content_type = response.headers.get("content-type", "")
                    if not is_probable_video_url(str(response.url), content_type):
                        attempt["reason"] = "not_video"
                        result.attempts.append(attempt)
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
                        attempt["reason"] = "too_large" if too_large else "empty_output"
                        delete_temp_file(temp_path)
                        result.attempts.append(attempt)
                        continue

                    attempt["result"] = "saved"
                    attempt["output_size_bytes"] = size_bytes
                    result.assets.append(
                        CapturedVideo(
                            source_url=str(response.url),
                            temp_path=temp_path,
                            size_bytes=size_bytes,
                            content_type=content_type,
                        )
                    )
                    if is_scoped_social_capture_domain(page_domain):
                        result.attempts.append(attempt)
                        break
            except Exception:
                attempt["result"] = "error"
                attempt["reason"] = "download_exception"
                delete_temp_file(temp_path)
            result.attempts.append(attempt)
    return result


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
            browser = playwright.chromium.launch(headless=True)
            storage_state_path = get_playwright_storage_state_path(page_domain)
            capture_domain = page_domain or urlparse(url).netloc
            viewport_defaults = {
                "width": settings.CAPTURE_VIEWPORT_WIDTH,
                "height": settings.CAPTURE_VIEWPORT_HEIGHT,
            }
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
                    if not is_observed_media_response(
                        raw_url,
                        capture_domain,
                        content_type=content_type,
                        resource_type=resource_type,
                    ):
                        return
                    entry = {
                        "url": raw_url,
                        "resource_type": resource_type,
                        "content_type": content_type,
                        "content_length": content_length,
                        "response_status": getattr(response, "status", None),
                    }
                    if entry not in observed_media_responses:
                        observed_media_responses.append(entry)
                    if classify_media_candidate_kind(
                        raw_url,
                        content_type=content_type,
                        resource_type=resource_type,
                    ) != "audio" and raw_url not in observed_video_urls:
                        observed_video_urls.append(raw_url)

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
    capture_videos = resource.capture_videos and supports_video_capture(resource.domain)
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


def normalize_ai_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def build_translation_source_text(snapshot: Snapshot) -> str:
    source = snapshot.extracted_text or snapshot.og_description or snapshot.page_title
    normalized = normalize_ai_text(source)
    if not normalized:
        return ""
    source_limit = min(getattr(settings, "AI_MAX_INPUT_CHARS", TRANSLATION_MAX_SOURCE_CHARS), TRANSLATION_MAX_SOURCE_CHARS)
    return normalized[:source_limit].strip()


def split_translation_chunks(text: str, *, max_chars: int = TRANSLATION_MAX_CHUNK_CHARS) -> list[str]:
    normalized = normalize_ai_text(text)
    if not normalized:
        return []
    if len(normalized) <= max_chars:
        return [normalized]

    chunks: list[str] = []
    current = ""
    segments = [segment for segment in re.split(r"(?<=[.!?。！？])\s+", normalized) if segment]
    for segment in segments:
        if len(segment) > max_chars:
            if current:
                chunks.append(current)
                current = ""
            for start in range(0, len(segment), max_chars):
                piece = segment[start : start + max_chars].strip()
                if piece:
                    chunks.append(piece)
            continue
        candidate = segment if not current else f"{current} {segment}"
        if len(candidate) <= max_chars:
            current = candidate
        else:
            chunks.append(current)
            current = segment
    if current:
        chunks.append(current)
    return chunks


def is_probably_japanese_text(text: str) -> bool:
    sample = normalize_ai_text(text)[:800]
    if not sample:
        return False
    kana_count = len(re.findall(r"[ぁ-ゖァ-ヺー]", sample))
    cjk_count = len(re.findall(r"[一-龯々〆ヵヶ]", sample))
    return kana_count >= 3 or (kana_count >= 1 and cjk_count >= 4)


def translate_text_chunk_to_japanese(text: str) -> tuple[str, str]:
    with httpx.Client(
        timeout=15.0,
        headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
    ) as client:
        response = client.get(
            TRANSLATION_ENDPOINT,
            params={
                "client": "gtx",
                "sl": "auto",
                "tl": "ja",
                "dt": "t",
                "q": text,
            },
        )
    response.raise_for_status()
    payload = response.json()
    translated_parts: list[str] = []
    detected_language = ""
    if isinstance(payload, list):
        if len(payload) > 2 and isinstance(payload[2], str):
            detected_language = payload[2]
        if payload and isinstance(payload[0], list):
            for item in payload[0]:
                if isinstance(item, list) and item and isinstance(item[0], str):
                    translated_parts.append(item[0])
    return normalize_ai_text("".join(translated_parts)), detected_language


def translate_text_to_japanese(text: str) -> tuple[str, dict]:
    normalized = normalize_ai_text(text)
    if not normalized:
        return "", {"translation_status": "empty_source", "detected_language": ""}
    if is_probably_japanese_text(normalized):
        return "", {"translation_status": "source_already_japanese", "detected_language": "ja"}

    translated_chunks: list[str] = []
    detected_language = ""
    try:
        for chunk in split_translation_chunks(normalized):
            translated_chunk, chunk_language = translate_text_chunk_to_japanese(chunk)
            if translated_chunk:
                translated_chunks.append(translated_chunk)
            if chunk_language and not detected_language:
                detected_language = chunk_language
    except Exception as exc:  # pragma: no cover
        logger.warning("Japanese translation failed: %s", exc)
        return "", {
            "translation_status": "translation_failed",
            "detected_language": detected_language,
            "error_message": str(exc),
        }

    translation = normalize_ai_text(" ".join(translated_chunks))
    if detected_language.startswith("ja"):
        return "", {"translation_status": "source_already_japanese", "detected_language": detected_language}
    return translation, {
        "translation_status": "translated" if translation else "translation_unavailable",
        "detected_language": detected_language,
    }


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

    translation, translation_meta = translate_text_to_japanese(build_translation_source_text(snapshot))
    category = infer_category(snapshot)
    payload = {
        "provider": provider,
        "tag_candidates": suggest_tags(snapshot),
        "similar_resource_ids": similar_resource_ids(snapshot.resource),
        **translation_meta,
    }
    return AIResult(summary=translation, category=category, payload=payload)


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
            sync_capture_flags(resource)
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
