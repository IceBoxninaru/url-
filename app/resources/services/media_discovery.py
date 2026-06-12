from __future__ import annotations

import html
import mimetypes
import re
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from django.conf import settings

VIDEO_EXTENSIONS = {".mp4", ".mov", ".webm", ".m4v"}
AUDIO_EXTENSIONS = {".aac", ".m4a", ".mp3", ".ogg", ".oga", ".wav"}
MEDIA_TEXT_SCAN_MAX_CHARS = 2_000_000
RAW_MEDIA_URL_PATTERN = re.compile(r"https?:\\?/\\?/[^\"'<>\s]+")
ENCODED_MEDIA_URL_PATTERN = re.compile(r"https?%3a%2f%2f[^\"'<>\s]+", re.IGNORECASE)


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


def get_playwright_profile_path(page_domain: str) -> Path | None:
    if not is_x_domain(page_domain):
        return None
    raw_path = getattr(settings, "CAPTURE_X_PROFILE_PATH", "").strip()
    if not raw_path:
        return None
    candidate = resolve_storage_state_path(raw_path)
    if not candidate.exists() or not candidate.is_dir():
        return None
    try:
        next(candidate.iterdir())
    except StopIteration:
        return None
    except OSError:
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
    return (
        parsed.netloc.endswith("video.twimg.com")
        and parsed.path.endswith(".m3u8")
        and "/mp4a/" not in parsed.path
    )


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
    if re.search(r"/vid/[^/]+/\d+/\d+/\d+x\d+/", parsed.path):
        return False
    if parsed.path.endswith("/init.mp4"):
        return False
    return True


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

    for text_url in extract_media_candidate_urls_from_text(html, source_url, page_domain=page_domain):
        push(text_url)

    return filter_video_candidate_urls(candidates, page_domain)


def html_unescape_and_clean_url(raw_url: str) -> str:
    cleaned = html.unescape(raw_url).strip()
    cleaned = re.sub(r'["\')\]\};,]+$', "", cleaned)
    return cleaned


def decode_media_text_url(raw_url: str) -> str:
    cleaned = html_unescape_and_clean_url(raw_url)
    cleaned = (
        cleaned.replace("\\/", "/")
        .replace("\\u0026", "&")
        .replace("\\U0026", "&")
        .replace("\\u003d", "=")
        .replace("\\U003D", "=")
        .replace("\\u002f", "/")
        .replace("\\U002F", "/")
    )
    if "%3a%2f%2f" in cleaned.lower():
        cleaned = unquote(cleaned)
    return html_unescape_and_clean_url(cleaned)


def extract_media_candidate_urls_from_text(
    text: str,
    source_url: str,
    *,
    page_domain: str = "",
    include_audio: bool = False,
) -> list[str]:
    if not text:
        return []
    scanned = text[:MEDIA_TEXT_SCAN_MAX_CHARS]
    raw_matches = RAW_MEDIA_URL_PATTERN.findall(scanned)
    raw_matches.extend(ENCODED_MEDIA_URL_PATTERN.findall(scanned))
    candidates: list[str] = []
    for raw_url in raw_matches:
        decoded = decode_media_text_url(raw_url)
        normalized = normalize_media_candidate_url(decoded, source_url)
        if not normalized:
            continue
        if is_scoped_social_capture_domain(page_domain):
            is_video = is_relevant_video_candidate(normalized, page_domain)
            is_audio = include_audio and is_instagram_domain(page_domain) and is_probable_audio_url(normalized)
            if not (is_video or is_audio):
                continue
        elif not is_probable_video_url(normalized) and not (include_audio and is_probable_audio_url(normalized)):
            continue
        if normalized not in candidates:
            candidates.append(normalized)
    return candidates


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


def should_scan_media_response_body(
    response_url: str,
    page_domain: str,
    *,
    content_type: str = "",
    content_length: str = "",
) -> bool:
    if not is_scoped_social_capture_domain(page_domain):
        return False
    try:
        if content_length and int(content_length) > MEDIA_TEXT_SCAN_MAX_CHARS:
            return False
    except ValueError:
        pass

    normalized_type = content_type.split(";")[0].strip().lower()
    is_text_like = (
        normalized_type.endswith("json")
        or normalized_type in {"text/plain", "text/html", "application/javascript", "text/javascript"}
    )
    parsed = urlparse(response_url.lower())
    if is_x_domain(page_domain):
        return (
            parsed.netloc.endswith(("x.com", "twitter.com"))
            and ("/i/api/" in parsed.path or "/graphql/" in parsed.path)
            and (is_text_like or not normalized_type)
        )
    if is_instagram_domain(page_domain):
        return (
            parsed.netloc.endswith(("instagram.com", "cdninstagram.com", "fbcdn.net"))
            and (is_text_like or not normalized_type)
        )
    return False


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
    if not is_scoped_social_capture_domain(domain):
        return True
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


def collect_video_candidate_details(
    source_url: str,
    html: str,
    *,
    extra_urls: list[str] | None = None,
    page_domain: str = "",
    extra_candidates: list[dict] | None = None,
) -> list[dict]:
    candidates: list[dict] = []
    for video_url in collect_video_urls(html, source_url, page_domain=page_domain):
        candidate = build_media_candidate(
            video_url,
            source_url,
            source="html",
            page_domain=page_domain,
        )
        if candidate is not None:
            candidates.append(candidate)
    for extra_url in extra_urls or []:
        candidate = build_media_candidate(
            extra_url,
            source_url,
            source="extra_url",
            page_domain=page_domain,
        )
        if candidate is not None:
            candidates.append(candidate)
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
    merged = merge_media_candidates(candidates)
    if is_x_domain(page_domain):
        return sorted(merged, key=lambda candidate: score_x_video_candidate(candidate.get("url", "")), reverse=True)
    return merged


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
