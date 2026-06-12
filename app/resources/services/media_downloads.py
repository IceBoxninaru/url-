from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import httpx
from django.conf import settings

from .media_discovery import (
    collect_image_urls,
    filter_image_candidate_urls,
    is_instagram_domain,
    is_x_domain,
    normalize_media_candidate_url,
)
from .media_download_common import (
    MediaProbe,
    cleanup_capture_result,
    create_temp_download_path,
    delete_temp_file,
    get_ffmpeg_executable,
    get_ffprobe_executable,
    validate_downloaded_video_file,
)
from .media_download_generic import download_direct_video_candidate, download_generic_video_assets
from .media_download_instagram import (
    append_instagram_skip_log,
    build_instagram_audio_exploration_candidates,
    build_instagram_media_candidates,
    download_instagram_candidate,
    download_instagram_candidate_without_probe,
    download_instagram_video_assets,
    durations_match,
    merge_instagram_streams,
    pick_instagram_merge_pair,
    score_downloaded_media_candidate,
    summarize_instagram_video_result,
)
from .media_download_x import download_x_video_assets, remux_x_hls_to_mp4
from .types import CapturedImage, DownloadedVideoAssets


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


@dataclass(frozen=True)
class VideoDownloadStrategy:
    name: str
    matches: Callable[[str], bool]
    download: Callable[..., DownloadedVideoAssets]


VIDEO_DOWNLOAD_STRATEGIES = (
    VideoDownloadStrategy("instagram", is_instagram_domain, download_instagram_video_assets),
    VideoDownloadStrategy("x", is_x_domain, download_x_video_assets),
    VideoDownloadStrategy("generic", lambda _domain: True, download_generic_video_assets),
)


def select_video_download_strategy(page_domain: str) -> VideoDownloadStrategy:
    for strategy in VIDEO_DOWNLOAD_STRATEGIES:
        if strategy.matches(page_domain):
            return strategy
    return VIDEO_DOWNLOAD_STRATEGIES[-1]


def download_video_assets(
    source_url: str,
    html: str,
    extra_urls: list[str] | None = None,
    page_domain: str = "",
    extra_candidates: list[dict] | None = None,
) -> DownloadedVideoAssets:
    strategy = select_video_download_strategy(page_domain)
    return strategy.download(
        source_url,
        html,
        extra_urls=extra_urls,
        page_domain=page_domain,
        extra_candidates=extra_candidates,
    )
