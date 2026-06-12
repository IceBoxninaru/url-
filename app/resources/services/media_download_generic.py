from __future__ import annotations

from pathlib import Path

import httpx
from django.conf import settings

from .media_discovery import (
    collect_video_candidate_details,
    guess_video_extension,
    is_probable_video_url,
    is_scoped_social_capture_domain,
    is_x_domain,
)
from .media_download_common import create_temp_download_path, delete_temp_file, validate_downloaded_video_file
from .types import CapturedVideo, DownloadedVideoAssets


def download_direct_video_candidate(
    client: httpx.Client,
    candidate: dict,
    *,
    page_domain: str = "",
) -> tuple[CapturedVideo | None, dict]:
    video_url = candidate["url"]
    temp_path: Path | None = None
    attempt = {
        "candidate_url": video_url,
        "candidate_sources": candidate.get("sources", []),
        "candidate_media_kind": candidate.get("media_kind", "unknown"),
        "final_url": "",
        "mode": "direct",
        "result": "skipped",
        "reason": "",
        "response_status": None,
        "content_type": "",
        "content_length": "",
        "output_size_bytes": 0,
        "has_video": False,
        "has_audio": False,
        "duration_sec": None,
    }
    try:
        with client.stream("GET", video_url) as response:
            attempt["final_url"] = str(response.url)
            attempt["response_status"] = response.status_code
            attempt["content_type"] = response.headers.get("content-type", "")
            attempt["content_length"] = response.headers.get("content-length", "")
            if response.status_code >= 400:
                attempt["reason"] = f"http_{response.status_code}"
                return None, attempt
            content_type = response.headers.get("content-type", "")
            if not is_probable_video_url(str(response.url), content_type):
                attempt["reason"] = "not_video"
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

            attempt["output_size_bytes"] = size_bytes
            is_valid, probe, reason = validate_downloaded_video_file(temp_path)
            attempt["probe"] = probe.to_dict()
            attempt["has_video"] = probe.has_video
            attempt["has_audio"] = probe.has_audio
            attempt["duration_sec"] = probe.duration_sec
            if not is_valid:
                attempt["result"] = "error"
                attempt["reason"] = reason
                delete_temp_file(temp_path)
                return None, attempt

            attempt["result"] = "saved"
            return (
                CapturedVideo(
                    source_url=str(response.url),
                    temp_path=temp_path,
                    size_bytes=size_bytes,
                    content_type=content_type,
                    metadata={
                        "has_video": probe.has_video,
                        "has_audio": probe.has_audio,
                        "duration_sec": probe.duration_sec,
                        "extraction_strategy": "direct",
                        "failure_reason": probe.failure_reason,
                        "probe": probe.to_dict(),
                    },
                ),
                attempt,
            )
    except Exception:
        attempt["result"] = "error"
        attempt["reason"] = "download_exception"
        delete_temp_file(temp_path)
        return None, attempt


def download_generic_video_assets(
    source_url: str,
    html: str,
    extra_urls: list[str] | None = None,
    page_domain: str = "",
    extra_candidates: list[dict] | None = None,
) -> DownloadedVideoAssets:
    candidate_details = collect_video_candidate_details(
        source_url,
        html,
        extra_urls=extra_urls,
        page_domain=page_domain,
        extra_candidates=extra_candidates,
    )
    video_candidates = [candidate for candidate in candidate_details if candidate.get("media_kind") != "audio"]
    result = DownloadedVideoAssets(
        candidate_urls=[candidate["url"] for candidate in video_candidates],
        candidate_details=candidate_details,
    )
    if not video_candidates:
        result.extraction_status = "failed"
        result.extraction_strategy = "video_candidates"
        result.failure_reason = "no_media_candidates"
        return result

    with httpx.Client(
        follow_redirects=True,
        timeout=settings.CAPTURE_HTTP_TIMEOUT,
        headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
    ) as client:
        for candidate in video_candidates:
            if len(result.assets) >= settings.CAPTURE_MAX_VIDEOS:
                break
            captured_video, attempt = download_direct_video_candidate(client, candidate, page_domain=page_domain)
            result.attempts.append(attempt)
            if captured_video is not None:
                result.assets.append(captured_video)
                if is_scoped_social_capture_domain(page_domain):
                    break
    if result.assets:
        result.extraction_status = "success"
        result.extraction_strategy = "x_hls_ffmpeg" if is_x_domain(page_domain) else "direct"
    elif result.attempts:
        result.extraction_status = "failed"
        result.extraction_strategy = "download"
        result.failure_reason = result.attempts[-1].get("reason", "download_failed")
    return result
