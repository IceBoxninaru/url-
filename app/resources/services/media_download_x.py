from __future__ import annotations

import subprocess
from pathlib import Path

import httpx
from django.conf import settings

from .media_discovery import collect_video_candidate_details, is_x_hls_playlist_url
from .media_download_common import (
    create_temp_download_path,
    delete_temp_file,
    get_ffmpeg_executable,
    validate_downloaded_video_file,
)
from .media_download_generic import download_direct_video_candidate
from .types import CapturedVideo, DownloadedVideoAssets


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
        "has_video": False,
        "has_audio": False,
        "duration_sec": None,
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
                source_url=video_url,
                temp_path=temp_path,
                size_bytes=size_bytes,
                content_type="video/mp4",
                metadata={
                    "has_video": probe.has_video,
                    "has_audio": probe.has_audio,
                    "duration_sec": probe.duration_sec,
                    "extraction_strategy": "x_hls_ffmpeg",
                    "failure_reason": probe.failure_reason,
                    "probe": probe.to_dict(),
                },
            ),
            attempt,
        )
    except Exception as exc:
        attempt["result"] = "error"
        attempt["reason"] = str(exc)
        delete_temp_file(temp_path)
        return None, attempt


def download_x_video_assets(
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
            video_url = candidate["url"]
            if len(result.assets) >= settings.CAPTURE_MAX_VIDEOS:
                break
            if is_x_hls_playlist_url(video_url):
                captured_video, attempt = remux_x_hls_to_mp4(video_url, settings.CAPTURE_MAX_VIDEO_BYTES)
                attempt["candidate_sources"] = candidate.get("sources", [])
                attempt["candidate_media_kind"] = candidate.get("media_kind", "unknown")
            else:
                captured_video, attempt = download_direct_video_candidate(client, candidate, page_domain=page_domain)
            result.attempts.append(attempt)
            if captured_video is not None:
                result.assets.append(captured_video)
                break

    if result.assets:
        result.extraction_status = "success"
        result.extraction_strategy = "x_hls_ffmpeg"
    elif result.attempts:
        result.extraction_status = "failed"
        result.extraction_strategy = "download"
        result.failure_reason = result.attempts[-1].get("reason", "download_failed")
    return result
