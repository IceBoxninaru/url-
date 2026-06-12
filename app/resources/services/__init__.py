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

logger = logging.getLogger(__name__)


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
    configured = getattr(settings, "CAPTURE_FFMPEG_PATH", "").strip()
    if configured:
        candidate = Path(configured)
        if not candidate.is_absolute():
            candidate = settings.ROOT_DIR / candidate
        if candidate.exists() and candidate.is_file():
            return str(candidate)
    try:
        import imageio_ffmpeg
    except Exception:
        imageio_ffmpeg = None
    if imageio_ffmpeg is not None:
        try:
            return imageio_ffmpeg.get_ffmpeg_exe()
        except Exception:
            pass
    return shutil.which("ffmpeg")


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


def validate_downloaded_video_file(path: Path) -> tuple[bool, MediaProbeResult, str]:
    probe = MediaProbe.probe_file(path)
    if probe.failure_reason:
        if probe.failure_reason == "ffprobe_unavailable":
            return True, probe, ""
        return False, probe, probe.failure_reason
    if not probe.has_video:
        return False, probe, "missing_video_stream"
    if probe.duration_sec is not None and probe.duration_sec <= 0:
        return False, probe, "invalid_duration"
    return True, probe, ""


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


def download_instagram_candidate_without_probe(client: httpx.Client, candidate: dict) -> tuple[CapturedVideo | None, dict]:
    temp_path: Path | None = None
    attempt = {
        "candidate_url": candidate.get("url", ""),
        "candidate_sources": candidate.get("sources", []),
        "candidate_media_kind": candidate.get("media_kind", "unknown"),
        "final_url": "",
        "mode": "instagram_direct_no_probe",
        "result": "skipped",
        "reason": "",
        "response_status": candidate.get("response_status"),
        "content_type": candidate.get("content_type", ""),
        "content_length": candidate.get("content_length", ""),
        "output_size_bytes": 0,
        "has_video": True,
        "has_audio": None,
        "duration_sec": None,
        "extraction_status": "success",
        "extraction_strategy": "instagram_direct_no_probe",
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

            attempt["result"] = "saved"
            attempt["output_size_bytes"] = size_bytes
            return (
                CapturedVideo(
                    source_url=str(response.url),
                    temp_path=temp_path,
                    size_bytes=size_bytes,
                    content_type=content_type,
                    metadata={
                        "has_video": True,
                        "has_audio": None,
                        "duration_sec": None,
                        "extraction_strategy": "instagram_direct_no_probe",
                        "failure_reason": "",
                        "probe": {
                            "has_video": None,
                            "has_audio": None,
                            "duration_sec": None,
                            "video_streams": None,
                            "audio_streams": None,
                            "format_name": "",
                            "probe_tool": "",
                            "failure_reason": "ffprobe_unavailable",
                            "raw": {},
                        },
                    },
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
        primary_candidates = [candidate for candidate in candidate_details if candidate.get("media_kind") != "audio"]
        with httpx.Client(
            follow_redirects=True,
            timeout=settings.CAPTURE_HTTP_TIMEOUT,
            headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
        ) as client:
            for candidate in primary_candidates:
                captured_video, attempt = download_instagram_candidate_without_probe(client, candidate)
                result.attempts.append(attempt)
                if captured_video is not None:
                    result.assets.append(captured_video)
                    result.extraction_status = "success"
                    result.extraction_strategy = "instagram_direct_no_probe"
                    result.selected_asset = captured_video.metadata
                    summarize_instagram_video_result(source_url, result, candidate_details, [])
                    return result
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
            if is_x_domain(page_domain) and is_x_hls_playlist_url(video_url):
                captured_video, attempt = remux_x_hls_to_mp4(video_url, settings.CAPTURE_MAX_VIDEO_BYTES)
                attempt["candidate_sources"] = candidate.get("sources", [])
                attempt["candidate_media_kind"] = candidate.get("media_kind", "unknown")
                result.attempts.append(attempt)
                if captured_video is not None:
                    result.assets.append(captured_video)
                    if is_scoped_social_capture_domain(page_domain):
                        break
                continue
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
                        result.attempts.append(attempt)
                        continue

                    attempt["result"] = "saved"
                    result.assets.append(
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
    if result.assets:
        result.extraction_status = "success"
        result.extraction_strategy = "x_hls_ffmpeg" if is_x_domain(page_domain) else "direct"
    elif result.attempts:
        result.extraction_status = "failed"
        result.extraction_strategy = "download"
        result.failure_reason = result.attempts[-1].get("reason", "download_failed")
    return result


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
