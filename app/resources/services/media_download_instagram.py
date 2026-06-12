from __future__ import annotations

import logging
import subprocess
from pathlib import Path

import httpx
from django.conf import settings

from .media_discovery import (
    build_media_candidate,
    collect_video_urls,
    explain_media_candidate_skip,
    guess_video_extension,
    is_probable_media_url,
    is_probable_video_url,
    merge_media_candidates,
)
from .media_download_common import (
    MediaProbe,
    create_temp_download_path,
    delete_temp_file,
    get_ffmpeg_executable,
    get_ffprobe_executable,
)
from .types import CapturedVideo, DownloadedMediaCandidate, DownloadedVideoAssets

logger = logging.getLogger(__name__)


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
