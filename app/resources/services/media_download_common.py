from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from django.conf import settings

from .types import CaptureResult, MediaProbeResult


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
