from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


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
    translation: str = ""
    category: str = ""
    summary: str = ""
    payload: dict = field(default_factory=dict)


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
