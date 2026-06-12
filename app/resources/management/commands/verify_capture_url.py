from __future__ import annotations

import json
from urllib.parse import urlparse

from django.core.management.base import BaseCommand, CommandError

from resources.models import Resource
from resources.services import cleanup_capture_result, normalize_url
from resources.services.capture import choose_capture_result, fetch_with_http, fetch_with_playwright
from resources.services.media_downloads import get_ffmpeg_executable, get_ffprobe_executable


def build_ephemeral_resource(url: str, *, capture_images: bool, capture_videos: bool) -> Resource:
    normalized = normalize_url(url)
    domain = urlparse(normalized).netloc
    return Resource(
        original_url=url,
        normalized_url=normalized,
        domain=domain,
        title_manual="Capture verification",
        capture_images=capture_images,
        capture_videos=capture_videos,
        search_only=False,
    )


def summarize_video(video) -> dict:
    return {
        "source_url": video.source_url,
        "size_bytes": video.size_bytes,
        "content_type": video.content_type,
        "metadata": video.metadata,
    }


def summarize_result(resource: Resource, result, *, include_details: bool) -> dict:
    video_capture = (result.response_payload or {}).get("video_capture") or {}
    summary = {
        "url": resource.original_url,
        "normalized_url": resource.normalized_url,
        "domain": resource.domain,
        "fetch_url": result.fetch_url,
        "fetch_method": str(result.fetch_method),
        "http_status": result.http_status,
        "is_success": result.is_success,
        "error_message": result.error_message,
        "deleted_like": result.deleted_like,
        "page_title": result.metadata.get("page_title", ""),
        "text_chars": len(result.extracted_text or ""),
        "html_chars": len(result.html or ""),
        "screenshot_bytes": len(result.screenshot_bytes or b""),
        "image_count": len(result.captured_images),
        "video_count": len(result.captured_videos),
        "videos": [summarize_video(video) for video in result.captured_videos],
        "video_capture": {
            "extraction_status": video_capture.get("extraction_status", ""),
            "extraction_strategy": video_capture.get("extraction_strategy", ""),
            "failure_reason": video_capture.get("failure_reason", ""),
            "candidate_count": len(video_capture.get("candidate_urls") or []),
            "attempt_count": len(video_capture.get("attempts") or []),
            "skip_count": len(video_capture.get("skip_logs") or []),
            "summary": video_capture.get("summary") or {},
            "ffmpeg_available": bool(get_ffmpeg_executable()),
            "ffprobe_available": bool(get_ffprobe_executable()),
        },
        "playwright_auth": {
            "storage_state_used": bool((result.response_payload or {}).get("storage_state_used")),
            "profile_path_used": bool((result.response_payload or {}).get("profile_path_used")),
        },
    }
    if include_details:
        summary["video_capture"].update(
            {
                "candidate_urls": video_capture.get("candidate_urls") or video_capture.get("collected_urls") or [],
                "candidate_details": video_capture.get("candidate_details") or [],
                "attempts": video_capture.get("attempts") or [],
                "skip_logs": video_capture.get("skip_logs") or [],
                "observed_urls": video_capture.get("observed_urls") or [],
                "observed_media_requests": video_capture.get("observed_media_requests") or [],
                "observed_media_responses": video_capture.get("observed_media_responses") or [],
            }
        )
    return summary


class Command(BaseCommand):
    help = "Run a non-persistent capture verification for one or more URLs and print JSON summaries."

    def add_arguments(self, parser):
        parser.add_argument("urls", nargs="+")
        parser.add_argument("--method", choices=["auto", "http", "playwright"], default="auto")
        parser.add_argument("--no-images", action="store_true")
        parser.add_argument("--no-videos", action="store_true")
        parser.add_argument("--include-details", action="store_true")
        parser.add_argument("--require-success", action="store_true")
        parser.add_argument("--require-video", action="store_true")
        parser.add_argument("--indent", type=int, default=2)
        parser.add_argument("--unicode", action="store_true", help="Emit unescaped Unicode JSON.")

    def handle(self, *args, **options):
        capture_images = not options["no_images"]
        capture_videos = not options["no_videos"]
        include_details = options["include_details"]
        method = options["method"]
        summaries = []
        failed = False

        for url in options["urls"]:
            resource = build_ephemeral_resource(url, capture_images=capture_images, capture_videos=capture_videos)
            if method == "http":
                result = fetch_with_http(
                    resource.normalized_url,
                    capture_images=resource.capture_images,
                    capture_videos=resource.capture_videos,
                    page_domain=resource.domain,
                )
            elif method == "playwright":
                result = fetch_with_playwright(
                    resource.normalized_url,
                    capture_images=resource.capture_images,
                    capture_videos=resource.capture_videos,
                    page_domain=resource.domain,
                )
            else:
                result = choose_capture_result(resource)

            try:
                summary = summarize_result(resource, result, include_details=include_details)
                summary["ok"] = True
                if options["require_success"] and not result.is_success:
                    summary["ok"] = False
                    summary["failure"] = "capture_not_successful"
                if options["require_video"] and not result.captured_videos:
                    summary["ok"] = False
                    summary["failure"] = "missing_captured_video"
                failed = failed or not summary["ok"]
                summaries.append(summary)
            finally:
                cleanup_capture_result(result)

        payload = {
            "ok": not failed,
            "count": len(summaries),
            "results": summaries,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=not options["unicode"], indent=options["indent"]))
        if failed:
            raise CommandError("capture verification failed")
