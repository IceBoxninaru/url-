from datetime import date, timedelta
import builtins
from io import StringIO
import shutil
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch
import os

from django.conf import settings
from django.core.management import call_command
from django.test import Client, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from jobs.models import CaptureJob, JobStatus, JobType
from jobs.services import run_one_job
from resources.forms import NOTE_TEMPLATE_CHOICES, SAVE_REASON_CUSTOM_VALUE
from resources.models import InterestFeedback, LinkStatus, Resource, ResourceStatus, ReviewState
from resources.services import (
    CaptureResult,
    CapturedImage,
    CapturedVideo,
    LinkCheckResult,
    MediaProbeResult,
    check_resource_link_status,
    choose_capture_result,
    collect_image_urls,
    collect_video_urls,
    download_video_assets,
    enqueue_capture_job,
    filter_video_candidate_urls,
    get_ffmpeg_executable,
    get_ffprobe_executable,
    get_playwright_profile_path,
    get_playwright_storage_state_path,
    is_observed_video_response,
    normalize_url,
    normalize_media_candidate_url,
    run_ai_pipeline,
    resolve_storage_state_path,
    translate_text_to_japanese,
)
from snapshots.models import FetchMethod, Snapshot
from tags.models import Tag


class StorageOverrideMixin:
    def setUp(self):
        super().setUp()
        self.storage_base = Path(tempfile.mkdtemp(prefix="url-archive-test-", dir=settings.ROOT_DIR))
        self.settings_override = override_settings(
            HTML_STORAGE_ROOT=self.storage_base / "html",
            TEXT_STORAGE_ROOT=self.storage_base / "text",
            JSON_STORAGE_ROOT=self.storage_base / "json",
            SCREENSHOT_STORAGE_ROOT=self.storage_base / "screenshots",
            IMAGE_STORAGE_ROOT=self.storage_base / "images",
            VIDEO_STORAGE_ROOT=self.storage_base / "videos",
        )
        self.settings_override.enable()
        self.addCleanup(self.settings_override.disable)
        self.addCleanup(lambda: shutil.rmtree(self.storage_base, ignore_errors=True))


class FakeStreamResponse:
    def __init__(self, url: str, *, content_type: str, content: bytes, status_code: int = 200):
        self.url = url
        self.status_code = status_code
        self.headers = {
            "content-type": content_type,
            "content-length": str(len(content)),
        }
        self._content = content

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def iter_bytes(self):
        yield self._content


class FakeHttpClient:
    def __init__(self, responses: dict[str, FakeStreamResponse]):
        self.responses = responses

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def stream(self, method: str, url: str):
        return self.responses[url]


class URLNormalizationTests(TestCase):
    def test_normalize_url_removes_tracking_fragment_and_normalizes_domain(self):
        normalized = normalize_url("HTTPS://Example.COM/path/?utm_source=x&id=1#frag")
        self.assertEqual(normalized, "https://example.com/path?id=1")

    def test_normalize_url_adds_scheme_when_missing(self):
        normalized = normalize_url("example.com/articles/1/")
        self.assertEqual(normalized, "https://example.com/articles/1")

    def test_collect_image_urls_filters_non_content_assets(self):
        html = """
        <html>
            <head>
                <meta property="og:image" content="https://abs.twimg.com/rweb/ssr/default/v2/og/image.png">
            </head>
            <body>
                <img src="https://pbs.twimg.com/profile_images/avatar.jpg">
                <img src="https://abs-0.twimg.com/emoji/v2/svg/1f60e.svg">
                <img src="https://pbs.twimg.com/media/abc123?format=jpg&amp;name=small">
                <img data-src="https://cdn.example.com/content/photo.png">
            </body>
        </html>
        """

        urls = collect_image_urls(html, "https://x.com/example/status/1")

        self.assertEqual(
            urls,
            [
                "https://pbs.twimg.com/media/abc123?format=jpg&name=small",
                "https://cdn.example.com/content/photo.png",
            ],
        )

    def test_collect_video_urls_skips_blob_urls_and_collects_meta_and_sources(self):
        html = """
        <html>
            <head>
                <meta property="og:video" content="https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/sample.mp4">
            </head>
            <body>
                <video src="blob:https://x.com/example"></video>
                <video>
                    <source src="/media/reel.mp4">
                </video>
            </body>
        </html>
        """

        urls = collect_video_urls(html, "https://www.instagram.com/p/example/")

        self.assertEqual(
            urls,
            [
                "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/sample.mp4",
                "https://www.instagram.com/media/reel.mp4",
            ],
        )

    def test_normalize_media_candidate_url_removes_partial_range_query(self):
        normalized = normalize_media_candidate_url(
            "https://scontent.cdninstagram.com/o1/v/t16/f2/m86/ABCDEFG.mp4?_nc_ht=scontent.cdninstagram.com&bytestart=0&byteend=999",
            "https://www.instagram.com/reel/example/",
        )

        self.assertEqual(
            normalized,
            "https://scontent.cdninstagram.com/o1/v/t16/f2/m86/ABCDEFG.mp4?_nc_ht=scontent.cdninstagram.com",
        )

    def test_collect_video_urls_extracts_escaped_x_urls_from_script_payloads(self):
        html = """
        <html>
            <body>
                <script>
                    {"variants":[
                        {"url":"https:\\/\\/video.twimg.com\\/ext_tw_video\\/123\\/pu\\/pl\\/playlist.m3u8?tag=12\\u0026variant_version=1"},
                        {"url":"https:\\/\\/video.twimg.com\\/ext_tw_video\\/123\\/pu\\/vid\\/avc1\\/720x1280\\/sample.mp4?tag=12\\u0026container=fmp4"}
                    ]}
                </script>
            </body>
        </html>
        """

        urls = collect_video_urls(html, "https://x.com/example/status/1", page_domain="x.com")

        self.assertEqual(
            urls,
            [
                "https://video.twimg.com/ext_tw_video/123/pu/pl/playlist.m3u8?tag=12&variant_version=1",
                "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/720x1280/sample.mp4?tag=12&container=fmp4",
            ],
        )

    def test_collect_video_urls_extracts_url_encoded_x_urls(self):
        html = (
            "https%3A%2F%2Fvideo.twimg.com%2Ftweet_video%2Fsample.mp4"
            "%3Ftag%3D12%26container%3Dfmp4"
        )

        urls = collect_video_urls(html, "https://x.com/example/status/1", page_domain="x.com")

        self.assertEqual(
            urls,
            ["https://video.twimg.com/tweet_video/sample.mp4?tag=12&container=fmp4"],
        )

    def test_filter_video_candidate_urls_for_x_prefers_master_playlist_and_skips_init_fragment(self):
        candidates = [
            "https://video.twimg.com/ext_tw_video/123/pu/pl/playlist.m3u8?tag=12&variant_version=1",
            "https://video.twimg.com/ext_tw_video/123/pu/pl/avc1/1280x720/playlist.m3u8?tag=12",
            "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/0/0/init.mp4?tag=12",
            "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/0/0/720x1280/partial.mp4",
            "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/320x568/low.mp4?tag=12",
            "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/720x1280/high.mp4?tag=12",
        ]

        self.assertEqual(
            filter_video_candidate_urls(candidates, "x.com"),
            [
                "https://video.twimg.com/ext_tw_video/123/pu/pl/playlist.m3u8?tag=12&variant_version=1",
                "https://video.twimg.com/ext_tw_video/123/pu/pl/avc1/1280x720/playlist.m3u8?tag=12",
                "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/720x1280/high.mp4?tag=12",
                "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/320x568/low.mp4?tag=12",
            ],
        )

    def test_filter_video_candidate_urls_for_instagram_keeps_multiple_candidates(self):
        candidates = [
            "https://scontent.cdninstagram.com/o1/v/t16/f2/m86/320x568/low.mp4",
            "https://scontent.cdninstagram.com/o1/v/t16/f2/m86/720x1280/high.mp4",
        ]

        self.assertEqual(
            filter_video_candidate_urls(candidates, "instagram.com"),
            [
                "https://scontent.cdninstagram.com/o1/v/t16/f2/m86/720x1280/high.mp4",
                "https://scontent.cdninstagram.com/o1/v/t16/f2/m86/320x568/low.mp4",
            ],
        )

    def test_filter_video_candidate_urls_for_x_accepts_tweet_video_mp4(self):
        candidates = ["https://video.twimg.com/tweet_video/sample.mp4"]

        self.assertEqual(
            filter_video_candidate_urls(candidates, "x.com"),
            ["https://video.twimg.com/tweet_video/sample.mp4"],
        )

    def test_is_observed_video_response_for_x_accepts_playlist_and_rejects_init_fragment(self):
        self.assertTrue(
            is_observed_video_response(
                "https://video.twimg.com/ext_tw_video/123/pu/pl/playlist.m3u8?tag=12&variant_version=1",
                "x.com",
                content_type="application/x-mpegURL",
            )
        )
        self.assertFalse(
            is_observed_video_response(
                "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/0/0/init.mp4?tag=12",
                "x.com",
                content_type="video/mp4",
            )
        )
        self.assertFalse(
            is_observed_video_response(
                "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/0/0/720x1280/partial.mp4",
                "x.com",
                content_type="video/mp4",
            )
        )

    @override_settings(CAPTURE_X_STORAGE_STATE_PATH="storage/auth/x.json")
    def test_resolve_storage_state_path_expands_relative_path_under_root(self):
        resolved = resolve_storage_state_path(settings.CAPTURE_X_STORAGE_STATE_PATH)
        self.assertEqual(resolved, settings.ROOT_DIR / "storage" / "auth" / "x.json")

    @override_settings(CAPTURE_X_STORAGE_STATE_PATH="storage/auth/x.json")
    def test_get_playwright_storage_state_path_returns_none_when_file_missing(self):
        self.assertIsNone(get_playwright_storage_state_path("x.com"))

    def test_get_ffmpeg_executable_falls_back_to_system_binary(self):
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "imageio_ffmpeg":
                raise ImportError("missing imageio ffmpeg")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            with patch("resources.services.media_downloads.shutil.which", return_value="/usr/bin/ffmpeg"):
                self.assertEqual(get_ffmpeg_executable(), "/usr/bin/ffmpeg")

    @override_settings(CAPTURE_X_STORAGE_STATE_PATH="storage/auth/x.json")
    def test_get_playwright_storage_state_path_returns_file_for_x(self):
        auth_dir = settings.ROOT_DIR / "storage" / "auth"
        auth_dir.mkdir(parents=True, exist_ok=True)
        target = auth_dir / "x.json"
        target.write_text("{}", encoding="utf-8")
        self.addCleanup(lambda: target.unlink(missing_ok=True))

        self.assertEqual(get_playwright_storage_state_path("x.com"), target)
        self.assertIsNone(get_playwright_storage_state_path("instagram.com"))

    @override_settings(CAPTURE_X_PROFILE_PATH="storage/auth/x_profile")
    def test_get_playwright_profile_path_returns_none_when_directory_missing_or_empty(self):
        auth_dir = settings.ROOT_DIR / "storage" / "auth"
        auth_dir.mkdir(parents=True, exist_ok=True)
        profile_dir = auth_dir / "x_profile"
        profile_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(profile_dir, ignore_errors=True))

        self.assertIsNone(get_playwright_profile_path("x.com"))

    @override_settings(CAPTURE_X_PROFILE_PATH="storage/auth/x_profile")
    def test_get_playwright_profile_path_returns_directory_for_non_empty_x_profile(self):
        auth_dir = settings.ROOT_DIR / "storage" / "auth"
        auth_dir.mkdir(parents=True, exist_ok=True)
        profile_dir = auth_dir / "x_profile"
        profile_dir.mkdir(parents=True, exist_ok=True)
        marker = profile_dir / "Default"
        marker.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(profile_dir, ignore_errors=True))

        self.assertEqual(get_playwright_profile_path("x.com"), profile_dir)
        self.assertIsNone(get_playwright_profile_path("instagram.com"))

    @override_settings(CAPTURE_FFPROBE_PATH="storage/tools/ffprobe.exe")
    def test_get_ffprobe_executable_uses_configured_path(self):
        tools_dir = settings.ROOT_DIR / "storage" / "tools"
        tools_dir.mkdir(parents=True, exist_ok=True)
        target = tools_dir / "ffprobe.exe"
        target.write_text("", encoding="utf-8")
        self.addCleanup(lambda: target.unlink(missing_ok=True))

        self.assertEqual(get_ffprobe_executable(), str(target))


class ResourceViewTests(StorageOverrideMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.client = Client()
        self.tag_a = Tag.objects.create(name="alpha", color="#123456", sort_order=10)
        self.tag_b = Tag.objects.create(name="beta", color="#654321", sort_order=20)

    def test_get_create_page(self):
        response = self.client.get(reverse("resources:create"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'name="new_tags"', html=False)
        self.assertContains(response, 'name="interest_labels"', html=False)
        self.assertEqual(response.context["form"].fields["new_tags"].widget.__class__.__name__, "Textarea")

    def test_api_recent_resources_returns_latest_visible_urls(self):
        older = Resource.objects.create(
            original_url="https://example.com/older",
            normalized_url="https://example.com/older",
            domain="example.com",
            title_manual="Older Entry",
        )
        newer = Resource.objects.create(
            original_url="https://example.com/newer",
            normalized_url="https://example.com/newer",
            domain="example.com",
            title_manual="Newer Entry",
        )
        Resource.objects.create(
            original_url="https://example.com/search-only",
            normalized_url="https://example.com/search-only",
            domain="example.com",
            title_manual="Search Only Entry",
            search_only=True,
        )

        response = self.client.get(reverse("resources:api_recent"), {"limit": 5})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 2)
        self.assertEqual([item["id"] for item in payload["items"]], [newer.id, older.id])
        self.assertEqual(payload["items"][0]["title"], "Newer Entry")

    def test_api_search_resources_returns_matching_urls(self):
        resource = Resource.objects.create(
            original_url="https://example.com/keyword",
            normalized_url="https://example.com/keyword",
            domain="example.com",
            title_manual="Keyword Entry",
        )
        resource.tags.add(self.tag_a)
        snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="Keyword page",
            extracted_text="keyword body",
            ai_summary="summary text",
        )
        resource.latest_snapshot = snapshot
        resource.save(update_fields=["latest_snapshot"])
        Resource.objects.create(
            original_url="https://example.com/other",
            normalized_url="https://example.com/other",
            domain="example.com",
            title_manual="Other Entry",
        )

        response = self.client.get(reverse("resources:api_search"), {"q": "keyword"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["query"], "keyword")
        self.assertEqual(payload["total_count"], 1)
        self.assertEqual(payload["items"][0]["id"], resource.id)
        self.assertEqual(payload["items"][0]["latest_snapshot"]["page_title"], "Keyword page")
        self.assertEqual(payload["items"][0]["tags"][0]["name"], "alpha")

    def test_api_search_resources_matches_tag_names(self):
        resource = Resource.objects.create(
            original_url="https://example.com/tagged",
            normalized_url="https://example.com/tagged",
            domain="example.com",
            title_manual="Tagged Entry",
        )
        resource.tags.add(self.tag_a)
        Resource.objects.create(
            original_url="https://example.com/other-tagged",
            normalized_url="https://example.com/other-tagged",
            domain="example.com",
            title_manual="Other Tagged Entry",
        )

        response = self.client.get(reverse("resources:api_search"), {"q": "alpha"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["total_count"], 1)
        self.assertEqual(payload["items"][0]["id"], resource.id)


    def test_api_ai_search_resources_defaults_to_today_search_only(self):
        visible = Resource.objects.create(
            original_url="https://example.com/visible",
            normalized_url="https://example.com/visible",
            domain="example.com",
            title_manual="Visible Entry",
        )
        Resource.objects.create(
            original_url="https://example.com/ai",
            normalized_url="https://example.com/ai",
            domain="example.com",
            title_manual="AI Entry",
            search_only=True,
        )
        old = Resource.objects.create(
            original_url="https://example.com/old-ai",
            normalized_url="https://example.com/old-ai",
            domain="example.com",
            title_manual="Old AI Entry",
            search_only=True,
        )
        Resource.objects.filter(pk=old.pk).update(created_at=timezone.now() - timedelta(days=2))

        response = self.client.get(reverse("resources:api_ai_search"), {"limit": 20})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["items"][0]["title"], "AI Entry")
        self.assertEqual(payload["items"][0]["sendable_url"], "https://example.com/ai")
        self.assertNotEqual(payload["items"][0]["resource_id"], visible.id)

    def test_api_ai_search_resources_returns_source_and_query_from_payload(self):
        resource = Resource.objects.create(
            original_url="https://example.com/ai-payload",
            normalized_url="https://example.com/ai-payload",
            domain="example.com",
            title_manual="AI Payload",
            search_only=True,
        )
        snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            ai_summary="summary text",
            ai_translation="translation text",
            ai_payload={"source": "ai-search", "search_query": "agent tools"},
        )
        resource.latest_snapshot = snapshot
        resource.save(update_fields=["latest_snapshot"])

        response = self.client.get(reverse("resources:api_search_only"), {"query": "Payload", "limit": 5})

        self.assertEqual(response.status_code, 200)
        item = response.json()["items"][0]
        self.assertEqual(item["resource_id"], resource.id)
        self.assertEqual(item["summary"], "summary text")
        self.assertEqual(item["translation"], "translation text")
        self.assertEqual(item["source"], "ai-search")
        self.assertEqual(item["search_query"], "agent tools")

    def test_api_resource_detail_includes_latest_snapshot_media_assets(self):
        resource = Resource.objects.create(
            original_url="https://example.com/media",
            normalized_url="https://example.com/media",
            domain="example.com",
            title_manual="Media Entry",
            note="private note",
        )
        image_name = "snapshot_0001_img_01.jpg"
        video_name = "snapshot_0001_vid_01.mp4"
        image_dir = self.storage_base / "images" / f"resource_{resource.id:04d}"
        video_dir = self.storage_base / "videos" / f"resource_{resource.id:04d}"
        image_dir.mkdir(parents=True, exist_ok=True)
        video_dir.mkdir(parents=True, exist_ok=True)
        (image_dir / image_name).write_bytes(b"fake-image")
        (video_dir / video_name).write_bytes(b"fake-video")
        snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="Media page",
            extracted_text="Full extracted text",
            ai_translation="Full translation",
            image_assets=[
                {
                    "source_url": "https://example.com/image.jpg",
                    "path": f"storage/images/resource_{resource.id:04d}/{image_name}",
                    "content_type": "image/jpeg",
                    "size_bytes": 10,
                }
            ],
            video_assets=[
                {
                    "source_url": "https://example.com/video.mp4",
                    "path": f"storage/videos/resource_{resource.id:04d}/{video_name}",
                    "content_type": "video/mp4",
                    "size_bytes": 10,
                }
            ],
        )
        resource.latest_snapshot = snapshot
        resource.save(update_fields=["latest_snapshot"])

        response = self.client.get(reverse("resources:api_detail", args=[resource.id]))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        latest_snapshot = payload["latest_snapshot"]
        self.assertEqual(payload["title"], "Media Entry")
        self.assertEqual(payload["note"], "private note")
        self.assertEqual(latest_snapshot["image_count"], 1)
        self.assertEqual(latest_snapshot["video_count"], 1)
        self.assertEqual([item["media_type"] for item in latest_snapshot["media_assets"]], ["image", "video"])
        self.assertEqual(
            latest_snapshot["media_assets"][0]["url"],
            f"/storage/images/resource_{resource.id:04d}/{image_name}",
        )

    def test_post_create_creates_resource_and_capture_job(self):
        response = self.client.post(
            reverse("resources:create"),
            {
                "original_url": "example.com/post/1?utm_source=mail",
                "title_manual": "Manual title",
                "note": "Important note",
                "favorite": "on",
                "capture_images": "on",
                "capture_videos": "on",
                "interest_labels": ["reference", "deep_dive"],
                "tags": [self.tag_a.id, self.tag_b.id],
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], reverse("resources:list"))
        resource = Resource.objects.get()
        self.assertEqual(resource.original_url, "example.com/post/1?utm_source=mail")
        self.assertEqual(resource.normalized_url, "https://example.com/post/1")
        self.assertEqual(resource.domain, "example.com")
        self.assertTrue(resource.favorite)
        self.assertTrue(resource.capture_images)
        self.assertTrue(resource.capture_videos)
        self.assertFalse(resource.search_only)
        self.assertEqual(resource.interest_labels, ["reference", "deep_dive"])
        self.assertEqual(resource.tags.count(), 2)
        job = CaptureJob.objects.get()
        self.assertEqual(job.job_type, JobType.CAPTURE)
        self.assertEqual(job.status, JobStatus.QUEUED)

    def test_post_create_creates_new_tags_and_search_only_setting(self):
        response = self.client.post(
            reverse("resources:create"),
            {
                "original_url": "https://example.com/post/search-only",
                "title_manual": "Search Only",
                "search_only": "on",
                "capture_images": "on",
                "new_tags": "gamma\ndelta",
            },
        )

        self.assertEqual(response.status_code, 302)
        resource = Resource.objects.get()
        self.assertTrue(resource.search_only)
        self.assertEqual(
            list(resource.tags.order_by("name").values_list("name", flat=True)),
            ["delta", "gamma"],
        )
        self.assertTrue(Tag.objects.filter(name="gamma").exists())
        self.assertTrue(Tag.objects.filter(name="delta").exists())

    def test_post_create_duplicate_url_shows_registered_message(self):
        Resource.objects.create(
            original_url="https://example.com/post/1",
            normalized_url="https://example.com/post/1",
            domain="example.com",
            title_manual="Existing",
        )

        response = self.client.post(
            reverse("resources:create"),
            {
                "original_url": "https://example.com/post/1?utm_source=mail#frag",
                "title_manual": "Duplicate",
                "note": "",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(Resource.objects.count(), 1)
        self.assertContains(response, "このURLは登録済みです。")

    def test_post_create_saves_structured_reason_next_action_and_review_state(self):
        template_value = NOTE_TEMPLATE_CHOICES[0][0]

        response = self.client.post(
            reverse("resources:create"),
            {
                "original_url": "https://example.com/post/template",
                "title_manual": "Template",
                "save_reason": template_value,
                "next_action": "実装で試す",
                "recheck_at": "2026-05-01",
                "capture_images": "on",
                "capture_videos": "on",
                "review_state": ReviewState.NEEDS_REVIEW,
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], reverse("resources:list"))
        resource = Resource.objects.get()
        self.assertEqual(resource.save_reason, template_value)
        self.assertEqual(resource.next_action, "実装で試す")
        self.assertEqual(resource.recheck_at, date(2026, 5, 1))
        self.assertEqual(resource.review_state, ReviewState.NEEDS_REVIEW)

    def test_post_create_accepts_custom_save_reason(self):
        response = self.client.post(
            reverse("resources:create"),
            {
                "original_url": "https://example.com/post/custom-reason",
                "title_manual": "Custom Reason",
                "save_reason": SAVE_REASON_CUSTOM_VALUE,
                "custom_save_reason": "授業メモ",
                "capture_images": "on",
                "capture_videos": "on",
            },
        )

        self.assertEqual(response.status_code, 302)
        resource = Resource.objects.get()
        self.assertEqual(resource.save_reason, "授業メモ")

    def test_filter_by_query_tag_and_favorite(self):
        resource_a = Resource.objects.create(
            original_url="https://example.com/a",
            normalized_url="https://example.com/a",
            domain="example.com",
            title_manual="Alpha Entry",
            favorite=True,
        )
        resource_a.tags.add(self.tag_a)
        Snapshot.objects.create(
            resource=resource_a,
            snapshot_no=1,
            fetch_url=resource_a.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="Alpha page",
            extracted_text="keyword document",
        )
        resource_a.latest_snapshot = resource_a.snapshots.first()
        resource_a.save(update_fields=["latest_snapshot"])

        resource_b = Resource.objects.create(
            original_url="https://example.com/b",
            normalized_url="https://example.com/b",
            domain="example.com",
            title_manual="Beta Entry",
            favorite=False,
        )
        resource_b.tags.add(self.tag_b)

        response = self.client.get(
            reverse("resources:list"),
            {"q": "keyword", "tags": [self.tag_a.id], "favorite_only": "on"},
        )

        self.assertEqual(response.status_code, 200)
        resources = list(response.context["resources"])
        self.assertEqual(resources, [resource_a])

    def test_filter_by_domain(self):
        resource_a = Resource.objects.create(
            original_url="https://x.com/example/status/1",
            normalized_url="https://x.com/example/status/1",
            domain="x.com",
            title_manual="X Entry",
        )
        Resource.objects.create(
            original_url="https://note.com/example/n/1",
            normalized_url="https://note.com/example/n/1",
            domain="note.com",
            title_manual="Note Entry",
        )

        response = self.client.get(
            reverse("resources:list"),
            {"domain": "x.com"},
        )

        self.assertEqual(response.status_code, 200)
        resources = list(response.context["resources"])
        self.assertEqual(resources, [resource_a])

    def test_filter_by_review_state(self):
        resource_a = Resource.objects.create(
            original_url="https://example.com/review-a",
            normalized_url="https://example.com/review-a",
            domain="example.com",
            title_manual="Needs review",
            review_state=ReviewState.NEEDS_REVIEW,
        )
        Resource.objects.create(
            original_url="https://example.com/review-b",
            normalized_url="https://example.com/review-b",
            domain="example.com",
            title_manual="Done",
            review_state=ReviewState.DONE,
        )

        response = self.client.get(
            reverse("resources:list"),
            {"review_state": ReviewState.NEEDS_REVIEW},
        )

        self.assertEqual(response.status_code, 200)
        resources = list(response.context["resources"])
        self.assertEqual(resources, [resource_a])

    def test_filter_by_multiple_tags_requires_all_selected_tags(self):
        resource_a = Resource.objects.create(
            original_url="https://example.com/a",
            normalized_url="https://example.com/a",
            domain="example.com",
            title_manual="Alpha Entry",
        )
        resource_a.tags.add(self.tag_a, self.tag_b)

        resource_b = Resource.objects.create(
            original_url="https://example.com/b",
            normalized_url="https://example.com/b",
            domain="example.com",
            title_manual="Beta Entry",
        )
        resource_b.tags.add(self.tag_a)

        response = self.client.get(
            reverse("resources:list"),
            {"tags": [self.tag_a.id, self.tag_b.id]},
        )

        self.assertEqual(response.status_code, 200)
        resources = list(response.context["resources"])
        self.assertEqual(resources, [resource_a])

    def test_filter_by_save_reason(self):
        resource_a = Resource.objects.create(
            original_url="https://example.com/save-reason-a",
            normalized_url="https://example.com/save-reason-a",
            domain="example.com",
            title_manual="Read Later",
            save_reason=NOTE_TEMPLATE_CHOICES[0][0],
        )
        Resource.objects.create(
            original_url="https://example.com/save-reason-b",
            normalized_url="https://example.com/save-reason-b",
            domain="example.com",
            title_manual="Shopping",
            save_reason=NOTE_TEMPLATE_CHOICES[-1][0],
        )

        response = self.client.get(
            reverse("resources:list"),
            {"save_reason": NOTE_TEMPLATE_CHOICES[0][0]},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(list(response.context["resources"]), [resource_a])

    def test_filter_by_recheck_due_only(self):
        due_resource = Resource.objects.create(
            original_url="https://example.com/recheck-due",
            normalized_url="https://example.com/recheck-due",
            domain="example.com",
            title_manual="Due",
            recheck_at=timezone.localdate() - timedelta(days=1),
        )
        Resource.objects.create(
            original_url="https://example.com/recheck-future",
            normalized_url="https://example.com/recheck-future",
            domain="example.com",
            title_manual="Future",
            recheck_at=timezone.localdate() + timedelta(days=3),
        )
        Resource.objects.create(
            original_url="https://example.com/recheck-none",
            normalized_url="https://example.com/recheck-none",
            domain="example.com",
            title_manual="None",
        )

        response = self.client.get(reverse("resources:list"), {"recheck_due_only": "on"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(list(response.context["resources"]), [due_resource])

    def test_search_only_resource_is_hidden_until_query_matches(self):
        visible_resource = Resource.objects.create(
            original_url="https://example.com/visible",
            normalized_url="https://example.com/visible",
            domain="example.com",
            title_manual="Visible Entry",
        )
        hidden_resource = Resource.objects.create(
            original_url="https://example.com/hidden",
            normalized_url="https://example.com/hidden",
            domain="example.com",
            title_manual="Hidden Entry",
            search_only=True,
        )

        list_response = self.client.get(reverse("resources:list"))
        self.assertEqual(list_response.status_code, 200)
        self.assertEqual(list(list_response.context["resources"]), [visible_resource])
        self.assertNotContains(list_response, "Hidden Entry")

        search_response = self.client.get(reverse("resources:list"), {"q": "Hidden"})
        self.assertEqual(search_response.status_code, 200)
        self.assertEqual(list(search_response.context["resources"]), [hidden_resource])
        self.assertContains(search_response, "検索専用")

    def test_ai_search_list_shows_only_search_only_resources(self):
        Resource.objects.create(
            original_url="https://example.com/visible",
            normalized_url="https://example.com/visible",
            domain="example.com",
            title_manual="Visible Entry",
        )
        ai_resource = Resource.objects.create(
            original_url="https://example.com/ai-found",
            normalized_url="https://example.com/ai-found",
            domain="example.com",
            title_manual="AI Found Entry",
            search_only=True,
        )

        response = self.client.get(reverse("resources:ai_search_list"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(list(response.context["resources"]), [ai_resource])
        self.assertContains(response, "AI検索URL")
        self.assertContains(response, "AI Found Entry")
        self.assertContains(response, "保存日")
        self.assertContains(response, "興味あり")
        self.assertContains(response, "興味なし")
        self.assertContains(response, reverse("resources:interest_feedback", args=[ai_resource.id]))
        self.assertContains(response, reverse("resources:interest_label", args=[ai_resource.id]))
        self.assertContains(response, 'name="interest_label"', html=False)
        self.assertContains(response, "ラベル追加")
        self.assertEqual(response.context["selected_tag_count"], 0)
        self.assertContains(response, "タグで絞り込む")
        self.assertContains(response, "未選択")
        self.assertContains(response, "filter-details__caret")
        self.assertNotContains(response, "Visible Entry")
        self.assertNotContains(response, 'name="domain"', html=False)
        self.assertNotContains(response, 'name="status"', html=False)
        self.assertNotContains(response, 'name="review_state"', html=False)
        self.assertNotContains(response, 'name="save_reason"', html=False)
        self.assertNotContains(response, 'name="favorite_only"', html=False)
        self.assertNotContains(response, 'name="recheck_due_only"', html=False)

    def test_ai_search_list_tag_filter_opens_when_tags_are_selected(self):
        ai_resource = Resource.objects.create(
            original_url="https://example.com/tagged-ai",
            normalized_url="https://example.com/tagged-ai",
            domain="example.com",
            title_manual="Tagged AI Entry",
            search_only=True,
        )
        ai_resource.tags.add(self.tag_a)
        other_resource = Resource.objects.create(
            original_url="https://example.com/other-ai",
            normalized_url="https://example.com/other-ai",
            domain="example.com",
            title_manual="Other AI Entry",
            search_only=True,
        )
        other_resource.tags.add(self.tag_b)

        response = self.client.get(reverse("resources:ai_search_list"), {"tags": [self.tag_a.id]})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(list(response.context["resources"]), [ai_resource])
        self.assertEqual(response.context["selected_tag_count"], 1)
        self.assertContains(response, "1 件選択中")
        self.assertContains(response, '<details class="filter-details filter-details--ai-search" open>', html=False)
        self.assertNotContains(response, "Other AI Entry")

    def test_ai_search_list_query_stays_in_search_only_scope(self):
        Resource.objects.create(
            original_url="https://example.com/normal-ai",
            normalized_url="https://example.com/normal-ai",
            domain="example.com",
            title_manual="Shared Keyword",
        )
        ai_resource = Resource.objects.create(
            original_url="https://example.com/hidden-ai",
            normalized_url="https://example.com/hidden-ai",
            domain="example.com",
            title_manual="Shared Keyword",
            search_only=True,
        )

        response = self.client.get(reverse("resources:ai_search_list"), {"q": "Shared"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(list(response.context["resources"]), [ai_resource])

    def test_ai_search_list_is_ordered_by_created_at(self):
        older_resource = Resource.objects.create(
            original_url="https://example.com/older-ai",
            normalized_url="https://example.com/older-ai",
            domain="example.com",
            title_manual="Older AI Entry",
            search_only=True,
        )
        newer_resource = Resource.objects.create(
            original_url="https://example.com/newer-ai",
            normalized_url="https://example.com/newer-ai",
            domain="example.com",
            title_manual="Newer AI Entry",
            search_only=True,
        )

        response = self.client.get(reverse("resources:ai_search_list"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(list(response.context["resources"]), [newer_resource, older_resource])

    def test_ai_search_list_is_paginated_fifteen_per_date_labeled_page(self):
        today = timezone.localtime(timezone.now())
        yesterday = today - timezone.timedelta(days=1)
        for index in range(15):
            Resource.objects.create(
                original_url=f"https://example.com/today-ai-{index}",
                normalized_url=f"https://example.com/today-ai-{index}",
                domain="example.com",
                title_manual=f"Today AI {index}",
                search_only=True,
            )
        older_resource = Resource.objects.create(
            original_url="https://example.com/yesterday-ai",
            normalized_url="https://example.com/yesterday-ai",
            domain="example.com",
            title_manual="Yesterday AI",
            search_only=True,
        )
        Resource.objects.filter(pk=older_resource.pk).update(created_at=yesterday)

        response = self.client.get(reverse("resources:ai_search_list"))
        second_page = self.client.get(reverse("resources:ai_search_list"), {"page": 2})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["resources"]), 15)
        self.assertTrue(response.context["pagination"]["is_paginated"])
        self.assertContains(response, today.strftime("%Y/%m/%d"))
        self.assertContains(response, yesterday.strftime("%Y/%m/%d"))
        self.assertEqual(second_page.status_code, 200)
        self.assertEqual(list(second_page.context["resources"]), [older_resource])

    def test_ai_search_list_fragment_returns_search_only_html(self):
        ai_resource = Resource.objects.create(
            original_url="https://example.com/live-ai",
            normalized_url="https://example.com/live-ai",
            domain="example.com",
            title_manual="Live AI Entry",
            search_only=True,
        )
        Resource.objects.create(
            original_url="https://example.com/live-normal",
            normalized_url="https://example.com/live-normal",
            domain="example.com",
            title_manual="Live Normal Entry",
        )

        response = self.client.get(reverse("resources:ai_search_list_fragment"))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 1)
        self.assertIn("Live AI Entry", payload["html"])
        self.assertIn(str(ai_resource.id), payload["html"])
        self.assertNotIn("Live Normal Entry", payload["html"])
        self.assertTrue(payload["signature"])

    def test_ai_search_list_fragment_action_next_points_to_page_not_live_endpoint(self):
        Resource.objects.create(
            original_url="https://example.com/live-next-ai",
            normalized_url="https://example.com/live-next-ai",
            domain="example.com",
            title_manual="Live Next AI Entry",
            search_only=True,
        )

        response = self.client.get(reverse("resources:ai_search_list_fragment"), {"page": 2, "_ts": "123"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn('name="next" value="/ai-search/resources/?page=2"', payload["html"])
        self.assertNotIn('name="next" value="/ai-search/resources/live/', payload["html"])

    def test_import_may10_ai_urls_disables_video_capture(self):
        output = StringIO()

        call_command("import_may10_ai_urls", "--no-jobs", stdout=output)

        self.assertEqual(Resource.objects.filter(search_only=True).count(), 15)
        self.assertFalse(Resource.objects.filter(search_only=True, capture_videos=True).exists())

    def test_interest_feedback_marks_ai_search_resource_as_interested(self):
        ai_resource = Resource.objects.create(
            original_url="https://example.com/interest-ai",
            normalized_url="https://example.com/interest-ai",
            domain="example.com",
            title_manual="Interest AI Entry",
            search_only=True,
        )

        response = self.client.post(
            reverse("resources:interest_feedback", args=[ai_resource.id]),
            {
                "interest_feedback": InterestFeedback.INTERESTED,
                "next": reverse("resources:ai_search_list"),
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], reverse("resources:ai_search_list"))
        ai_resource.refresh_from_db()
        self.assertEqual(ai_resource.interest_feedback, InterestFeedback.INTERESTED)

    def test_interest_feedback_fetch_returns_json_without_redirect(self):
        ai_resource = Resource.objects.create(
            original_url="https://example.com/interest-fetch-ai",
            normalized_url="https://example.com/interest-fetch-ai",
            domain="example.com",
            title_manual="Interest Fetch AI Entry",
            search_only=True,
        )

        response = self.client.post(
            reverse("resources:interest_feedback", args=[ai_resource.id]),
            {
                "interest_feedback": InterestFeedback.INTERESTED,
                "next": reverse("resources:ai_search_list"),
            },
            HTTP_X_REQUESTED_WITH="fetch",
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["resource"]["interest_feedback"], InterestFeedback.INTERESTED)
        ai_resource.refresh_from_db()
        self.assertEqual(ai_resource.interest_feedback, InterestFeedback.INTERESTED)

    def test_interest_feedback_toggles_same_value_back_to_none(self):
        ai_resource = Resource.objects.create(
            original_url="https://example.com/not-interest-ai",
            normalized_url="https://example.com/not-interest-ai",
            domain="example.com",
            title_manual="Not Interest AI Entry",
            search_only=True,
            interest_feedback=InterestFeedback.NOT_INTERESTED,
        )

        response = self.client.post(
            reverse("resources:interest_feedback", args=[ai_resource.id]),
            {
                "interest_feedback": InterestFeedback.NOT_INTERESTED,
                "next": reverse("resources:ai_search_list"),
            },
        )

        self.assertEqual(response.status_code, 302)
        ai_resource.refresh_from_db()
        self.assertEqual(ai_resource.interest_feedback, InterestFeedback.NONE)

    def test_interest_label_adds_label_from_ai_search_list(self):
        ai_resource = Resource.objects.create(
            original_url="https://example.com/label-ai",
            normalized_url="https://example.com/label-ai",
            domain="example.com",
            title_manual="Label AI Entry",
            search_only=True,
        )

        response = self.client.post(
            reverse("resources:interest_label", args=[ai_resource.id]),
            {
                "label_action": "add",
                "interest_label": "reference",
                "next": reverse("resources:ai_search_list"),
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], reverse("resources:ai_search_list"))
        ai_resource.refresh_from_db()
        self.assertEqual(ai_resource.interest_labels, ["reference"])

    def test_interest_label_fetch_adds_and_removes_label(self):
        ai_resource = Resource.objects.create(
            original_url="https://example.com/label-fetch-ai",
            normalized_url="https://example.com/label-fetch-ai",
            domain="example.com",
            title_manual="Label Fetch AI Entry",
            search_only=True,
            interest_labels=["reference"],
        )

        add_response = self.client.post(
            reverse("resources:interest_label", args=[ai_resource.id]),
            {
                "label_action": "add",
                "interest_label": "deep_dive",
                "next": reverse("resources:ai_search_list"),
            },
            HTTP_X_REQUESTED_WITH="fetch",
        )

        self.assertEqual(add_response.status_code, 200)
        add_payload = add_response.json()
        self.assertEqual(add_payload["resource"]["interest_labels"], ["reference", "deep_dive"])

        remove_response = self.client.post(
            reverse("resources:interest_label", args=[ai_resource.id]),
            {
                "label_action": "remove",
                "interest_label": "reference",
                "next": reverse("resources:ai_search_list"),
            },
            HTTP_X_REQUESTED_WITH="fetch",
        )

        self.assertEqual(remove_response.status_code, 200)
        remove_payload = remove_response.json()
        self.assertEqual(remove_payload["resource"]["interest_labels"], ["deep_dive"])
        ai_resource.refresh_from_db()
        self.assertEqual(ai_resource.interest_labels, ["deep_dive"])

    def test_interest_label_rejects_invalid_label(self):
        ai_resource = Resource.objects.create(
            original_url="https://example.com/label-invalid-ai",
            normalized_url="https://example.com/label-invalid-ai",
            domain="example.com",
            title_manual="Label Invalid AI Entry",
            search_only=True,
        )

        response = self.client.post(
            reverse("resources:interest_label", args=[ai_resource.id]),
            {
                "label_action": "add",
                "interest_label": "invalid",
                "next": reverse("resources:ai_search_list"),
            },
            HTTP_X_REQUESTED_WITH="fetch",
        )

        self.assertEqual(response.status_code, 400)
        ai_resource.refresh_from_db()
        self.assertEqual(ai_resource.interest_labels, [])

    def test_api_resource_detail_includes_interest_feedback(self):
        ai_resource = Resource.objects.create(
            original_url="https://example.com/interested-api",
            normalized_url="https://example.com/interested-api",
            domain="example.com",
            title_manual="Interested API Entry",
            search_only=True,
            interest_feedback=InterestFeedback.INTERESTED,
            interest_labels=["reference", "deep_dive"],
        )

        response = self.client.get(reverse("resources:api_detail", args=[ai_resource.id]))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["interest_feedback"], InterestFeedback.INTERESTED)
        self.assertEqual(payload["interest_feedback_label"], "興味あり")
        self.assertEqual(payload["interest_labels"], ["reference", "deep_dive"])
        self.assertEqual(payload["interest_label_names"], ["実装参考", "深掘り候補"])

    def test_ai_interested_page_is_machine_readable_and_filters_interested_resources(self):
        interested = Resource.objects.create(
            original_url="https://example.com/interested-reader",
            normalized_url="https://example.com/interested-reader",
            domain="example.com",
            title_manual="Interested Reader Entry",
            note="興味あり理由メモ",
            next_action="関連実装を探す",
            search_only=True,
            interest_feedback=InterestFeedback.INTERESTED,
            interest_labels=["reference", "deep_dive"],
        )
        interested.tags.add(self.tag_a)
        snapshot = Snapshot.objects.create(
            resource=interested,
            snapshot_no=1,
            fetch_url=interested.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="Interested Page Title",
            extracted_text="本文抜粋としてAIが読むテキストです。",
            ai_summary="一次要約です。",
            ai_translation="日本語翻訳です。",
            ai_category="documentation",
        )
        interested.latest_snapshot = snapshot
        interested.save(update_fields=["latest_snapshot"])
        Resource.objects.create(
            original_url="https://example.com/not-reader",
            normalized_url="https://example.com/not-reader",
            domain="example.com",
            title_manual="Not Reader Entry",
            interest_feedback=InterestFeedback.NOT_INTERESTED,
        )
        Resource.objects.create(
            original_url="https://example.com/none-reader",
            normalized_url="https://example.com/none-reader",
            domain="example.com",
            title_manual="None Reader Entry",
        )

        response = self.client.get(reverse("resources:ai_interested"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["feedback"], InterestFeedback.INTERESTED)
        self.assertEqual(response.context["total_count"], 1)
        self.assertContains(response, 'data-ai-reader-page="interest-feedback"')
        self.assertContains(response, 'data-feedback="interested"')
        self.assertContains(response, "Interested Reader Entry")
        self.assertContains(response, "https://example.com/interested-reader")
        self.assertContains(response, "実装参考, 深掘り候補")
        self.assertContains(response, "alpha")
        self.assertContains(response, "一次要約です。")
        self.assertContains(response, "日本語翻訳です。")
        self.assertContains(response, "本文抜粋としてAIが読むテキストです。")
        self.assertNotContains(response, "Not Reader Entry")
        self.assertNotContains(response, "None Reader Entry")
        self.assertContains(response, reverse("resources:ai_interested_markdown"))
        self.assertNotContains(response, "app-sidebar")
        self.assertNotContains(response, "URL登録")

    def test_ai_not_interested_page_is_machine_readable_and_filters_not_interested_resources(self):
        not_interested = Resource.objects.create(
            original_url="https://example.com/not-interested-reader",
            normalized_url="https://example.com/not-interested-reader",
            domain="example.com",
            title_manual="Not Interested Reader Entry",
            note="宣伝寄りなので次回は避けたい",
            interest_feedback=InterestFeedback.NOT_INTERESTED,
            interest_labels=["promotional", "not_now"],
        )
        Resource.objects.create(
            original_url="https://example.com/interested-reader",
            normalized_url="https://example.com/interested-reader",
            domain="example.com",
            title_manual="Positive Reader Entry",
            interest_feedback=InterestFeedback.INTERESTED,
        )

        response = self.client.get(reverse("resources:ai_not_interested"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["feedback"], InterestFeedback.NOT_INTERESTED)
        self.assertEqual(response.context["total_count"], 1)
        self.assertContains(response, 'data-feedback="not_interested"')
        self.assertContains(response, "AI読取用: 興味なしURL")
        self.assertContains(response, "Not Interested Reader Entry")
        self.assertContains(response, "宣伝っぽい, 今は不要")
        self.assertContains(response, "宣伝寄りなので次回は避けたい")
        self.assertContains(response, "latest_snapshot: none")
        self.assertNotContains(response, "Positive Reader Entry")
        self.assertContains(response, reverse("resources:ai_interested"))
        self.assertContains(response, reverse("resources:ai_not_interested"))
        self.assertContains(response, reverse("resources:ai_not_interested_markdown"))

    def test_ai_feedback_page_limit_is_capped(self):
        for index in range(3):
            Resource.objects.create(
                original_url=f"https://example.com/interested-limit-{index}",
                normalized_url=f"https://example.com/interested-limit-{index}",
                domain="example.com",
                title_manual=f"Interested Limit {index}",
                interest_feedback=InterestFeedback.INTERESTED,
            )

        response = self.client.get(reverse("resources:ai_interested"), {"limit": 2})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["total_count"], 3)
        self.assertEqual(response.context["shown_count"], 2)
        self.assertContains(response, "shown_count=2")

    def test_ai_interested_markdown_exports_same_machine_readable_content(self):
        interested = Resource.objects.create(
            original_url="https://example.com/interested-md",
            normalized_url="https://example.com/interested-md",
            domain="example.com",
            title_manual="Interested Markdown Entry",
            note="Markdownにも保存する",
            interest_feedback=InterestFeedback.INTERESTED,
            interest_labels=["reference"],
        )
        snapshot = Snapshot.objects.create(
            resource=interested,
            snapshot_no=1,
            fetch_url=interested.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="Markdown Page Title",
            extracted_text="Markdown向け本文です。",
            ai_summary="Markdown向け要約です。",
            ai_translation="Markdown向け翻訳です。",
        )
        interested.latest_snapshot = snapshot
        interested.save(update_fields=["latest_snapshot"])

        response = self.client.get(reverse("resources:ai_interested_markdown"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Type"], "text/markdown; charset=utf-8")
        self.assertIn('filename="interested-urls.md"', response.headers["Content-Disposition"])
        content = response.content.decode()
        self.assertIn("# AI読取用: 興味ありURL", content)
        self.assertIn("## 1. Interested Markdown Entry", content)
        self.assertIn("- feedback: 興味あり (interested)", content)
        self.assertIn("- interest_labels: 実装参考", content)
        self.assertIn("- url: https://example.com/interested-md", content)
        self.assertIn("Markdownにも保存する", content)
        self.assertIn("Markdown向け要約です。", content)
        self.assertIn("Markdown向け翻訳です。", content)
        self.assertIn("Markdown向け本文です。", content)
        self.assertIn(reverse("resources:ai_interested"), content)
        self.assertIn(reverse("resources:ai_not_interested_markdown"), content)

    def test_ai_not_interested_markdown_filters_not_interested_resources(self):
        Resource.objects.create(
            original_url="https://example.com/not-interested-md",
            normalized_url="https://example.com/not-interested-md",
            domain="example.com",
            title_manual="Not Interested Markdown Entry",
            note="Markdown除外メモ",
            interest_feedback=InterestFeedback.NOT_INTERESTED,
            interest_labels=["off_topic", "duplicate"],
        )
        Resource.objects.create(
            original_url="https://example.com/interested-md-hidden",
            normalized_url="https://example.com/interested-md-hidden",
            domain="example.com",
            title_manual="Hidden Markdown Entry",
            interest_feedback=InterestFeedback.INTERESTED,
        )

        response = self.client.get(reverse("resources:ai_not_interested_markdown"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Type"], "text/markdown; charset=utf-8")
        self.assertIn('filename="not-interested-urls.md"', response.headers["Content-Disposition"])
        content = response.content.decode()
        self.assertIn("# AI読取用: 興味なしURL", content)
        self.assertIn("## 1. Not Interested Markdown Entry", content)
        self.assertIn("- feedback: 興味なし (not_interested)", content)
        self.assertIn("- interest_labels: テーマ外, 重複", content)
        self.assertIn("Markdown除外メモ", content)
        self.assertIn("latest_snapshot: none", content)
        self.assertNotIn("Hidden Markdown Entry", content)

    def test_settings_links_to_ai_reader_pages(self):
        response = self.client.get(reverse("resources:settings"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "AI読取用ページ")
        self.assertContains(response, reverse("resources:ai_interested"))
        self.assertContains(response, reverse("resources:ai_not_interested"))
        self.assertContains(response, reverse("resources:ai_interested_markdown"))
        self.assertContains(response, reverse("resources:ai_not_interested_markdown"))
        self.assertContains(response, "興味ありを見る")
        self.assertContains(response, "興味なし Markdown")

    def test_delete_fetch_returns_json_without_redirect(self):
        ai_resource = Resource.objects.create(
            original_url="https://example.com/delete-fetch-ai",
            normalized_url="https://example.com/delete-fetch-ai",
            domain="example.com",
            title_manual="Delete Fetch AI Entry",
            search_only=True,
        )

        response = self.client.post(
            reverse("resources:detail", args=[ai_resource.id]),
            {
                "_method": "DELETE",
                "next": reverse("resources:ai_search_list"),
            },
            HTTP_X_REQUESTED_WITH="fetch",
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["deleted_id"], ai_resource.id)
        self.assertFalse(Resource.objects.filter(pk=ai_resource.pk).exists())

    def test_list_shows_link_to_create_page(self):
        response = self.client.get(reverse("resources:list"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse("resources:create"))
        self.assertContains(response, "ドメイン")
        self.assertContains(response, 'data-resource-autorefresh', html=False)

    def test_list_shows_screenshot_preview_and_status_chips(self):
        resource = Resource.objects.create(
            original_url="https://example.com/preview",
            normalized_url="https://example.com/preview",
            domain="example.com",
            title_manual="Preview Entry",
            favorite=True,
            review_state=ReviewState.NEEDS_REVIEW,
            link_status=LinkStatus.GONE,
            last_link_check_at=timezone.now(),
            last_link_check_http_status=404,
        )
        snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.PLAYWRIGHT,
            http_status=200,
            page_title="Preview",
            screenshot_full_path="storage/screenshots/resource_0001/snapshot_0001_full.png",
        )
        resource.latest_snapshot = snapshot
        resource.save(update_fields=["latest_snapshot"])

        response = self.client.get(reverse("resources:list"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, snapshot.screenshot_full_path)
        self.assertContains(response, "リンク切れ")
        self.assertContains(response, "要確認")

    def test_bulk_edit_updates_selected_resources(self):
        resource_a = Resource.objects.create(
            original_url="https://example.com/bulk-a",
            normalized_url="https://example.com/bulk-a",
            domain="example.com",
            title_manual="Bulk A",
        )
        resource_b = Resource.objects.create(
            original_url="https://example.com/bulk-b",
            normalized_url="https://example.com/bulk-b",
            domain="example.com",
            title_manual="Bulk B",
        )
        resource_c = Resource.objects.create(
            original_url="https://example.com/bulk-c",
            normalized_url="https://example.com/bulk-c",
            domain="example.com",
            title_manual="Bulk C",
        )

        response = self.client.post(
            reverse("resources:bulk_edit"),
            {
                "resource_ids": [str(resource_a.id), str(resource_b.id)],
                "review_state": ReviewState.DONE,
                "save_reason": NOTE_TEMPLATE_CHOICES[0][0],
                "next_action": "比較する",
                "recheck_at": "2026-05-02",
                "favorite_state": "on",
                "visibility_state": "search_only",
                "interest_labels": ["compare", "idea"],
                "tags": [self.tag_a.id],
                "new_tags": "gamma",
                "next": reverse("resources:list"),
            },
        )

        self.assertEqual(response.status_code, 302)
        resource_a.refresh_from_db()
        resource_b.refresh_from_db()
        resource_c.refresh_from_db()

        for resource in (resource_a, resource_b):
            self.assertEqual(resource.review_state, ReviewState.DONE)
            self.assertEqual(resource.save_reason, NOTE_TEMPLATE_CHOICES[0][0])
            self.assertEqual(resource.next_action, "比較する")
            self.assertEqual(resource.recheck_at, date(2026, 5, 2))
            self.assertTrue(resource.favorite)
            self.assertTrue(resource.search_only)
            self.assertEqual(resource.interest_labels, ["compare", "idea"])
            self.assertEqual(
                list(resource.tags.order_by("name").values_list("name", flat=True)),
                ["alpha", "gamma"],
            )

        self.assertEqual(resource_c.review_state, ReviewState.NONE)
        self.assertEqual(resource_c.save_reason, "")
        self.assertEqual(resource_c.next_action, "")
        self.assertIsNone(resource_c.recheck_at)
        self.assertFalse(resource_c.favorite)
        self.assertFalse(resource_c.search_only)
        self.assertEqual(resource_c.interest_labels, [])
        self.assertFalse(resource_c.tags.exists())

    def test_bulk_edit_page_shows_selected_resources(self):
        resource_a = Resource.objects.create(
            original_url="https://example.com/bulk-page-a",
            normalized_url="https://example.com/bulk-page-a",
            domain="example.com",
            title_manual="Bulk Page A",
        )
        resource_b = Resource.objects.create(
            original_url="https://example.com/bulk-page-b",
            normalized_url="https://example.com/bulk-page-b",
            domain="example.com",
            title_manual="Bulk Page B",
        )

        response = self.client.get(
            reverse("resources:bulk_edit"),
            {
                "resource_ids": [str(resource_a.id), str(resource_b.id)],
                "next": reverse("resources:list"),
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "まとめて編集")
        self.assertContains(response, "Bulk Page A")
        self.assertContains(response, "Bulk Page B")
        self.assertContains(response, 'name="resource_ids"', html=False)

    def test_bulk_edit_page_allows_opening_without_selection(self):
        response = self.client.get(reverse("resources:bulk_edit"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "一括編集")
        self.assertContains(response, "選択対象のリソース")

    def test_bulk_edit_page_is_paginated_ten_per_page(self):
        for index in range(11):
            Resource.objects.create(
                original_url=f"https://example.com/bulk-pagination-{index}",
                normalized_url=f"https://example.com/bulk-pagination-{index}",
                domain="example.com",
                title_manual=f"Bulk Pagination {index}",
            )

        response = self.client.get(reverse("resources:bulk_edit"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["resource_choices"]), 10)
        self.assertTrue(response.context["pagination"]["is_paginated"])
        self.assertEqual(response.context["page_obj"].paginator.count, 11)

    def test_list_fragment_returns_html_and_signature(self):
        resource = Resource.objects.create(
            original_url="https://example.com/live",
            normalized_url="https://example.com/live",
            domain="example.com",
            title_manual="Live Entry",
        )

        response = self.client.get(reverse("resources:list_fragment"))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 1)
        self.assertIn("Live Entry", payload["html"])
        self.assertIn(str(resource.id), payload["html"])
        self.assertTrue(payload["signature"])

    def test_list_shows_delete_button_for_resource(self):
        resource = Resource.objects.create(
            original_url="https://example.com/delete-me",
            normalized_url="https://example.com/delete-me",
            domain="example.com",
            title_manual="Delete Me",
        )

        response = self.client.get(reverse("resources:list"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse("resources:detail", args=[resource.id]))
        self.assertContains(response, 'value="DELETE"', html=False)

    def test_list_is_paginated_ten_per_page(self):
        for index in range(11):
            Resource.objects.create(
                original_url=f"https://example.com/paginated-{index}",
                normalized_url=f"https://example.com/paginated-{index}",
                domain="example.com",
                title_manual=f"Paginated {index}",
            )

        response = self.client.get(reverse("resources:list"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["resources"]), 10)
        self.assertTrue(response.context["pagination"]["is_paginated"])
        self.assertEqual(response.context["page_obj"].paginator.count, 11)

        second_page = self.client.get(reverse("resources:list"), {"page": 2})
        self.assertEqual(second_page.status_code, 200)
        self.assertEqual(len(second_page.context["resources"]), 1)

    def test_detail_links_to_edit_page_without_inline_edit_form(self):
        resource = Resource.objects.create(
            original_url="https://example.com/detail-order",
            normalized_url="https://example.com/detail-order",
            domain="example.com",
            title_manual="Detail Order",
        )
        snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="Captured",
            extracted_text="summary text",
        )
        resource.latest_snapshot = snapshot
        resource.save(update_fields=["latest_snapshot"])

        with patch("resources.views.check_resource_link_status", side_effect=lambda current, force=False: current):
            response = self.client.get(reverse("resources:detail", args=[resource.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse("resources:edit", args=[resource.id]))
        self.assertContains(response, ">編集<", html=False)
        self.assertNotContains(response, 'id="resource-edit"')
        self.assertNotContains(response, 'href="#resource-edit"')

    def test_edit_page_updates_resource(self):
        resource = Resource.objects.create(
            original_url="https://example.com/edit-me",
            normalized_url="https://example.com/edit-me",
            domain="example.com",
            title_manual="Before Edit",
        )

        get_response = self.client.get(reverse("resources:edit", args=[resource.id]))
        self.assertEqual(get_response.status_code, 200)
        self.assertContains(get_response, "URLを編集")

        response = self.client.post(
            reverse("resources:edit", args=[resource.id]),
            {
                "original_url": "https://example.com/edit-me",
                "title_manual": "After Edit",
                "save_reason": NOTE_TEMPLATE_CHOICES[0][0],
                "next_action": "あとで確認する",
                "recheck_at": "2026-05-02",
                "review_state": ReviewState.DONE,
                "capture_images": "on",
                "capture_videos": "on",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], reverse("resources:detail", args=[resource.id]))
        resource.refresh_from_db()
        self.assertEqual(resource.title_manual, "After Edit")
        self.assertEqual(resource.save_reason, NOTE_TEMPLATE_CHOICES[0][0])
        self.assertEqual(resource.next_action, "あとで確認する")
        self.assertEqual(resource.review_state, ReviewState.DONE)

    def test_detail_shows_translation_when_available(self):
        resource = Resource.objects.create(
            original_url="https://example.com/translated",
            normalized_url="https://example.com/translated",
            domain="example.com",
            title_manual="Translated Entry",
        )
        snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="Translated",
            extracted_text="Hello world",
            ai_translation="こんにちは、世界",
        )
        resource.latest_snapshot = snapshot
        resource.save(update_fields=["latest_snapshot"])

        with patch("resources.views.check_resource_link_status", side_effect=lambda current, force=False: current):
            response = self.client.get(reverse("resources:detail", args=[resource.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "日本語翻訳")
        self.assertContains(response, "こんにちは、世界")
        self.assertNotContains(response, "AI要約")

    def test_detail_hides_translation_when_snapshot_has_none(self):
        resource = Resource.objects.create(
            original_url="https://example.com/japanese",
            normalized_url="https://example.com/japanese",
            domain="example.com",
            title_manual="Japanese Entry",
        )
        snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="日本語の記事",
            extracted_text="これは日本語の本文です。",
            ai_translation="",
        )
        resource.latest_snapshot = snapshot
        resource.save(update_fields=["latest_snapshot"])

        with patch("resources.views.check_resource_link_status", side_effect=lambda current, force=False: current):
            response = self.client.get(reverse("resources:detail", args=[resource.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "日本語翻訳はまだありません。")

    def test_detail_displays_saved_images_from_latest_snapshot(self):
        resource = Resource.objects.create(
            original_url="https://example.com/image-post",
            normalized_url="https://example.com/image-post",
            domain="example.com",
            title_manual="Image Post",
            capture_images=True,
            capture_videos=False,
        )
        snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="Image",
            image_assets=[
                {
                    "source_url": "https://example.com/hero.jpg",
                    "path": f"storage/images/resource_{resource.id:04d}/snapshot_0001_img_01.jpg",
                    "content_type": "image/jpeg",
                    "size_bytes": 42,
                }
            ],
        )
        resource.latest_snapshot = snapshot
        resource.save(update_fields=["latest_snapshot"])
        image_name = "snapshot_0001_img_01.jpg"
        image_dir = self.storage_base / "images" / f"resource_{resource.id:04d}"
        image_dir.mkdir(parents=True, exist_ok=True)
        (image_dir / image_name).write_bytes(b"fake-image-bytes")

        with patch("resources.views.check_resource_link_status", side_effect=lambda current, force=False: current):
            response = self.client.get(reverse("resources:detail", args=[resource.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "保存した画像")
        self.assertContains(response, f"/storage/images/resource_{resource.id:04d}/{image_name}")
        self.assertContains(response, 'data-media-viewer="image"')
        self.assertNotContains(response, 'target="_blank" rel="noreferrer" class="image-card"')
        self.assertFalse(response.context["capture_mismatch"])

    def test_detail_displays_screenshot_in_image_tab_without_false_mismatch(self):
        resource = Resource.objects.create(
            original_url="https://x.com/example/status/1",
            normalized_url="https://x.com/example/status/1",
            domain="x.com",
            title_manual="Screenshot Post",
            capture_images=True,
            capture_videos=False,
        )
        screenshot_name = "snapshot_0001_full.png"
        screenshot_path = f"storage/screenshots/resource_{resource.id:04d}/{screenshot_name}"
        snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.PLAYWRIGHT,
            http_status=200,
            page_title="Screenshot",
            ai_translation="保存済みの翻訳です。",
            screenshot_full_path=screenshot_path,
        )
        resource.latest_snapshot = snapshot
        resource.save(update_fields=["latest_snapshot"])
        screenshot_dir = self.storage_base / "screenshots" / f"resource_{resource.id:04d}"
        screenshot_dir.mkdir(parents=True, exist_ok=True)
        (screenshot_dir / screenshot_name).write_bytes(b"fake-screenshot")

        with patch("resources.views.check_resource_link_status", side_effect=lambda current, force=False: current):
            response = self.client.get(reverse("resources:detail", args=[resource.id]))

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.context["has_image_files"])
        self.assertTrue(response.context["has_screenshot_file"])
        self.assertTrue(response.context["has_image_panel_files"])
        self.assertFalse(response.context["capture_mismatch"])
        rendered = response.content.decode()
        translation_panel = rendered.split('data-tab-panel="translation"', 1)[1].split(
            'data-tab-panel="body"', 1
        )[0]
        image_panel = rendered.split('data-tab-panel="images"', 1)[1].split('data-tab-panel="videos"', 1)[0]
        self.assertNotIn(screenshot_path, translation_panel)
        self.assertIn(screenshot_path, image_panel)
        self.assertIn('data-media-viewer="image"', image_panel)
        self.assertIn("ページ全体スクリーンショット", image_panel)
        self.assertNotContains(response, "保存済みファイルが見つかりません。再取得してください。")

    def test_detail_displays_only_latest_snapshot_images(self):
        resource = Resource.objects.create(
            original_url="https://example.com/repeated-image-post",
            normalized_url="https://example.com/repeated-image-post",
            domain="example.com",
            title_manual="Repeated Image Post",
            capture_images=True,
            capture_videos=False,
        )
        first_snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="First",
            image_assets=[
                {
                    "source_url": "https://example.com/hero.jpg",
                    "path": f"storage/images/resource_{resource.id:04d}/snapshot_0001_img_01.jpg",
                    "content_type": "image/jpeg",
                    "size_bytes": 42,
                }
            ],
        )
        second_snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=2,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="Second",
            image_assets=[
                {
                    "source_url": "https://example.com/hero.jpg",
                    "path": f"storage/images/resource_{resource.id:04d}/snapshot_0002_img_01.jpg",
                    "content_type": "image/jpeg",
                    "size_bytes": 42,
                }
            ],
        )
        resource.latest_snapshot = second_snapshot
        resource.save(update_fields=["latest_snapshot"])
        image_dir = self.storage_base / "images" / f"resource_{resource.id:04d}"
        image_dir.mkdir(parents=True, exist_ok=True)
        (image_dir / "snapshot_0001_img_01.jpg").write_bytes(b"first-image")
        (image_dir / "snapshot_0002_img_01.jpg").write_bytes(b"second-image")

        with patch("resources.views.check_resource_link_status", side_effect=lambda current, force=False: current):
            response = self.client.get(reverse("resources:detail", args=[resource.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "/storage/images/resource_{0:04d}/snapshot_0002_img_01.jpg".format(resource.id))
        self.assertNotContains(response, "/storage/images/resource_{0:04d}/snapshot_0001_img_01.jpg".format(resource.id))

    def test_detail_get_displays_checked_link_status(self):
        resource = Resource.objects.create(
            original_url="https://example.com/link-check",
            normalized_url="https://example.com/link-check",
            domain="example.com",
            title_manual="Link Check",
        )

        def fake_check(current, force=False):
            current.link_status = LinkStatus.GONE
            current.last_link_check_at = timezone.now()
            current.last_link_check_http_status = 404
            return current

        with patch("resources.views.check_resource_link_status", side_effect=fake_check):
            response = self.client.get(reverse("resources:detail", args=[resource.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "リンク切れ")
        self.assertContains(response, "HTTP 404")

    def test_detail_hides_snapshot_diff_section(self):
        resource = Resource.objects.create(
            original_url="https://example.com/diff",
            normalized_url="https://example.com/diff",
            domain="example.com",
            title_manual="Diff Entry",
        )
        Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="Old title",
            extracted_text="abc",
            image_assets=[
                {
                    "source_url": "https://example.com/old.jpg",
                    "path": f"storage/images/resource_{resource.id:04d}/snapshot_0001_img_01.jpg",
                    "content_type": "image/jpeg",
                    "size_bytes": 42,
                }
            ],
        )
        latest_snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=2,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.PLAYWRIGHT,
            http_status=404,
            page_title="New title",
            extracted_text="abcdef",
            error_message="HTTP 404",
            image_assets=[
                {
                    "source_url": "https://example.com/new.jpg",
                    "path": f"storage/images/resource_{resource.id:04d}/snapshot_0002_img_01.jpg",
                    "content_type": "image/jpeg",
                    "size_bytes": 42,
                },
                {
                    "source_url": "https://example.com/new-2.jpg",
                    "path": f"storage/images/resource_{resource.id:04d}/snapshot_0002_img_02.jpg",
                    "content_type": "image/jpeg",
                    "size_bytes": 42,
                },
            ],
        )
        resource.latest_snapshot = latest_snapshot
        resource.save(update_fields=["latest_snapshot"])

        with patch("resources.views.check_resource_link_status", side_effect=lambda current, force=False: current):
            response = self.client.get(reverse("resources:detail", args=[resource.id]))

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "前回との差分")
        self.assertNotContains(response, "Old title")
        self.assertContains(response, "スナップショット履歴")

    def test_snapshot_detail_hides_previous_snapshot_diff(self):
        resource = Resource.objects.create(
            original_url="https://example.com/snapshot-diff",
            normalized_url="https://example.com/snapshot-diff",
            domain="example.com",
            title_manual="Snapshot Diff",
        )
        previous_snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="Before",
            extracted_text="before",
        )
        current_snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=2,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.PLAYWRIGHT,
            http_status=201,
            page_title="After",
            extracted_text="after text",
        )

        response = self.client.get(reverse("snapshots:detail", args=[current_snapshot.id]))

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "前回との差分")
        self.assertNotContains(response, "Before")
        self.assertContains(response, "After")

    def test_detail_displays_saved_videos(self):
        resource = Resource.objects.create(
            original_url="https://x.com/example/status/1",
            normalized_url="https://x.com/example/status/1",
            domain="x.com",
            title_manual="Video Post",
            capture_images=False,
            capture_videos=True,
        )
        snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.PLAYWRIGHT,
            http_status=200,
            page_title="Video",
            video_assets=[
                {
                    "source_url": "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/sample.mp4",
                    "path": "storage/videos/resource_0001/snapshot_0001_vid_01.mp4",
                    "content_type": "video/mp4",
                    "size_bytes": 42,
                }
            ],
        )
        resource.latest_snapshot = snapshot
        resource.save(update_fields=["latest_snapshot"])
        video_name = "snapshot_0001_vid_01.mp4"
        video_dir = self.storage_base / "videos" / f"resource_{resource.id:04d}"
        video_dir.mkdir(parents=True, exist_ok=True)
        (video_dir / video_name).write_bytes(b"fake-video-bytes")

        with patch("resources.views.check_resource_link_status", side_effect=lambda current, force=False: current):
            response = self.client.get(reverse("resources:detail", args=[resource.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "保存動画")
        self.assertContains(response, f"/storage/videos/resource_{resource.id:04d}/{video_name}")
        self.assertContains(response, 'data-media-viewer="video"')
        self.assertNotContains(response, 'target="_blank" rel="noreferrer" class="video-card"')

    def test_detail_prompts_recapture_when_capture_flag_and_files_are_out_of_sync(self):
        resource = Resource.objects.create(
            original_url="https://example.com/mismatch",
            normalized_url="https://example.com/mismatch",
            domain="example.com",
            title_manual="Mismatch Entry",
            capture_images=True,
            capture_videos=False,
        )
        snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            page_title="Mismatch",
            image_assets=[
                {
                    "source_url": "https://example.com/hero.jpg",
                    "path": f"storage/images/resource_{resource.id:04d}/snapshot_0001_img_01.jpg",
                    "content_type": "image/jpeg",
                    "size_bytes": 42,
                }
            ],
        )
        resource.latest_snapshot = snapshot
        resource.save(update_fields=["latest_snapshot"])

        with patch("resources.views.check_resource_link_status", side_effect=lambda current, force=False: current):
            response = self.client.get(reverse("resources:detail", args=[resource.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "保存済みファイルが見つかりません。再取得してください。")
        self.assertNotContains(response, "保存した画像")
        self.assertTrue(response.context["capture_mismatch"])

    def test_post_link_check_redirects_after_refresh(self):
        resource = Resource.objects.create(
            original_url="https://example.com/manual-check",
            normalized_url="https://example.com/manual-check",
            domain="example.com",
            title_manual="Manual Check",
        )

        with patch("resources.views.check_resource_link_status", side_effect=lambda current, force=False: current) as mocked_check:
            response = self.client.post(
                reverse("resources:detail", args=[resource.id]),
                {"_method": "LINK_CHECK"},
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], reverse("resources:detail", args=[resource.id]))
        mocked_check.assert_called_once()
        self.assertTrue(mocked_check.call_args.kwargs["force"])


class CapturePipelineTests(StorageOverrideMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.resource = Resource.objects.create(
            original_url="https://example.com/article",
            normalized_url="https://example.com/article",
            domain="example.com",
            title_manual="Article",
        )

    def create_temp_video(self, content: bytes = b"fake-video-bytes") -> tuple[Path, int]:
        handle, raw_path = tempfile.mkstemp(prefix="url-archive-test-video-", suffix=".mp4")
        with os.fdopen(handle, "wb") as temp_file:
            temp_file.write(content)
        self.addCleanup(lambda: Path(raw_path).unlink(missing_ok=True))
        return Path(raw_path), len(content)

    def test_capture_success_persists_snapshot_updates_resource_and_enqueues_ai(self):
        enqueue_capture_job(self.resource)
        capture_result = CaptureResult(
            fetch_url="https://example.com/article",
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            html="<html><head><title>Captured</title></head><body>Hello world</body></html>",
            extracted_text="Hello world summary text",
            metadata={"page_title": "Captured", "og_description": "Desc"},
            response_payload={"status_code": 200},
        )

        with patch("resources.services.choose_capture_result", return_value=capture_result):
            self.assertTrue(run_one_job())

        self.resource.refresh_from_db()
        snapshot = Snapshot.objects.get()
        self.assertEqual(self.resource.latest_snapshot, snapshot)
        self.assertEqual(self.resource.current_status, ResourceStatus.ACTIVE)
        self.assertTrue(Path(settings.ROOT_DIR / snapshot.raw_html_path).exists())
        self.assertEqual(CaptureJob.objects.filter(job_type=JobType.CAPTURE).get().status, JobStatus.SUCCEEDED)
        ai_job = CaptureJob.objects.filter(job_type=JobType.AI_ENRICH).get()
        self.assertEqual(ai_job.status, JobStatus.QUEUED)

        with patch(
            "resources.services.ai_pipeline.translate_text_to_japanese",
            return_value=("これは英語本文の日本語訳です。", {"translation_status": "translated", "detected_language": "en"}),
        ):
            self.assertTrue(run_one_job())
        snapshot.refresh_from_db()
        ai_job.refresh_from_db()
        self.assertEqual(ai_job.status, JobStatus.SUCCEEDED)
        self.assertEqual(snapshot.ai_summary, "")
        self.assertEqual(snapshot.ai_translation, "これは英語本文の日本語訳です。")

    def test_choose_capture_result_respects_capture_preferences(self):
        self.resource.capture_images = False
        self.resource.capture_videos = False
        self.resource.save(update_fields=["capture_images", "capture_videos"])

        http_result = CaptureResult(
            fetch_url=self.resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            html="<html></html>",
            extracted_text="body",
        )

        with self.assertLogs("resources.services", level="WARNING") as captured_logs:
            with patch("resources.services.fetch_with_http", return_value=http_result) as mocked_http:
                with patch("resources.services.should_use_playwright", return_value=False):
                    result = choose_capture_result(self.resource)

        self.assertEqual(result, http_result)
        self.assertTrue(
            any("Video capture is disabled by resource preference" in log for log in captured_logs.output)
        )
        mocked_http.assert_called_once_with(
            self.resource.normalized_url,
            capture_images=False,
            capture_videos=False,
            page_domain=self.resource.domain,
        )

    def test_choose_capture_result_attempts_video_capture_for_generic_domains(self):
        self.resource.capture_images = False
        self.resource.capture_videos = True
        self.resource.save(update_fields=["capture_images", "capture_videos"])

        http_result = CaptureResult(
            fetch_url=self.resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            html="<html><video src='https://cdn.example.com/sample.mp4'></video></html>",
            extracted_text="body",
        )

        with patch("resources.services.fetch_with_http", return_value=http_result) as mocked_http:
            with patch("resources.services.should_use_playwright", return_value=False):
                result = choose_capture_result(self.resource)

        self.assertEqual(result, http_result)
        mocked_http.assert_called_once_with(
            self.resource.normalized_url,
            capture_images=False,
            capture_videos=True,
            page_domain=self.resource.domain,
        )

    def test_choose_capture_result_skips_video_capture_for_search_only_resources(self):
        self.resource.capture_images = False
        self.resource.capture_videos = True
        self.resource.search_only = True
        self.resource.save(update_fields=["capture_images", "capture_videos", "search_only"])

        http_result = CaptureResult(
            fetch_url=self.resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            html="<html><video src='https://cdn.example.com/sample.mp4'></video></html>",
            extracted_text="body",
        )

        with patch("resources.services.fetch_with_http", return_value=http_result) as mocked_http:
            with patch("resources.services.should_use_playwright", return_value=False):
                result = choose_capture_result(self.resource)

        self.assertEqual(result, http_result)
        mocked_http.assert_called_once_with(
            self.resource.normalized_url,
            capture_images=False,
            capture_videos=False,
            page_domain=self.resource.domain,
        )

    def test_translate_text_to_japanese_skips_japanese_source(self):
        translation, payload = translate_text_to_japanese("これは日本語の文章です。")

        self.assertEqual(translation, "")
        self.assertEqual(payload["translation_status"], "source_already_japanese")

    def test_run_ai_pipeline_returns_translation_for_non_japanese_source(self):
        snapshot = Snapshot(
            resource=self.resource,
            snapshot_no=1,
            fetch_url=self.resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            extracted_text="Hello world from article body.",
            page_title="English article",
        )

        with patch(
            "resources.services.ai_pipeline.translate_text_to_japanese",
            return_value=("これは英語記事の日本語訳です。", {"translation_status": "translated", "detected_language": "en"}),
        ):
            result = run_ai_pipeline(snapshot)

        self.assertEqual(result.translation, "これは英語記事の日本語訳です。")
        self.assertEqual(result.payload["translation_status"], "translated")
        self.assertEqual(result.payload["translation_detected_language"], "en")

    def test_run_ai_pipeline_skips_translation_for_japanese_source(self):
        snapshot = Snapshot(
            resource=self.resource,
            snapshot_no=1,
            fetch_url=self.resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            extracted_text="これは日本語の本文です。最初に大事な結論があります。次に補足があります。",
            page_title="日本語記事",
        )

        result = run_ai_pipeline(snapshot)

        self.assertEqual(result.translation, "")
        self.assertEqual(result.payload["translation_status"], "source_already_japanese")
        self.assertEqual(result.payload["translation_detected_language"], "ja")

    @override_settings(
        AI_PROVIDER="openclaw",
        AI_MODEL="local-model",
        AI_API_BASE="http://llm.local/v1",
        AI_REQUEST_TIMEOUT=10,
        AI_TEMPERATURE=0.1,
        AI_MAX_OUTPUT_TOKENS=500,
        AI_SUMMARY_MAX_CHARS=80,
    )
    def test_run_ai_pipeline_uses_local_llm_chat_completions(self):
        snapshot = Snapshot(
            resource=self.resource,
            snapshot_no=1,
            fetch_url=self.resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            page_title="English article",
            extracted_text="Hello world from article body.",
        )
        captured = {}

        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "choices": [
                        {
                            "message": {
                                "content": (
                                    '{"summary":"短い要約です。",'
                                    '"translation":"これは英語記事の日本語訳です。",'
                                    '"category":"documentation",'
                                    '"tag_candidates":["local llm","archive"]}'
                                )
                            }
                        }
                    ]
                }

        class FakeClient:
            def __init__(self, *args, **kwargs):
                captured["client_kwargs"] = kwargs

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def post(self, url, json):
                captured["url"] = url
                captured["request"] = json
                return FakeResponse()

        with patch("resources.services.httpx.Client", FakeClient):
            result = run_ai_pipeline(snapshot)

        self.assertEqual(captured["url"], "http://llm.local/v1/chat/completions")
        self.assertEqual(captured["request"]["model"], "local-model")
        self.assertEqual(captured["request"]["temperature"], 0.1)
        self.assertEqual(captured["request"]["max_tokens"], 500)
        self.assertEqual(result.summary, "短い要約です。")
        self.assertEqual(result.translation, "これは英語記事の日本語訳です。")
        self.assertEqual(result.category, "documentation")
        self.assertEqual(result.payload["tag_candidates"], ["local llm", "archive"])
        self.assertEqual(result.payload["model"], "local-model")

    @override_settings(AI_PROVIDER="openclaw", AI_MODEL="local-model", AI_API_BASE="http://llm.local/v1")
    def test_execute_ai_job_persists_local_llm_summary(self):
        snapshot = Snapshot.objects.create(
            resource=self.resource,
            snapshot_no=1,
            fetch_url=self.resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            page_title="English article",
            extracted_text="Hello world from article body.",
        )
        ai_job = CaptureJob.objects.create(
            resource=self.resource,
            snapshot=snapshot,
            job_type=JobType.AI_ENRICH,
            status=JobStatus.QUEUED,
        )

        with patch(
            "resources.services.ai_pipeline.local_llm_chat",
            return_value=(
                '{"summary":"保存用の要約です。",'
                '"translation":"保存用の翻訳です。",'
                '"category":"news",'
                '"tag_candidates":["summary"]}'
            ),
        ):
            self.assertTrue(run_one_job())

        snapshot.refresh_from_db()
        ai_job.refresh_from_db()
        self.assertEqual(ai_job.status, JobStatus.SUCCEEDED)
        self.assertEqual(snapshot.ai_summary, "保存用の要約です。")
        self.assertEqual(snapshot.ai_translation, "保存用の翻訳です。")
        self.assertEqual(snapshot.ai_category, "news")

    @override_settings(AI_PROVIDER="openclaw", AI_MODEL="local-model", AI_API_BASE="http://llm.local/v1")
    def test_run_ai_pipeline_rejects_non_japanese_local_llm_translation(self):
        snapshot = Snapshot(
            resource=self.resource,
            snapshot_no=1,
            fetch_url=self.resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            page_title="English article",
            extracted_text="Hello world from article body.",
        )

        with patch(
            "resources.services.ai_pipeline.local_llm_chat",
            return_value=(
                '{"summary":"保存用の要約です。",'
                '"translation":"Hello world from article body.",'
                '"category":"general",'
                '"tag_candidates":["summary"]}'
            ),
        ):
            result = run_ai_pipeline(snapshot)

        self.assertEqual(result.translation, "")
        self.assertEqual(result.payload["translation_status"], "llm_translation_unavailable")

    def test_capture_success_persists_downloaded_images(self):
        enqueue_capture_job(self.resource)
        capture_result = CaptureResult(
            fetch_url="https://example.com/article",
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            html="<html><body><img src='hero.jpg'></body></html>",
            extracted_text="Article body",
            metadata={"page_title": "Captured"},
            response_payload={"status_code": 200},
            captured_images=[
                CapturedImage(
                    source_url="https://example.com/hero.jpg",
                    content=b"fake-image-bytes",
                    content_type="image/jpeg",
                )
            ],
        )

        with patch("resources.services.choose_capture_result", return_value=capture_result):
            self.assertTrue(run_one_job())

        snapshot = Snapshot.objects.get()
        self.assertEqual(snapshot.image_count, 1)
        self.assertEqual(snapshot.image_assets[0]["source_url"], "https://example.com/hero.jpg")
        image_path = settings.ROOT_DIR / snapshot.image_assets[0]["path"]
        self.assertTrue(image_path.exists())

    def test_capture_success_persists_downloaded_videos(self):
        self.resource.domain = "x.com"
        self.resource.normalized_url = "https://x.com/example/status/1"
        self.resource.original_url = "https://x.com/example/status/1"
        self.resource.save(update_fields=["domain", "normalized_url", "original_url"])
        enqueue_capture_job(self.resource)
        temp_path, size_bytes = self.create_temp_video()
        capture_result = CaptureResult(
            fetch_url="https://x.com/example/status/1",
            fetch_method=FetchMethod.PLAYWRIGHT,
            http_status=200,
            html="<html><body><video src='https://video.twimg.com/ext_tw_video/sample.mp4'></video></body></html>",
            extracted_text="Video body",
            metadata={"page_title": "Captured video"},
            response_payload={"status_code": 200},
            captured_videos=[
                CapturedVideo(
                    source_url="https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/sample.mp4",
                    temp_path=temp_path,
                    size_bytes=size_bytes,
                    content_type="video/mp4",
                )
            ],
        )

        with patch("resources.services.choose_capture_result", return_value=capture_result):
            self.assertTrue(run_one_job())

        snapshot = Snapshot.objects.get()
        self.assertEqual(snapshot.video_count, 1)
        self.assertEqual(snapshot.video_assets[0]["source_url"], "https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/sample.mp4")
        video_path = settings.ROOT_DIR / snapshot.video_assets[0]["path"]
        self.assertTrue(video_path.exists())
        self.assertFalse(temp_path.exists())

    def test_capture_success_does_not_override_capture_preferences(self):
        self.resource.capture_images = False
        self.resource.capture_videos = False
        self.resource.save(update_fields=["capture_images", "capture_videos"])
        enqueue_capture_job(self.resource)
        temp_path, size_bytes = self.create_temp_video()
        capture_result = CaptureResult(
            fetch_url="https://example.com/article",
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            html="<html><body>Captured</body></html>",
            extracted_text="Captured body",
            metadata={"page_title": "Captured"},
            response_payload={"status_code": 200},
            captured_images=[
                CapturedImage(
                    source_url="https://example.com/hero.jpg",
                    content=b"fake-image-bytes",
                    content_type="image/jpeg",
                )
            ],
            captured_videos=[
                CapturedVideo(
                    source_url="https://cdn.example.com/video.mp4",
                    temp_path=temp_path,
                    size_bytes=size_bytes,
                    content_type="video/mp4",
                )
            ],
        )

        with patch("resources.services.choose_capture_result", return_value=capture_result):
            self.assertTrue(run_one_job())

        self.resource.refresh_from_db()
        self.assertFalse(self.resource.capture_images)
        self.assertFalse(self.resource.capture_videos)

    def test_capture_success_persists_downloaded_video_metadata(self):
        self.resource.domain = "instagram.com"
        self.resource.normalized_url = "https://www.instagram.com/reel/1"
        self.resource.original_url = "https://www.instagram.com/reel/1"
        self.resource.save(update_fields=["domain", "normalized_url", "original_url"])
        enqueue_capture_job(self.resource)
        temp_path, size_bytes = self.create_temp_video()
        capture_result = CaptureResult(
            fetch_url="https://www.instagram.com/reel/1",
            fetch_method=FetchMethod.PLAYWRIGHT,
            http_status=200,
            html="<html><body>Instagram</body></html>",
            extracted_text="Instagram body",
            metadata={"page_title": "Instagram video"},
            response_payload={"status_code": 200},
            captured_videos=[
                CapturedVideo(
                    source_url="https://scontent.cdninstagram.com/video.mp4",
                    temp_path=temp_path,
                    size_bytes=size_bytes,
                    content_type="video/mp4",
                    metadata={
                        "has_video": True,
                        "has_audio": True,
                        "duration_sec": 12.3,
                        "extraction_strategy": "instagram_direct",
                        "failure_reason": "",
                    },
                )
            ],
        )

        with patch("resources.services.choose_capture_result", return_value=capture_result):
            self.assertTrue(run_one_job())

        snapshot = Snapshot.objects.get()
        self.assertTrue(snapshot.video_assets[0]["has_video"])
        self.assertTrue(snapshot.video_assets[0]["has_audio"])
        self.assertEqual(snapshot.video_assets[0]["duration_sec"], 12.3)
        self.assertEqual(snapshot.video_assets[0]["extraction_strategy"], "instagram_direct")

    def test_download_video_assets_for_instagram_marks_video_only_as_partial(self):
        responses = {
            "https://scontent.cdninstagram.com/video-only.mp4": FakeStreamResponse(
                "https://scontent.cdninstagram.com/video-only.mp4",
                content_type="video/mp4",
                content=b"video-only",
            )
        }
        extra_candidates = [
            {
                "url": "https://scontent.cdninstagram.com/video-only.mp4",
                "source": "network_response",
                "media_kind": "video",
                "content_type": "video/mp4",
                "resource_type": "media",
            }
        ]

        with patch("resources.services.media_downloads.httpx.Client", return_value=FakeHttpClient(responses)):
            with patch("resources.services.media_downloads.get_ffprobe_executable", return_value="ffprobe"):
                with patch(
                    "resources.services.media_downloads.MediaProbe.probe_file",
                    return_value=MediaProbeResult(has_video=True, has_audio=False, duration_sec=9.5),
                ):
                    result = download_video_assets(
                        "https://www.instagram.com/reel/example/",
                        "<html></html>",
                        page_domain="instagram.com",
                        extra_candidates=extra_candidates,
                    )

        self.assertEqual(result.extraction_status, "partial")
        self.assertEqual(result.failure_reason, "video_only_candidate")
        self.assertEqual(result.assets, [])
        self.assertTrue(result.attempts[0]["has_video"])
        self.assertFalse(result.attempts[0]["has_audio"])
        self.assertEqual(result.summary["video_only_count"], 1)

    def test_download_video_assets_for_instagram_muxes_video_and_audio_candidates(self):
        responses = {
            "https://scontent.cdninstagram.com/video.mp4": FakeStreamResponse(
                "https://scontent.cdninstagram.com/video.mp4",
                content_type="video/mp4",
                content=b"video-track",
            ),
            "https://scontent.cdninstagram.com/audio.mp4": FakeStreamResponse(
                "https://scontent.cdninstagram.com/audio.mp4",
                content_type="audio/mp4",
                content=b"audio-track",
            ),
        }
        extra_candidates = [
            {
                "url": "https://scontent.cdninstagram.com/video.mp4",
                "source": "network_response",
                "media_kind": "video",
                "content_type": "video/mp4",
                "resource_type": "media",
            },
            {
                "url": "https://scontent.cdninstagram.com/audio.mp4",
                "source": "network_response",
                "media_kind": "audio",
                "content_type": "audio/mp4",
                "resource_type": "media",
            },
        ]

        def fake_ffmpeg_run(command, capture_output=True, text=True):
            Path(command[-1]).write_bytes(b"muxed-video")
            return subprocess.CompletedProcess(command, 0, "", "")

        with patch("resources.services.media_downloads.httpx.Client", return_value=FakeHttpClient(responses)):
            with patch("resources.services.media_downloads.get_ffprobe_executable", return_value="ffprobe"):
                with patch(
                    "resources.services.media_downloads.MediaProbe.probe_file",
                    side_effect=[
                        MediaProbeResult(has_video=True, has_audio=False, duration_sec=12.0),
                        MediaProbeResult(has_video=False, has_audio=True, duration_sec=12.1),
                        MediaProbeResult(has_video=True, has_audio=True, duration_sec=12.0),
                    ],
                ):
                    with patch("resources.services.media_downloads.get_ffmpeg_executable", return_value="ffmpeg"):
                        with patch("resources.services.media_downloads.subprocess.run", side_effect=fake_ffmpeg_run):
                            result = download_video_assets(
                                "https://www.instagram.com/reel/example/",
                                "<html></html>",
                                page_domain="instagram.com",
                                extra_candidates=extra_candidates,
                            )

        self.assertEqual(result.extraction_status, "success")
        self.assertEqual(result.extraction_strategy, "instagram_mux_ffmpeg")
        self.assertEqual(len(result.assets), 1)
        self.assertTrue(result.assets[0].metadata["has_video"])
        self.assertTrue(result.assets[0].metadata["has_audio"])
        self.assertEqual(result.attempts[-1]["mode"], "instagram_ffmpeg_mux")
        self.assertEqual(result.summary["audio_only_count"], 1)

    def test_download_video_assets_for_instagram_requires_ffprobe(self):
        responses = {
            "https://scontent.cdninstagram.com/video.mp4": FakeStreamResponse(
                "https://scontent.cdninstagram.com/video.mp4",
                content_type="video/mp4",
                content=b"video-track",
            )
        }

        with patch("resources.services.media_downloads.httpx.Client", return_value=FakeHttpClient(responses)):
            with patch("resources.services.media_downloads.get_ffprobe_executable", return_value=None):
                result = download_video_assets(
                    "https://www.instagram.com/reel/example/",
                    "<html></html>",
                    page_domain="instagram.com",
                    extra_candidates=[
                        {
                            "url": "https://scontent.cdninstagram.com/video.mp4",
                            "source": "network_response",
                            "media_kind": "video",
                            "content_type": "video/mp4",
                            "resource_type": "media",
                        }
                    ],
                )

        self.assertEqual(result.extraction_status, "success")
        self.assertEqual(result.extraction_strategy, "instagram_direct_no_probe")
        self.assertEqual(len(result.assets), 1)
        self.assertTrue(result.assets[0].metadata["has_video"])
        self.assertIsNone(result.assets[0].metadata["has_audio"])
        self.assertEqual(result.attempts[0]["mode"], "instagram_direct_no_probe")

    def test_download_video_assets_for_x_uses_playwright_extra_candidates(self):
        responses = {
            "https://video.twimg.com/tweet_video/sample.mp4": FakeStreamResponse(
                "https://video.twimg.com/tweet_video/sample.mp4",
                content_type="video/mp4",
                content=b"x-video-track",
            )
        }

        with patch("resources.services.media_downloads.httpx.Client", return_value=FakeHttpClient(responses)):
            with patch(
                "resources.services.media_downloads.MediaProbe.probe_file",
                return_value=MediaProbeResult(has_video=True, has_audio=False, duration_sec=8.0),
            ):
                result = download_video_assets(
                    "https://x.com/example/status/1",
                    "<html></html>",
                    page_domain="x.com",
                    extra_candidates=[
                        {
                            "url": "https://video.twimg.com/tweet_video/sample.mp4",
                            "source": "network_response",
                            "media_kind": "video",
                            "content_type": "video/mp4",
                            "resource_type": "media",
                        }
                    ],
                )

        self.assertEqual(result.extraction_status, "success")
        self.assertEqual(result.candidate_urls, ["https://video.twimg.com/tweet_video/sample.mp4"])
        self.assertEqual(len(result.assets), 1)
        self.assertEqual(result.assets[0].source_url, "https://video.twimg.com/tweet_video/sample.mp4")
        self.assertTrue(result.attempts[0]["has_video"])
        self.assertEqual(result.attempts[0]["duration_sec"], 8.0)

    def test_download_video_assets_rejects_invalid_x_mp4_after_probe(self):
        responses = {
            "https://video.twimg.com/tweet_video/broken.mp4": FakeStreamResponse(
                "https://video.twimg.com/tweet_video/broken.mp4",
                content_type="video/mp4",
                content=b"not-a-real-video",
            )
        }

        with patch("resources.services.media_downloads.httpx.Client", return_value=FakeHttpClient(responses)):
            with patch(
                "resources.services.media_downloads.MediaProbe.probe_file",
                return_value=MediaProbeResult(failure_reason="moov atom not found"),
            ):
                result = download_video_assets(
                    "https://x.com/example/status/1",
                    "<html></html>",
                    page_domain="x.com",
                    extra_candidates=[
                        {
                            "url": "https://video.twimg.com/tweet_video/broken.mp4",
                            "source": "network_response",
                            "media_kind": "video",
                            "content_type": "video/mp4",
                            "resource_type": "media",
                        }
                    ],
                )

        self.assertEqual(result.extraction_status, "failed")
        self.assertEqual(result.failure_reason, "moov atom not found")
        self.assertEqual(result.assets, [])
        self.assertEqual(result.attempts[0]["result"], "error")
        self.assertEqual(result.attempts[0]["output_size_bytes"], len(b"not-a-real-video"))

    def test_download_video_assets_for_instagram_logs_duration_mismatch_skip(self):
        responses = {
            "https://scontent.cdninstagram.com/video.mp4": FakeStreamResponse(
                "https://scontent.cdninstagram.com/video.mp4",
                content_type="video/mp4",
                content=b"video-track",
            ),
            "https://scontent.cdninstagram.com/audio.mp4": FakeStreamResponse(
                "https://scontent.cdninstagram.com/audio.mp4",
                content_type="audio/mp4",
                content=b"audio-track",
            ),
        }
        extra_candidates = [
            {
                "url": "https://scontent.cdninstagram.com/video.mp4",
                "source": "network_response",
                "media_kind": "video",
                "content_type": "video/mp4",
                "resource_type": "media",
            },
            {
                "url": "https://scontent.cdninstagram.com/audio.mp4",
                "source": "network_response",
                "media_kind": "audio",
                "content_type": "audio/mp4",
                "resource_type": "media",
            },
        ]

        with patch("resources.services.media_downloads.httpx.Client", return_value=FakeHttpClient(responses)):
            with patch("resources.services.media_downloads.get_ffprobe_executable", return_value="ffprobe"):
                with patch(
                    "resources.services.media_downloads.MediaProbe.probe_file",
                    side_effect=[
                        MediaProbeResult(has_video=True, has_audio=False, duration_sec=12.0),
                        MediaProbeResult(has_video=False, has_audio=True, duration_sec=30.0),
                    ],
                ):
                    result = download_video_assets(
                        "https://www.instagram.com/reel/example/",
                        "<html></html>",
                        page_domain="instagram.com",
                        extra_candidates=extra_candidates,
                    )

        self.assertEqual(result.extraction_status, "partial")
        self.assertTrue(any(log["reason"] == "duration_mismatch" for log in result.skip_logs))

    def test_failed_capture_retries_and_records_failure_snapshot(self):
        enqueue_capture_job(self.resource)
        failure = CaptureResult(
            fetch_url="https://example.com/article",
            fetch_method=FetchMethod.HTTP,
            error_message="timeout",
            response_payload={"error": "timeout"},
        )

        with patch("resources.services.choose_capture_result", return_value=failure):
            self.assertTrue(run_one_job())

        job = CaptureJob.objects.get(job_type=JobType.CAPTURE)
        snapshot = Snapshot.objects.get()
        self.resource.refresh_from_db()
        self.assertEqual(snapshot.error_message, "timeout")
        self.assertEqual(self.resource.current_status, ResourceStatus.FETCH_FAILED)
        self.assertTrue(self.resource.capture_images)
        self.assertTrue(self.resource.capture_videos)
        self.assertEqual(job.status, JobStatus.RETRY_WAIT)
        self.assertEqual(job.attempt_count, 1)
        self.assertGreater(job.scheduled_at, timezone.now())

    def test_gone_result_finishes_without_retry(self):
        enqueue_capture_job(self.resource)
        gone_result = CaptureResult(
            fetch_url="https://example.com/article",
            fetch_method=FetchMethod.HTTP,
            http_status=404,
            extracted_text="This page was deleted",
            metadata={"page_title": "Not found"},
            response_payload={"status_code": 404},
            error_message="HTTP 404",
            deleted_like=True,
        )

        with patch("resources.services.choose_capture_result", return_value=gone_result):
            self.assertTrue(run_one_job())

        job = CaptureJob.objects.get(job_type=JobType.CAPTURE)
        self.resource.refresh_from_db()
        self.assertEqual(self.resource.current_status, ResourceStatus.GONE)
        self.assertEqual(job.status, JobStatus.SUCCEEDED)

    def test_check_resource_link_status_updates_cached_fields(self):
        with patch(
            "resources.services.link_checks.perform_link_check",
            return_value=LinkCheckResult(status=LinkStatus.GONE, http_status=404),
        ):
            check_resource_link_status(self.resource, force=True)

        self.resource.refresh_from_db()
        self.assertEqual(self.resource.link_status, LinkStatus.GONE)
        self.assertEqual(self.resource.last_link_check_http_status, 404)
        self.assertIsNotNone(self.resource.last_link_check_at)

    def test_delete_removes_artifacts_and_related_rows(self):
        enqueue_capture_job(self.resource)
        temp_path, size_bytes = self.create_temp_video()
        capture_result = CaptureResult(
            fetch_url="https://example.com/article",
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            html="<html><head><title>Captured</title></head><body>Hello world</body></html>",
            extracted_text="Hello world summary text",
            metadata={"page_title": "Captured"},
            response_payload={"status_code": 200},
            captured_images=[
                CapturedImage(
                    source_url="https://example.com/hero.jpg",
                    content=b"fake-image-bytes",
                    content_type="image/jpeg",
                )
            ],
            captured_videos=[
                CapturedVideo(
                    source_url="https://video.twimg.com/ext_tw_video/123/pu/vid/avc1/sample.mp4",
                    temp_path=temp_path,
                    size_bytes=size_bytes,
                    content_type="video/mp4",
                )
            ],
        )

        with patch("resources.services.choose_capture_result", return_value=capture_result):
            run_one_job()

        snapshot = Snapshot.objects.get()
        html_path = settings.ROOT_DIR / snapshot.raw_html_path
        image_path = settings.ROOT_DIR / snapshot.image_assets[0]["path"]
        video_path = settings.ROOT_DIR / snapshot.video_assets[0]["path"]
        self.assertTrue(html_path.exists())
        self.assertTrue(image_path.exists())
        self.assertTrue(video_path.exists())

        client = Client()
        response = client.post(
            reverse("resources:detail", args=[self.resource.id]),
            {"_method": "DELETE"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertFalse(Resource.objects.exists())
        self.assertFalse(Snapshot.objects.exists())
        self.assertFalse(CaptureJob.objects.exists())
        self.assertFalse(html_path.exists())
        self.assertFalse(image_path.exists())
        self.assertFalse(video_path.exists())

    def test_detail_delete_accepts_csrf_protected_method_override(self):
        client = Client(enforce_csrf_checks=True)
        detail_url = reverse("resources:detail", args=[self.resource.id])
        with patch("resources.views.check_resource_link_status", side_effect=lambda resource, force=False: resource):
            get_response = client.get(detail_url)
        self.assertEqual(get_response.status_code, 200)
        csrf_token = client.cookies["csrftoken"].value

        response = client.post(
            detail_url,
            {
                "_method": "DELETE",
                "csrfmiddlewaretoken": csrf_token,
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertFalse(Resource.objects.filter(pk=self.resource.id).exists())


class ResetCaptureFlagsCommandTests(StorageOverrideMixin, TestCase):
    def create_storage_file(self, root: Path, resource_id: int, filename: str) -> Path:
        resource_dir = root / f"resource_{resource_id:04d}"
        resource_dir.mkdir(parents=True, exist_ok=True)
        file_path = resource_dir / filename
        file_path.write_bytes(b"stored-asset")
        return file_path

    def test_reset_capture_flags_dry_run_does_not_update_preferences(self):
        resource = Resource.objects.create(
            original_url="https://x.com/example/status/1",
            normalized_url="https://x.com/example/status/1",
            domain="x.com",
            title_manual="Disabled Capture",
            capture_images=False,
            capture_videos=False,
        )
        self.create_storage_file(settings.IMAGE_STORAGE_ROOT, resource.id, "snapshot_0001_img_01.jpg")
        self.create_storage_file(settings.VIDEO_STORAGE_ROOT, resource.id, "snapshot_0001_vid_01.mp4")

        output = StringIO()
        call_command("reset_capture_flags", "--dry-run", stdout=output)

        resource.refresh_from_db()
        self.assertFalse(resource.capture_images)
        self.assertFalse(resource.capture_videos)
        rendered = output.getvalue()
        self.assertIn("Resources to update: 1", rendered)
        self.assertIn("capture_images false -> true: 1 (with files: 1, without files: 0)", rendered)
        self.assertIn("capture_videos false -> true: 1 (with files: 1, without files: 0)", rendered)
        self.assertIn("Dry run only. No database rows were changed.", rendered)

    def test_reset_capture_flags_updates_disabled_preferences_with_yes(self):
        with_files = Resource.objects.create(
            original_url="https://www.instagram.com/reel/1",
            normalized_url="https://www.instagram.com/reel/1",
            domain="instagram.com",
            title_manual="Video With Files",
            capture_images=True,
            capture_videos=False,
        )
        without_files = Resource.objects.create(
            original_url="https://example.com/article",
            normalized_url="https://example.com/article",
            domain="example.com",
            title_manual="No Files",
            capture_images=False,
            capture_videos=False,
        )
        already_enabled = Resource.objects.create(
            original_url="https://example.com/enabled",
            normalized_url="https://example.com/enabled",
            domain="example.com",
            title_manual="Already Enabled",
        )
        self.create_storage_file(settings.VIDEO_STORAGE_ROOT, with_files.id, "snapshot_0001_vid_01.mp4")

        output = StringIO()
        call_command("reset_capture_flags", "--yes", stdout=output)

        with_files.refresh_from_db()
        without_files.refresh_from_db()
        already_enabled.refresh_from_db()
        self.assertTrue(with_files.capture_images)
        self.assertTrue(with_files.capture_videos)
        self.assertTrue(without_files.capture_images)
        self.assertTrue(without_files.capture_videos)
        self.assertTrue(already_enabled.capture_images)
        self.assertTrue(already_enabled.capture_videos)
        rendered = output.getvalue()
        self.assertIn("Resources to update: 2", rendered)
        self.assertIn("capture_videos false -> true: 2 (with files: 1, without files: 1)", rendered)
        self.assertIn("Updated resources: 2", rendered)
