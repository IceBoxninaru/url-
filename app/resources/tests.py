import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.conf import settings
from django.test import Client, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from jobs.models import CaptureJob, JobStatus, JobType
from jobs.services import run_one_job
from resources.forms import NOTE_TEMPLATE_CHOICES
from resources.models import LinkStatus, Resource, ResourceStatus, ReviewState
from resources.services import (
    CaptureResult,
    CapturedImage,
    LinkCheckResult,
    check_resource_link_status,
    collect_image_urls,
    enqueue_capture_job,
    normalize_url,
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
        )
        self.settings_override.enable()
        self.addCleanup(self.settings_override.disable)
        self.addCleanup(lambda: shutil.rmtree(self.storage_base, ignore_errors=True))


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


class ResourceViewTests(StorageOverrideMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.client = Client()
        self.tag_a = Tag.objects.create(name="alpha", color="#123456", sort_order=10)
        self.tag_b = Tag.objects.create(name="beta", color="#654321", sort_order=20)

    def test_get_create_page(self):
        response = self.client.get(reverse("resources:create"))
        self.assertEqual(response.status_code, 200)

    def test_post_create_creates_resource_and_capture_job(self):
        response = self.client.post(
            reverse("resources:create"),
            {
                "original_url": "example.com/post/1?utm_source=mail",
                "title_manual": "Manual title",
                "note": "Important note",
                "favorite": "on",
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
        self.assertEqual(resource.tags.count(), 2)
        job = CaptureJob.objects.get()
        self.assertEqual(job.job_type, JobType.CAPTURE)
        self.assertEqual(job.status, JobStatus.QUEUED)

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

    def test_post_create_applies_note_template_and_review_state(self):
        template_value = NOTE_TEMPLATE_CHOICES[0][0]

        response = self.client.post(
            reverse("resources:create"),
            {
                "original_url": "https://example.com/post/template",
                "title_manual": "Template",
                "note_template": template_value,
                "review_state": ReviewState.NEEDS_REVIEW,
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], reverse("resources:list"))
        resource = Resource.objects.get()
        self.assertEqual(resource.note, template_value)
        self.assertEqual(resource.review_state, ReviewState.NEEDS_REVIEW)

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

    def test_detail_shows_snapshot_before_edit_section(self):
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
        content = response.content.decode("utf-8")
        self.assertLess(content.find("最新スナップショット"), content.find("編集"))

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

        self.assertTrue(run_one_job())
        snapshot.refresh_from_db()
        ai_job.refresh_from_db()
        self.assertEqual(ai_job.status, JobStatus.SUCCEEDED)
        self.assertNotEqual(snapshot.ai_summary, "")

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
            "resources.services.perform_link_check",
            return_value=LinkCheckResult(status=LinkStatus.GONE, http_status=404),
        ):
            check_resource_link_status(self.resource, force=True)

        self.resource.refresh_from_db()
        self.assertEqual(self.resource.link_status, LinkStatus.GONE)
        self.assertEqual(self.resource.last_link_check_http_status, 404)
        self.assertIsNotNone(self.resource.last_link_check_at)

    def test_delete_removes_artifacts_and_related_rows(self):
        enqueue_capture_job(self.resource)
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
        )

        with patch("resources.services.choose_capture_result", return_value=capture_result):
            run_one_job()

        snapshot = Snapshot.objects.get()
        html_path = settings.ROOT_DIR / snapshot.raw_html_path
        image_path = settings.ROOT_DIR / snapshot.image_assets[0]["path"]
        self.assertTrue(html_path.exists())
        self.assertTrue(image_path.exists())

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
