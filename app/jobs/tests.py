import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.conf import settings
from django.test import TestCase, override_settings

from jobs.models import CaptureJob, JobStatus, JobType
from jobs.services import run_one_job
from resources.models import Resource
from resources.services import CaptureResult, enqueue_capture_job
from snapshots.models import FetchMethod, Snapshot


class AIJobIsolationTests(TestCase):
    def setUp(self):
        super().setUp()
        self.storage_base = Path(tempfile.mkdtemp(prefix="url-archive-jobs-", dir=settings.ROOT_DIR))
        self.override = override_settings(
            HTML_STORAGE_ROOT=self.storage_base / "html",
            TEXT_STORAGE_ROOT=self.storage_base / "text",
            JSON_STORAGE_ROOT=self.storage_base / "json",
            SCREENSHOT_STORAGE_ROOT=self.storage_base / "screenshots",
        )
        self.override.enable()
        self.addCleanup(self.override.disable)
        self.addCleanup(lambda: shutil.rmtree(self.storage_base, ignore_errors=True))
        self.resource = Resource.objects.create(
            original_url="https://example.com/article",
            normalized_url="https://example.com/article",
            domain="example.com",
            title_manual="Article",
        )

    def test_ai_job_failure_does_not_break_completed_capture(self):
        enqueue_capture_job(self.resource)
        capture_result = CaptureResult(
            fetch_url="https://example.com/article",
            fetch_method=FetchMethod.HTTP,
            http_status=200,
            html="<html><head><title>Captured</title></head><body>Hello world</body></html>",
            extracted_text="Hello world summary text",
            metadata={"page_title": "Captured"},
            response_payload={"status_code": 200},
        )

        with patch("resources.services.choose_capture_result", return_value=capture_result):
            run_one_job()

        capture_job = CaptureJob.objects.get(job_type=JobType.CAPTURE)
        ai_job = CaptureJob.objects.get(job_type=JobType.AI_ENRICH)
        snapshot = Snapshot.objects.get()
        self.assertEqual(capture_job.status, JobStatus.SUCCEEDED)

        with patch("resources.services.run_ai_pipeline", side_effect=RuntimeError("ai down")):
            run_one_job()

        ai_job.refresh_from_db()
        snapshot.refresh_from_db()
        self.assertEqual(ai_job.status, JobStatus.RETRY_WAIT)
        self.assertEqual(snapshot.ai_summary, "")
