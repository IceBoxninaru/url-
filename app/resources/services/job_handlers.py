from __future__ import annotations

from django.db import transaction

from jobs.models import CaptureJob
from resources.models import Resource, ResourceStatus
from snapshots.models import Snapshot

from .ai_pipeline import run_ai_pipeline
from .capture import choose_capture_result
from .jobs import enqueue_ai_job, status_from_snapshot
from .media_downloads import cleanup_capture_result
from .persistence import persist_snapshot
from .urls import normalize_url


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
