from __future__ import annotations

from django.utils import timezone

from jobs.models import CaptureJob, JobStatus, JobType
from resources.models import Resource, ResourceStatus
from snapshots.models import Snapshot


def status_from_snapshot(snapshot: Snapshot) -> str:
    if snapshot.http_status in {404, 410}:
        return ResourceStatus.GONE
    if snapshot.is_deleted_like:
        return ResourceStatus.MAYBE_DELETED
    if snapshot.error_message or (snapshot.http_status and snapshot.http_status >= 400):
        return ResourceStatus.FETCH_FAILED
    return ResourceStatus.ACTIVE


def enqueue_capture_job(resource: Resource, priority: int = 100) -> CaptureJob:
    return CaptureJob.objects.create(
        owner=resource.owner,
        resource=resource,
        job_type=JobType.CAPTURE,
        status=JobStatus.QUEUED,
        priority=priority,
        scheduled_at=timezone.now(),
    )


def enqueue_ai_job(resource: Resource, snapshot: Snapshot, priority: int = 50) -> CaptureJob:
    return CaptureJob.objects.create(
        owner=resource.owner,
        resource=resource,
        snapshot=snapshot,
        job_type=JobType.AI_ENRICH,
        status=JobStatus.QUEUED,
        priority=priority,
        scheduled_at=timezone.now(),
    )
