from __future__ import annotations

import time
from datetime import timedelta

from django.conf import settings
from django.db import connection, transaction
from django.utils import timezone

from jobs.models import CaptureJob, JobStatus, JobType
from resources.services import execute_ai_job, execute_capture_job


def due_jobs_queryset():
    return (
        CaptureJob.objects
        .filter(
            status__in=[JobStatus.QUEUED, JobStatus.RETRY_WAIT],
            scheduled_at__lte=timezone.now(),
        )
        .order_by("-priority", "scheduled_at", "id")
    )


def claim_next_job() -> CaptureJob | None:
    with transaction.atomic():
        queryset = due_jobs_queryset()
        if connection.vendor == "postgresql":
            queryset = queryset.select_for_update(skip_locked=True)
        elif connection.features.has_select_for_update:
            queryset = queryset.select_for_update()
        job = queryset.first()
        if job is None:
            return None
        job.status = JobStatus.RUNNING
        job.started_at = timezone.now()
        job.finished_at = None
        job.attempt_count += 1
        job.error_message = ""
        job.save(update_fields=["status", "started_at", "finished_at", "attempt_count", "error_message", "updated_at"])
        return (
            CaptureJob.objects.select_related("resource", "snapshot", "resource__latest_snapshot")
            .get(pk=job.pk)
        )


def complete_job(job: CaptureJob):
    job.status = JobStatus.SUCCEEDED
    job.finished_at = timezone.now()
    job.error_message = ""
    job.save(update_fields=["status", "finished_at", "error_message", "updated_at"])


def fail_or_retry_job(job: CaptureJob, message: str):
    max_retries = settings.JOB_MAX_RETRIES
    delays = settings.JOB_RETRY_DELAYS_SECONDS
    if job.attempt_count <= max_retries:
        delay = delays[min(job.attempt_count - 1, len(delays) - 1)]
        job.status = JobStatus.RETRY_WAIT
        job.scheduled_at = timezone.now() + timedelta(seconds=delay)
    else:
        job.status = JobStatus.FAILED
    job.finished_at = timezone.now()
    job.error_message = message
    job.save(update_fields=["status", "scheduled_at", "finished_at", "error_message", "updated_at"])


def run_job(job: CaptureJob):
    if job.job_type == JobType.CAPTURE:
        execute_capture_job(job)
        return
    if job.job_type == JobType.AI_ENRICH:
        execute_ai_job(job)
        return
    raise ValueError(f"Unsupported job type: {job.job_type}")


def run_one_job() -> bool:
    job = claim_next_job()
    if job is None:
        return False
    try:
        run_job(job)
    except Exception as exc:
        fail_or_retry_job(job, str(exc))
        return True
    complete_job(job)
    return True


def run_worker_loop(*, once: bool = False, sleep_seconds: int = 5, max_jobs: int | None = None):
    processed = 0
    while True:
        ran = run_one_job()
        if ran:
            processed += 1
        if once:
            if not ran or (max_jobs is not None and processed >= max_jobs):
                return processed
        else:
            if max_jobs is not None and processed >= max_jobs:
                return processed
            if not ran:
                time.sleep(sleep_seconds)
