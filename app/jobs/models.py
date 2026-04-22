from django.conf import settings
from django.db import models
from django.utils import timezone


class JobType(models.TextChoices):
    CAPTURE = "capture", "取得"
    AI_ENRICH = "ai_enrich", "AI補完"


class JobStatus(models.TextChoices):
    QUEUED = "queued", "待機中"
    RUNNING = "running", "実行中"
    RETRY_WAIT = "retry_wait", "再試行待ち"
    SUCCEEDED = "succeeded", "成功"
    FAILED = "failed", "失敗"


class CaptureJobQuerySet(models.QuerySet):
    def due(self):
        return self.filter(
            status__in=[JobStatus.QUEUED, JobStatus.RETRY_WAIT],
            scheduled_at__lte=timezone.now(),
        )

    def with_related(self):
        return self.select_related("resource", "snapshot", "resource__latest_snapshot")


class CaptureJob(models.Model):
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="owned_capture_jobs",
    )
    resource = models.ForeignKey("resources.Resource", on_delete=models.CASCADE, related_name="jobs")
    snapshot = models.ForeignKey(
        "snapshots.Snapshot",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="jobs",
    )
    job_type = models.CharField(max_length=32, choices=JobType.choices, default=JobType.CAPTURE)
    status = models.CharField(max_length=32, choices=JobStatus.choices, default=JobStatus.QUEUED, db_index=True)
    priority = models.IntegerField(default=100, db_index=True)
    attempt_count = models.PositiveIntegerField(default=0)
    scheduled_at = models.DateTimeField(default=timezone.now, db_index=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(blank=True)
    payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = CaptureJobQuerySet.as_manager()

    class Meta:
        ordering = ["-priority", "scheduled_at", "id"]

    def __str__(self):
        return f"{self.job_type}:{self.resource_id}:{self.status}"
