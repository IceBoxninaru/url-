from django.db import models
from django.urls import reverse
from django.utils import timezone


class FetchMethod(models.TextChoices):
    HTTP = "http", "HTTP取得"
    PLAYWRIGHT = "playwright", "ブラウザ取得"


class Snapshot(models.Model):
    resource = models.ForeignKey("resources.Resource", on_delete=models.CASCADE, related_name="snapshots")
    snapshot_no = models.PositiveIntegerField()
    fetch_url = models.URLField(max_length=2000)
    fetch_method = models.CharField(max_length=32, choices=FetchMethod.choices, default=FetchMethod.HTTP)
    http_status = models.PositiveIntegerField(null=True, blank=True)
    fetched_at = models.DateTimeField(default=timezone.now, db_index=True)
    page_title = models.CharField(max_length=500, blank=True)
    site_name = models.CharField(max_length=255, blank=True)
    author = models.CharField(max_length=255, blank=True)
    published_at = models.DateTimeField(null=True, blank=True)
    og_title = models.CharField(max_length=500, blank=True)
    og_description = models.TextField(blank=True)
    og_image_url = models.URLField(max_length=2000, blank=True)
    extracted_text = models.TextField(blank=True)
    ai_summary = models.TextField(blank=True)
    ai_category = models.CharField(max_length=120, blank=True)
    ai_payload = models.JSONField(default=dict, blank=True)
    image_assets = models.JSONField(default=list, blank=True)
    video_assets = models.JSONField(default=list, blank=True)
    content_hash = models.CharField(max_length=64, blank=True, db_index=True)
    raw_html_path = models.CharField(max_length=500, blank=True)
    raw_text_path = models.CharField(max_length=500, blank=True)
    raw_json_path = models.CharField(max_length=500, blank=True)
    screenshot_full_path = models.CharField(max_length=500, blank=True)
    screenshot_taken_at = models.DateTimeField(null=True, blank=True)
    page_height = models.PositiveIntegerField(null=True, blank=True)
    viewport_width = models.PositiveIntegerField(null=True, blank=True)
    viewport_height = models.PositiveIntegerField(null=True, blank=True)
    is_deleted_like = models.BooleanField(default=False)
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-fetched_at", "-id"]
        unique_together = ("resource", "snapshot_no")

    def __str__(self):
        return f"{self.resource_id}#{self.snapshot_no}"

    @property
    def is_success(self) -> bool:
        return (
            not self.error_message
            and (self.http_status is None or self.http_status < 400)
            and bool(self.extracted_text or self.raw_html_path or self.screenshot_full_path)
        )

    @property
    def image_count(self) -> int:
        return len(self.image_assets or [])

    @property
    def video_count(self) -> int:
        return len(self.video_assets or [])

    @property
    def ai_translation(self) -> str:
        return self.ai_summary

    @property
    def has_translation(self) -> bool:
        return bool(self.ai_summary.strip())

    def get_absolute_url(self):
        return reverse("snapshots:detail", args=[self.pk])
