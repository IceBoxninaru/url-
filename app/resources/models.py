from __future__ import annotations

from urllib.parse import urlparse

from django.conf import settings
from django.contrib.postgres.search import SearchQuery, SearchRank, SearchVector
from django.db import connection, models
from django.db.models import Q
from django.urls import reverse


class ResourceStatus(models.TextChoices):
    ACTIVE = "active", "取得済み"
    FETCH_FAILED = "fetch_failed", "取得失敗"
    MAYBE_DELETED = "maybe_deleted", "削除の可能性"
    GONE = "gone", "削除済み"


class ReviewState(models.TextChoices):
    NONE = "none", "未設定"
    NEEDS_REVIEW = "needs_review", "要確認"
    DONE = "done", "処理済み"
    ON_HOLD = "on_hold", "保留"


class LinkStatus(models.TextChoices):
    UNCHECKED = "unchecked", "未確認"
    ACTIVE = "active", "有効"
    MAYBE_DELETED = "maybe_deleted", "要注意"
    GONE = "gone", "リンク切れ"
    ERROR = "error", "確認失敗"


class ResourceQuerySet(models.QuerySet):
    def with_related(self):
        return self.select_related("latest_snapshot").prefetch_related("tags")

    def apply_filters(
        self,
        *,
        query: str = "",
        domain: str = "",
        tag_ids: list[int] | None = None,
        favorite_only: bool = False,
        status: str = "",
        review_state: str = "",
    ):
        queryset = self.with_related()
        query = (query or "").strip()
        if query:
            if connection.vendor == "postgresql":
                vector = (
                    SearchVector("original_url", weight="A")
                    + SearchVector("title_manual", weight="B")
                    + SearchVector("domain", weight="B")
                    + SearchVector("note", weight="C")
                    + SearchVector("snapshots__extracted_text", weight="C")
                    + SearchVector("snapshots__ai_summary", weight="B")
                )
                search_query = SearchQuery(query)
                queryset = (
                    queryset.annotate(search=vector, rank=SearchRank(vector, search_query))
                    .filter(search=search_query)
                    .order_by("-rank", "-favorite", "-updated_at")
                    .distinct()
                )
            else:
                queryset = (
                    queryset.filter(
                        Q(original_url__icontains=query)
                        | Q(normalized_url__icontains=query)
                        | Q(title_manual__icontains=query)
                        | Q(domain__icontains=query)
                        | Q(note__icontains=query)
                        | Q(snapshots__extracted_text__icontains=query)
                        | Q(snapshots__ai_summary__icontains=query)
                    )
                    .distinct()
                    .order_by("-favorite", "-updated_at")
                )
        else:
            queryset = queryset.exclude(search_only=True).order_by("-favorite", "-updated_at")

        if domain:
            queryset = queryset.filter(domain=domain)
        if tag_ids:
            for tag_id in dict.fromkeys(tag_ids):
                queryset = queryset.filter(tags__id=tag_id)
            queryset = queryset.distinct()
        if favorite_only:
            queryset = queryset.filter(favorite=True)
        if status:
            queryset = queryset.filter(current_status=status)
        if review_state:
            queryset = queryset.filter(review_state=review_state)
        return queryset


class Resource(models.Model):
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="owned_resources",
    )
    original_url = models.CharField(max_length=2000)
    normalized_url = models.URLField(max_length=2000, db_index=True)
    domain = models.CharField(max_length=255, db_index=True)
    title_manual = models.CharField(max_length=255, blank=True)
    note = models.TextField(blank=True)
    favorite = models.BooleanField(default=False)
    capture_images = models.BooleanField(default=True)
    capture_videos = models.BooleanField(default=True)
    search_only = models.BooleanField(default=False, db_index=True)
    review_state = models.CharField(
        max_length=32,
        choices=ReviewState.choices,
        default=ReviewState.NONE,
        db_index=True,
    )
    current_status = models.CharField(
        max_length=32,
        choices=ResourceStatus.choices,
        default=ResourceStatus.ACTIVE,
        db_index=True,
    )
    link_status = models.CharField(
        max_length=32,
        choices=LinkStatus.choices,
        default=LinkStatus.UNCHECKED,
        db_index=True,
    )
    last_link_check_at = models.DateTimeField(null=True, blank=True)
    last_link_check_http_status = models.PositiveIntegerField(null=True, blank=True)
    last_link_check_error = models.TextField(blank=True)
    latest_snapshot = models.ForeignKey(
        "snapshots.Snapshot",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
    )
    tags = models.ManyToManyField("tags.Tag", through="tags.ResourceTag", blank=True, related_name="resources")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = ResourceQuerySet.as_manager()

    class Meta:
        ordering = ["-favorite", "-updated_at", "-id"]

    def __str__(self) -> str:
        return self.display_title

    @property
    def display_title(self) -> str:
        if self.title_manual:
            return self.title_manual
        if self.latest_snapshot and self.latest_snapshot.page_title:
            return self.latest_snapshot.page_title
        return self.domain or self.original_url

    @property
    def latest_summary(self) -> str:
        if self.latest_snapshot:
            return self.latest_snapshot.ai_summary
        return ""

    @property
    def latest_translation(self) -> str:
        return self.latest_summary

    @property
    def latest_screenshot_path(self) -> str:
        if self.latest_snapshot:
            return self.latest_snapshot.screenshot_full_path
        return ""

    @property
    def visibility_label(self) -> str:
        return "検索時のみ" if self.search_only else "通常表示"

    def get_absolute_url(self):
        return reverse("resources:detail", args=[self.pk])

    def update_domain_from_url(self):
        parsed = urlparse(self.normalized_url)
        self.domain = parsed.netloc
