from __future__ import annotations

from urllib.parse import urlparse

from django.conf import settings
from django.contrib.postgres.search import SearchQuery, SearchRank, SearchVector
from django.db import connection, models
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone


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


class SaveReason(models.TextChoices):
    READ_LATER = "後で読む", "後で読む"
    LIKELY_TO_DISAPPEAR = "消えそう", "消えそう"
    REFERENCE_IMPLEMENTATION = "参考実装", "参考実装"
    JOB_HUNTING = "就活用", "就活用"
    SHOPPING_CANDIDATE = "買い物候補", "買い物候補"


class ResourceQuerySet(models.QuerySet):
    def with_related(self):
        return self.select_related("latest_snapshot").prefetch_related("tags")

    def _apply_postgresql_search(self, query: str):
        vector = (
            SearchVector("original_url", weight="A")
            + SearchVector("title_manual", weight="B")
            + SearchVector("domain", weight="B")
            + SearchVector("save_reason", weight="B")
            + SearchVector("next_action", weight="B")
            + SearchVector("note", weight="C")
            + SearchVector("snapshots__extracted_text", weight="C")
            + SearchVector("snapshots__ai_summary", weight="B")
        )
        search_query = SearchQuery(query)
        return (
            self.annotate(search=vector, rank=SearchRank(vector, search_query))
            .filter(search=search_query)
            .order_by("-rank", "-favorite", "-updated_at")
            .distinct()
        )

    def _apply_fallback_search(self, query: str):
        return (
            self.filter(
                Q(original_url__icontains=query)
                | Q(normalized_url__icontains=query)
                | Q(title_manual__icontains=query)
                | Q(domain__icontains=query)
                | Q(save_reason__icontains=query)
                | Q(next_action__icontains=query)
                | Q(note__icontains=query)
                | Q(snapshots__extracted_text__icontains=query)
                | Q(snapshots__ai_summary__icontains=query)
            )
            .distinct()
            .order_by("-favorite", "-updated_at")
        )

    def _apply_text_search(self, query: str):
        if connection.vendor == "postgresql":
            return self._apply_postgresql_search(query)
        return self._apply_fallback_search(query)

    def _apply_tag_filters(self, tag_ids: list[int] | None):
        queryset = self
        for tag_id in dict.fromkeys(tag_ids or []):
            queryset = queryset.filter(tags__id=tag_id)
        if tag_ids:
            queryset = queryset.distinct()
        return queryset

    def apply_filters(
        self,
        *,
        query: str = "",
        domain: str = "",
        tag_ids: list[int] | None = None,
        favorite_only: bool = False,
        status: str = "",
        review_state: str = "",
        save_reason: str = "",
        recheck_due_only: bool = False,
    ):
        queryset = self.with_related()
        query = (query or "").strip()
        if query:
            queryset = queryset._apply_text_search(query)
        else:
            queryset = queryset.exclude(search_only=True).order_by("-favorite", "-updated_at")

        if domain:
            queryset = queryset.filter(domain=domain)
        queryset = queryset._apply_tag_filters(tag_ids)
        if favorite_only:
            queryset = queryset.filter(favorite=True)
        if status:
            queryset = queryset.filter(current_status=status)
        if review_state:
            queryset = queryset.filter(review_state=review_state)
        if save_reason:
            queryset = queryset.filter(save_reason=save_reason)
        if recheck_due_only:
            queryset = queryset.filter(recheck_at__isnull=False, recheck_at__lte=timezone.localdate())
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
    save_reason = models.CharField(max_length=40, blank=True, db_index=True)
    next_action = models.CharField(max_length=255, blank=True)
    recheck_at = models.DateField(null=True, blank=True, db_index=True)
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
    def is_recheck_due(self) -> bool:
        return bool(self.recheck_at and self.recheck_at <= timezone.localdate())

    @property
    def visibility_label(self) -> str:
        return "検索時のみ" if self.search_only else "通常表示"

    def get_absolute_url(self):
        return reverse("resources:detail", args=[self.pk])

    def get_save_reason_display(self) -> str:
        return dict(SaveReason.choices).get(self.save_reason, self.save_reason)

    def update_domain_from_url(self):
        parsed = urlparse(self.normalized_url)
        self.domain = parsed.netloc
