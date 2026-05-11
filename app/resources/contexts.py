from __future__ import annotations

import hashlib

from django.core.paginator import Paginator
from django.urls import reverse
from django.utils import timezone

from resources.forms import ResourceFilterForm
from resources.models import INTEREST_LABEL_CHOICES, InterestFeedback, Resource, ReviewState
from resources.services import get_capture_files, get_snapshot_screenshot_file
from snapshots.models import Snapshot

LIST_PAGE_SIZE = 10
AI_SEARCH_PAGE_SIZE = 15
AI_FEEDBACK_PAGE_LIMIT = 100
BULK_EDIT_PAGE_SIZE = 10


def get_similar_resources(snapshot: Snapshot | None):
    if snapshot is None:
        return Resource.objects.none()

    payload = snapshot.ai_payload or {}
    similar_ids = payload.get("similar_resource_ids", [])
    if not similar_ids:
        return Resource.objects.none()
    return Resource.objects.filter(id__in=similar_ids).select_related("latest_snapshot")


def build_snapshot_payload_context(snapshot: Snapshot | None) -> dict:
    payload = snapshot.ai_payload or {} if snapshot is not None else {}
    return {
        "tag_candidates": payload.get("tag_candidates", []),
        "similar_resources": get_similar_resources(snapshot),
    }


def build_snapshot_detail_context(snapshot: Snapshot) -> dict:
    image_files, video_files = get_capture_files(snapshot)
    screenshot_file = get_snapshot_screenshot_file(snapshot)
    return {
        "snapshot": snapshot,
        "screenshot_file": screenshot_file,
        "image_files": image_files,
        "video_files": video_files,
        "has_screenshot_file": bool(screenshot_file),
        "has_image_files": bool(image_files),
        "has_video_files": bool(video_files),
        **build_snapshot_payload_context(snapshot),
    }


def build_resource_detail_context(resource) -> dict:
    snapshot = resource.latest_snapshot
    image_files, video_files = get_capture_files(snapshot)
    screenshot_file = get_snapshot_screenshot_file(snapshot)
    expected_image_count = len(snapshot.image_assets or []) if snapshot is not None else 0
    expected_video_count = len(snapshot.video_assets or []) if snapshot is not None else 0
    missing_screenshot = bool(snapshot and snapshot.screenshot_full_path and not screenshot_file)
    missing_image_files = expected_image_count > len(image_files)
    missing_video_files = expected_video_count > len(video_files)
    return {
        "resource": resource,
        "snapshots": resource.snapshots.all()[:10],
        "latest_snapshot_context": build_snapshot_payload_context(snapshot),
        "screenshot_file": screenshot_file,
        "image_files": image_files,
        "video_files": video_files,
        "has_screenshot_file": bool(screenshot_file),
        "has_image_files": bool(image_files),
        "has_video_files": bool(video_files),
        "has_image_panel_files": bool(screenshot_file or image_files),
        "image_capture_mismatch": missing_screenshot or missing_image_files,
        "video_capture_mismatch": missing_video_files,
        "capture_mismatch": missing_screenshot or missing_image_files or missing_video_files,
    }


def build_overview_metrics() -> list[dict]:
    today = timezone.localdate()
    recent_cutoff = today - timezone.timedelta(days=30)
    metric_definitions = [
        {
            "label": "保存したURL総数",
            "count": Resource.objects.count(),
            "delta": Resource.objects.filter(created_at__date__gte=recent_cutoff).count(),
            "delta_prefix": "+",
            "tone": "blue",
            "icon": "link",
        },
        {
            "label": "要確認",
            "count": Resource.objects.filter(review_state=ReviewState.NEEDS_REVIEW).count(),
            "delta": Resource.objects.filter(
                review_state=ReviewState.NEEDS_REVIEW,
                updated_at__date__gte=recent_cutoff,
            ).count(),
            "delta_prefix": "+",
            "tone": "amber",
            "icon": "alert",
        },
        {
            "label": "再確認待ち",
            "count": Resource.objects.filter(recheck_at__isnull=False, recheck_at__lte=today).count(),
            "delta": Resource.objects.filter(
                recheck_at__isnull=False,
                recheck_at__gte=recent_cutoff,
            ).count(),
            "delta_prefix": "+",
            "tone": "violet",
            "icon": "clock",
        },
        {
            "label": "お気に入り",
            "count": Resource.objects.filter(favorite=True).count(),
            "delta": Resource.objects.filter(
                favorite=True,
                updated_at__date__gte=recent_cutoff,
            ).count(),
            "delta_prefix": "+",
            "tone": "gold",
            "icon": "star",
        },
    ]
    return metric_definitions


def build_dashboard_context() -> dict:
    return {
        "overview_metrics": build_overview_metrics(),
    }


def paginate_queryset(queryset, page_number, *, per_page: int):
    paginator = Paginator(queryset, per_page)
    return paginator.get_page(page_number or 1)


def build_pagination_items(page_obj) -> list[int | None]:
    total_pages = page_obj.paginator.num_pages
    if total_pages <= 1:
        return []

    current_page = page_obj.number
    page_numbers: list[int | None] = [1]
    window_start = max(2, current_page - 2)
    window_end = min(total_pages - 1, current_page + 2)

    if window_start > 2:
        page_numbers.append(None)

    for number in range(window_start, window_end + 1):
        page_numbers.append(number)

    if window_end < total_pages - 1:
        page_numbers.append(None)

    if total_pages > 1:
        page_numbers.append(total_pages)

    normalized: list[int | None] = []
    for number in page_numbers:
        if normalized and normalized[-1] == number:
            continue
        normalized.append(number)
    return normalized


def build_date_page_labels(queryset, page_obj, *, per_page: int) -> dict[int, str]:
    labels: dict[int, str] = {}
    for number in build_pagination_items(page_obj):
        if number is None:
            continue
        offset = (number - 1) * per_page
        created_at = queryset.values_list("created_at", flat=True)[offset : offset + 1].first()
        if created_at:
            labels[number] = timezone.localtime(created_at).strftime("%Y/%m/%d")
    return labels


def build_pagination_context(page_obj, query_params, *, page_labels: dict[int, str] | None = None) -> dict:
    params = query_params.copy()
    if "page" in params:
        del params["page"]

    def build_url(page_number: int) -> str:
        page_params = params.copy()
        page_params["page"] = str(page_number)
        return f"?{page_params.urlencode()}"

    items = []
    for number in build_pagination_items(page_obj):
        if number is None:
            items.append({"ellipsis": True})
            continue
        items.append(
            {
                "number": number,
                "label": (page_labels or {}).get(number, str(number)),
                "current": number == page_obj.number,
                "url": build_url(number),
            }
        )

    return {
        "is_paginated": page_obj.paginator.num_pages > 1,
        "items": items,
        "prev_url": build_url(page_obj.previous_page_number()) if page_obj.has_previous() else "",
        "next_url": build_url(page_obj.next_page_number()) if page_obj.has_next() else "",
    }


def build_resource_action_next_url(request, clear_url_name: str) -> str:
    params = request.GET.copy()
    if "_ts" in params:
        del params["_ts"]
    query_string = params.urlencode()
    base_url = reverse(clear_url_name)
    if query_string:
        return f"{base_url}?{query_string}"
    return base_url


def build_resource_list_signature(resources) -> str:
    basis = "|".join(
        (
            f"{resource.id}:"
            f"{resource.updated_at.isoformat()}:"
            f"{resource.current_status}:"
            f"{resource.link_status}:"
            f"{resource.review_state}:"
            f"{resource.save_reason}:"
            f"{resource.next_action}:"
            f"{resource.recheck_at or ''}:"
            f"{resource.is_recheck_due}:"
            f"{resource.latest_snapshot_id or 0}:"
            f"{resource.search_only}:"
            f"{resource.interest_feedback}:"
            f"{','.join(resource.interest_labels or [])}:"
            f"{resource.latest_translation}"
        )
        for resource in resources
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def build_resource_list_context(
    request,
    *,
    visibility: str = "normal",
    date_ordered: bool = False,
    filter_variant: str = "full",
    table_variant: str = "standard",
    page_size: int = LIST_PAGE_SIZE,
    date_page_labels: bool = False,
    fragment_url_name: str = "resources:list_fragment",
    clear_url_name: str = "resources:list",
    page_title: str = "保存したURL一覧",
    page_subtitle: str = "保存したURLの取得状態や見直し状況を確認・管理できます。",
    result_title: str = "保存したURL一覧",
    result_empty_title: str = "保存されたURLはまだありません",
    result_empty_text: str = "左側サイドバーの「URL登録」から新しいURLを追加できます。",
) -> dict:
    filter_form = ResourceFilterForm(request.GET)
    resources = Resource.objects.all()
    selected_tags = []
    if filter_form.is_valid():
        selected_tags = list(filter_form.cleaned_data.get("tags") or [])
        resources = resources.apply_filters(
            query=filter_form.cleaned_data.get("q") or "",
            domain=filter_form.cleaned_data.get("domain") or "",
            tag_ids=[tag.id for tag in selected_tags],
            favorite_only=filter_form.cleaned_data.get("favorite_only") or False,
            status=filter_form.cleaned_data.get("status") or "",
            review_state=filter_form.cleaned_data.get("review_state") or "",
            save_reason=filter_form.cleaned_data.get("save_reason") or "",
            recheck_due_only=filter_form.cleaned_data.get("recheck_due_only") or False,
            visibility=visibility,
        )
    else:
        resources = resources.with_related()
        if visibility == "search_only":
            resources = resources.filter(search_only=True)
        elif visibility == "normal":
            resources = resources.exclude(search_only=True)

    if date_ordered:
        resources = resources.order_by("-created_at", "-id")

    page_obj = paginate_queryset(resources, request.GET.get("page"), per_page=page_size)
    resource_list = list(page_obj.object_list)
    page_labels = build_date_page_labels(resources, page_obj, per_page=page_size) if date_page_labels else None
    return {
        "page_title": page_title,
        "page_subtitle": page_subtitle,
        "filter_variant": filter_variant,
        "table_variant": table_variant,
        "result_title": result_title,
        "result_empty_title": result_empty_title,
        "result_empty_text": result_empty_text,
        "list_form_action_url": reverse(clear_url_name),
        "list_clear_url": reverse(clear_url_name),
        "filter_form": filter_form,
        "selected_tag_count": len(selected_tags),
        "resources": resource_list,
        "resource_action_next_url": build_resource_action_next_url(request, clear_url_name),
        "page_obj": page_obj,
        "pagination": build_pagination_context(page_obj, request.GET, page_labels=page_labels),
        "resource_count": page_obj.paginator.count,
        "resource_start": page_obj.start_index() if page_obj.paginator.count else 0,
        "resource_end": page_obj.end_index() if page_obj.paginator.count else 0,
        "resource_signature": build_resource_list_signature(resource_list),
        "resource_fragment_url": reverse(fragment_url_name),
        "resource_poll_ms": 10000,
        "interest_label_choices": INTEREST_LABEL_CHOICES,
    }


def build_ai_search_resource_list_context(request) -> dict:
    return build_resource_list_context(
        request,
        visibility="search_only",
        date_ordered=True,
        filter_variant="ai_search",
        table_variant="date_focused",
        page_size=AI_SEARCH_PAGE_SIZE,
        date_page_labels=True,
        fragment_url_name="resources:ai_search_list_fragment",
        clear_url_name="resources:ai_search_list",
        page_title="AI検索URL",
        page_subtitle="AIに探させたURLを通常一覧とは分け、保存日が新しい順で確認できます。",
        result_title="AI検索URL一覧（保存日順）",
        result_empty_title="AI検索URLはまだありません",
        result_empty_text="今後AIに探させたURLは、検索専用URLとしてここに保存できます。",
    )


def truncate_ai_reader_text(value: str, *, limit: int = 3000) -> str:
    text = (value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit].rstrip()}..."


def parse_ai_feedback_limit(raw_limit: str | None) -> int:
    try:
        limit = int(raw_limit or AI_FEEDBACK_PAGE_LIMIT)
    except (TypeError, ValueError):
        limit = AI_FEEDBACK_PAGE_LIMIT
    return max(1, min(limit, 200))


def build_ai_feedback_item(resource: Resource) -> dict:
    snapshot = resource.latest_snapshot
    return {
        "id": resource.id,
        "title": resource.display_title,
        "url": resource.original_url,
        "normalized_url": resource.normalized_url,
        "domain": resource.domain,
        "feedback": resource.interest_feedback,
        "feedback_label": resource.get_interest_feedback_display(),
        "interest_labels": resource.interest_labels or [],
        "interest_label_names": resource.interest_label_names,
        "search_only": resource.search_only,
        "save_reason": resource.get_save_reason_display() if resource.save_reason else "",
        "next_action": resource.next_action,
        "note": truncate_ai_reader_text(resource.note, limit=1200),
        "tags": [tag.name for tag in resource.tags.all()],
        "created_at": resource.created_at,
        "updated_at": resource.updated_at,
        "snapshot": {
            "id": snapshot.id,
            "snapshot_no": snapshot.snapshot_no,
            "fetched_at": snapshot.fetched_at,
            "page_title": snapshot.page_title,
            "fetch_url": snapshot.fetch_url,
            "fetch_method": snapshot.get_fetch_method_display(),
            "http_status": snapshot.http_status,
            "summary": truncate_ai_reader_text(snapshot.ai_summary, limit=1200),
            "translation": truncate_ai_reader_text(snapshot.ai_translation, limit=1600),
            "category": snapshot.ai_category,
            "text_excerpt": truncate_ai_reader_text(snapshot.extracted_text),
            "error_message": snapshot.error_message,
            "image_count": snapshot.image_count,
            "video_count": snapshot.video_count,
        }
        if snapshot
        else None,
    }


def build_ai_feedback_page_context(request, feedback: str) -> dict:
    feedback_labels = {
        InterestFeedback.INTERESTED: "興味あり",
        InterestFeedback.NOT_INTERESTED: "興味なし",
    }
    page_titles = {
        InterestFeedback.INTERESTED: "AI読取用: 興味ありURL",
        InterestFeedback.NOT_INTERESTED: "AI読取用: 興味なしURL",
    }
    page_descriptions = {
        InterestFeedback.INTERESTED: "次の検索で広げたいURLです。",
        InterestFeedback.NOT_INTERESTED: "次の検索で避けたいURL・除外判断の材料です。",
    }
    limit = parse_ai_feedback_limit(request.GET.get("limit"))
    queryset = (
        Resource.objects.with_related()
        .filter(interest_feedback=feedback)
        .order_by("-updated_at", "-id")
    )
    total_count = queryset.count()
    resources = list(queryset[:limit])
    return {
        "page_title": page_titles[feedback],
        "page_description": page_descriptions[feedback],
        "feedback": feedback,
        "feedback_label": feedback_labels[feedback],
        "items": [build_ai_feedback_item(resource) for resource in resources],
        "total_count": total_count,
        "shown_count": len(resources),
        "limit": limit,
        "generated_at": timezone.now(),
        "interested_url": reverse("resources:ai_interested"),
        "not_interested_url": reverse("resources:ai_not_interested"),
        "interested_markdown_url": reverse("resources:ai_interested_markdown"),
        "not_interested_markdown_url": reverse("resources:ai_not_interested_markdown"),
    }
