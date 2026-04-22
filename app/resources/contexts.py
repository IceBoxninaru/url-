from __future__ import annotations

import hashlib

from django.urls import reverse
from django.utils import timezone

from resources.forms import ResourceFilterForm, ResourceForm
from resources.models import Resource, ReviewState
from resources.services import build_snapshot_diff_context, get_capture_files
from jobs.models import CaptureJob, JobStatus, JobType
from snapshots.models import Snapshot
from tags.models import Tag


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
    return {
        "snapshot": snapshot,
        **build_snapshot_payload_context(snapshot),
        "snapshot_diff": build_snapshot_diff_context(snapshot),
    }


def build_resource_detail_context(resource, form=None) -> dict:
    image_files, video_files = get_capture_files(resource.latest_snapshot)
    return {
        "resource": resource,
        "form": form or ResourceForm(instance=resource),
        "snapshots": resource.snapshots.all()[:10],
        "latest_snapshot_context": build_snapshot_payload_context(resource.latest_snapshot),
        "image_files": image_files,
        "video_files": video_files,
        "has_image_files": bool(image_files),
        "has_video_files": bool(video_files),
        "latest_snapshot_diff": build_snapshot_diff_context(resource.latest_snapshot),
        "capture_mismatch": (
            (resource.capture_images and not image_files)
            or (resource.capture_videos and not video_files)
        ),
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


def describe_job_activity(job: CaptureJob) -> dict:
    if job.job_type == JobType.CAPTURE:
        if job.status == JobStatus.SUCCEEDED:
            return {
                "tone": "success",
                "icon": "check",
                "title": "ページを取得しました",
            }
        if job.status == JobStatus.RETRY_WAIT:
            return {
                "tone": "warning",
                "icon": "clock",
                "title": "再確認タスクを追加しました",
            }
        if job.status == JobStatus.FAILED:
            return {
                "tone": "danger",
                "icon": "alert",
                "title": "取得に失敗しました",
            }
        return {
            "tone": "info",
            "icon": "link",
            "title": "新しいURLを登録しました",
        }

    if job.status == JobStatus.SUCCEEDED:
        return {
            "tone": "info",
            "icon": "spark",
            "title": "AI要約を更新しました",
        }
    return {
        "tone": "info",
        "icon": "spark",
        "title": "AI補完を処理しました",
    }


def build_recent_activity(limit: int = 5) -> list[dict]:
    activity_items: list[dict] = []
    for job in CaptureJob.objects.with_related().filter(resource__search_only=False)[:limit]:
        descriptor = describe_job_activity(job)
        activity_items.append(
            {
                **descriptor,
                "resource": job.resource,
                "timestamp": job.updated_at,
            }
        )

    if activity_items:
        return activity_items

    for resource in Resource.objects.with_related().filter(search_only=False)[:limit]:
        activity_items.append(
            {
                "tone": "info",
                "icon": "link",
                "title": "URLを登録しました",
                "resource": resource,
                "timestamp": resource.updated_at,
            }
        )
    return activity_items


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
            f"{resource.latest_translation}"
        )
        for resource in resources
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def build_resource_list_context(request) -> dict:
    filter_form = ResourceFilterForm(request.GET)
    resources = Resource.objects.all()
    if filter_form.is_valid():
        resources = resources.apply_filters(
            query=filter_form.cleaned_data.get("q") or "",
            domain=filter_form.cleaned_data.get("domain") or "",
            tag_ids=[tag.id for tag in filter_form.cleaned_data.get("tags") or []],
            favorite_only=filter_form.cleaned_data.get("favorite_only") or False,
            status=filter_form.cleaned_data.get("status") or "",
            review_state=filter_form.cleaned_data.get("review_state") or "",
            save_reason=filter_form.cleaned_data.get("save_reason") or "",
            recheck_due_only=filter_form.cleaned_data.get("recheck_due_only") or False,
        )
    else:
        resources = resources.with_related()

    resource_list = list(resources)
    return {
        "filter_form": filter_form,
        "resources": resource_list,
        "resource_count": len(resource_list),
        "resource_signature": build_resource_list_signature(resource_list),
        "resource_fragment_url": reverse("resources:list_fragment"),
        "resource_poll_ms": 10000,
        "overview_metrics": build_overview_metrics(),
        "recent_activity": build_recent_activity(),
        "tag_count": Tag.objects.count(),
    }
