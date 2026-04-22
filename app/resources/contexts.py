from __future__ import annotations

import hashlib

from django.urls import reverse

from resources.forms import ResourceFilterForm, ResourceForm
from resources.models import Resource
from resources.services import build_snapshot_diff_context, get_capture_files
from snapshots.models import Snapshot


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
    }
