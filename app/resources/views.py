import hashlib

from django.conf import settings
from django.contrib import messages
from django.http import HttpResponseNotAllowed, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.template.loader import render_to_string
from django.urls import reverse
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods

from resources.forms import ResourceFilterForm, ResourceForm
from resources.models import Resource
from resources.services import (
    check_resource_link_status,
    delete_resource_with_artifacts,
    enqueue_capture_job,
    get_capture_files,
)


def build_snapshot_payload_context(snapshot):
    if snapshot is None:
        return {"tag_candidates": [], "similar_resources": []}
    payload = snapshot.ai_payload or {}
    similar_ids = payload.get("similar_resource_ids", [])
    similar_resources = Resource.objects.filter(id__in=similar_ids).select_related("latest_snapshot")
    return {
        "tag_candidates": payload.get("tag_candidates", []),
        "similar_resources": similar_resources,
    }


def build_resource_detail_context(resource, form=None):
    image_names, video_names = get_capture_files(resource.pk)
    storage_url = settings.STORAGE_URL.rstrip("/")
    image_files = [
        {
            "name": image_name,
            "path": f"{storage_url}/images/resource_{resource.pk:04d}/{image_name}",
        }
        for image_name in image_names
    ]
    video_files = [
        {
            "name": video_name,
            "path": f"{storage_url}/videos/resource_{resource.pk:04d}/{video_name}",
        }
        for video_name in video_names
    ]
    return {
        "resource": resource,
        "form": form or ResourceForm(instance=resource),
        "snapshots": resource.snapshots.all()[:10],
        "latest_snapshot_context": build_snapshot_payload_context(resource.latest_snapshot),
        "image_files": image_files,
        "video_files": video_files,
        "has_image_files": bool(image_files),
        "has_video_files": bool(video_files),
        "capture_mismatch": (
            (resource.capture_images and not image_files)
            or (resource.capture_videos and not video_files)
        ),
    }


def build_resource_list_signature(resources):
    basis = "|".join(
        (
            f"{resource.id}:"
            f"{resource.updated_at.isoformat()}:"
            f"{resource.current_status}:"
            f"{resource.link_status}:"
            f"{resource.review_state}:"
            f"{resource.latest_snapshot_id or 0}:"
            f"{resource.search_only}:"
            f"{resource.latest_translation}"
        )
        for resource in resources
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def build_resource_list_context(request):
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


@require_GET
def resource_list(request):
    return render(request, "resources/list.html", build_resource_list_context(request))


@require_GET
def resource_list_fragment(request):
    context = build_resource_list_context(request)
    html = render_to_string("resources/_resource_results.html", context, request=request)
    return JsonResponse(
        {
            "html": html,
            "signature": context["resource_signature"],
            "count": context["resource_count"],
        }
    )


@ensure_csrf_cookie
@require_http_methods(["GET", "POST"])
def resource_create(request):
    form = ResourceForm()
    if request.method == "POST":
        form = ResourceForm(request.POST)
        if form.is_valid():
            resource = form.save()
            enqueue_capture_job(resource)
            messages.success(request, "URLを登録し、取得ジョブを作成しました。")
            return redirect("resources:list")

        if form.existing_resource is not None:
            messages.info(request, "このURLは登録済みです。")
        else:
            messages.error(request, "URLの登録に失敗しました。入力内容を確認してください。")

    return render(request, "resources/create.html", {"form": form})


@ensure_csrf_cookie
@require_http_methods(["GET", "POST"])
def resource_detail(request, pk: int):
    resource = get_object_or_404(Resource.objects.with_related(), pk=pk)
    if request.method == "GET":
        resource = check_resource_link_status(resource)
        return render(request, "resources/detail.html", build_resource_detail_context(resource))

    method = request.POST.get("_method", "POST").upper()

    if method == "LINK_CHECK":
        check_resource_link_status(resource, force=True)
        messages.success(request, "リンク状態を確認しました。")
        return redirect(resource)

    if method == "PATCH":
        form = ResourceForm(request.POST, instance=resource)
        if form.is_valid():
            form.save()
            messages.success(request, "URL情報を更新しました。")
            return redirect(resource)
        if form.existing_resource is not None:
            messages.info(request, "そのURLは別の登録で使われています。")
        else:
            messages.error(request, "更新に失敗しました。")
        return render(request, "resources/detail.html", build_resource_detail_context(resource, form), status=400)

    if method == "DELETE":
        delete_resource_with_artifacts(resource)
        messages.success(request, "URLと関連する画像・動画を含む保存ファイルをすぐに削除しました。")
        return redirect("resources:list")

    return HttpResponseNotAllowed(["GET", "POST"])


@require_http_methods(["POST"])
def resource_capture(request, pk: int):
    resource = get_object_or_404(Resource, pk=pk)
    enqueue_capture_job(resource, priority=150)
    messages.success(request, "再取得ジョブを作成しました。")
    return redirect(resource)


@require_GET
def resource_snapshots(request, pk: int):
    resource = get_object_or_404(Resource.objects.with_related(), pk=pk)
    return render(
        request,
        "resources/snapshot_list.html",
        {
            "resource": resource,
            "snapshots": resource.snapshots.all(),
        },
    )
