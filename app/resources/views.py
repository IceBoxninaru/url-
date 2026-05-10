from django.contrib import messages
from django.http import HttpResponseNotAllowed, JsonResponse, QueryDict
from django.shortcuts import get_object_or_404, redirect, render
from django.template.loader import render_to_string
from django.urls import reverse
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods

from resources.contexts import (
    BULK_EDIT_PAGE_SIZE,
    build_ai_search_resource_list_context,
    build_dashboard_context,
    build_pagination_context,
    build_resource_detail_context,
    build_resource_list_context,
    paginate_queryset,
)
from resources.forms import ResourceBulkEditForm, ResourceForm
from resources.models import Resource
from resources.services import (
    check_resource_link_status,
    delete_resource_with_artifacts,
    enqueue_capture_job,
    get_capture_files,
)
from jobs.models import CaptureJob, JobStatus
from tags.models import Tag


def normalize_next_url(raw_next: str) -> str:
    next_url = (raw_next or "").strip() or reverse("resources:list")
    if not next_url.startswith("/"):
        return reverse("resources:list")
    return next_url


def parse_resource_ids(raw_ids) -> list[int]:
    seen_ids: set[int] = set()
    resource_ids: list[int] = []
    for raw_id in raw_ids:
        if not str(raw_id).isdigit():
            continue
        resource_id = int(raw_id)
        if resource_id in seen_ids:
            continue
        seen_ids.add(resource_id)
        resource_ids.append(resource_id)
    return resource_ids


def get_selected_resources(resource_ids: list[int]) -> list[Resource]:
    resources_by_id = Resource.objects.with_related().in_bulk(resource_ids)
    return [resources_by_id[resource_id] for resource_id in resource_ids if resource_id in resources_by_id]


def build_bulk_edit_context(
    request,
    *,
    form: ResourceBulkEditForm,
    resource_ids: list[int],
    next_url: str,
    page_number=None,
) -> dict:
    selected_resources = get_selected_resources(resource_ids)
    page_obj = paginate_queryset(
        Resource.objects.with_related(),
        page_number or request.GET.get("page"),
        per_page=BULK_EDIT_PAGE_SIZE,
    )
    resource_choices = list(page_obj.object_list)
    current_page_ids = {resource.id for resource in resource_choices}
    preserved_ids = [resource_id for resource_id in resource_ids if resource_id not in current_page_ids]
    pagination_params = QueryDict(mutable=True)
    pagination_params.setlist("resource_ids", [str(resource_id) for resource_id in resource_ids])
    if next_url:
        pagination_params["next"] = next_url
    return {
        "form": form,
        "resource_ids": resource_ids,
        "selected_resource_ids": set(resource_ids),
        "selected_resources": selected_resources,
        "selected_count": len(selected_resources),
        "resource_choices": resource_choices,
        "preserved_resource_ids": preserved_ids,
        "page_obj": page_obj,
        "pagination": build_pagination_context(page_obj, pagination_params),
        "resource_count": page_obj.paginator.count,
        "resource_start": page_obj.start_index() if page_obj.paginator.count else 0,
        "resource_end": page_obj.end_index() if page_obj.paginator.count else 0,
        "next_url": next_url,
    }


def parse_api_limit(raw_limit: str | None, *, default: int = 5, maximum: int = 20) -> int:
    try:
        limit = int(raw_limit or default)
    except (TypeError, ValueError):
        limit = default
    return max(1, min(limit, maximum))


def isoformat_or_none(value) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def truncate_text(value: str, *, limit: int = 1000) -> str:
    text = (value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit].rstrip()}..."


def storage_url_for_path(path: str) -> str:
    if not path:
        return ""
    if path.startswith(("http://", "https://", "/")):
        return path
    return f"/{path}"


def serialize_tag(tag: Tag) -> dict:
    return {
        "id": tag.id,
        "name": tag.name,
        "color": tag.color,
    }


def serialize_media_asset(asset: dict, media_type: str) -> dict:
    path = str(asset.get("path", "")).strip()
    return {
        **asset,
        "media_type": media_type,
        "path": path,
        "url": storage_url_for_path(path),
    }


def serialize_snapshot(snapshot, *, include_text: bool = False, include_media: bool = False) -> dict | None:
    if snapshot is None:
        return None

    image_files, video_files = get_capture_files(snapshot)
    image_assets = [serialize_media_asset(asset, "image") for asset in image_files]
    video_assets = [serialize_media_asset(asset, "video") for asset in video_files]
    payload = {
        "id": snapshot.id,
        "snapshot_no": snapshot.snapshot_no,
        "fetch_url": snapshot.fetch_url,
        "fetch_method": snapshot.fetch_method,
        "http_status": snapshot.http_status,
        "fetched_at": isoformat_or_none(snapshot.fetched_at),
        "page_title": snapshot.page_title,
        "site_name": snapshot.site_name,
        "author": snapshot.author,
        "published_at": isoformat_or_none(snapshot.published_at),
        "summary": snapshot.ai_summary,
        "translation": snapshot.ai_translation if include_text else truncate_text(snapshot.ai_translation),
        "category": snapshot.ai_category,
        "image_count": len(image_assets),
        "video_count": len(video_assets),
        "screenshot_path": snapshot.screenshot_full_path,
        "screenshot_url": storage_url_for_path(snapshot.screenshot_full_path),
    }
    if include_media:
        payload.update(
            {
                "image_assets": image_assets,
                "video_assets": video_assets,
                "media_assets": [*image_assets, *video_assets],
            }
        )
    if include_text:
        payload["extracted_text"] = snapshot.extracted_text
    else:
        payload["text_excerpt"] = truncate_text(snapshot.extracted_text)
    return payload


def serialize_resource(resource: Resource, *, detail: bool = False) -> dict:
    payload = {
        "id": resource.id,
        "title": resource.display_title,
        "url": resource.original_url,
        "normalized_url": resource.normalized_url,
        "domain": resource.domain,
        "favorite": resource.favorite,
        "search_only": resource.search_only,
        "save_reason": resource.save_reason,
        "save_reason_label": resource.get_save_reason_display(),
        "next_action": resource.next_action,
        "review_state": resource.review_state,
        "review_state_label": resource.get_review_state_display(),
        "current_status": resource.current_status,
        "current_status_label": resource.get_current_status_display(),
        "link_status": resource.link_status,
        "link_status_label": resource.get_link_status_display(),
        "tags": [serialize_tag(tag) for tag in resource.tags.all()],
        "summary": resource.latest_summary,
        "translation": resource.latest_translation if detail else truncate_text(resource.latest_translation),
        "detail_path": resource.get_absolute_url(),
        "created_at": isoformat_or_none(resource.created_at),
        "updated_at": isoformat_or_none(resource.updated_at),
        "latest_snapshot": serialize_snapshot(resource.latest_snapshot, include_text=detail, include_media=detail),
    }
    if detail:
        payload.update(
            {
                "note": resource.note,
                "recheck_at": isoformat_or_none(resource.recheck_at),
                "capture_images": resource.capture_images,
                "capture_videos": resource.capture_videos,
                "last_link_check_at": isoformat_or_none(resource.last_link_check_at),
                "last_link_check_http_status": resource.last_link_check_http_status,
                "last_link_check_error": resource.last_link_check_error,
            }
        )
    return payload


@require_GET
def api_recent_resources(request):
    limit = parse_api_limit(request.GET.get("limit"), default=5, maximum=20)
    queryset = (
        Resource.objects.with_related()
        .filter(search_only=False)
        .order_by("-updated_at", "-id")
    )
    resources = list(queryset[:limit])
    return JsonResponse(
        {
            "items": [serialize_resource(resource) for resource in resources],
            "count": len(resources),
            "limit": limit,
        }
    )


@require_GET
def api_search_resources(request):
    query = (request.GET.get("q") or request.GET.get("query") or "").strip()
    limit = parse_api_limit(request.GET.get("limit"), default=20, maximum=20)
    if not query:
        return JsonResponse({"items": [], "count": 0, "total_count": 0, "query": query, "limit": limit})

    queryset = Resource.objects.apply_filters(query=query)
    total_count = queryset.count()
    resources = list(queryset[:limit])
    return JsonResponse(
        {
            "items": [serialize_resource(resource) for resource in resources],
            "count": len(resources),
            "total_count": total_count,
            "query": query,
            "limit": limit,
        }
    )


@require_GET
def api_resource_detail(request, pk: int):
    resource = get_object_or_404(Resource.objects.with_related(), pk=pk)
    return JsonResponse(serialize_resource(resource, detail=True))


@require_GET
def resource_dashboard(request):
    return render(request, "resources/dashboard.html", build_dashboard_context())


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


@require_GET
def ai_search_resource_list(request):
    return render(request, "resources/list.html", build_ai_search_resource_list_context(request))


@require_GET
def ai_search_resource_list_fragment(request):
    context = build_ai_search_resource_list_context(request)
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


@require_http_methods(["GET", "POST"])
def resource_bulk_edit(request):
    if request.method == "GET":
        next_url = normalize_next_url(request.GET.get("next", ""))
        resource_ids = parse_resource_ids(request.GET.getlist("resource_ids"))
        return render(
            request,
            "resources/bulk_edit.html",
            build_bulk_edit_context(
                request,
                form=ResourceBulkEditForm(),
                resource_ids=resource_ids,
                next_url=next_url,
            ),
        )

    form = ResourceBulkEditForm(request.POST)
    next_url = normalize_next_url(request.POST.get("next", ""))
    resource_ids = parse_resource_ids(request.POST.getlist("resource_ids"))
    if not resource_ids:
        messages.error(request, "一括操作するURLを1件以上選択してください。")
        return render(
            request,
            "resources/bulk_edit.html",
            build_bulk_edit_context(
                request,
                form=form,
                resource_ids=resource_ids,
                next_url=next_url,
                page_number=request.POST.get("page"),
            ),
            status=400,
        )

    if not form.is_valid():
        first_error = next((error for errors in form.errors.values() for error in errors), "一括更新に失敗しました。")
        messages.error(request, first_error)
        return render(
            request,
            "resources/bulk_edit.html",
            build_bulk_edit_context(
                request,
                form=form,
                resource_ids=resource_ids,
                next_url=next_url,
                page_number=request.POST.get("page"),
            ),
            status=400,
        )

    updated_count = form.apply_to_resources(Resource.objects.filter(pk__in=resource_ids))
    messages.success(request, f"{updated_count} 件のURLに一括操作を適用しました。")
    return redirect(next_url)


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
        return render(request, "resources/edit.html", {"resource": resource, "form": form}, status=400)

    if method == "DELETE":
        delete_resource_with_artifacts(resource)
        messages.success(request, "URLと関連する画像・動画を含む保存ファイルをすぐに削除しました。")
        return redirect("resources:list")

    return HttpResponseNotAllowed(["GET", "POST"])


@ensure_csrf_cookie
@require_http_methods(["GET", "POST"])
def resource_edit(request, pk: int):
    resource = get_object_or_404(Resource.objects.with_related(), pk=pk)
    form = ResourceForm(instance=resource)
    if request.method == "POST":
        form = ResourceForm(request.POST, instance=resource)
        if form.is_valid():
            resource = form.save()
            messages.success(request, "URL情報を更新しました。")
            return redirect(resource)
        if form.existing_resource is not None:
            messages.info(request, "そのURLは別の登録で使われています。")
        else:
            messages.error(request, "更新に失敗しました。入力内容を確認してください。")

    return render(request, "resources/edit.html", {"resource": resource, "form": form})


@require_http_methods(["POST"])
def resource_capture(request, pk: int):
    resource = get_object_or_404(Resource, pk=pk)
    enqueue_capture_job(resource, priority=150)
    messages.success(request, "再取得ジョブを作成しました。")
    return redirect(resource)


@require_GET
def resource_snapshots(request, pk: int):
    resource = get_object_or_404(Resource, pk=pk)
    return redirect(
        f"{reverse('snapshots:list')}?resource={resource.pk}"
    )


@require_GET
def resource_settings(request):
    return render(
        request,
        "resources/settings.html",
        {
            "tag_count": Tag.objects.count(),
            "job_count": CaptureJob.objects.count(),
            "queued_job_count": CaptureJob.objects.filter(
                status__in=[JobStatus.QUEUED, JobStatus.RETRY_WAIT]
            ).count(),
        },
    )
