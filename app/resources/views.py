from django.contrib import messages
from django.http import HttpResponseNotAllowed, JsonResponse, QueryDict
from django.shortcuts import get_object_or_404, redirect, render
from django.template.loader import render_to_string
from django.urls import reverse
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods

from resources.contexts import (
    BULK_EDIT_PAGE_SIZE,
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
    resource = get_object_or_404(Resource, pk=pk)
    return redirect(
        f"{reverse('snapshots:list')}?resource={resource.pk}"
    )


@require_GET
def resource_settings(request):
    jobs = CaptureJob.objects.with_related()[:8]
    return render(
        request,
        "resources/settings.html",
        {
            "tag_count": Tag.objects.count(),
            "job_count": CaptureJob.objects.count(),
            "queued_job_count": CaptureJob.objects.filter(
                status__in=[JobStatus.QUEUED, JobStatus.RETRY_WAIT]
            ).count(),
            "jobs": jobs,
        },
    )
