from django.db.models import QuerySet
from django.shortcuts import get_object_or_404, render
from django.views.decorators.http import require_GET

from resources.contexts import build_snapshot_detail_context
from snapshots.models import Snapshot


def summarize_snapshot_status(snapshot: Snapshot) -> dict:
    if snapshot.error_message:
        return {"label": "エラー", "tone": "danger"}
    if snapshot.http_status and snapshot.http_status >= 400:
        return {"label": "警告あり", "tone": "warning"}
    return {"label": "成功", "tone": "success"}


def format_size(bytes_value: int) -> str:
    if bytes_value <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB"]
    size = float(bytes_value)
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    return f"{size:.1f} {units[unit_index]}"


def build_artifact_cards(snapshot: Snapshot) -> list[dict]:
    image_size = sum(int(asset.get("size_bytes", 0) or 0) for asset in snapshot.image_assets or [])
    video_size = sum(int(asset.get("size_bytes", 0) or 0) for asset in snapshot.video_assets or [])
    return [
        {"label": "HTML", "count": 1 if snapshot.raw_html_path else 0, "meta": snapshot.raw_html_path and "保存済み" or "-"},
        {"label": "テキスト", "count": 1 if snapshot.raw_text_path else 0, "meta": snapshot.raw_text_path and "保存済み" or "-"},
        {"label": "JSON", "count": 1 if snapshot.raw_json_path else 0, "meta": snapshot.raw_json_path and "保存済み" or "-"},
        {"label": "スクリーンショット", "count": 1 if snapshot.screenshot_full_path else 0, "meta": snapshot.screenshot_full_path and "保存済み" or "-"},
        {"label": "画像", "count": snapshot.image_count, "meta": format_size(image_size)},
        {"label": "動画", "count": snapshot.video_count, "meta": format_size(video_size)},
    ]


def build_snapshot_timeline(selected_snapshot: Snapshot, base_queryset: QuerySet[Snapshot] | None = None) -> list[Snapshot]:
    queryset = base_queryset or Snapshot.objects.all()
    return list(
        queryset.filter(resource=selected_snapshot.resource)
        .select_related("resource")
        .order_by("-snapshot_no")[:12]
    )


def build_snapshot_page_context(
    selected_snapshot: Snapshot | None,
    *,
    timeline: list[Snapshot],
    active_resource,
) -> dict:
    if selected_snapshot is None:
        return {
            "snapshot": None,
            "timeline": [],
            "active_resource": None,
            "artifact_cards": [],
            "snapshot_status": None,
            "selected_snapshot_ids": [],
        }

    context = build_snapshot_detail_context(selected_snapshot)
    context.update(
        {
            "timeline": timeline,
            "active_resource": active_resource,
            "artifact_cards": build_artifact_cards(selected_snapshot),
            "snapshot_status": summarize_snapshot_status(selected_snapshot),
        }
    )
    return context


@require_GET
def snapshot_overview(request):
    resource_id = request.GET.get("resource", "")
    selected_snapshot_id = request.GET.get("snapshot", "")
    base_queryset = Snapshot.objects.select_related("resource").order_by("-fetched_at", "-id")
    if resource_id.isdigit():
        base_queryset = base_queryset.filter(resource_id=int(resource_id))

    selected_snapshot = None
    if selected_snapshot_id.isdigit():
        selected_snapshot = base_queryset.filter(pk=int(selected_snapshot_id)).first()
    if selected_snapshot is None:
        selected_snapshot = base_queryset.first()

    timeline = build_snapshot_timeline(selected_snapshot, base_queryset) if selected_snapshot else []
    return render(
        request,
        "snapshots/overview.html",
        build_snapshot_page_context(
            selected_snapshot,
            timeline=timeline,
            active_resource=selected_snapshot.resource if selected_snapshot else None,
        ),
    )


@require_GET
def snapshot_detail(request, pk: int):
    snapshot = get_object_or_404(Snapshot.objects.select_related("resource"), pk=pk)
    timeline = build_snapshot_timeline(snapshot)
    return render(
        request,
        "snapshots/overview.html",
        build_snapshot_page_context(
            snapshot,
            timeline=timeline,
            active_resource=snapshot.resource,
        ),
    )
