from django.shortcuts import get_object_or_404, render
from django.views.decorators.http import require_GET

from resources.models import Resource
from snapshots.models import Snapshot


@require_GET
def snapshot_detail(request, pk: int):
    snapshot = get_object_or_404(Snapshot.objects.select_related("resource"), pk=pk)
    payload = snapshot.ai_payload or {}
    similar_resources = Resource.objects.filter(id__in=payload.get("similar_resource_ids", [])).select_related("latest_snapshot")
    return render(
        request,
        "snapshots/detail.html",
        {
            "snapshot": snapshot,
            "tag_candidates": payload.get("tag_candidates", []),
            "similar_resources": similar_resources,
        },
    )
