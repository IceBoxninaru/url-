from django.shortcuts import get_object_or_404, render
from django.views.decorators.http import require_GET

from resources.contexts import build_snapshot_detail_context
from snapshots.models import Snapshot


@require_GET
def snapshot_detail(request, pk: int):
    snapshot = get_object_or_404(Snapshot.objects.select_related("resource"), pk=pk)
    return render(request, "snapshots/detail.html", build_snapshot_detail_context(snapshot))
