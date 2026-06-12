from __future__ import annotations

from datetime import date, datetime, time, timedelta

from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.views.decorators.http import require_GET

from resources.models import Resource
from resources.serializers import serialize_ai_search_resource, serialize_resource


def parse_api_limit(raw_limit: str | None, *, default: int = 5, maximum: int = 20) -> int:
    try:
        limit = int(raw_limit or default)
    except (TypeError, ValueError):
        limit = default
    return max(1, min(limit, maximum))


def parse_api_date(raw_value: str | None):
    if not raw_value:
        return None
    try:
        return date.fromisoformat(raw_value.strip())
    except (TypeError, ValueError):
        return None


def local_day_bounds(target_date):
    tz = timezone.get_current_timezone()
    start = timezone.make_aware(datetime.combine(target_date, time.min), tz)
    end = timezone.make_aware(datetime.combine(target_date + timedelta(days=1), time.min), tz)
    return start, end


def build_ai_search_queryset(request):
    query = (request.GET.get("q") or request.GET.get("query") or "").strip()
    date_value = parse_api_date(request.GET.get("date"))
    from_value = parse_api_date(request.GET.get("from"))
    to_value = parse_api_date(request.GET.get("to"))

    queryset = Resource.objects.with_related().filter(search_only=True)
    if query:
        queryset = Resource.objects.apply_filters(query=query, visibility="search_only")

    if date_value:
        start, end = local_day_bounds(date_value)
        queryset = queryset.filter(created_at__gte=start, created_at__lt=end)
    else:
        if from_value:
            start, _ = local_day_bounds(from_value)
        else:
            start, _ = local_day_bounds(timezone.localdate())
        queryset = queryset.filter(created_at__gte=start)
        if to_value:
            _, end = local_day_bounds(to_value)
            queryset = queryset.filter(created_at__lt=end)

    return queryset.order_by("-created_at", "-id"), query


@require_GET
def api_ai_search_resources(request):
    limit = parse_api_limit(request.GET.get("limit"), default=20, maximum=100)
    queryset, query = build_ai_search_queryset(request)
    total_count = queryset.count()
    resources = list(queryset[:limit])
    return JsonResponse(
        {
            "items": [serialize_ai_search_resource(resource) for resource in resources],
            "count": len(resources),
            "total_count": total_count,
            "query": query,
            "limit": limit,
        }
    )


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

