from __future__ import annotations

from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.template.loader import render_to_string
from django.urls import reverse
from django.views.decorators.http import require_GET

from jobs.models import CaptureJob, JobStatus
from resources.contexts import (
    build_ai_feedback_page_context,
    build_ai_search_resource_list_context,
    build_dashboard_context,
    build_resource_list_context,
)
from resources.models import InterestFeedback, Resource
from tags.models import Tag

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


@require_GET
def ai_interested_page(request):
    return render(
        request,
        "resources/ai_feedback.html",
        build_ai_feedback_page_context(request, InterestFeedback.INTERESTED),
    )


@require_GET
def ai_not_interested_page(request):
    return render(
        request,
        "resources/ai_feedback.html",
        build_ai_feedback_page_context(request, InterestFeedback.NOT_INTERESTED),
    )


def render_ai_feedback_markdown(request, feedback: str, filename: str) -> HttpResponse:
    context = build_ai_feedback_page_context(request, feedback)
    content = render_to_string("resources/ai_feedback.md", context, request=request)
    response = HttpResponse(content, content_type="text/markdown; charset=utf-8")
    response["Content-Disposition"] = f'inline; filename="{filename}"'
    return response


@require_GET
def ai_interested_markdown(request):
    return render_ai_feedback_markdown(request, InterestFeedback.INTERESTED, "interested-urls.md")


@require_GET
def ai_not_interested_markdown(request):
    return render_ai_feedback_markdown(request, InterestFeedback.NOT_INTERESTED, "not-interested-urls.md")


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
