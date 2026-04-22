from django.shortcuts import render
from django.views.decorators.http import require_GET

from jobs.models import CaptureJob


@require_GET
def job_list(request):
    jobs = CaptureJob.objects.with_related()[:200]
    return render(request, "jobs/list.html", {"jobs": jobs})
