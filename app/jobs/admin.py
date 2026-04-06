from django.contrib import admin

from jobs.models import CaptureJob


@admin.register(CaptureJob)
class CaptureJobAdmin(admin.ModelAdmin):
    list_display = ("id", "job_type", "resource", "status", "priority", "attempt_count", "scheduled_at")
    list_filter = ("job_type", "status")
    search_fields = ("resource__original_url", "error_message")
