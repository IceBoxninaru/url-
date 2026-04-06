from django.contrib import admin

from resources.models import Resource


@admin.register(Resource)
class ResourceAdmin(admin.ModelAdmin):
    list_display = ("id", "display_title", "domain", "favorite", "current_status", "updated_at")
    list_filter = ("favorite", "current_status")
    search_fields = ("original_url", "normalized_url", "title_manual", "note", "domain")
