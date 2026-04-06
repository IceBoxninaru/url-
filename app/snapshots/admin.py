from django.contrib import admin

from snapshots.models import Snapshot


@admin.register(Snapshot)
class SnapshotAdmin(admin.ModelAdmin):
    list_display = ("id", "resource", "snapshot_no", "fetch_method", "http_status", "fetched_at", "is_deleted_like")
    list_filter = ("fetch_method", "is_deleted_like")
    search_fields = ("page_title", "site_name", "og_title", "extracted_text")
