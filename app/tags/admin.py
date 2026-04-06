from django.contrib import admin

from tags.models import ResourceTag, Tag


@admin.register(Tag)
class TagAdmin(admin.ModelAdmin):
    list_display = ("name", "color", "sort_order", "created_at")
    search_fields = ("name",)


@admin.register(ResourceTag)
class ResourceTagAdmin(admin.ModelAdmin):
    list_display = ("resource", "tag")
