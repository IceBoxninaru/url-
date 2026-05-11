from django.urls import path

from resources import views

app_name = "resources"

urlpatterns = [
    path("api/resources/recent/", views.api_recent_resources, name="api_recent"),
    path("api/resources/search/", views.api_search_resources, name="api_search"),
    path("api/resources/<int:pk>/", views.api_resource_detail, name="api_detail"),
    path("dashboard/", views.resource_dashboard, name="dashboard"),
    path("resources/", views.resource_list, name="list"),
    path("resources/live/", views.resource_list_fragment, name="list_fragment"),
    path("ai-search/resources/", views.ai_search_resource_list, name="ai_search_list"),
    path("ai-search/resources/live/", views.ai_search_resource_list_fragment, name="ai_search_list_fragment"),
    path("ai/interested/", views.ai_interested_page, name="ai_interested"),
    path("ai/interested.md", views.ai_interested_markdown, name="ai_interested_markdown"),
    path("ai/not-interested/", views.ai_not_interested_page, name="ai_not_interested"),
    path("ai/not-interested.md", views.ai_not_interested_markdown, name="ai_not_interested_markdown"),
    path("resources/bulk/", views.resource_bulk_edit, name="bulk_edit"),
    path("resources/new/", views.resource_create, name="create"),
    path("settings/", views.resource_settings, name="settings"),
    path("resources/<int:pk>/edit/", views.resource_edit, name="edit"),
    path("resources/<int:pk>/interest/", views.resource_interest_feedback, name="interest_feedback"),
    path("resources/<int:pk>/interest-label/", views.resource_interest_label, name="interest_label"),
    path("resources/<int:pk>/", views.resource_detail, name="detail"),
    path("resources/<int:pk>/capture/", views.resource_capture, name="capture"),
    path("resources/<int:pk>/snapshots/", views.resource_snapshots, name="snapshots"),
]
