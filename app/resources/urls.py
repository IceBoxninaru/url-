from django.urls import path

from resources import views

app_name = "resources"

urlpatterns = [
    path("resources/", views.resource_list, name="list"),
    path("resources/live/", views.resource_list_fragment, name="list_fragment"),
    path("resources/new/", views.resource_create, name="create"),
    path("resources/<int:pk>/", views.resource_detail, name="detail"),
    path("resources/<int:pk>/capture/", views.resource_capture, name="capture"),
    path("resources/<int:pk>/snapshots/", views.resource_snapshots, name="snapshots"),
]
