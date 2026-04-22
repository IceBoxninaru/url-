from django.urls import path

from snapshots import views

app_name = "snapshots"

urlpatterns = [
    path("snapshots/", views.snapshot_overview, name="list"),
    path("snapshots/<int:pk>/", views.snapshot_detail, name="detail"),
]
