from django.urls import path

from tags import views

app_name = "tags"

urlpatterns = [
    path("tags/", views.tag_list, name="list"),
    path("tags/<int:pk>/", views.tag_detail, name="detail"),
]
