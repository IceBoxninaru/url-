from django.urls import path

from jobs import views

app_name = "jobs"

urlpatterns = [
    path("jobs/", views.job_list, name="list"),
]
