from django.contrib import admin
from django.shortcuts import redirect
from django.urls import include, path
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', lambda request: redirect('resources:list')),
    path('', include('resources.urls')),
    path('', include('snapshots.urls')),
    path('', include('tags.urls')),
    path('', include('jobs.urls')),
]

if settings.DEBUG:
    urlpatterns += static(settings.STORAGE_URL, document_root=settings.STORAGE_ROOT)
