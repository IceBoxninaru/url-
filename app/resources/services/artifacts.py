from __future__ import annotations

import shutil
from pathlib import Path

from django.conf import settings

from resources.models import Resource

from .storage import build_resource_directory


def delete_resource_with_artifacts(resource: Resource) -> None:
    resource_id = resource.id
    resource.delete()
    for root in (
        settings.HTML_STORAGE_ROOT,
        settings.TEXT_STORAGE_ROOT,
        settings.JSON_STORAGE_ROOT,
        settings.SCREENSHOT_STORAGE_ROOT,
        settings.IMAGE_STORAGE_ROOT,
        settings.VIDEO_STORAGE_ROOT,
    ):
        target = build_resource_directory(root, resource_id)
        if target.exists():
            target = target.resolve()
            root_path = Path(root).resolve()
            if root_path in target.parents:
                shutil.rmtree(target)
