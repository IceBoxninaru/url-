from __future__ import annotations

import shutil
from pathlib import Path

from django.conf import settings

from snapshots.models import Snapshot


def build_resource_directory(root: Path, resource_id: int) -> Path:
    return root / f"resource_{resource_id:04d}"


def resolve_storage_file_path(raw_path: str) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = settings.ROOT_DIR / candidate
    return candidate


def build_storage_asset_path(storage_root: Path, resource_id: int, filename: str) -> str:
    return (Path("storage") / Path(storage_root).name / f"resource_{resource_id:04d}" / filename).as_posix()


def resolve_asset_file_path(
    raw_path: str,
    storage_root: Path | None = None,
    *,
    resource_id: int | None = None,
) -> tuple[Path, str | None]:
    candidate = resolve_storage_file_path(raw_path)
    if candidate.exists():
        return candidate, None

    if storage_root is None:
        return candidate, None

    fallback_paths: list[Path] = []
    relative_path = Path(raw_path)
    if resource_id is not None and relative_path.name:
        fallback_paths.append(build_resource_directory(Path(storage_root), resource_id) / relative_path.name)
    if len(relative_path.parts) >= 2:
        fallback_paths.append(Path(storage_root).joinpath(*relative_path.parts[-2:]))
    for fallback in fallback_paths:
        if fallback.exists():
            display_path = None
            if resource_id is not None and fallback.name:
                display_path = build_storage_asset_path(Path(storage_root), resource_id, fallback.name)
            return fallback, display_path
    return candidate, None


def filter_existing_snapshot_assets(
    assets: list[dict] | None,
    *,
    storage_root: Path | None = None,
    resource_id: int | None = None,
) -> list[dict]:
    existing_assets: list[dict] = []
    for asset in assets or []:
        path = str(asset.get("path", "")).strip()
        if not path:
            continue
        file_path, display_path = resolve_asset_file_path(path, storage_root, resource_id=resource_id)
        if file_path.exists() and file_path.is_file():
            existing_assets.append({**asset, "path": display_path or path})
    return existing_assets


def get_capture_files(snapshot: Snapshot | None) -> tuple[list[dict], list[dict]]:
    if snapshot is None:
        return [], []
    return (
        filter_existing_snapshot_assets(
            snapshot.image_assets,
            storage_root=settings.IMAGE_STORAGE_ROOT,
            resource_id=snapshot.resource_id,
        ),
        filter_existing_snapshot_assets(
            snapshot.video_assets,
            storage_root=settings.VIDEO_STORAGE_ROOT,
            resource_id=snapshot.resource_id,
        ),
    )


def get_snapshot_screenshot_file(snapshot: Snapshot | None) -> dict | None:
    if snapshot is None:
        return None
    path = (snapshot.screenshot_full_path or "").strip()
    if not path:
        return None
    file_path, display_path = resolve_asset_file_path(
        path,
        settings.SCREENSHOT_STORAGE_ROOT,
        resource_id=snapshot.resource_id,
    )
    if not file_path.exists() or not file_path.is_file():
        return None
    try:
        size_bytes = file_path.stat().st_size
    except OSError:
        size_bytes = 0
    return {
        "source_url": snapshot.fetch_url,
        "path": display_path or path,
        "content_type": "image/png",
        "size_bytes": size_bytes,
    }


def write_storage_file(root: Path, resource_id: int, filename: str, content, binary: bool = False) -> str:
    resource_dir = build_resource_directory(root, resource_id)
    resource_dir.mkdir(parents=True, exist_ok=True)
    target = resource_dir / filename
    if binary:
        target.write_bytes(content)
    else:
        target.write_text(content, encoding="utf-8")
    return target.relative_to(settings.ROOT_DIR).as_posix()


def move_storage_file(root: Path, resource_id: int, filename: str, source_path: Path) -> str:
    resource_dir = build_resource_directory(root, resource_id)
    resource_dir.mkdir(parents=True, exist_ok=True)
    target = resource_dir / filename
    shutil.move(str(source_path), target)
    return target.relative_to(settings.ROOT_DIR).as_posix()
