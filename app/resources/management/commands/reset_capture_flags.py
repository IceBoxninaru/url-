from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from resources.models import Resource


IMAGE_EXTENSIONS = {".avif", ".gif", ".jpeg", ".jpg", ".png", ".webp"}
VIDEO_EXTENSIONS = {".m4v", ".mov", ".mp4", ".webm"}


def build_resource_directory(root: Path, resource_id: int) -> Path:
    return Path(root) / f"resource_{resource_id:04d}"


def count_files(root: Path, resource_id: int, extensions: set[str]) -> int:
    resource_dir = build_resource_directory(root, resource_id)
    if not resource_dir.exists() or not resource_dir.is_dir():
        return 0
    return sum(
        1
        for path in resource_dir.iterdir()
        if path.is_file() and path.suffix.lower() in extensions
    )


class Command(BaseCommand):
    help = "Reset capture_images/capture_videos preferences to their default enabled state."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show resources that would be updated without writing to the database.",
        )
        parser.add_argument(
            "--yes",
            action="store_true",
            help="Apply changes without the interactive confirmation prompt.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        assume_yes = options["yes"]
        resources = list(Resource.objects.order_by("id"))
        changes = []
        image_false_with_files = 0
        image_false_without_files = 0
        video_false_with_files = 0
        video_false_without_files = 0

        for resource in resources:
            image_count = count_files(settings.IMAGE_STORAGE_ROOT, resource.pk, IMAGE_EXTENSIONS)
            video_count = count_files(settings.VIDEO_STORAGE_ROOT, resource.pk, VIDEO_EXTENSIONS)
            update_images = not resource.capture_images
            update_videos = not resource.capture_videos

            if update_images:
                if image_count:
                    image_false_with_files += 1
                else:
                    image_false_without_files += 1
            if update_videos:
                if video_count:
                    video_false_with_files += 1
                else:
                    video_false_without_files += 1

            if update_images or update_videos:
                changes.append(
                    {
                        "resource": resource,
                        "update_images": update_images,
                        "update_videos": update_videos,
                        "image_count": image_count,
                        "video_count": video_count,
                    }
                )

        self.stdout.write(f"Checked resources: {len(resources)}")
        self.stdout.write(f"Resources to update: {len(changes)}")
        self.stdout.write(
            "capture_images false -> true: "
            f"{image_false_with_files + image_false_without_files} "
            f"(with files: {image_false_with_files}, without files: {image_false_without_files})"
        )
        self.stdout.write(
            "capture_videos false -> true: "
            f"{video_false_with_files + video_false_without_files} "
            f"(with files: {video_false_with_files}, without files: {video_false_without_files})"
        )

        for change in changes[:50]:
            resource = change["resource"]
            fields = []
            if change["update_images"]:
                fields.append("capture_images")
            if change["update_videos"]:
                fields.append("capture_videos")
            self.stdout.write(
                f"  resource_id={resource.pk} fields={','.join(fields)} "
                f"image_files={change['image_count']} video_files={change['video_count']} "
                f"url={resource.original_url}"
            )
        if len(changes) > 50:
            self.stdout.write(f"  ... and {len(changes) - 50} more")

        if dry_run:
            self.stdout.write("Dry run only. No database rows were changed.")
            return
        if not changes:
            self.stdout.write("No database rows need changes.")
            return
        if not assume_yes:
            self.stdout.write("Type RESET to update these resource preferences, or anything else to abort:")
            if self.stdin.readline().strip() != "RESET":
                raise CommandError("Aborted. No database rows were changed.")

        with transaction.atomic():
            for change in changes:
                resource = change["resource"]
                update_fields = ["updated_at"]
                if change["update_images"]:
                    resource.capture_images = True
                    update_fields.append("capture_images")
                if change["update_videos"]:
                    resource.capture_videos = True
                    update_fields.append("capture_videos")
                resource.save(update_fields=update_fields)

        self.stdout.write(self.style.SUCCESS(f"Updated resources: {len(changes)}"))
