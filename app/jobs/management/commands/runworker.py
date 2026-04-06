from django.core.management.base import BaseCommand

from jobs.services import run_worker_loop


class Command(BaseCommand):
    help = "Run queued capture and AI jobs."

    def add_arguments(self, parser):
        parser.add_argument("--once", action="store_true", help="Process until the queue is empty.")
        parser.add_argument("--sleep", type=int, default=5, help="Idle sleep seconds in daemon mode.")
        parser.add_argument("--max-jobs", type=int, default=None, help="Optional hard cap for processed jobs.")

    def handle(self, *args, **options):
        processed = run_worker_loop(
            once=options["once"],
            sleep_seconds=options["sleep"],
            max_jobs=options["max_jobs"],
        )
        self.stdout.write(self.style.SUCCESS(f"Processed jobs: {processed}"))
