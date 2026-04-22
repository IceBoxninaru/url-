from django.test import TestCase
from django.urls import reverse

from resources.models import Resource
from snapshots.models import FetchMethod, Snapshot


class SnapshotViewTests(TestCase):
    def test_snapshot_detail_uses_shared_context_builder(self):
        resource = Resource.objects.create(
            original_url="https://example.com/article",
            normalized_url="https://example.com/article",
            domain="example.com",
            title_manual="Article",
        )
        snapshot = Snapshot.objects.create(
            resource=resource,
            snapshot_no=1,
            fetch_url=resource.normalized_url,
            fetch_method=FetchMethod.HTTP,
            page_title="Captured title",
            extracted_text="snapshot body",
            ai_payload={"tag_candidates": ["refactor"]},
        )

        response = self.client.get(reverse("snapshots:detail", args=[snapshot.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["snapshot"], snapshot)
        self.assertEqual(response.context["tag_candidates"], ["refactor"])
        self.assertNotIn("snapshot_diff", response.context)
