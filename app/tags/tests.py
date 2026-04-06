from django.test import TestCase
from django.urls import reverse

from tags.models import Tag


class TagViewTests(TestCase):
    def test_tag_list_renders_compact_management_ui(self):
        Tag.objects.create(name="AI", color="#3454d1", sort_order=100)

        response = self.client.get(reverse("tags:list"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "タグ管理")
        self.assertContains(response, "tag-management-row")

    def test_post_create_tag(self):
        response = self.client.post(
            reverse("tags:list"),
            {
                "name": "OpenAI",
                "color": "#123456",
                "sort_order": 10,
            },
        )

        self.assertEqual(response.status_code, 302)
        tag = Tag.objects.get()
        self.assertEqual(tag.name, "OpenAI")
        self.assertEqual(tag.color, "#123456")
        self.assertEqual(tag.sort_order, 10)

    def test_patch_and_delete_tag(self):
        tag = Tag.objects.create(name="AI", color="#3454d1", sort_order=100)

        update_response = self.client.post(
            reverse("tags:detail", args=[tag.id]),
            {
                "_method": "PATCH",
                "name": "AI Updated",
                "color": "#654321",
                "sort_order": 5,
            },
        )
        self.assertEqual(update_response.status_code, 302)
        tag.refresh_from_db()
        self.assertEqual(tag.name, "AI Updated")
        self.assertEqual(tag.color, "#654321")
        self.assertEqual(tag.sort_order, 5)

        delete_response = self.client.post(
            reverse("tags:detail", args=[tag.id]),
            {"_method": "DELETE"},
        )
        self.assertEqual(delete_response.status_code, 302)
        self.assertFalse(Tag.objects.filter(id=tag.id).exists())
