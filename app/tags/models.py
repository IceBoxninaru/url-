from django.conf import settings
from django.db import models


class Tag(models.Model):
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="owned_tags",
    )
    name = models.CharField(max_length=80, unique=True)
    color = models.CharField(max_length=20, default="#3454d1")
    sort_order = models.PositiveIntegerField(default=100)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["sort_order", "name", "id"]

    def __str__(self):
        return self.name


class ResourceTag(models.Model):
    resource = models.ForeignKey("resources.Resource", on_delete=models.CASCADE)
    tag = models.ForeignKey("tags.Tag", on_delete=models.CASCADE)

    class Meta:
        unique_together = ("resource", "tag")
        ordering = ["resource_id", "tag_id"]

    def __str__(self):
        return f"{self.resource_id}:{self.tag_id}"
