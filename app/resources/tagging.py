from __future__ import annotations

import re
from collections.abc import Iterable

from django import forms

from tags.models import Tag

NEW_TAG_SPLIT_RE = re.compile(r"[\n,、]+")


def ordered_tags_queryset():
    return Tag.objects.all().order_by("sort_order", "name")


def parse_new_tag_names(raw_value: str) -> list[str]:
    if not raw_value:
        return []

    max_length = Tag._meta.get_field("name").max_length
    normalized_names: list[str] = []
    seen_names: set[str] = set()
    for candidate in NEW_TAG_SPLIT_RE.split(raw_value):
        name = candidate.strip()
        if not name:
            continue
        if len(name) > max_length:
            raise forms.ValidationError(f"タグ名は {max_length} 文字以内で入力してください。")
        key = name.casefold()
        if key in seen_names:
            continue
        seen_names.add(key)
        normalized_names.append(name)
    return normalized_names


def resolve_tags(selected_tags: Iterable[Tag], new_tag_names: Iterable[str]) -> list[Tag]:
    resolved_tags = list(selected_tags)
    existing_names = {tag.name.casefold(): tag for tag in resolved_tags}
    for name in new_tag_names:
        key = name.casefold()
        if key in existing_names:
            continue
        tag = Tag.objects.filter(name__iexact=name).first()
        if tag is None:
            tag = Tag.objects.create(name=name)
        existing_names[key] = tag
        resolved_tags.append(tag)
    return resolved_tags
