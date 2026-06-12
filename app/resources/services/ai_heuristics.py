from __future__ import annotations

import re

from resources.models import Resource
from snapshots.models import Snapshot


STOP_WORDS = {
    "the",
    "this",
    "that",
    "with",
    "from",
    "have",
    "your",
    "about",
    "into",
    "https",
    "http",
    "www",
    "com",
    "net",
    "org",
}


def infer_category(snapshot: Snapshot) -> str:
    combined = f"{snapshot.page_title} {snapshot.site_name} {snapshot.og_description} {snapshot.extracted_text[:1200]}".lower()
    category_rules = {
        "social": ["tweet", "post", "instagram", "thread", "social"],
        "shopping": ["cart", "price", "shop", "buy", "product"],
        "documentation": ["docs", "reference", "api", "guide"],
        "news": ["news", "breaking", "press", "report"],
        "video": ["video", "watch", "stream", "episode"],
    }
    for category, markers in category_rules.items():
        if any(marker in combined for marker in markers):
            return category
    return "general"


def suggest_tags(snapshot: Snapshot) -> list[str]:
    source = f"{snapshot.page_title} {snapshot.og_description} {snapshot.site_name} {snapshot.extracted_text[:1000]}".lower()
    tokens = re.findall(r"[a-z0-9][a-z0-9_-]{2,}", source)
    ranked: list[str] = []
    for token in tokens:
        if token in STOP_WORDS:
            continue
        if token not in ranked:
            ranked.append(token)
        if len(ranked) == 5:
            break
    category = infer_category(snapshot)
    if category != "general" and category not in ranked:
        ranked.insert(0, category)
    return ranked[:5]


def similar_resource_ids(resource: Resource) -> list[int]:
    queryset = Resource.objects.exclude(pk=resource.pk)
    if resource.domain:
        queryset = queryset.filter(domain=resource.domain)
    return list(queryset.order_by("-updated_at").values_list("id", flat=True)[:5])
