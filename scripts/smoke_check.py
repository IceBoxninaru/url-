#!/usr/bin/env python3
"""Smoke-check the main URL archive pages and read-only API endpoints."""
from __future__ import annotations

from _django import configure_django, setup_django


configure_django()

from django.test import Client  # noqa: E402
from django.urls import reverse  # noqa: E402


CHECKS = [
    ("root", "/", {302}),
    ("resources:list", lambda: reverse("resources:list"), {200}),
    ("resources:dashboard", lambda: reverse("resources:dashboard"), {200}),
    ("resources:create", lambda: reverse("resources:create"), {200}),
    ("resources:ai_search_list", lambda: reverse("resources:ai_search_list"), {200}),
    ("resources:api_recent", lambda: reverse("resources:api_recent"), {200}),
    ("resources:api_search", lambda: f"{reverse('resources:api_search')}?query=smoke", {200}),
    ("resources:api_ai_search", lambda: reverse("resources:api_ai_search"), {200}),
    ("jobs:list", lambda: reverse("jobs:list"), {200}),
    ("tags:list", lambda: reverse("tags:list"), {200}),
]


def resolve_path(path_or_factory) -> str:
    if callable(path_or_factory):
        return path_or_factory()
    return path_or_factory


def main() -> int:
    setup_django()
    client = Client()
    failures: list[str] = []

    for name, path_or_factory, expected_statuses in CHECKS:
        path = resolve_path(path_or_factory)
        response = client.get(path)
        if response.status_code not in expected_statuses:
            failures.append(
                f"{name}: GET {path} returned {response.status_code}, "
                f"expected one of {sorted(expected_statuses)}"
            )

    if failures:
        print("Smoke check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print(f"Smoke check passed: {len(CHECKS)} endpoints")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
