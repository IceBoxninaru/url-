from __future__ import annotations

import re

import trafilatura
from bs4 import BeautifulSoup
from django.utils.dateparse import parse_datetime


def extract_text_from_html(html: str, source_url: str) -> str:
    extracted = trafilatura.extract(
        html,
        url=source_url,
        include_comments=False,
        include_images=False,
        include_formatting=False,
        favor_precision=True,
    )
    if extracted:
        return extracted.strip()

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    return re.sub(r"\n{3,}", "\n\n", text)


def extract_metadata(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    def meta_value(*keys: str) -> str:
        for key in keys:
            tag = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
            if tag and tag.get("content"):
                return tag["content"].strip()
        return ""

    published_raw = meta_value(
        "article:published_time",
        "og:published_time",
        "published_time",
        "datePublished",
    )
    published_at = parse_datetime(published_raw) if published_raw else None
    return {
        "page_title": (soup.title.string.strip() if soup.title and soup.title.string else ""),
        "site_name": meta_value("og:site_name", "application-name"),
        "author": meta_value("author", "article:author"),
        "published_at": published_at,
        "og_title": meta_value("og:title"),
        "og_description": meta_value("og:description", "description"),
        "og_image_url": meta_value("og:image"),
    }
