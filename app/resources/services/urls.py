from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


def normalize_url(raw_url: str) -> str:
    candidate = raw_url.strip()
    if not candidate:
        raise ValueError("URL is required.")
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Only http(s) URLs are supported.")

    netloc = parsed.netloc.lower()
    if parsed.port:
        is_default_port = (parsed.scheme == "http" and parsed.port == 80) or (
            parsed.scheme == "https" and parsed.port == 443
        )
        if is_default_port and parsed.hostname:
            netloc = parsed.hostname.lower()

    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/") or "/"

    query_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower().startswith("utm_"):
            continue
        query_pairs.append((key, value))

    return urlunparse(
        (
            parsed.scheme.lower(),
            netloc,
            path,
            "",
            urlencode(query_pairs, doseq=True),
            "",
        )
    )
