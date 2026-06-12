from __future__ import annotations

import os
from pathlib import Path


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


def env_path(name: str, default: Path) -> Path:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return Path(value)


def env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value


DEFAULT_API_BASE = env_str("URL_ARCHIVE_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
DEFAULT_HTTP_TIMEOUT_SECONDS = env_int("URL_ARCHIVE_HTTP_TIMEOUT", 20)

DEFAULT_AUTOMATION_OUTPUT_ROOT = env_path(
    "AI_URL_AUTOMATION_OUTPUT_ROOT",
    Path.home() / ".codex" / "automations" / "ai-url",
)
DEFAULT_X_SEARCH_OUTPUT_DIR = DEFAULT_AUTOMATION_OUTPUT_ROOT / "x_search"
DEFAULT_TWEET_OUTPUT_DIR = DEFAULT_AUTOMATION_OUTPUT_ROOT / "tweet_drafts"

DEFAULT_AI_NEWS_SAVE_MAX_ITEMS = env_int("AI_NEWS_SAVE_MAX_ITEMS", 12)
DEFAULT_AI_NEWS_VERIFY_EXPECTED_MIN = env_int("AI_NEWS_VERIFY_EXPECTED_MIN", 1)
DEFAULT_AI_NEWS_VERIFY_EXPECTED_MAX = env_int("AI_NEWS_VERIFY_EXPECTED_MAX", 12)
