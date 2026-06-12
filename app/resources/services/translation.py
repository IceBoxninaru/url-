from __future__ import annotations

import logging
import re
from typing import Protocol

import httpx
from django.conf import settings

from snapshots.models import Snapshot

logger = logging.getLogger(__name__)

TRANSLATION_MAX_SOURCE_CHARS = 1600
TRANSLATION_MAX_CHUNK_CHARS = 400
TRANSLATION_ENDPOINT = "https://translate.googleapis.com/translate_a/single"


class TranslationProvider(Protocol):
    def translate_chunk_to_japanese(self, text: str) -> tuple[str, str]:
        """Return translated text and detected source language."""


class GoogleTranslateProvider:
    def translate_chunk_to_japanese(self, text: str) -> tuple[str, str]:
        with httpx.Client(
            timeout=15.0,
            headers={"User-Agent": settings.CAPTURE_HTTP_USER_AGENT},
        ) as client:
            response = client.get(
                TRANSLATION_ENDPOINT,
                params={
                    "client": "gtx",
                    "sl": "auto",
                    "tl": "ja",
                    "dt": "t",
                    "q": text,
                },
            )
        response.raise_for_status()
        payload = response.json()
        translated_parts: list[str] = []
        detected_language = ""
        if isinstance(payload, list):
            if len(payload) > 2 and isinstance(payload[2], str):
                detected_language = payload[2]
            if payload and isinstance(payload[0], list):
                for item in payload[0]:
                    if isinstance(item, list) and item and isinstance(item[0], str):
                        translated_parts.append(item[0])
        return normalize_ai_text("".join(translated_parts)), detected_language


def get_translation_provider() -> TranslationProvider:
    return GoogleTranslateProvider()


def normalize_ai_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def build_translation_source_text(snapshot: Snapshot) -> str:
    source = snapshot.extracted_text or snapshot.og_description or snapshot.page_title
    normalized = normalize_ai_text(source)
    if not normalized:
        return ""
    source_limit = min(getattr(settings, "AI_MAX_INPUT_CHARS", TRANSLATION_MAX_SOURCE_CHARS), TRANSLATION_MAX_SOURCE_CHARS)
    return normalized[:source_limit].strip()


def split_translation_chunks(text: str, *, max_chars: int = TRANSLATION_MAX_CHUNK_CHARS) -> list[str]:
    normalized = normalize_ai_text(text)
    if not normalized:
        return []
    if len(normalized) <= max_chars:
        return [normalized]

    chunks: list[str] = []
    current = ""
    segments = [segment for segment in re.split(r"(?<=[.!?。！？])\s+", normalized) if segment]
    for segment in segments:
        if len(segment) > max_chars:
            if current:
                chunks.append(current)
                current = ""
            for start in range(0, len(segment), max_chars):
                piece = segment[start : start + max_chars].strip()
                if piece:
                    chunks.append(piece)
            continue
        candidate = segment if not current else f"{current} {segment}"
        if len(candidate) <= max_chars:
            current = candidate
        else:
            chunks.append(current)
            current = segment
    if current:
        chunks.append(current)
    return chunks


def is_probably_japanese_text(text: str) -> bool:
    sample = normalize_ai_text(text)[:800]
    if not sample:
        return False
    kana_count = len(re.findall(r"[ぁ-ゖァ-ヺー]", sample))
    cjk_count = len(re.findall(r"[一-龯々〆ヵヶ]", sample))
    return kana_count >= 3 or (kana_count >= 1 and cjk_count >= 4)


def translate_text_chunk_to_japanese(text: str) -> tuple[str, str]:
    return get_translation_provider().translate_chunk_to_japanese(text)


def translate_text_to_japanese(text: str, *, provider: TranslationProvider | None = None) -> tuple[str, dict]:
    normalized = normalize_ai_text(text)
    if not normalized:
        return "", {"translation_status": "empty_source", "detected_language": ""}
    if is_probably_japanese_text(normalized):
        return "", {"translation_status": "source_already_japanese", "detected_language": "ja"}

    provider = provider or get_translation_provider()
    translated_chunks: list[str] = []
    detected_language = ""
    try:
        for chunk in split_translation_chunks(normalized):
            translated_chunk, chunk_language = provider.translate_chunk_to_japanese(chunk)
            if translated_chunk:
                translated_chunks.append(translated_chunk)
            if chunk_language and not detected_language:
                detected_language = chunk_language
    except Exception as exc:  # pragma: no cover
        logger.warning("Japanese translation failed: %s", exc)
        return "", {
            "translation_status": "translation_failed",
            "detected_language": detected_language,
            "error_message": str(exc),
        }

    translation = normalize_ai_text(" ".join(translated_chunks))
    if detected_language.startswith("ja"):
        return "", {"translation_status": "source_already_japanese", "detected_language": detected_language}
    return translation, {
        "translation_status": "translated" if translation else "translation_unavailable",
        "detected_language": detected_language,
    }
