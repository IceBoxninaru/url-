from __future__ import annotations

import json

import httpx
from django.conf import settings

from snapshots.models import Snapshot

from .ai_heuristics import infer_category, similar_resource_ids, suggest_tags
from .local_llm import (
    local_llm_chat,
    local_llm_models,
    normalize_local_llm_tags,
    parse_local_llm_json,
    truncate_ai_output,
)
from .translation import (
    build_translation_source_text,
    is_probably_japanese_text,
    normalize_ai_text,
    translate_text_to_japanese,
)
from .types import AIResult

LOCAL_LLM_PROVIDERS = {"openclaw", "local_llm", "openai_compatible", "ollama"}
LOCAL_LLM_CATEGORIES = {"general", "social", "shopping", "documentation", "news", "video"}


def build_ai_source_text(snapshot: Snapshot) -> str:
    parts = [
        f"URL: {snapshot.fetch_url}",
        f"Title: {snapshot.page_title}",
        f"Site: {snapshot.site_name}",
        f"Description: {snapshot.og_description}",
        f"Text: {snapshot.extracted_text}",
    ]
    source = "\n".join(part for part in parts if part.split(": ", 1)[-1].strip())
    max_chars = getattr(settings, "AI_MAX_INPUT_CHARS", 12000)
    return source[:max_chars].strip()


def build_local_llm_messages(snapshot: Snapshot) -> list[dict]:
    summary_limit = getattr(settings, "AI_SUMMARY_MAX_CHARS", 320)
    source_text = build_ai_source_text(snapshot)
    return [
        {
            "role": "system",
            "content": (
                "You are a URL archive assistant. Analyze saved web page content and return only valid JSON. "
                "Do not wrap the JSON in markdown. summary must be Japanese. "
                "translation must be Japanese when the source text is not Japanese. "
                "Never copy English source text into translation."
            ),
        },
        {
            "role": "user",
            "content": (
                "次の保存済みページを整理してください。\n"
                f"- summary: 日本語で{summary_limit}文字以内の要約\n"
                "- translation: 元本文が日本語以外なら自然な日本語訳。英語原文をそのまま返さない。元本文が日本語なら空文字\n"
                "- category: general, social, shopping, documentation, news, video のどれか\n"
                "- tag_candidates: 1から5個の短いタグ候補\n\n"
                "Example output:\n"
                "{\"summary\":\"記事の要点です。\",\"translation\":\"これは日本語訳です。\","
                "\"category\":\"documentation\",\"tag_candidates\":[\"AI\",\"資料\"]}\n\n"
                "JSON schema:\n"
                "{"
                "\"summary\":\"...\","
                "\"translation\":\"...\","
                "\"category\":\"general\","
                "\"tag_candidates\":[\"tag\"]"
                "}\n\n"
                f"{source_text}"
            ),
        },
    ]


def run_local_llm_pipeline(snapshot: Snapshot, base_payload: dict) -> AIResult:
    messages = build_local_llm_messages(snapshot)
    models = local_llm_models()
    if not models:
        raise ValueError("AI_MODEL is required for local LLM providers.")

    parsed = None
    selected_model = ""
    last_error: Exception | None = None
    for model in models:
        try:
            content = local_llm_chat(messages, model=model)
            parsed = parse_local_llm_json(content)
            selected_model = model
            break
        except (httpx.HTTPError, ValueError, json.JSONDecodeError) as exc:
            last_error = exc

    if parsed is None:
        raise ValueError("Local LLM failed for all configured models.") from last_error

    heuristic_tags = base_payload.get("tag_candidates", [])
    tag_candidates = normalize_local_llm_tags(parsed.get("tag_candidates")) or heuristic_tags

    category = normalize_ai_text(str(parsed.get("category", ""))).lower()
    if category not in LOCAL_LLM_CATEGORIES:
        category = infer_category(snapshot)

    source_is_japanese = is_probably_japanese_text(build_translation_source_text(snapshot))
    translation = "" if source_is_japanese else normalize_ai_text(str(parsed.get("translation", "")))
    if translation and not is_probably_japanese_text(translation):
        translation = ""
    summary = truncate_ai_output(
        str(parsed.get("summary", "")),
        limit=getattr(settings, "AI_SUMMARY_MAX_CHARS", 320),
    )
    if source_is_japanese:
        translation_status = "source_already_japanese"
    elif translation:
        translation_status = "llm_generated"
    else:
        translation_status = "llm_translation_unavailable"
    return AIResult(
        summary=summary,
        translation=translation,
        category=category,
        payload={
            **base_payload,
            "model": selected_model,
            "tag_candidates": tag_candidates,
            "translation_status": translation_status,
            "translation_detected_language": "ja" if source_is_japanese else "",
            "summary_status": "llm_generated" if summary else "empty",
        },
    )


def run_ai_pipeline(snapshot: Snapshot) -> AIResult:
    provider = settings.AI_PROVIDER.lower()
    base_payload = {
        "provider": provider,
        "tag_candidates": suggest_tags(snapshot),
        "similar_resource_ids": similar_resource_ids(snapshot.resource),
    }
    if provider == "noop":
        return AIResult(
            translation="",
            category="",
            payload={
                **base_payload,
                "translation_status": "provider_disabled",
                "translation_detected_language": "",
            },
        )

    if provider in LOCAL_LLM_PROVIDERS:
        return run_local_llm_pipeline(snapshot, base_payload)

    translation, translation_meta = translate_text_to_japanese(build_translation_source_text(snapshot))
    category = infer_category(snapshot)
    payload = {
        **base_payload,
        "translation_status": translation_meta.get("translation_status", ""),
        "translation_detected_language": translation_meta.get("detected_language", ""),
    }
    if translation_meta.get("error_message"):
        payload["translation_error_message"] = translation_meta["error_message"]
    return AIResult(translation=translation, category=category, payload=payload)
