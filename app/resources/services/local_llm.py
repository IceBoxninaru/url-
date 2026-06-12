from __future__ import annotations

import json
import re

import httpx
from django.conf import settings

from .translation import normalize_ai_text


def local_llm_chat_completion_url() -> str:
    base_url = getattr(settings, "AI_API_BASE", "").strip().rstrip("/")
    if not base_url:
        raise ValueError("AI_API_BASE is required for local LLM providers.")
    if base_url.endswith("/chat/completions"):
        return base_url
    return f"{base_url}/chat/completions"


def extract_local_llm_content(payload: dict) -> tuple[str, bool]:
    choices = payload.get("choices") or []
    if not choices:
        raise ValueError("Local LLM response did not include choices.")

    message = choices[0].get("message") or {}
    content = message.get("content", "")
    if isinstance(content, list):
        content = "".join(
            item.get("text", "") if isinstance(item, dict) else str(item)
            for item in content
        )
    has_reasoning = bool(message.get("reasoning") or message.get("reasoning_content"))
    return content.strip() if isinstance(content, str) else "", has_reasoning


def local_llm_models() -> list[str]:
    configured_models = [getattr(settings, "AI_MODEL", "")]
    fallback_models = getattr(settings, "AI_FALLBACK_MODELS", [])
    if isinstance(fallback_models, str):
        fallback_models = fallback_models.split(",")
    configured_models.extend(fallback_models)

    models: list[str] = []
    for model in configured_models:
        model_name = str(model).strip()
        if model_name and model_name not in models:
            models.append(model_name)
    return models


def local_llm_chat(messages: list[dict], *, model: str | None = None) -> str:
    model = (model or getattr(settings, "AI_MODEL", "")).strip()
    if not model:
        raise ValueError("AI_MODEL is required for local LLM providers.")

    headers = {"Content-Type": "application/json"}
    api_key = getattr(settings, "AI_API_KEY", "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    request_payload = {
        "model": model,
        "messages": messages,
        "temperature": getattr(settings, "AI_TEMPERATURE", 0.2),
        "max_tokens": getattr(settings, "AI_MAX_OUTPUT_TOKENS", 1200),
        "stream": False,
    }

    with httpx.Client(
        timeout=getattr(settings, "AI_REQUEST_TIMEOUT", 90),
        headers=headers,
    ) as client:
        response = client.post(local_llm_chat_completion_url(), json=request_payload)
        response.raise_for_status()
        content, has_reasoning = extract_local_llm_content(response.json())
        if content:
            return content

        if has_reasoning and messages:
            retry_messages = [dict(message) for message in messages]
            retry_messages[-1]["content"] = (
                "/no_think\nReturn only the final JSON object. Do not write reasoning.\n"
                f"{retry_messages[-1].get('content', '')}"
            )
            response = client.post(
                local_llm_chat_completion_url(),
                json={**request_payload, "messages": retry_messages},
            )
            response.raise_for_status()
            content, _ = extract_local_llm_content(response.json())
            if content:
                return content

    raise ValueError("Local LLM response did not include message content.")


def parse_local_llm_json(content: str) -> dict:
    cleaned = content.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        return json.loads(cleaned[start : end + 1])


def normalize_local_llm_tags(raw_tags) -> list[str]:
    tags: list[str] = []
    if not isinstance(raw_tags, list):
        return tags
    for raw_tag in raw_tags:
        tag = normalize_ai_text(str(raw_tag)).strip(" #、,")
        if not tag or tag in tags:
            continue
        tags.append(tag[:40])
        if len(tags) == 5:
            break
    return tags


def truncate_ai_output(value: str, *, limit: int) -> str:
    text = normalize_ai_text(value)
    if len(text) <= limit:
        return text
    return f"{text[:limit].rstrip()}..."
