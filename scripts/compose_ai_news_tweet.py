from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_DIR = Path.home() / ".codex" / "automations" / "ai-url" / "tweet_drafts"
DEFAULT_API_BASE = "http://127.0.0.1:8000"
LONG_POST_CHAR_LIMIT = 25_000
PREMIUM_POST_CHAR_LIMIT = 1_200

MORNING_PREFIX = "\u4eca\u671d\u306eAI\u958b\u767a\u30cb\u30e5\u30fc\u30b9\u3002"
EVENING_PREFIX = "\u4eca\u591c\u306eAI\u958b\u767a\u30cb\u30e5\u30fc\u30b9\u3002"
GENERIC_PREFIX = "\u4eca\u65e5\u306eAI\u958b\u767a\u30cb\u30e5\u30fc\u30b9\u3002"
FOCUS = (
    "MCP/\u30a8\u30fc\u30b8\u30a7\u30f3\u30c8\u5468\u8fba\u3067\u516c\u5f0f\u30c4\u30fc\u30eb\u5316\u3068"
    "\u5b89\u5168\u904b\u7528\u306e\u66f4\u65b0\u304c\u7d9a\u304f\u3002"
)
TAIL = (
    "\u958b\u767a\u8005\u5411\u3051\u306b\u306f\u3001\u30c7\u30d0\u30c3\u30b0\u57fa\u76e4\u3001"
    "\u30c4\u30fc\u30eb\u6a29\u9650\u3001\u5b9f\u88c5\u624b\u9806\u304c\u7126\u70b9\u3002"
)
PRIORITY_UPDATE_KEYWORDS = (
    "ChatGPT",
    "OpenAI",
    "GPT-",
    "Claude",
    "Anthropic",
    "Gemini",
    "Google AI",
    "DeepMind",
    "ai.google.dev",
    "openai.com",
    "anthropic.com",
)


def today_jst() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).date().isoformat()


def load_today_items(api_base: str, run_date: str, limit: int) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode({"date": run_date, "limit": limit})
    url = f"{api_base.rstrip('/')}/api/resources/ai-search/?{params}"
    with urllib.request.urlopen(url, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return list(payload.get("items") or [])


def compact_title(title: str) -> str:
    title = re.sub(r"\s+", " ", title).strip()
    title = re.sub(r"\u3067\u304d\u308b$", "", title)
    title = re.sub(r"\u5b66\u3079\u308b$", "", title)
    return title[:34]


def resolve_edition(edition: str) -> str:
    if edition != "auto":
        return edition
    hour = dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).hour
    return "morning" if hour < 12 else "evening"


def prefix_for_edition(edition: str) -> str:
    if edition == "morning":
        return MORNING_PREFIX
    if edition == "evening":
        return EVENING_PREFIX
    return GENERIC_PREFIX


def digest_title(run_date: str, edition: str) -> str:
    parsed = dt.date.fromisoformat(run_date)
    label = "\u671d" if edition == "morning" else "\u591c" if edition == "evening" else "\u4eca\u65e5"
    return f"\u3010{parsed.month}\u6708{parsed.day}\u65e5 {label}\u306e\u30cb\u30e5\u30fc\u30b9\u307e\u3068\u3081\u3011"


def clean_summary(item: dict[str, Any]) -> str:
    summary = str(item.get("summary") or item.get("translation") or "").strip()
    summary = re.sub(r"\s+", " ", summary)
    return summary


def headline_from_item(item: dict[str, Any]) -> str:
    return compact_title(str(item.get("title") or ""))


def item_search_text(item: dict[str, Any]) -> str:
    return " ".join(
        str(item.get(key) or "")
        for key in ("title", "domain", "url", "summary", "translation")
    )


def priority_update_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    matched: list[dict[str, Any]] = []
    for item in items:
        text = item_search_text(item)
        if any(keyword in text for keyword in PRIORITY_UPDATE_KEYWORDS):
            matched.append(item)
    return matched


def unique_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[Any] = set()
    for item in items:
        key = item.get("resource_id") or item.get("url") or item.get("title")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def first_sentence(summary: str, max_chars: int = 150) -> str:
    summary = clean_summary({"summary": summary})
    if not summary:
        return ""
    sentence = re.split(r"(?<=[。.!?])\s*", summary)[0].strip()
    if len(sentence) <= max_chars:
        return sentence
    return sentence[: max_chars - 1].rstrip("、。,. ") + "。"


def infer_axis(items: list[dict[str, Any]]) -> str:
    searchable = "\n".join(item_search_text(item) for item in items)
    if any(term in searchable for term in ("secret", "秘密", "Scan", "脆弱", "隔離", "sandbox", "Sandboxes", "MCP設定")):
        return "AIエージェントを強くする前に、実行環境と権限をどう閉じるか"
    if any(term in searchable for term in ("Chrome", "browser", "ブラウザ", "CDP")):
        return "ブラウザ操作エージェントを、ログイン済み環境やDevTools連携でどう実用化するか"
    if any(term in searchable for term in ("ChatGPT", "OpenAI", "Claude", "Anthropic", "Gemini", "Google")):
        return "主要AIモデルの更新を、API・ツール連携・運用変更としてどう取り込むか"
    return "AI開発環境に今日から入れられる実装材料をどう選ぶか"


def build_premium_tweet(
    items: list[dict[str, Any]],
    *,
    edition: str = "auto",
    max_chars: int = PREMIUM_POST_CHAR_LIMIT,
) -> str:
    resolved_edition = resolve_edition(edition)
    ordered = unique_items(priority_update_items(items) + items)
    selected = [item for item in ordered if headline_from_item(item)][:5]
    if not selected:
        return build_short_tweet(items, edition=edition)

    paragraphs = [
        prefix_for_edition(resolved_edition),
        f"今日の軸は、{infer_axis(selected)}です。",
    ]
    for item in selected[:4]:
        headline = headline_from_item(item)
        summary = first_sentence(clean_summary(item), 150)
        if summary:
            paragraphs.append(f"{headline}は、{summary}")
        else:
            paragraphs.append(f"{headline}は、公開一次ソースから実装や運用への影響を確認したい動きです。")
    paragraphs.append(
        "見るべき点はモデル性能だけでなく、どの権限で、どこまで実行させ、"
        "漏えいや侵入をどう検知するかです。"
    )
    text = "\n\n".join(paragraphs).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def build_digest(items: list[dict[str, Any]], *, run_date: str, edition: str = "auto", max_chars: int = LONG_POST_CHAR_LIMIT) -> str:
    resolved_edition = resolve_edition(edition)
    headlines = [headline_from_item(item) for item in items if headline_from_item(item)]
    lines: list[str] = [digest_title(run_date, resolved_edition)]
    lines.extend(f"\u30fb{headline}" for headline in headlines[:10])
    for item in items[:10]:
        headline = headline_from_item(item)
        if not headline:
            continue
        summary = clean_summary(item)
        lines.extend(["", "===", "", headline, ""])
        if summary:
            sentences = [segment.strip() for segment in re.split(r"(?<=[\u3002.!?])\s*", summary) if segment.strip()]
            for sentence in sentences[:3]:
                lines.append(f"\u30fb{sentence}")
        else:
            lines.append("\u30fb\u516c\u958b\u3055\u308c\u305f\u4e00\u6b21\u30bd\u30fc\u30b9\u304b\u3089\u3001AI\u30a8\u30fc\u30b8\u30a7\u30f3\u30c8\u3084\u958b\u767a\u8005\u30c4\u30fc\u30eb\u306e\u5b9f\u7528\u9762\u3067\u78ba\u8a8d\u3057\u305f\u3044\u52d5\u304d\u3067\u3059\u3002")
        lines.append("\u30fb\u958b\u767a\u8005\u5411\u3051\u306b\u306f\u3001\u5b9f\u88c5\u30d1\u30bf\u30fc\u30f3\u3001\u904b\u7528\u30ac\u30fc\u30c9\u30ec\u30fc\u30eb\u3001\u30c4\u30fc\u30eb\u9023\u643a\u306e\u8a2d\u8a08\u6750\u6599\u3068\u3057\u3066\u8aad\u3080\u4fa1\u5024\u304c\u3042\u308a\u307e\u3059\u3002")
    text = "\n".join(lines).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "\u2026"


def build_short_tweet(items: list[dict[str, Any]], *, edition: str = "auto") -> str:
    resolved_edition = resolve_edition(edition)
    priority_items = priority_update_items(items)
    if priority_items:
        titles = [headline_from_item(item) for item in priority_items[:3]]
        titles = [title for title in titles if title]
        if titles:
            text = (
                prefix_for_edition(resolved_edition)
                + "\u3001".join(titles[:2])
                + "\u304c\u4e3b\u306a\u52d5\u304d\u3002"
                + "ChatGPT\u3001Claude\u3001Gemini\u7cfb\u306e\u66f4\u65b0\u306f\u3001"
                + "\u516c\u5f0f\u30bd\u30fc\u30b9\u304b\u3089API\u30fb\u30c4\u30fc\u30eb\u9023\u643a\u30fb"
                + "\u904b\u7528\u5909\u66f4\u306e\u5f71\u97ff\u3092\u898b\u308b\u6d41\u308c\u3067\u3059\u3002"
            )
            if len(text) <= 280:
                return text
            shorter = (
                prefix_for_edition(resolved_edition)
                + "\u3001".join(titles[:2])
                + "\u304c\u4e3b\u306a\u52d5\u304d\u3002"
                + "\u516c\u5f0f\u30bd\u30fc\u30b9\u304b\u3089\u5b9f\u88c5\u30fb\u904b\u7528\u3078\u306e"
                + "\u5f71\u97ff\u3092\u898b\u308b\u66f4\u65b0\u3067\u3059\u3002"
            )
            if len(shorter) <= 280:
                return shorter
            return shorter[:279]

    searchable = "\n".join(
        [
            str(item.get("title") or "")
            + " "
            + str(item.get("domain") or "")
            + " "
            + str(item.get("url") or "")
            for item in items
        ]
    )
    sentences: list[str] = []
    if "\u30af\u30ed\u30fc\u30e0" in searchable or "Chrome" in searchable or "DevTools" in searchable:
        sentences.append("Chrome DevTools\u3084Claude Code\u3067\u3001\u30a8\u30fc\u30b8\u30a7\u30f3\u30c8\u5b9f\u884c\u306e\u53ef\u8996\u5316\u3068\u30c7\u30d0\u30c3\u30b0\u4f53\u9a13\u304c\u524d\u9032\u3002")
    if "MCP" in searchable or "Agent Skills" in searchable:
        sentences.append("MCP/Agent Skills\u306fConfluent\u3001MongoDB\u3001Twilio\u3001Salesforce\u307e\u3067\u5e83\u304c\u308a\u3001\u696d\u52d9\u30c7\u30fc\u30bf\u3084API\u3092AI\u306b\u958b\u304f\u6d41\u308c\u304c\u52a0\u901f\u3057\u3066\u3044\u307e\u3059\u3002")
    if "\u30bb\u30ad\u30e5\u30ea\u30c6\u30a3" in searchable or "Shield" in searchable:
        sentences.append("\u540c\u6642\u306bMCP-Shield\u3084ShieldNet\u306a\u3069\u3001\u5b89\u5168\u904b\u7528\u3082\u5927\u304d\u306a\u8ad6\u70b9\u306b\u3002")
    if not sentences:
        titles = [compact_title(str(item.get("title") or "")) for item in items[:2]]
        titles = [title for title in titles if title]
        if titles:
            sentences.append("\u3001".join(titles) + "\u304c\u4e3b\u306a\u52d5\u304d\u3002")
        sentences.append(FOCUS)
    text = prefix_for_edition(resolved_edition) + "".join(sentences)
    if len(text) <= 280:
        return text
    shorter = f"{prefix_for_edition(resolved_edition)}{FOCUS}{TAIL}"
    if len(shorter) <= 280:
        return shorter
    return shorter[:279]


def build_tweet(
    items: list[dict[str, Any]],
    *,
    run_date: str,
    edition: str = "auto",
    style: str = "premium",
    max_chars: int = LONG_POST_CHAR_LIMIT,
) -> str:
    if style == "premium":
        return build_premium_tweet(items, edition=edition, max_chars=max_chars)
    if style == "short":
        return build_short_tweet(items, edition=edition)
    return build_digest(items, run_date=run_date, edition=edition, max_chars=max_chars)


def write_outputs(
    output_dir: Path,
    run_date: str,
    tweet: str,
    items: list[dict[str, Any]],
    *,
    edition: str,
    style: str,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    txt_path = output_dir / f"tweet_{run_date}.txt"
    json_path = output_dir / f"tweet_{run_date}.json"
    txt_path.write_text(tweet, encoding="utf-8")
    payload = {
        "run_date": run_date,
        "edition": edition,
        "style": style,
        "tweet": tweet,
        "char_count": len(tweet),
        "resource_ids": [item.get("resource_id") for item in items],
        "source_titles": [item.get("title") for item in items],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return txt_path, json_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Compose a news-style X post from today's AI-search resources.")
    parser.add_argument("--date", default=today_jst())
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--limit", type=int, default=15)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--edition", choices=["auto", "morning", "evening"], default="auto")
    parser.add_argument("--style", choices=["premium", "digest", "short"], default="premium")
    parser.add_argument("--max-chars", type=int, default=PREMIUM_POST_CHAR_LIMIT)
    args = parser.parse_args()

    items = load_today_items(args.api_base, args.date, args.limit)
    edition = resolve_edition(args.edition)
    tweet = build_tweet(items, run_date=args.date, edition=edition, style=args.style, max_chars=args.max_chars)
    txt_path, json_path = write_outputs(args.output_dir, args.date, tweet, items, edition=edition, style=args.style)
    print(json.dumps({"tweet": tweet, "chars": len(tweet), "text": str(txt_path), "json": str(json_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
