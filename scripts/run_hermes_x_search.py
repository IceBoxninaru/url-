from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


DEFAULT_HERMES_HOME = Path.home() / "AppData" / "Local" / "hermes"
DEFAULT_HERMES_PYTHON = (
    DEFAULT_HERMES_HOME / "hermes-agent" / "venv" / "Scripts" / "python.exe"
)
DEFAULT_OUTPUT_DIR = Path.home() / ".codex" / "automations" / "ai-url" / "x_search"

MLBEAR2_NEWS_SOURCE_QUERY = (
    "@MLBear2 今朝のAIニュース 今夜のAIニュース AI開発ニュース "
    "ChatGPT OpenAI Claude Anthropic Gemini Google DeepMind 公式 発表"
)

MLBEAR2_COMPANY_FOLLOWUP_QUERY = (
    "@MLBear2 AIニュース OpenAI Anthropic Claude ChatGPT Gemini Google DeepMind "
    "Microsoft GitHub xAI Meta Perplexity Cursor Cognition NVIDIA 公式 blog docs release"
)

DEFAULT_QUERIES: list[dict[str, Any]] = [
    {
        "query": MLBEAR2_NEWS_SOURCE_QUERY,
        "allowed_x_handles": ["MLBear2"],
        "purpose": "mlbear2_news_source",
    },
    {
        "query": MLBEAR2_COMPANY_FOLLOWUP_QUERY,
        "allowed_x_handles": ["MLBear2"],
        "purpose": "mlbear2_company_followup",
    },
    {
        "query": "AI coding agent CLI MCP hooks browser automation runnable repo demo release",
        "allowed_x_handles": None,
        "purpose": "agent_tooling",
    },
    {
        "query": "Codex Claude Code Copilot CLI Gemini CLI workflow automation debugging practical",
        "allowed_x_handles": None,
        "purpose": "developer_workflow",
    },
    {
        "query": "local AI agent desktop automation browser control MCP security eval open source",
        "allowed_x_handles": None,
        "purpose": "local_agent_runtime",
    },
    {
        "query": "AI agent developer tool runnable repo demo official release Codex Claude Code MCP",
        "allowed_x_handles": ["MLBear2"],
        "purpose": "mlbear2_agent_tools",
    },
]

FOLLOWED_COMPANIES: dict[str, dict[str, Any]] = {
    "OpenAI": {
        "aliases": ["OpenAI", "ChatGPT", "GPT-", "Codex"],
        "domains": ["openai.com", "developers.openai.com", "help.openai.com"],
        "query": "OpenAI ChatGPT Codex GPT official release notes blog docs announcement",
    },
    "Anthropic": {
        "aliases": ["Anthropic", "Claude", "Claude Code"],
        "domains": ["anthropic.com", "claude.com", "docs.anthropic.com", "platform.claude.com"],
        "query": "Anthropic Claude Claude Code official release notes blog docs announcement",
    },
    "Google / DeepMind": {
        "aliases": ["Google", "Gemini", "DeepMind", "Gemma", "Kaggle"],
        "domains": ["ai.google.dev", "developers.googleblog.com", "blog.google", "deepmind.google"],
        "query": "Google Gemini DeepMind Gemma official release notes blog docs announcement",
    },
    "Microsoft / GitHub": {
        "aliases": ["Microsoft", "GitHub", "Copilot", "Azure"],
        "domains": ["github.blog", "docs.github.com", "microsoft.com", "azure.microsoft.com"],
        "query": "Microsoft GitHub Copilot Azure AI official changelog release notes docs",
    },
    "xAI": {
        "aliases": ["xAI", "Grok"],
        "domains": ["x.ai", "docs.x.ai"],
        "query": "xAI Grok official release notes docs announcement",
    },
    "Meta": {
        "aliases": ["Meta", "Llama"],
        "domains": ["ai.meta.com", "llama.meta.com", "engineering.fb.com"],
        "query": "Meta Llama AI official release notes blog docs announcement",
    },
    "Perplexity": {
        "aliases": ["Perplexity"],
        "domains": ["perplexity.ai", "docs.perplexity.ai"],
        "query": "Perplexity AI official release notes docs announcement",
    },
    "Cursor / Anysphere": {
        "aliases": ["Cursor", "Anysphere"],
        "domains": ["cursor.com", "docs.cursor.com", "anysphere.co"],
        "query": "Cursor Anysphere official changelog release notes docs",
    },
    "Cognition / Devin": {
        "aliases": ["Cognition", "Devin"],
        "domains": ["cognition.ai", "docs.devin.ai"],
        "query": "Cognition Devin official release notes blog docs announcement",
    },
    "NVIDIA": {
        "aliases": ["NVIDIA", "Nvidia", "CUDA", "NeMo"],
        "domains": ["developer.nvidia.com", "blogs.nvidia.com", "docs.nvidia.com"],
        "query": "NVIDIA AI CUDA NeMo official release notes blog docs announcement",
    },
    "Hugging Face": {
        "aliases": ["Hugging Face", "HuggingFace"],
        "domains": ["huggingface.co", "huggingface.co/blog"],
        "query": "Hugging Face AI official model card blog release announcement",
    },
}


def today_jst() -> dt.date:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).date()


def default_from_date(day: dt.date) -> str:
    return (day - dt.timedelta(days=2)).isoformat()


def run_hermes_code(
    *,
    hermes_python: Path,
    hermes_home: Path,
    code: str,
    extra_env: dict[str, str] | None = None,
    timeout: int = 240,
) -> str:
    env = os.environ.copy()
    env["HERMES_HOME"] = str(hermes_home)
    env["PYTHONIOENCODING"] = "utf-8"
    if extra_env:
        env.update(extra_env)
    result = subprocess.run(
        [str(hermes_python), "-c", code],
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Hermes python failed with code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result.stdout.strip()


def check_x_search(hermes_python: Path, hermes_home: Path) -> bool:
    code = (
        "from tools.x_search_tool import check_x_search_requirements\n"
        "print('true' if check_x_search_requirements() else 'false')\n"
    )
    return run_hermes_code(
        hermes_python=hermes_python,
        hermes_home=hermes_home,
        code=code,
        timeout=60,
    ).splitlines()[-1].strip().lower() == "true"


def run_query(
    *,
    hermes_python: Path,
    hermes_home: Path,
    query: str,
    allowed_x_handles: list[str] | None,
    purpose: str,
    from_date: str,
    to_date: str,
) -> dict[str, Any]:
    payload = {
        "query": query,
        "allowed_x_handles": allowed_x_handles,
        "purpose": purpose,
        "from_date": from_date,
        "to_date": to_date,
    }
    code = r"""
import json
import os
from tools.x_search_tool import x_search_tool

payload = json.loads(os.environ["HERMES_X_SEARCH_PAYLOAD"])
raw = x_search_tool(
    query=payload["query"],
    allowed_x_handles=payload.get("allowed_x_handles"),
    from_date=payload.get("from_date", ""),
    to_date=payload.get("to_date", ""),
)
print(raw)
"""
    raw = run_hermes_code(
        hermes_python=hermes_python,
        hermes_home=hermes_home,
        code=code,
        extra_env={"HERMES_X_SEARCH_PAYLOAD": json.dumps(payload, ensure_ascii=False)},
        timeout=300,
    )
    parsed = json.loads(raw)
    parsed["_input"] = payload
    parsed["_source"] = "hermes_x_search"
    return parsed


def extract_urls(result: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for item in result.get("inline_citations") or []:
        url = item.get("url") if isinstance(item, dict) else None
        if url:
            urls.append(str(url))
    for item in result.get("citations") or []:
        url = item.get("url") if isinstance(item, dict) else None
        if url:
            urls.append(str(url))
    answer = str(result.get("answer") or "")
    urls.extend(re.findall(r"https?://[^\s)\]]+", answer))
    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        cleaned = url.rstrip(".,;`'\">)]}")
        if cleaned not in seen:
            seen.add(cleaned)
            deduped.append(cleaned)
    return deduped


def preferred_handle_for_result(result: dict[str, Any]) -> str:
    handles = (result.get("_input") or {}).get("allowed_x_handles") or []
    if len(handles) == 1:
        return str(handles[0]).lstrip("@")
    return ""


def normalize_x_status_url(url: str, *, preferred_handle: str = "") -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower().split("@")[-1].split(":")[0]
    if host not in {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}:
        return ""
    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) >= 3 and path_parts[0] == "i" and path_parts[1] == "status" and path_parts[2].isdigit():
        if preferred_handle:
            return f"https://x.com/{preferred_handle}/status/{path_parts[2]}"
        return f"https://x.com/i/status/{path_parts[2]}"
    if len(path_parts) >= 3 and path_parts[1] == "status" and path_parts[2].isdigit():
        return f"https://x.com/{path_parts[0]}/status/{path_parts[2]}"
    return ""


def extract_x_status_urls(result: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    preferred_handle = preferred_handle_for_result(result)
    for url in extract_urls(result):
        normalized = normalize_x_status_url(url, preferred_handle=preferred_handle)
        if normalized:
            urls.append(normalized)
    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def extract_company_followups(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    haystack = "\n".join(
        [
            str(result.get("answer") or "")
            + "\n"
            + str((result.get("_input") or {}).get("query") or "")
            for result in results
        ]
    )
    followups: list[dict[str, Any]] = []
    for company, config in FOLLOWED_COMPANIES.items():
        aliases = [str(alias) for alias in config.get("aliases", [])]
        if not any(alias in haystack for alias in aliases):
            continue
        domains = [str(domain) for domain in config.get("domains", [])]
        domain_filter = " OR ".join(f"site:{domain}" for domain in domains)
        followups.append(
            {
                "company": company,
                "matched_aliases": [alias for alias in aliases if alias in haystack],
                "preferred_domains": domains,
                "search_query": f"({domain_filter}) {config['query']}".strip(),
            }
        )
    return followups


def x_handle_from_status_url(url: str) -> str:
    path_parts = [part for part in urlparse(url).path.split("/") if part]
    if len(path_parts) >= 3 and path_parts[1] == "status":
        return path_parts[0]
    return "i"


def title_for_x_seed(url: str, result: dict[str, Any]) -> str:
    input_payload = result.get("_input") or {}
    purpose = str(input_payload.get("purpose") or "")
    handle = x_handle_from_status_url(url)
    if handle.lower() == "mlbear2":
        if purpose == "mlbear2_news_source":
            return "@MLBear2の朝/夜AIニュース投稿"
        if purpose == "mlbear2_company_followup":
            return "@MLBear2のAI会社ニュース投稿"
        return "@MLBear2のAIニュース関連投稿"
    if handle == "i":
        return "Xで見つかったAIニュース投稿"
    return f"@{handle}のAIニュース関連投稿"


def build_x_seed_items(results: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    seed_items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for result in results:
        input_payload = result.get("_input") or {}
        query = str(input_payload.get("query") or "")
        purpose = str(input_payload.get("purpose") or "")
        for url in extract_x_status_urls(result):
            if url in seen:
                continue
            seen.add(url)
            handle = x_handle_from_status_url(url)
            tags = ["AI", "X", "AIニュース"]
            if handle.lower() == "mlbear2":
                tags.append("MLBear2")
            if purpose.startswith("mlbear2"):
                note = (
                    "保存理由: HermesAgent x_searchで記事探索前の一次シードとして確認。"
                    "@MLBear2の朝/夜AIニュースの内容・方向性から、よく拾われる会社の"
                    "公式発表、ドキュメント、リリースノートへ展開するため。"
                    "発見元: HermesAgent x_search / 保存対象: X投稿。"
                )
            else:
                note = (
                    "保存理由: HermesAgent x_searchでAI開発ニュースの一次シードとして確認。"
                    "この投稿から公式記事、GitHub、ドキュメントなどの保存対象へ展開するため。"
                    "発見元: HermesAgent x_search / 保存対象: X投稿。"
                )
            seed_items.append(
                {
                    "url": url,
                    "title": title_for_x_seed(url, result),
                    "note": note,
                    "source": "HermesAgent x_search",
                    "search_query": query,
                    "tags": tags,
                    "save_reason": "AI検索",
                    "capture_images": True,
                    "capture_videos": False,
                }
            )
            if len(seed_items) >= limit:
                return seed_items
    return seed_items


def write_outputs(
    *,
    output_dir: Path,
    run_date: str,
    from_date: str,
    to_date: str,
    results: list[dict[str, Any]],
    seed_item_limit: int,
) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"x_search_{run_date}.json"
    md_path = output_dir / f"x_search_{run_date}.md"
    seed_items_path = output_dir / f"x_seed_items_{run_date}.json"
    company_followups = extract_company_followups(results)
    seed_items = build_x_seed_items(results, limit=seed_item_limit)
    payload = {
        "run_date": run_date,
        "from_date": from_date,
        "to_date": to_date,
        "workflow": [
            "Run HermesAgent x_search first.",
            "Save x_seed_items as AI-search URLs so X source posts stay visible.",
            "Use company_followup_targets to find official articles, docs, changelogs, and primary sources.",
        ],
        "mlbear2_direction": {
            "format": "Morning/evening AI development news roundup with short topic clusters.",
            "priority": [
                "OpenAI / ChatGPT / Codex",
                "Anthropic / Claude / Claude Code",
                "Google / Gemini / DeepMind",
                "Microsoft / GitHub Copilot",
                "AI agent tooling, MCP, browser/computer-use, security, and developer workflows",
            ],
            "source_policy": "Keep the X post URL as the discovery seed, then save official or near-official article URLs.",
        },
        "company_followup_targets": company_followups,
        "x_seed_items_path": str(seed_items_path),
        "x_seed_item_count": len(seed_items),
        "results": results,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    seed_items_path.write_text(
        json.dumps({"run_date": run_date, "items": seed_items}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    lines = [
        f"# Hermes x_search handoff for {run_date}",
        "",
        f"- date_range: {from_date} to {to_date}",
        f"- result_count: {len(results)}",
        f"- x_seed_item_count: {len(seed_items)}",
        f"- x_seed_items: {seed_items_path}",
        "",
        "## Workflow",
        "",
        "1. Save the X source posts from `x_seed_items` into AI検索URL first.",
        "2. Treat @MLBear2 morning/evening news posts as discovery seeds, not as final article substitutes.",
        "3. Follow the company targets below to collect official articles, docs, changelogs, release notes, GitHub repos, and papers.",
        "",
        "## @MLBear2 direction to mirror",
        "",
        "- Morning/evening AI development news roundup.",
        "- Main lanes: OpenAI/ChatGPT/Codex, Anthropic/Claude/Claude Code, Google/Gemini/DeepMind.",
        "- Secondary lanes: Microsoft/GitHub Copilot, xAI/Grok, Meta/Llama, Perplexity, Cursor/Anysphere, Cognition/Devin, NVIDIA, Hugging Face.",
        "- Prefer practical implications: model limits, pricing, developer tools, agents, MCP, browser/computer-use, security, official docs, and runnable repos.",
        "",
        "## Company follow-up targets",
        "",
    ]
    for target in company_followups:
        lines.extend(
            [
                f"### {target['company']}",
                "",
                f"- matched_aliases: {', '.join(target['matched_aliases'])}",
                f"- preferred_domains: {', '.join(target['preferred_domains'])}",
                f"- search_query: {target['search_query']}",
                "",
            ]
        )
    for index, result in enumerate(results, start=1):
        input_payload = result.get("_input") or {}
        query = input_payload.get("query", "")
        handles = input_payload.get("allowed_x_handles") or []
        x_source_urls = extract_x_status_urls(result)
        lines.extend(
            [
                f"## {index}. {query}",
                "",
                f"- success: {result.get('success')}",
                f"- degraded: {result.get('degraded')}",
                f"- purpose: {input_payload.get('purpose', '-')}",
                f"- allowed_x_handles: {', '.join(handles) if handles else '-'}",
                "",
                "### x_source_urls",
            ]
        )
        for url in x_source_urls[:40]:
            lines.append(f"- {url}")
        lines.extend(
            [
                "",
                "### candidate_urls",
            ]
        )
        for url in extract_urls(result)[:40]:
            lines.append(f"- {url}")
        lines.extend(["", "### answer_excerpt", ""])
        answer = str(result.get("answer") or "")
        lines.append(answer[:4000])
        lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path, seed_items_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Hermes x_search and save a Codex handoff file.")
    parser.add_argument("--date", default=today_jst().isoformat())
    parser.add_argument("--from-date", default="")
    parser.add_argument("--to-date", default="")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--hermes-home", type=Path, default=DEFAULT_HERMES_HOME)
    parser.add_argument("--hermes-python", type=Path, default=DEFAULT_HERMES_PYTHON)
    parser.add_argument("--queries-json", type=Path, default=None)
    parser.add_argument("--seed-item-limit", type=int, default=12)
    args = parser.parse_args()

    run_date = dt.date.fromisoformat(args.date)
    from_date = args.from_date or default_from_date(run_date)
    to_date = args.to_date or args.date

    if not args.hermes_python.exists():
        print(f"missing hermes python: {args.hermes_python}", file=sys.stderr)
        return 2
    if not check_x_search(args.hermes_python, args.hermes_home):
        print("Hermes x_search requirements are not satisfied.", file=sys.stderr)
        return 3

    queries = DEFAULT_QUERIES
    if args.queries_json:
        queries = json.loads(args.queries_json.read_text(encoding="utf-8"))

    results = []
    for query_def in queries:
        results.append(
            run_query(
                hermes_python=args.hermes_python,
                hermes_home=args.hermes_home,
                query=str(query_def["query"]),
                allowed_x_handles=query_def.get("allowed_x_handles"),
                purpose=str(query_def.get("purpose") or "general"),
                from_date=from_date,
                to_date=to_date,
            )
        )

    json_path, md_path, seed_items_path = write_outputs(
        output_dir=args.output_dir,
        run_date=args.date,
        from_date=from_date,
        to_date=to_date,
        results=results,
        seed_item_limit=args.seed_item_limit,
    )
    print(
        json.dumps(
            {"json": str(json_path), "markdown": str(md_path), "seed_items": str(seed_items_path)},
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
