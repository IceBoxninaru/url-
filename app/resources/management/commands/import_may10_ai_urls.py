from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Q

from jobs.models import CaptureJob, JobStatus, JobType
from resources.models import Resource
from resources.services import delete_resource_with_artifacts, enqueue_capture_job, normalize_url
from tags.models import Tag


MAX_URLS_PER_DAY = 15


@dataclass(frozen=True)
class AiSearchUrl:
    url: str
    title: str
    tags: tuple[str, ...]
    note: str


# 2026-05-11にChromeで開いて、404/削除表示/汎用AIチャット画面ではないことを確認済み。
# 選定基準: 1日最大15件、説明しているX投稿・記事・動画を優先し、公式ページは補助に絞る。
MAY10_AI_URLS: tuple[AiSearchUrl, ...] = (
    AiSearchUrl(
        "https://x.com/OpenAI/status/2052480800004956323",
        "CodexがChrome作業を代行",
        ("AI", "OpenAI", "X"),
        "5月10日分。CodexがChrome内の反復作業や複数タブ作業を扱えるようになったことが分かるX投稿。",
    ),
    AiSearchUrl(
        "https://developers.openai.com/codex/app/chrome-extension",
        "Codex Chrome拡張の使い方",
        ("AI", "OpenAI", "公式"),
        "5月10日分。Codex Chrome拡張の設定と使い方を確認するための公式ドキュメント。",
    ),
    AiSearchUrl(
        "https://x.com/paji_a/status/2053456459757977987",
        "ElevenLabs進化を拾う投稿",
        ("AI", "ElevenLabs", "X"),
        "5月10日分。ElevenLabsの動画生成統合に関する話題を追う入口になっているX投稿。",
    ),
    AiSearchUrl(
        "https://x.com/MakeAI_CEO/status/2052741973350769102",
        "ElevenLabsで動画制作を統合",
        ("AI", "ElevenLabs", "X", "ソース"),
        "5月10日分。画像、動画、音声、音楽のツール契約をElevenLabs中心にまとめられる可能性を説明しているX記事。",
    ),
    AiSearchUrl(
        "https://elevenlabs.io/",
        "ElevenLabsで音声と動画生成",
        ("AI", "ElevenLabs", "公式"),
        "5月10日分。ElevenLabsの音声生成、エージェント、動画関連機能を確認する公式ページ。",
    ),
    AiSearchUrl(
        "https://x.com/ClaudeCode_UT/status/2052893838537355524",
        "Claude Codeで資料を参照",
        ("AI", "Claude Code", "NotebookLM", "X"),
        "5月10日分。NotebookLMに置いた資料をClaude CodeからMCP経由で参照する流れを説明しているX投稿。",
    ),
    AiSearchUrl(
        "https://x.com/saidstetic/status/2052469867173781817",
        "NotebookLM MCP連携の手順",
        ("AI", "NotebookLM", "X", "ソース"),
        "5月10日分。Claude CodeとNotebookLMをMCPでつなぐチュートリアル元のX投稿。",
    ),
    AiSearchUrl(
        "https://code.claude.com/docs/en/overview",
        "Claude Codeの基本を確認",
        ("AI", "Claude Code", "公式"),
        "5月10日分。Claude Codeで何ができるかを確認する公式ドキュメント。",
    ),
    AiSearchUrl(
        "https://modelcontextprotocol.io/docs/getting-started/intro",
        "MCP連携の仕組みを確認",
        ("AI", "MCP", "公式"),
        "5月10日分。AIツールと外部データやツールをつなぐMCPの考え方を確認する公式ドキュメント。",
    ),
    AiSearchUrl(
        "https://x.com/kis/status/2052949475816341831",
        "AI出力をHTMLで読みやすく",
        ("AI", "HTML", "X"),
        "5月10日分。AIにMarkdownではなくHTMLで設計や図を出させると確認しやすい、という話題のX投稿。",
    ),
    AiSearchUrl(
        "https://nowokay.hatenablog.com/entry/2026/05/09/164006",
        "HTML出力が便利な理由",
        ("AI", "HTML", "ソース"),
        "5月10日分。AIが書く確認用ドキュメントはMarkdownよりHTMLが読みやすい理由を説明する記事。",
    ),
    AiSearchUrl(
        "https://x.com/trq212/status/2052811606032269638",
        "Claude CodeでHTML文書化",
        ("AI", "HTML", "X", "ソース"),
        "5月10日分。Claude CodeにHTMLを生成させると調査、設計、レビュー資料が読みやすくなるという元投稿。",
    ),
    AiSearchUrl(
        "https://thariqs.github.io/html-effectiveness/",
        "HTML成果物の実例集",
        ("AI", "HTML", "ソース"),
        "5月10日分。AIが生成したHTML資料の具体例を見られる実例集。",
    ),
    AiSearchUrl(
        "https://x.com/jetdaizu/status/2053312756011749616",
        "Huxeで自分専用AIラジオ",
        ("AI", "Huxe", "X"),
        "5月10日分。メール、カレンダー、ニュースから自分専用AIラジオを作るHuxeの概要が分かるX投稿。",
    ),
    AiSearchUrl(
        "https://www.youtube.com/watch?v=MsH5q_gA07w",
        "Huxeの使い方を動画で確認",
        ("AI", "Huxe", "ソース"),
        "5月10日分。Huxeの使い方と便利になる点を確認できる紹介動画。",
    ),
)


PREVIOUS_MAY10_URLS: tuple[str, ...] = (
    "https://x.com/OpenAI/status/2052480800004956323",
    "https://developers.openai.com/codex/app/chrome-extension",
    "https://x.com/paji_a/status/2053456459757977987",
    "https://x.com/MakeAI_CEO/status/2052741973350769102",
    "https://elevenlabs.io/",
    "https://x.com/ClaudeCode_UT/status/2052893838537355524",
    "https://x.com/saidstetic/status/2052469867173781817",
    "https://x.com/oscarmartin/status/2044469339554795728",
    "https://github.com/jacob-bd/notebooklm-mcp-cli",
    "https://github.com/microsoft/markitdown",
    "https://notebooklm.google.com/",
    "https://code.claude.com/docs/en/overview",
    "https://claude.com/product/claude-code",
    "https://modelcontextprotocol.io/docs/getting-started/intro",
    "https://macos-use.dev/",
    "https://x.com/kis/status/2052949475816341831",
    "https://nowokay.hatenablog.com/entry/2026/05/09/164006",
    "https://x.com/trq212/status/2052811606032269638",
    "https://mermaid.js.org/",
    "https://developer.mozilla.org/en-US/docs/Web/HTML",
    "https://www.youtube.com/watch?v=MsH5q_gA07w",
    "https://deepmind.google/models/veo/",
    "https://grok.com/imagine",
    "https://openai.com/index/gpt-image-1/",
)


class Command(BaseCommand):
    help = "Import the curated 2026-05-10 AI-discovered URLs as search-only resources."

    def add_arguments(self, parser):
        parser.add_argument(
            "--no-jobs",
            action="store_true",
            help="Do not enqueue capture jobs for newly imported URLs.",
        )
        parser.add_argument(
            "--prune",
            action="store_true",
            help="Remove earlier May 10 imports that are no longer in the curated max-15 list.",
        )

    @transaction.atomic
    def handle(self, *args, **options):
        if len(MAY10_AI_URLS) > MAX_URLS_PER_DAY:
            raise CommandError(f"May 10 import has {len(MAY10_AI_URLS)} URLs; max is {MAX_URLS_PER_DAY}.")

        created = 0
        updated = 0
        pruned = 0
        jobs_created = 0
        selected_normalized_urls = {normalize_url(item.url) for item in MAY10_AI_URLS}

        if options["prune"]:
            stale_normalized_urls = {
                normalize_url(url)
                for url in PREVIOUS_MAY10_URLS
                if normalize_url(url) not in selected_normalized_urls
            }
            stale_original_urls = {
                url
                for url in PREVIOUS_MAY10_URLS
                if normalize_url(url) not in selected_normalized_urls
            }
            stale_resources = Resource.objects.filter(search_only=True).filter(
                Q(normalized_url__in=stale_normalized_urls) | Q(original_url__in=stale_original_urls)
            )
            for resource in stale_resources:
                delete_resource_with_artifacts(resource)
                pruned += 1

        for item in MAY10_AI_URLS:
            normalized_url = normalize_url(item.url)
            resource, was_created = Resource.objects.get_or_create(
                normalized_url=normalized_url,
                defaults={
                    "original_url": item.url,
                    "domain": urlparse(normalized_url).netloc,
                    "title_manual": item.title,
                    "note": item.note,
                    "search_only": True,
                    "capture_images": True,
                    "capture_videos": False,
                },
            )

            if was_created:
                created += 1
            else:
                changed_fields: list[str] = []
                updates = {
                    "original_url": item.url,
                    "domain": urlparse(normalized_url).netloc,
                    "title_manual": item.title,
                    "note": item.note,
                    "search_only": True,
                    "capture_images": True,
                    "capture_videos": False,
                }
                for field, value in updates.items():
                    if getattr(resource, field) != value:
                        setattr(resource, field, value)
                        changed_fields.append(field)
                if changed_fields:
                    resource.save(update_fields=[*changed_fields, "updated_at"])
                    updated += 1

            tag_objects = []
            for name in item.tags:
                tag, _ = Tag.objects.get_or_create(name=name)
                tag_objects.append(tag)
            resource.tags.set(tag_objects)

            if not options["no_jobs"] and was_created:
                has_pending_capture = CaptureJob.objects.filter(
                    resource=resource,
                    job_type=JobType.CAPTURE,
                    status__in=[JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.RETRY_WAIT],
                ).exists()
                if not has_pending_capture:
                    enqueue_capture_job(resource)
                    jobs_created += 1

        self.stdout.write(
            self.style.SUCCESS(
                "Imported May 10 AI URLs: "
                f"created={created}, updated={updated}, pruned={pruned}, capture_jobs={jobs_created}"
            )
        )
