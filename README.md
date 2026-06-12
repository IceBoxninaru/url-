# URLアーカイブ管理アプリ

## セットアップ
1. `.env.example` を `.env` にコピー
2. `python -m venv .venv`
3. `.venv\Scripts\python -m pip install -r requirements.txt`
4. `.venv\Scripts\python app\manage.py migrate`
5. `.venv\Scripts\python app\manage.py runserver 127.0.0.1:8000`

別プロセスでワーカーを動かす:

```powershell
.venv\Scripts\python app\manage.py runworker
```

## X のログイン状態
X の取得でログイン済み状態を使いたい場合は、`storage/auth/x.json` に Playwright の `storage_state` を置きます。
アプリは X / Twitter のURL取得時だけこのファイルを読みます。

`storage/auth/x_profile/` に Playwright / Chromium のプロファイルを置いた場合も、`x.json` が無いときの代替として使います。

設定値:

```powershell
CAPTURE_X_STORAGE_STATE_PATH=storage/auth/x.json
CAPTURE_X_PROFILE_PATH=storage/auth/x_profile
```

## Capture verification

Run a non-persistent capture check for real URLs without saving a `Resource` or `Snapshot`.
Use `--method playwright` for X / Instagram checks, and `--include-details` when you need
candidate URLs, attempts, skips, and observed media requests in the JSON output.

```powershell
.venv\Scripts\python app\manage.py verify_capture_url https://x.com/example/status/1 --method playwright --include-details --require-video
.venv\Scripts\python app\manage.py verify_capture_url https://www.instagram.com/reel/example/ --method playwright --include-details --require-video
```

## Docker Compose
```powershell
docker compose up --build
```

Docker では `web` はコンテナ内で `0.0.0.0:8000` で待ち受け、ホスト側は `127.0.0.1:8000` にのみ公開されます。ワーカーは `capture_jobs` を継続処理します。

## OpenClaw / MCP 連携

Django サーバを起動した状態で、OpenClaw 側の mcporter に stdio MCP サーバを登録します。

```bash
mcporter config add url-archive --stdio 'python3 /mnt/f/AI/rul管理/url-/scripts/mcp_server.py'
mcporter list
```

AI検索URL専用ツール:

- `list_ai_search_urls`
- `list_search_only_urls`

どちらも `date` / `from` / `to` / `limit` / `query` を受け取ります。未指定時は `created_at >= 今日(JST)` かつ `search_only=true` のURLを返します。

## AI検索URL ニュース収集

AI開発ニュースを追加するときは、通常のWeb検索より先に HermesAgent の X 検索を走らせます。

```powershell
python scripts\run_hermes_x_search.py --date 2026-06-12
```

出力先:

- `C:\Users\carpo\.codex\automations\ai-url\x_search\x_search_<date>.md`
- `C:\Users\carpo\.codex\automations\ai-url\x_search\x_search_<date>.json`
- `C:\Users\carpo\.codex\automations\ai-url\x_search\x_seed_items_<date>.json`

`x_seed_items_<date>.json` は AI検索URL に入れる X 発見元候補です。@MLBear2 の朝・夜AIニュース投稿を2〜4件は先に保存し、その後で OpenAI / Anthropic / Google DeepMind / Microsoft GitHub など、@MLBear2 がよく拾う会社の公式発表、リリースノート、API/SDKドキュメント、GitHubリポジトリを探して保存します。

X投稿は発見元として残し、記事本文や最終判断は公式・一次ソースに寄せます。
