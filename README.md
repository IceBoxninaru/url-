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

## Docker Compose
```powershell
docker compose up --build
```

Docker では `web` はコンテナ内で `0.0.0.0:8000` で待ち受け、ホスト側は `127.0.0.1:8000` にのみ公開されます。ワーカーは `capture_jobs` を継続処理します。
