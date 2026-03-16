# garoon-icloud-sync

Garoon の予定を取得し、iCloud を含む CalDAV カレンダーへ同期するツールです。現在は「1人運用でも壊さず、複数 profile でも state を混線させない」ことを重視した実行基盤になっています。

## この版の安全方針

- `sync_state.json` は profile 間で共有しません
- `dry-run` と本番実行はログと run summary で明確に区別します
- 通常運用と backfill を混同しにくいよう、広い fetch window には warning を残します
- delete が 1 件以上ある run は warning を残します
- state 内の profile と実行 profile が不一致なら fail-fast します
- 既存の削除判定ロジックは安全側のまま維持します

## 推奨ディレクトリ構成

新規運用では、profile ごとに `.env` と runtime を分ける構成を推奨します。

```text
runtime/
  profiles/
    tomoya/
      .env
      data/
        events.json
        calendar.ics
        sync_plan.json
        caldav_sync_result.json
        sync_state.json
        run_summary.json
        diagnostics/
          run_summaries/
      logs/
        garoon-icloud-sync.log
```

`run_summary.json` の最新は profile ごとの `data/run_summary.json` に保存されます。履歴は `data/diagnostics/run_summaries/` に timestamp 付きで残ります。

## 後方互換

- `PROFILE_NAME` 未指定時は `default` を使います
- `APP_DATA_DIR` などの相対パスは、実行時の working directory を基準に解決されます
- `APP_DATA_DIR` 未指定時は、従来どおり `./data` を使います
- リポジトリ直下の `.env` をそのまま使う既存運用では、従来どおり `./data` が既定です
- 既存の `sync_state.json` に `profile` が無くても読めます
- ただし、`profile` が入った state を別 profile で実行すると fail-fast します

## クイックスタート

### 1. リポジトリを取得

```bash
git clone <YOUR_REPOSITORY_URL>
cd garoon-icloud-sync
mkdir -p runtime/profiles/default
cp .env.example runtime/profiles/default/.env
```

### 2. `.env` を設定

`.env` は Git 管理しないでください。`chmod 600 runtime/profiles/default/.env` のように権限も絞るのが安全です。

通常運用の最小例:

```dotenv
PROFILE_NAME=default
APP_DATA_DIR=runtime/profiles/default

GAROON_BASE_URL=https://example.cybozu.com/g
GAROON_USERNAME=your-username
GAROON_PASSWORD=your-password

CALDAV_URL=https://caldav.icloud.com
CALDAV_USERNAME=your-apple-id-or-app-specific-user
CALDAV_PASSWORD=your-app-specific-password
CALDAV_CALENDAR_NAME=Garoon Sync Test

GAROON_START_DAYS_OFFSET=0
GAROON_END_DAYS_OFFSET=92
CALDAV_DRY_RUN=false
```

初回確認時は `CALDAV_DRY_RUN=true` にして、必ずテスト用カレンダーで差分を見てください。

### 3. ビルド

```bash
docker compose build garoon-sync
```

### 4. 実行

通常運用用 `.env` を使う例:

```bash
SYNC_ENV_FILE=runtime/profiles/default/.env ./scripts/run_docker_sync.sh
```

## 環境変数

主要項目:

| 変数 | 役割 | 推奨値 / 補足 |
| --- | --- | --- |
| `PROFILE_NAME` | 実行 profile 識別子 | 新規運用では必須推奨 |
| `APP_DATA_DIR` | profile ごとの runtime ルート | 例: `runtime/profiles/tomoya` |
| `GAROON_BASE_URL` | Garoon のベース URL | 必須 |
| `GAROON_USERNAME` | Garoon ユーザー名 | 必須 |
| `GAROON_PASSWORD` | Garoon パスワード | 必須 |
| `CALDAV_URL` | CalDAV discovery 起点 URL | iCloud でも `CALDAV_URL` を使います |
| `CALDAV_USERNAME` | CalDAV ユーザー名 | 必須 |
| `CALDAV_PASSWORD` | CalDAV パスワード | 必須 |
| `CALDAV_CALENDAR_NAME` | 同期先カレンダー名 | 初回はテスト用推奨 |
| `GAROON_START_DAYS_OFFSET` | 取得開始オフセット | 通常運用は `0` |
| `GAROON_END_DAYS_OFFSET` | 取得終了オフセット | 通常運用は `92` |
| `CALDAV_DRY_RUN` | dry-run 切り替え | 通常運用は `false` |

補助項目:

| 変数 | 役割 | 既定 |
| --- | --- | --- |
| `OUTPUT_JSON_PATH` | `events.json` の保存先 | `<APP_DATA_DIR>/data/events.json` 相当 |
| `LOG_LEVEL` | ログレベル | `INFO` |
| `DRY_RUN_WARN_CREATE_COUNT` | dry-run warning の create 閾値 | `10` |
| `DRY_RUN_WARN_DELETE_COUNT` | dry-run warning の delete 閾値 | `10` |
| `CALDAV_DIAGNOSTIC_DUMP_FAILED_ICS` | 失敗 ICS 保存 | `false` |
| `CALDAV_DIAGNOSTIC_DUMP_SUCCESS_ICS` | 成功相当 ICS 保存 | `false` |
| `CALDAV_DIAGNOSTIC_DUMP_UID_LOOKUP_JSON` | UID lookup 診断保存 | `false` |
| `GAROON_TARGET_USER` | Garoon 対象ユーザー絞り込み | 任意 |
| `GAROON_TARGET_CALENDAR` | Garoon 対象カレンダー絞り込み | 任意 |

## 生成物

profile ごとの `data/` には次のファイルが保存されます。

- `events.json`
- `calendar.ics`
- `sync_plan.json`
- `caldav_sync_result.json`
- `sync_state.json`
- `run_summary.json`
- `diagnostics/`
- `reports/`
- `backups/`

`logs/garoon-icloud-sync.log` には profile 別ログを保存します。`journalctl` や Docker の stdout と併用できます。

## run summary

各 run の最後に `run_summary.json` を出力します。最低限、次の情報を含みます。

- `profile`
- `started_at`
- `finished_at`
- `mode`
- `dry_run`
- `fetch_window`
- `result`
- `counts.create`
- `counts.update`
- `counts.delete`
- `counts.skip`
- `warnings`
- `error`

例:

```json
{
  "profile": "tomoya",
  "started_at": "2026-03-16T23:30:00+09:00",
  "finished_at": "2026-03-16T23:30:12+09:00",
  "mode": "normal",
  "dry_run": false,
  "fetch_window": {
    "start_days_offset": 0,
    "end_days_offset": 92,
    "start": "2026-03-16T00:00:00+09:00",
    "end": "2026-06-16T23:59:59+09:00"
  },
  "result": "success",
  "counts": {
    "create": 1,
    "update": 2,
    "delete": 0,
    "skip": 53
  },
  "warnings": [],
  "error": null
}
```

delete が 1 件以上なら `DELETE_DETECTED`、通常運用の 0..92 日を超える window なら `BACKFILL_WINDOW` が warning に残ります。

## 通常運用

通常運用の推奨値は次です。

```dotenv
GAROON_START_DAYS_OFFSET=0
GAROON_END_DAYS_OFFSET=92
CALDAV_DRY_RUN=false
```

通常運用例:

```bash
SYNC_ENV_FILE=runtime/profiles/tomoya/.env ./scripts/run_docker_sync.sh
```

確認コマンド例:

```bash
python -m src.sync_state_backup list --env-path runtime/profiles/tomoya/.env
python -m src.sync_plan_inspect --env-path runtime/profiles/tomoya/.env --action create --action delete
python -m src.caldav_sync_result_summary --env-path runtime/profiles/tomoya/.env
```

## backfill

backfill は通常運用とは別設定で、一時的にだけ実施してください。通常運用用 `.env` を広い window のまま残さないのが重要です。

backfill 用ファイル例:

```text
runtime/profiles/tomoya/.env
runtime/profiles/tomoya/.env.backfill
```

`runtime/profiles/tomoya/.env.backfill` の例:

```dotenv
PROFILE_NAME=tomoya
APP_DATA_DIR=runtime/profiles/tomoya
GAROON_START_DAYS_OFFSET=-365
GAROON_END_DAYS_OFFSET=183
CALDAV_DRY_RUN=true
```

backfill dry-run:

```bash
SYNC_ENV_FILE=runtime/profiles/tomoya/.env.backfill ./scripts/run_docker_sync.sh
```

backfill 本番:

```bash
SYNC_ENV_FILE=runtime/profiles/tomoya/.env.backfill CALDAV_DRY_RUN=false ./scripts/run_docker_sync.sh
```

backfill 後は、通常運用用 `.env` を必ず次へ戻してください。

```dotenv
GAROON_START_DAYS_OFFSET=0
GAROON_END_DAYS_OFFSET=92
CALDAV_DRY_RUN=false
```

## Docker Compose

`docker-compose.yml` は次を永続化します。

- `./data` for legacy single-user mode
- `./runtime` for profile-separated runtime
- `./logs` for legacy single-user file logs

profile 別 `.env` は `SYNC_ENV_FILE` で切り替えます。
相対パス設定は `/app`、つまりリポジトリルート基準で解決されます。

通常運用:

```bash
SYNC_ENV_FILE=runtime/profiles/tomoya/.env docker compose run --rm garoon-sync
```

backfill:

```bash
SYNC_ENV_FILE=runtime/profiles/tomoya/.env.backfill docker compose run --rm garoon-sync
```

## systemd --user

template unit の例を `deploy/systemd/user/garoon-icloud-sync@.service` と `deploy/systemd/user/garoon-icloud-sync@.timer` に置いています。

配置例:

```bash
mkdir -p ~/.config/systemd/user
install -D -m 0644 deploy/systemd/user/garoon-icloud-sync@.service ~/.config/systemd/user/garoon-icloud-sync@.service
install -D -m 0644 deploy/systemd/user/garoon-icloud-sync@.timer ~/.config/systemd/user/garoon-icloud-sync@.timer
systemctl --user daemon-reload
systemctl --user enable --now garoon-icloud-sync@tomoya.timer
```

この template は `SYNC_ENV_FILE=runtime/profiles/%i/.env` を使うので、通常運用 profile ごとの timer を作れます。

確認:

```bash
systemctl --user status garoon-icloud-sync@tomoya.service
systemctl --user status garoon-icloud-sync@tomoya.timer
journalctl --user -u garoon-icloud-sync@tomoya.service -n 100 --no-pager
```

## fail-fast と warning

- state 内の `profile` が実行時 `PROFILE_NAME` と違う場合は fail-fast します
- fail-fast 時は state を自動修復しません
- save も反映も進めません
- 実行開始ログには `profile`、`dry_run`、`mode`、`fetch window` を残します
- delete、backfill window、dry-run 大量差分は warning としてログと run summary に残します

## トラブルシュート

- `sync_state profile mismatch` が出た: 別 profile の state を読んでいます。`PROFILE_NAME` と `APP_DATA_DIR`、または指定した `.env` を見直してください
- delete が出た: すぐ本番に進めず、`sync_plan.json` と `run_summary.json` を確認してください
- backfill warning が出た: 一時実行なら正常です。終わったら通常運用値へ戻してください
- `404` / `412` が出た: `caldav_sync_result.json` と `reports/`、`diagnostics/` を確認してください
- `dry-run` なのに state が進んだように見える: `dry-run` では `sync_state.json` は更新しません。確認対象は `sync_plan.json`、`caldav_sync_result.json`、`run_summary.json` です

## 詳細運用

Raspberry Pi 上での配置、`systemd --user` timer、`linger`、ログ確認は [docs/raspberry-pi-operation.md](docs/raspberry-pi-operation.md) を参照してください。
