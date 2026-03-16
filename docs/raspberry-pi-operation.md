# Raspberry Pi Operation

このドキュメントは、Raspberry Pi 上で `garoon-icloud-sync` を複数 profile 運用するための実践向けメモです。前提パスは `/home/tomoya/projects/garoon-icloud-sync`、タイムゾーンは `Asia/Tokyo` です。

## 前提

- Docker Engine と `docker compose` が使える
- リポジトリが `/home/tomoya/projects/garoon-icloud-sync` にある
- profile ごとの `.env` を `runtime/profiles/<profile>/.env` に置く
- `.env` 内の相対パスは、このリポジトリルートを基準に書く
- `.env` は Git 管理せず、`chmod 600` で権限を絞る
- 通常運用値は `GAROON_START_DAYS_OFFSET=0`、`GAROON_END_DAYS_OFFSET=92`、`CALDAV_DRY_RUN=false`

## 推奨 profile 構成

```text
/home/tomoya/projects/garoon-icloud-sync/
  runtime/
    profiles/
      tomoya/
        .env
        .env.backfill
        data/
        logs/
```

通常運用と backfill は別ファイルで分けてください。

- 通常運用: `runtime/profiles/tomoya/.env`
- backfill: `runtime/profiles/tomoya/.env.backfill`

## 手動実行

通常運用:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
SYNC_ENV_FILE=runtime/profiles/tomoya/.env ./scripts/run_docker_sync.sh
```

通常運用の dry-run:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
SYNC_ENV_FILE=runtime/profiles/tomoya/.env CALDAV_DRY_RUN=true ./scripts/run_docker_sync.sh
```

backfill dry-run:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
SYNC_ENV_FILE=runtime/profiles/tomoya/.env.backfill ./scripts/run_docker_sync.sh
```

backfill 本番:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
SYNC_ENV_FILE=runtime/profiles/tomoya/.env.backfill CALDAV_DRY_RUN=false ./scripts/run_docker_sync.sh
```

backfill 後は、通常運用の `.env` を必ず次へ戻します。

```dotenv
GAROON_START_DAYS_OFFSET=0
GAROON_END_DAYS_OFFSET=92
CALDAV_DRY_RUN=false
```

## 確認ポイント

通常 run ごとに、少なくとも次を見れば状況を追えます。

- `runtime/profiles/tomoya/data/sync_plan.json`
- `runtime/profiles/tomoya/data/caldav_sync_result.json`
- `runtime/profiles/tomoya/data/run_summary.json`
- `runtime/profiles/tomoya/logs/garoon-icloud-sync.log`

warning の見方:

- `DELETE_DETECTED`: delete が 1 件以上ある
- `BACKFILL_WINDOW`: 通常運用の 0..92 日を超える fetch window
- `DRY_RUN_ANOMALOUS_CHANGE`: dry-run で大量差分

## state の扱い

- `sync_state.json` は profile ごとに分けます
- state 内の `profile` と実行 profile が違う場合は fail-fast します
- fail-fast 時は save も自動修復も行いません
- `sync_state.json` を安易に削除せず、必要なら先にバックアップしてください

バックアップ例:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
python -m src.sync_state_backup backup --env-path runtime/profiles/tomoya/.env
```

一覧確認:

```bash
python -m src.sync_state_backup list --env-path runtime/profiles/tomoya/.env
```

## systemd --user template

template unit を使うと、profile ごとに独立した timer を張れます。

配置:

```bash
mkdir -p /home/tomoya/.config/systemd/user
install -D -m 0644 /home/tomoya/projects/garoon-icloud-sync/deploy/systemd/user/garoon-icloud-sync@.service /home/tomoya/.config/systemd/user/garoon-icloud-sync@.service
install -D -m 0644 /home/tomoya/projects/garoon-icloud-sync/deploy/systemd/user/garoon-icloud-sync@.timer /home/tomoya/.config/systemd/user/garoon-icloud-sync@.timer
systemctl --user daemon-reload
systemctl --user enable --now garoon-icloud-sync@tomoya.timer
```

この template は内部で `SYNC_ENV_FILE=runtime/profiles/%i/.env` を使います。

確認コマンド:

```bash
systemctl --user status garoon-icloud-sync@tomoya.service
systemctl --user status garoon-icloud-sync@tomoya.timer
journalctl --user -u garoon-icloud-sync@tomoya.service -n 100 --no-pager
journalctl --user -u garoon-icloud-sync@tomoya.service -f
```

## linger

ログアウト後も timer を動かすなら linger を有効化します。

```bash
sudo loginctl enable-linger tomoya
```

## 運用ルール

- 初回は必ずテスト用カレンダーで dry-run から始める
- 通常運用値は `0 / 92 / false` を維持する
- backfill 用設定を通常運用へ残さない
- delete が想定外なら本番へ進めない
- profile をまたいで state を持ち込まない
