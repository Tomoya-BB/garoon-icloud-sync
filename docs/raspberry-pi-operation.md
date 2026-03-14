# Raspberry Pi Operation

このドキュメントは、`garoon-icloud-sync` を Raspberry Pi 上で常時運用するための手順です。README が導入の入口であるのに対して、ここでは `systemd --user` timer を使った定期実行、初回 backfill、通常運用時の確認ポイントをまとめます。

前提パスは `/home/tomoya/projects/garoon-icloud-sync`、タイムゾーンは `Asia/Tokyo` です。

## 前提条件

- Raspberry Pi OS 64bit 系で Docker Engine と `docker compose` が使える
- `tomoya` ユーザーが Docker を実行できる
- リポジトリが `/home/tomoya/projects/garoon-icloud-sync` に配置されている
- `.env` がリポジトリ直下にある
- `data/` をホスト側に永続化している
- 初回セットアップ時に一度は `docker compose build garoon-sync` を実行している
- Raspberry Pi 本体のタイムゾーンが `Asia/Tokyo` になっている

タイムゾーン確認と設定:

```bash
timedatectl status
sudo timedatectl set-timezone Asia/Tokyo
```

## 通常運用の推奨値

通常運用では、`.env` を次の値にしておく前提です。

```dotenv
GAROON_START_DAYS_OFFSET=0
GAROON_END_DAYS_OFFSET=92
CALDAV_DRY_RUN=false
```

確認時だけ `dry-run` にしたい場合は、`.env` を変更せずコマンド側で `CALDAV_DRY_RUN=true` を一時上書きします。

## 手動実行

本番実行:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
./scripts/run_docker_sync.sh
```

`dry-run` 実行:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
docker compose run --rm -e CALDAV_DRY_RUN=true garoon-sync
```

`docker compose` を直接使う本番実行:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
docker compose run --rm garoon-sync
```

確認ポイント:

- `data/sync_plan.json` の `create` / `update` / `delete` 件数が想定どおりか
- `data/caldav_sync_result.json` に想定外の失敗が出ていないか
- `data/sync_state.json` を意図せず消したり入れ替えたりしていないか

## 初回 backfill

初回だけは broad range で一度同期し、その後に通常運用の window へ戻す運用を想定しています。ここでは例として、過去 365 日から 183 日先までを同期します。

### 1. `sync_state.json` をバックアップする

```bash
cd /home/tomoya/projects/garoon-icloud-sync
docker compose run --rm garoon-sync python -m src.sync_state_backup backup
```

### 2. 広範囲の `dry-run` を実行する

```bash
cd /home/tomoya/projects/garoon-icloud-sync
docker compose run --rm \
  -e CALDAV_DRY_RUN=true \
  -e GAROON_START_DAYS_OFFSET=-365 \
  -e GAROON_END_DAYS_OFFSET=183 \
  garoon-sync
```

### 3. 差分を確認する

- `data/sync_plan.json` の `create` / `delete` 件数
- `data/caldav_sync_result.json` の失敗有無
- テスト用カレンダーでの見え方

### 4. 問題なければ広範囲の本番実行を行う

```bash
cd /home/tomoya/projects/garoon-icloud-sync
docker compose run --rm \
  -e CALDAV_DRY_RUN=false \
  -e GAROON_START_DAYS_OFFSET=-365 \
  -e GAROON_END_DAYS_OFFSET=183 \
  garoon-sync
```

### 5. 通常運用の設定に戻す

backfill 後は `.env` を通常運用の値へ戻します。

```dotenv
GAROON_START_DAYS_OFFSET=0
GAROON_END_DAYS_OFFSET=92
CALDAV_DRY_RUN=false
```

## 通常運用

通常運用では、毎回 broad range を取りにいかず、日常同期の window だけを取得します。

確認のために一時的に `dry-run` を行う場合:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
docker compose run --rm \
  -e CALDAV_DRY_RUN=true \
  -e GAROON_START_DAYS_OFFSET=0 \
  -e GAROON_END_DAYS_OFFSET=92 \
  garoon-sync
```

通常運用時のポイント:

- fetch window ベースの削除判定なので、通常運用の window 外にあるイベントを誤削除しにくい
- broad range backfill で作った state を残したまま通常運用へ切り替えてよい
- `delete` 件数が想定外に増えている場合は、そのまま本番へ進めず `dry-run` の結果を見直す

## systemd --user service / timer の配置

`systemd --user` では、配布している unit をホームディレクトリ配下へ配置して使います。

配置:

```bash
mkdir -p /home/tomoya/.config/systemd/user
install -D -m 0644 /home/tomoya/projects/garoon-icloud-sync/deploy/systemd/user/garoon-sync.service /home/tomoya/.config/systemd/user/garoon-sync.service
install -D -m 0644 /home/tomoya/projects/garoon-icloud-sync/deploy/systemd/user/garoon-sync.timer /home/tomoya/.config/systemd/user/garoon-sync.timer
```

設定反映:

```bash
systemctl --user daemon-reload
systemctl --user enable --now garoon-sync.timer
```

timer 設定を変更したあとに反映し直す場合:

```bash
systemctl --user daemon-reload
systemctl --user restart garoon-sync.timer
```

手動で service を実行して確認する場合:

```bash
systemctl --user start garoon-sync.service
```

## systemd の確認コマンド

timer 一覧:

```bash
systemctl --user list-timers
```

timer 状態:

```bash
systemctl --user status garoon-sync.timer
```

service 状態:

```bash
systemctl --user status garoon-sync.service
```

直近ログ:

```bash
journalctl --user -u garoon-sync.service -n 100 --no-pager
```

ログ追跡:

```bash
journalctl --user -u garoon-sync.service -f
```

## timer の運用方針

Raspberry Pi では、15 分または 30 分間隔での運用が可能です。配布している `deploy/systemd/user/garoon-sync.timer` は 30 分設定なので、必要に応じて `OnUnitActiveSec` を 15 分へ調整してください。

配布している timer の主な設定:

- `OnBootSec=5min`
- `OnUnitActiveSec=30min`
- `Persistent=true`

## ログアウト後も timer を動かす設定

`systemd --user` の timer をログアウト後も動かすには、linger を有効化します。

```bash
sudo loginctl enable-linger tomoya
```

root シェルなら次でも同じです。

```bash
loginctl enable-linger tomoya
```

有効化後は、再ログイン前でも次のコマンドで状態確認できます。

```bash
systemctl --user list-timers
journalctl --user -u garoon-sync.service -n 100 --no-pager
```

## 運用時の注意点

- 初回本番同期の前には、必ず `CALDAV_DRY_RUN=true` で差分件数を確認する
- 初回は本番カレンダーではなく、テスト用カレンダーで確認してから切り替える
- `data/sync_state.json` は差分同期の基準状態なので、安易に削除しない
- 別環境の `data/sync_state.json` をそのまま持ち込むと、現在の iCloud 側実態とずれて update / delete 判定に影響することがある
- `delete` が想定外に多い場合は、通常運用へ戻る前に `sync_plan.json` と `caldav_sync_result.json` を見直す
