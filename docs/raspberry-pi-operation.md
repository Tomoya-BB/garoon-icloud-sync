# Raspberry Pi Operation

Raspberry Pi で `garoon-icloud-sync` を `systemd --user` + timer で運用するための手順です。前提パスは `/home/tomoya/projects/garoon-icloud-sync`、タイムゾーンは `Asia/Tokyo` です。

## 前提条件

- Raspberry Pi OS 64bit 系で Docker Engine と `docker compose` が使える
- `tomoya` ユーザーが Docker を実行できる
- リポジトリが `/home/tomoya/projects/garoon-icloud-sync` に配置されている
- `.env` がリポジトリ直下にあり、通常運用では `CALDAV_DRY_RUN=false` にしてある
- 通常運用の `.env` は `GAROON_START_DAYS_OFFSET=0` と `GAROON_END_DAYS_OFFSET=31` を前提にする
- `data/` はホスト側に永続化済み
- 初回セットアップ時に一度は手動で `docker compose build garoon-sync` を実行してある
- Raspberry Pi 本体のタイムゾーンが `Asia/Tokyo` になっている

タイムゾーン確認と設定:

```bash
timedatectl status
sudo timedatectl set-timezone Asia/Tokyo
```

## 手動実行コマンド

本番実行:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
./scripts/run_docker_sync.sh
```

`./scripts/run_docker_sync.sh` はリポジトリ直下へ移動してから `docker compose run --rm garoon-sync` を実行するため、`systemd --user` からも手動でも同じ入口を使えます。

dry-run 実行:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
docker compose run --rm -e CALDAV_DRY_RUN=true garoon-sync
```

本番実行を `docker compose` で直接行う場合:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
docker compose run --rm garoon-sync
```

運用メモ:

- `.env` の `CALDAV_DRY_RUN` は通常 `false` のまま運用し、確認時だけコマンド側で `-e CALDAV_DRY_RUN=true` を付けて上書きします
- `data/` の内容はホスト側の実データなので、dry-run と本番の前後で `data/sync_plan.json`、`data/caldav_sync_result.json`、`data/sync_state.json` を確認しながら進めると安全です

## 初回 backfill

初回だけは broad range で `sync_state` を作ってから通常運用へ切り替えると、安全に過去 1 年から半年先まで取り込めます。例として、過去 365 日から 183 日先までを 1 回だけ流すなら次の順序です。

backfill dry-run:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
docker compose run --rm \
  -e CALDAV_DRY_RUN=true \
  -e GAROON_START_DAYS_OFFSET=-365 \
  -e GAROON_END_DAYS_OFFSET=183 \
  garoon-sync
```

backfill 本番:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
docker compose run --rm \
  -e CALDAV_DRY_RUN=false \
  -e GAROON_START_DAYS_OFFSET=-365 \
  -e GAROON_END_DAYS_OFFSET=183 \
  garoon-sync
```

backfill 本番後は `data/sync_state.json` を残したまま、通常運用の offset に戻します。

## 通常運用

通常運用では `.env` を次の値にしておく前提です。

```dotenv
GAROON_START_DAYS_OFFSET=0
GAROON_END_DAYS_OFFSET=31
CALDAV_DRY_RUN=false
```

確認時だけ dry-run で上書きする場合:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
docker compose run --rm \
  -e CALDAV_DRY_RUN=true \
  -e GAROON_START_DAYS_OFFSET=0 \
  -e GAROON_END_DAYS_OFFSET=31 \
  garoon-sync
```

この dry-run で `data/sync_plan.json` の `delete` が `0` 件になっていることを見てから、本番運用へ戻すのが安全です。

## delete の考え方

- delete 候補になるのは、event を最後に確認した fetch window が今回の fetch window に完全に含まれる state entry だけです
- broad range backfill で作った event は、通常運用の `0..31` window から外れていれば missing 扱いでも delete されません
- 既存 `sync_state.json` に fetch window metadata が無い古い entry も、安全側に倒して delete しません

つまり「broad range で作った state がある状態で通常運用を回しても、取得範囲外の過去イベントや半年先イベントを消さない」挙動を優先しています。

## systemd user service / timer の配置方法

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

状態確認:

```bash
systemctl --user list-timers
systemctl --user status garoon-sync.timer
systemctl --user status garoon-sync.service
```

ログ確認:

```bash
journalctl --user -u garoon-sync.service -n 100 --no-pager
journalctl --user -u garoon-sync.service -f
```

timer は以下の設定です。

- `OnBootSec=5min`
- `OnUnitActiveSec=30min`
- `Persistent=true`

このドキュメント時点では timer を 15 分化していません。まず delete 安全化後の dry-run を確認し、その後に必要なら `deploy/systemd/user/garoon-sync.timer` の `OnUnitActiveSec` を別途変更してください。

## ログインしていなくても timer を動かす設定

`systemd --user` の timer をログアウト後も動かすために linger を有効化します。

```bash
sudo loginctl enable-linger tomoya
```

root シェルなら次でも同じです。

```bash
loginctl enable-linger tomoya
```

有効化後は、再ログイン前でも `systemctl --user list-timers` と `journalctl --user -u garoon-sync.service -n 100 --no-pager` で状態確認できます。

## 初回同期時の注意点

- 既存の iCloud カレンダーに同じ予定がすでに入っている場合、初回本番同期で重複登録が起きる可能性があります。必ず先に `CALDAV_DRY_RUN=true` で差分件数を確認してください
- `data/sync_state.json` は同期の基準状態です。初回本番同期の前に不用意に削除すると、既存イベントを未同期とみなして再作成が増えることがあります
- 逆に、過去の別環境の `data/sync_state.json` をそのまま持ち込むと、現在の iCloud 側実態とずれて update/delete 判定に影響することがあります
- 初回だけはバックアップを取ってから進めるのが安全です

バックアップ例:

```bash
cd /home/tomoya/projects/garoon-icloud-sync
cp data/sync_state.json data/sync_state.json.manual-backup.$(date +%Y%m%d-%H%M%S)
```

`data/sync_state.json` が存在しない状態で初回同期を始める場合は、重複作成の有無を特に慎重に確認してください。
