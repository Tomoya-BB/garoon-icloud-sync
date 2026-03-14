# Raspberry Pi Operation

Raspberry Pi で `garoon-icloud-sync` を `systemd --user` + timer で運用するための手順です。前提パスは `/home/tomoya/projects/garoon-icloud-sync`、タイムゾーンは `Asia/Tokyo` です。

## 前提条件

- Raspberry Pi OS 64bit 系で Docker Engine と `docker compose` が使える
- `tomoya` ユーザーが Docker を実行できる
- リポジトリが `/home/tomoya/projects/garoon-icloud-sync` に配置されている
- `.env` がリポジトリ直下にあり、通常運用では `CALDAV_DRY_RUN=false` にしてある
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
