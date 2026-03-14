# garoon-icloud-sync

Garoon から予定を取得して、正規化済みイベントを JSON と iCalendar (`.ics`) に保存し、差分から CalDAV への同期アクションを実行する Python PoC です。PoC 第14段階では、PoC 第13段階の resource URL 優先 update / read path recovery / 1 回限り retry / tombstone 保存に加えて、tombstone 化されたイベントの再出現判定、`ics_uid` の再利用、CalDAV への再作成ポリシーを追加しました。PoC 第23段階では、`sync_state` に加えて `sync_plan` save failure と CalDAV delivery failure も key=value の structured log で追えるようにし、`sync_state.json` 破損時の退避・復旧手順を README に追加しています。PoC 第25段階では、その運用手順を `python -m src.sync_state_backup` の補助 CLI として実行できるようにしました。PoC 第26段階では、`restore --validate` 前提の復旧後チェックリストと、`data/backups/` の肥大化を防ぐ `prune` 運用を追加しました。PoC 第27段階では、`CALDAV_DRY_RUN=true` 実行時の `sync_plan` 件数を review し、`create` / `delete` が閾値以上なら本番前に warning と structured log を出す簡易ガードを追加しています。PoC 第30段階では、CalDAV discovery を `principal` -> `calendar-home-set` -> `calendar collection` の順で解決し、`CALDAV_URL=https://caldav.icloud.com` のような iCloud 系サーバーでも `CALDAV_CALENDAR_NAME` に一致する実カレンダー URL へ create/update/delete できるようにしました。

## 前提

- Python 3.11 以上が使える macOS / Linux / WSL のシェル環境
- 日常運用の確認は WSL を想定しています
- Garoon の接続先 URL と認証情報が必要です

## セットアップ

```bash
git clone <YOUR_REPOSITORY_URL>
cd garoon-icloud-sync
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

`.env.example` をコピーして `.env` を作成します。

```bash
cp .env.example .env
```

`.env`、`.venv/`、`__pycache__/`、`.pytest_cache/`、`data/` 配下の生成物は `.gitignore` で Git 管理対象から外しています。ローカルで生成された認証情報や同期結果をそのまま push しない前提です。

`.env` の主な項目:

- `GAROON_BASE_URL`: 例 `https://example.cybozu.com/g`
- `GAROON_USERNAME`
- `GAROON_PASSWORD`
- `GAROON_START_DAYS_OFFSET`: 今日から何日前を取得開始日にするか
- `GAROON_END_DAYS_OFFSET`: 今日から何日後を取得終了日にするか
- `OUTPUT_JSON_PATH`: 出力先。相対パスの場合は `.env` 基準で解決されます
- `LOG_LEVEL`: `DEBUG` / `INFO` / `WARNING` など
- `CALDAV_URL`: CalDAV の discovery 起点 URL。PoC 第30段階では root URL をそのまま calendar collection とみなさず、ここから `principal` / `calendar-home-set` / `calendar collection` を順に解決します
- `CALDAV_USERNAME`
- `CALDAV_PASSWORD`
- `CALDAV_CALENDAR_NAME`: 最初は本番ではなくテスト用カレンダー名を指定してください
- `CALDAV_DRY_RUN`: `true` の間は実送信せず payload 概要だけ出力します。初期値は `true` 推奨です
- `DRY_RUN_WARN_CREATE_COUNT`: `dry-run` で warning を出す `create` 件数の閾値。初期値は `10`
- `DRY_RUN_WARN_DELETE_COUNT`: `dry-run` で warning を出す `delete` 件数の閾値。初期値は `10`
- `CALDAV_DIAGNOSTIC_DUMP_FAILED_ICS`: `true` のとき、CalDAV `create` 失敗時に送信直前の ICS 本文を `data/diagnostics/` へ保存します。初期値は `false`
- `CALDAV_DIAGNOSTIC_DUMP_SUCCESS_ICS`: `true` のとき、比較用に `create` 成功相当の ICS 本文も保存します。初期値は `false`
- `CALDAV_DIAGNOSTIC_DUMP_UID_LOOKUP_JSON`: `true` のとき、`create 412` の UID lookup raw candidate 一覧、`calendar_query_uid_calendar_data` / `calendar_collection_scan_calendar_data` の REPORT raw response、selected candidate 情報、read-only candidate ranking を `data/diagnostics/` へ保存します。初期値は `false`

最初の確認は、必ず本番ではなくテスト用カレンダーで `CALDAV_DRY_RUN=true` のまま進めてください。`git clone` 直後は次の順序が安全です。

1. `.env.example` から `.env` を作成する
2. Garoon / CalDAV の接続先と認証情報を設定する
3. `CALDAV_CALENDAR_NAME` をテスト用カレンダー名にする
4. `CALDAV_DRY_RUN=true` のまま `python -m src.main` を実行する
5. `data/sync_plan.json` と `data/caldav_sync_result.json` を確認する

## 実行

```bash
source .venv/bin/activate
python -m src.main
```

GitHub から clone して試す最初の一回は、次の dry-run コマンドから始めるのがおすすめです。

```bash
source .venv/bin/activate
CALDAV_DRY_RUN=true python -m src.main
```

## Docker 実行

ローカル Python 実行フローはそのまま残しつつ、`docker compose run --rm garoon-sync` で同じ処理をコンテナ内に閉じ込めて実行できます。Docker 構成では、ホスト側の `.env` を `/app/.env` に read-only bind mount し、`data/` を `/app/data` へ bind mount します。これにより、認証情報をイメージへ焼き込まず、同期結果や診断ファイルだけをホストへ永続化できます。

この構成の前提:

- サービス名は `garoon-sync`
- コンテナの作業ディレクトリは `/app`
- デフォルト実行コマンドは `python -m src.main`
- タイムゾーンは `Asia/Tokyo`
- 永続化対象は `data/events.json`、`data/calendar.ics`、`data/sync_plan.json`、`data/caldav_sync_result.json`、`data/sync_state.json`、`data/diagnostics/`、`data/reports/`、`data/backups/`

初回準備:

```bash
cp .env.example .env
mkdir -p data/diagnostics data/reports data/backups
docker compose build garoon-sync
```

手動実行:

```bash
docker compose run --rm garoon-sync
```

補助スクリプトを使う場合:

```bash
./scripts/run_docker_sync.sh
```

引数を渡すと、`docker compose run --rm garoon-sync ...` の override としてそのまま実行できます。たとえばコンテナ内でテストを走らせる場合は次のようにします。

```bash
./scripts/run_docker_sync.sh pytest
```

Mac での運用メモ:

- Docker Desktop など、`docker` と `docker compose` が使える状態にしてから上記コマンドを実行してください
- Apple Silicon / Intel のどちらでも、同じ `docker compose` 手順で確認できます

Raspberry Pi OS 64bit Lite での運用メモ:

- ARM64 向けでも扱いやすいシンプルな Python slim ベースイメージを使っています
- Raspberry Pi 側でもリポジトリと `.env`、`data/` をそのまま持っていけば、同じ `docker compose build garoon-sync` と `docker compose run --rm garoon-sync` で実行できます
- 先に Mac 上で `CALDAV_DRY_RUN=true` の確認を済ませてから Raspberry Pi へ持っていくと安全です

Docker 実行時の注意:

- `.env` はホスト側ファイルを `/app/.env` へ read-only mount し、`python -m src.main` が通常どおり読み込みます
- `OUTPUT_JSON_PATH=data/events.json` のような相対パスは `/app` 基準で解決されるため、出力は bind mount 済みの `/app/data` に揃います
- `sync_state.json` や診断レポートをコンテナ内に閉じ込めないため、`data/` は named volume ではなく bind mount にしています
- 既存の `python -m src.main` / `pytest` によるローカル実行は、Docker を使わずこれまで通り継続できます

成功すると、取得件数と保存先が標準出力に表示され、次の 5 ファイルが UTF-8 で保存されます。

- JSON: `OUTPUT_JSON_PATH`。デフォルトは `data/events.json`
- ICS: `data/calendar.ics`
- Sync plan: `data/sync_plan.json`
- CalDAV sync result: `data/caldav_sync_result.json`
- Sync state: `data/sync_state.json`

初回は必ずテスト用カレンダーで `CALDAV_DRY_RUN=true` のまま動作確認してください。本番カレンダーへ切り替える前に、テスト用カレンダーで `dry-run` と実送信 (`CALDAV_DRY_RUN=false`) の両方を先に確認する運用を強く推奨します。

`dry-run` では CalDAV サーバーへ `PUT` / `DELETE` しません。代わりに `create` / `update` / `delete` 対象について、送信予定アクション、UID、payload サイズ、予定概要を `data/caldav_sync_result.json` とログで確認できます。`dry-run=true` の間は `data/sync_state.json` を新規作成せず、既存 state も更新せず、自動再送も行いません。PoC 第27段階では、この `dry-run` の前に保存された `data/sync_plan.json` を集計し、`create` または `delete` が閾値以上なら warning を表示します。warning が出た場合は、そのまま本番へ進まず、`sync_state.json` と `sync_plan.json` を見直してからテスト用カレンダーで代表予定を確認してください。PoC 第28段階では、warning 後に `python -m src.sync_plan_inspect` で `sync_plan.json` の対象 event を見やすく確認できるようにしました。

```bash
source .venv/bin/activate
python -m src.sync_plan_inspect
python -m src.sync_plan_inspect --action create
python -m src.sync_plan_inspect --action delete
python -m src.sync_plan_inspect --action update
python -m src.sync_plan_inspect --action create --result-path data/caldav_sync_result.json
python -m src.sync_plan_inspect --action create --result-path data/caldav_sync_result.json --only failed
python -m src.sync_plan_inspect --action create --result-path data/caldav_sync_result.json --drift-status generated
python -m src.sync_plan_inspect --action create --result-path data/caldav_sync_result.json --drift-status remote_fetch_failed
python -m src.sync_plan_inspect --action create --result-path data/caldav_sync_result.json --drift-status none
python -m src.sync_plan_inspect --action create --result-path data/caldav_sync_result.json --only failed --sort drift-diff-count
python -m src.sync_plan_inspect --action create --result-path data/caldav_sync_result.json --only failed --drift-status generated --sort drift-diff-count
python -m src.sync_plan_inspect --action create --result-path data/caldav_sync_result.json --only failed --conflict state-drift
python -m src.sync_plan_inspect --action create --result-path data/caldav_sync_result.json --only failed --conflict uid-match
python -m src.sync_plan_inspect --action create --result-path data/caldav_sync_result.json --only failed --conflict resource-exists
python -m src.caldav_sync_result_summary --result-path data/caldav_sync_result.json
```

`python -m src.caldav_sync_result_summary` は read-only の診断集計 CLI です。PoC 第40段階では、`create 412` かつ `create_conflict_existing_resource_url` がある result を対象に、`drift_report_status` / `drift_diff_count` / `drift_diff_fields` の傾向を人間が読みやすい形で集計し、`remote_fetch_failed` も別枠で件数表示します。PoC 第41段階ではこれに加えて、`SUMMARY` や `DTSTART` など単体フィールド差分の頻度と代表 `event_id` 例も見られるようにしました。同期本体や state は変更しません。

出力例:

```text
CalDAV sync result summary
result_path: ./data/caldav_sync_result.json
create_total: 12
create_failed: 5
create_failed_412: 4
state_drift_suspected: 4
uid_match_found: 3
resource_exists: 2
existing_resource_url: 4
state_drift_uid_match_only: 2
state_drift_resource_exists_only: 1
state_drift_both: 1

total create 412 with drift reports: 4
remote_fetch_failed: 1

drift_report_status summary
- generated: 3 (sample_event_ids: evt-a, evt-b, evt-c)
- remote_fetch_failed: 1 (sample_event_ids: evt-d)

drift_diff_count summary
- 1: 1 (sample_event_ids: evt-c)
- 2: 2 (sample_event_ids: evt-a, evt-b)
- null: 1 (sample_event_ids: evt-d)

drift_diff_fields combination summary
- SEQUENCE, SUMMARY: 2 (sample_event_ids: evt-a, evt-b)
- DESCRIPTION: 1 (sample_event_ids: evt-c)
- (no diff fields): 1 (sample_event_ids: evt-d)

individual drift field frequency
- SUMMARY: 2 (sample_event_ids: evt-a, evt-b)
- DESCRIPTION: 1 (sample_event_ids: evt-c)
- SEQUENCE: 2 (sample_event_ids: evt-a, evt-b)

sample event_ids
- SUMMARY: evt-a, evt-b
- DESCRIPTION: evt-c
- SEQUENCE: evt-a, evt-b
```

見方の目安:

- `drift_diff_fields combination summary` で「どの差分セットが多いか」を見る
- `individual drift field frequency` で「SUMMARY / DTSTART / DTEND など、どの単体項目が多いか」を見る
- `sample event_ids` から代表例を開いて drift report を確認する

デフォルトでは `create` と `delete` を優先表示します。各行には `event_id` / `ics_uid` / `action` / `action_reason` / `summary` を表示し、`summary` が `sync_plan.json` に無い delete 候補は `(not available)` と表示します。`--result-path` を付けると、`caldav_sync_result.json` の `success/failed`、`status_code`、`error_kind`、`create_conflict_*`、`existing_resource_url`、`selected_candidate_index`、`selected_candidate_reason`、`drift_report_status`、`drift_diff_count`、短縮表示の `drift_diff_fields`、`drift_report_path` を並べて比較できます。PoC 第42段階では `--sort drift-diff-count` を追加し、create failure を `drift_diff_count` 降順で優先確認しやすくしました。PoC 第43段階では `--drift-status generated|remote_fetch_failed|none` を追加し、drift report が生成できたケースだけ、remote fetch 失敗だけ、未生成だけを read-only で切り分けられます。iCloud 実運用では、まず `--only failed --drift-status generated --sort drift-diff-count` で失敗行のうち実際に report が生成できた event を優先し、必要に応じて `--conflict state-drift` / `uid-match` / `resource-exists` で絞り込むと確認しやすいです。同じ `existing_resource_url` に複数 event が寄っている場合は、その時点で drift report の比較相手より先に candidate 選定を疑ってください。

## CalDAV Create 診断モード

一時診断用として、CalDAV `create` に使う ICS 本文そのものを保存できます。通常運用では `false` のままにし、iCloud で `412 Precondition Failed` が出る代表予定だけを比較したい時だけ有効化してください。

有効化例:

```bash
source .venv/bin/activate
CALDAV_DIAGNOSTIC_DUMP_FAILED_ICS=true python -m src.main
```

成功イベントも比較したい場合は、必要な間だけ次を追加します。

```bash
source .venv/bin/activate
CALDAV_DIAGNOSTIC_DUMP_FAILED_ICS=true \
CALDAV_DIAGNOSTIC_DUMP_SUCCESS_ICS=true \
python -m src.main
```

保存先:

- 失敗した `create` の ICS 本文は `data/diagnostics/` に保存されます
- `CALDAV_DIAGNOSTIC_DUMP_FAILED_ICS=true` または `CALDAV_DIAGNOSTIC_DUMP_SUCCESS_ICS=true` の間は、`create` の request/response メタデータも `data/diagnostics/` に JSON で保存されます
- `CALDAV_DIAGNOSTIC_DUMP_UID_LOOKUP_JSON=true` の間は、`create 412` の UID lookup raw candidate 一覧、parsed remote UID、selected candidate の理由に加えて、`calendar_query_uid_calendar_data` / `calendar_collection_scan_calendar_data` の REPORT raw response と read-only candidate ranking を `data/diagnostics/` に保存します
- ファイル名には `success` / `failed`、`event_id`、`resource_name`、`sequence`、UTC timestamp を入れます
- `data/caldav_sync_result.json` の各 result には、保存した場合だけ `diagnostic_payload_path` が入ります
- HTTP メタデータを保存した場合は、各 result に `diagnostic_request_response_path` も入ります
- UID lookup 診断 JSON を保存した場合は、各 result に `create_conflict_uid_lookup_diagnostics_path` / `create_conflict_uid_query_raw_path` / `create_conflict_collection_scan_raw_path` も入ります
- `create_conflict_candidate_ranking` は summary / DTSTART / DTEND の近傍を見る read-only 補助情報です。`create_conflict_remote_uid_confirmed=false` の間は自動補正根拠に使わず、まず REPORT 応答差異と raw candidate を確認してください
- `create 412` かつ `create_conflict_existing_resource_url` がある result では、read-only の state drift 比較レポートを `data/reports/` に JSON 保存します

比較観点:

- 終日予定かどうか
- `DESCRIPTION` の有無
- `LOCATION` の有無
- 長文が含まれるか
- 特殊文字や改行を含むか

`CALDAV_DRY_RUN=true` では CalDAV 送信失敗は再現しませんが、`CALDAV_DIAGNOSTIC_DUMP_SUCCESS_ICS=true` を併用すると比較用の成功側 payload を先に採取できます。診断が終わったら、両フラグを `false` に戻して通常運用へ戻してください。

`.ics` には現時点で以下の項目を出力します。

- `UID`（Garoon の event ID ベースで安定生成）
- `DTSTAMP`
- `LAST-MODIFIED`（Garoon の `updatedAt` がある場合）
- `SEQUENCE`（`create` は `0`、`update` は `sync_state.json` の現在値を基準に次の値を payload に入れる）
- `DTSTART`
- `DTEND`
- `SUMMARY`
- `DESCRIPTION`
- `LOCATION`

日時付き予定は UTC に正規化して出力します。終日予定は Garoon 実データの `00:00:00` 開始 / `23:59:59` 終了をもとに `VALUE=DATE` で出力し、ICS の `DTEND` は排他的終了日に合わせて翌日に補正します。タイトルが空欄の予定は `SUMMARY` に予定メニュー名を使い、タイトルと予定メニューの両方が空欄の場合のみ `(no title)` を使います。Garoon 固有項目の細かい意味付けが未確定な部分はコード内に TODO を残しています。

## テスト

```bash
source .venv/bin/activate
pytest
```

`tests/test_sync_state.py` では `build_next_sync_state_from_delivery()` の invariant と、`create` / `update` / `delete` / 再出現 / retry が混在する state 遷移の回帰保護を優先して確認します。

## 配布後の運用メモ

まずはこの README の手順どおりにローカルで `dry-run` とテスト用カレンダーへの実送信を確認し、その後に Docker 化や Raspberry Pi 常駐へ進めるのが安全です。

- Docker 化する場合も、最初は `.env` と `data/` をホスト側で管理し、コンテナイメージに認証情報や同期結果を焼き込まない構成を推奨します
- Raspberry Pi で定期実行する場合も、いきなり常駐化せず、まずは同じコマンドを手動で `dry-run` し、出力ファイルとログの見え方を確認してください
- どちらの運用でも `data/` 配下は Git 管理せず、バックアップ方針だけを別途決めておくと扱いやすくなります

## 出力ファイル

`data/events.json` には以下を保存します。

- 取得日時
- 取得対象期間
- 正規化した予定一覧

正規化済みイベントには、Garoon 実データに合わせて次の情報を保持します。

- `start` / `end`: `date_time` と `time_zone`
- `created_at` / `updated_at`
- `event_type` / `event_menu` / `visibility_type`
- `is_all_day` / `is_start_only`
- `original_start_time_zone` / `original_end_time_zone`
- `repeat_id` / `repeat_info`

`data/calendar.ics` には正規化済み `EventRecord` から変換した `VCALENDAR` / `VEVENT` を保存します。

`data/sync_plan.json` には差分判定結果を CalDAV 向けの同期アクションキューとして保存します。PoC 第13段階では `create` / `update` / `delete` を `src/caldav_client.py` が消費し、最終送信結果と recovery 結果を state 更新判断に使います。

- `action`: `create` / `update` / `delete` / `skip`
- `event_id`
- `ics_uid`
- `sequence`
- `content_hash`
- `updated_at`
- `action_reason`
- `summary`: `create` / `update` / `skip` で分かる予定タイトル相当。`delete` では入らないことがあります

`data/caldav_sync_result.json` には CalDAV 実行結果を保存します。

- `dry_run`
- `processed_count`
- `ignored_count`
- `success_count`
- `failure_count`
- `results`: `create` / `update` / `delete` の実行結果
- `ignored_actions`: `skip`
- `results[].diagnostic_payload_path`: 診断ダンプを保存した場合の ICS ファイルパス
- `results[].diagnostic_request_response_path`: 診断ダンプを保存した場合の create request/response メタデータ JSON パス
- `results[].create_conflict_uid_lookup_diagnostics_path`: UID lookup raw candidate 診断 JSON パス
- `results[].create_conflict_uid_query_raw_path`: `calendar_query_uid_calendar_data` REPORT raw response パス
- `results[].create_conflict_collection_scan_raw_path`: `calendar_collection_scan_calendar_data` REPORT raw response パス
- `results[].create_conflict_state_drift_report_path`: `create 412` の read-only state drift レポート JSON パス
- `results[].create_conflict_state_drift_report_status`: レポート生成状況。`generated` / `remote_fetch_failed` / `report_write_failed`
- `results[].drift_report_status`: inspect 向けの要約 status。`create_conflict_state_drift_report_status` の短縮版
- `results[].drift_diff_count`: drift report `comparison` で `equal=false` だった項目数。remote fetch 失敗時は `null`
- `results[].drift_diff_fields`: drift report `comparison` で差分だった項目名一覧。例: `["SUMMARY", "SEQUENCE"]`
- `results[].create_conflict_remote_fetch_error`: remote existing resource の取得に失敗した場合のエラー文字列

各 `results` 要素には次の情報も入ります。

- `sequence`
- `payload_sequence`
- `payload_summary.summary`
- `payload_summary.is_all_day`
- `payload_summary.has_description`
- `payload_summary.has_location`
- `payload_bytes`
- `resource_name`
- `resource_url`
- `etag`
- `delivered_at`
- `resolution_strategy`
- `used_stored_resource_url`
- `uid_lookup_performed`
- `used_stored_etag`
- `conflict_kind`
- `retryable`
- `etag_mismatch`
- `attempted_conditional_update`
- `recovery_attempted`
- `recovery_succeeded`
- `refreshed_resource_url`
- `refreshed_etag`
- `initial_resource_url`
- `initial_etag`
- `retry_attempted`
- `retry_succeeded`
- `retry_count`
- `retry_resource_url`
- `retry_etag`
- `create_conflict_resource_exists`
- `create_conflict_uid_match_found`
- `create_conflict_uid_lookup_attempted`
- `create_conflict_uid_lookup_candidates`
- `create_conflict_uid_lookup_method`
- `create_conflict_remote_uid_confirmed`
- `create_conflict_state_drift_suspected`
- `create_conflict_existing_resource_url`
- `create_conflict_selected_candidate_reason`
- `create_conflict_selected_candidate_index`
- `create_conflict_uid_lookup_raw_candidates`
- `create_conflict_uid_lookup_diagnostics_path`
- `create_conflict_uid_query_raw_path`
- `create_conflict_collection_scan_raw_path`
- `create_conflict_candidate_ranking`
- `create_conflict_state_drift_report_path`
- `create_conflict_state_drift_report_status`
- `create_conflict_remote_fetch_error`
- `request_method`
- `request_url`
- `request_headers`
- `response_headers`
- `response_body_excerpt`

`data/sync_state.json` にはイベントごとのローカル同期状態を保存します。PoC 第14段階では active event と tombstone を別領域で管理し、再出現イベントの UID 再利用にも tombstone を使います。

- `events`: CalDAV 上で生存しているとみなす active event の state
- `tombstones`: delete 完了または「すでに存在しないことを確認できた」イベントの tombstone

`events` の各要素には次を保存します。

- `event_id`
- `ics_uid`
- `updated_at`
- `content_hash`
- `sequence`
- `is_deleted`
- `last_synced_at`
- `resource_url`
- `etag`
- `last_delivery_status`
- `last_delivery_at`

`tombstones` の各要素には少なくとも次を保存します。

- `event_id`
- `ics_uid`
- `deleted_at`
- `last_delivery_status`
- `resource_url`
- `etag`
- `last_delivery_at`

`is_deleted` と `deleted_at` の役割は分けています。

- `events[*].is_deleted`: 旧構造との互換のために残す active event 用フラグで、PoC 第13段階では常に `false`
- `tombstones[*].deleted_at`: delete が確定した時刻。削除状態の真実源は `tombstones` 側です

PoC 第15段階では `load_sync_state()` の時点で整合性チェックを行い、壊れた `data/sync_state.json` を黙って補正せず fail-fast で停止します。主な検証項目は次の通りです。

- top-level の `version` / `events` / `tombstones` が存在し、`version` は現在の `3` であること
- active event に必要な `event_id` / `ics_uid` / `updated_at` / `content_hash` / `sequence` / `is_deleted` / `last_synced_at` が揃っていること
- tombstone に必要な `event_id` / `ics_uid` / `deleted_at` / `last_delivery_status` が揃っていること
- 同じ `event_id` が `events` と `tombstones` に同時存在しないこと
- 同じ `ics_uid` が複数の active event / tombstone に重複していないこと
- `events[*].is_deleted` は常に `false` で、削除済み状態は `tombstones` だけで表現すること

不整合が見つかった場合は、そのまま同期を止めて「どの `event_id` / `ics_uid` / 項目が問題か」を含むエラーメッセージを出します。これにより delete 後の state 遷移や tombstone からの再出現時に、壊れた state を前提に誤同期するリスクを下げます。

PoC 第16段階では、この整合性チェックを `load_sync_state()` だけでなく `save_sync_state()` の直前にも必ず適用します。つまり validation ロジックは load/save で共通化されており、メモリ上で壊れた state を生成してしまっても `data/sync_state.json` へは書き出さず、その場で fail-fast します。

PoC 第18段階では、`build_next_sync_state_from_delivery()` が返す直前にも同じ validation を適用し、壊れた state を「保存時まで持ち越す」のではなく、生成直後に fail-fast で止めます。

PoC 第19段階では、`build_next_sync_state()` にも同じ共通 validation を適用し、主要な build 系 API はどの経路でも返却直前に fail-fast で invariant を検証します。

PoC 第20段階では、`main.py` が `SyncStateValidationError` を `load` / `build` / `save` の各段階で個別に捕捉し、`[sync_state:load]` のような共通 prefix で表示します。`load` / `save` では対象の `sync_state.json` パスも出し、運用時に「どの段階で壊れたか」を追いやすくします。

PoC 第21段階では、`load_sync_state()` の JSON 破損 (`JSONDecodeError`) も path と line/column 付きの独自例外へ包み、`main.py` で `[sync_state:load]` prefix のまま `validation failed` と `json decode failed` を見分けて表示できるようにしました。

PoC 第22段階では、これらの `sync_state` 失敗を人間向け表示に加えて構造化ログでも残します。`component=sync_state`、`phase=load|build|save`、`error_kind=validation_failed|json_decode_failed|io_failed|other`、`path` を最低限出し、エラー詳細から拾える場合だけ `event_id` / `ics_uid` も付けます。`load` / `save` の `path` には `sync_state.json` の実パスを、`build` の `path` には build source 名を入れます。

PoC 第23段階では、同じ key 名に寄せて `sync_plan` save failure と CalDAV delivery failure も structured log に追加しました。`sync_plan` は `component=sync_plan` / `phase=build|save` / `error_kind=io_failed|other` / `path` を出し、CalDAV delivery failure は `component=caldav` / `phase=deliver` / `error_kind` / `event_id` / `ics_uid` / `action` を最低限出します。CalDAV 側では必要に応じて `resource_url` / `status_code` / `conflict_kind` / `resolution_strategy` / `retryable` も併記されます。PoC 第24段階ではこれに加えて、`CalDAVClient.sync()` 自体が例外で中断した場合も `component=caldav` / `phase=sync` で structured log を残し、`action` / `event_id` / `ics_uid` / `resource_url` が取れるときは同じキーで出します。

## 運用復旧メモ

### まずテスト用カレンダーで運用する

- 個人運用に入る前でも、最初の確認先は本番カレンダーではなくテスト用カレンダーにしてください
- `CALDAV_DRY_RUN=true` で差分を確認したあと、同じテスト用カレンダーに対して `CALDAV_DRY_RUN=false` を試し、`data/caldav_sync_result.json` と `data/sync_state.json` の更新結果を見てから本番へ切り替えるのが安全です

### 障害時の最短復旧フロー

`sync_state.json` の JSON 破損や validation error が出たら、まずは補助 CLI で `sync_state.json` を安全に戻します。デフォルトでは `data/sync_state.json` と `data/backups/` を使います。

```bash
source .venv/bin/activate
python -m src.sync_state_backup list
```

`list` ではバックアップ件数、最新、最古が見えるので、戻す候補を先に絞れます。

次に、戻したいバックアップを `--validate` 付きで復元します。現行の `data/sync_state.json` がある場合は、上書き前に `data/backups/` へ自動で再退避します。

```bash
source .venv/bin/activate
python -m src.sync_state_backup restore sync_state-20260313-091530.json --validate
```

その後、必ずテスト用カレンダーに対して `CALDAV_DRY_RUN=true` で 1 回流し、復旧後の差分が安全かを確認します。

```bash
source .venv/bin/activate
CALDAV_DRY_RUN=true python -m src.main
```

最短でも次の順序は崩さない運用を推奨します。

1. `python -m src.sync_state_backup list`
2. `python -m src.sync_state_backup restore <backup-file> --validate`
3. `CALDAV_DRY_RUN=true python -m src.main`
4. テスト用カレンダーで必要なら `CALDAV_DRY_RUN=false python -m src.main`
5. ここで問題がなければ本番カレンダーへ戻す

### 復旧後チェックリスト

`restore --validate` のあとに、必ず次を確認してください。

- `CALDAV_DRY_RUN=true python -m src.main` が validation error なしで完走する
- `data/sync_plan.json` の `create` / `update` / `delete` 件数が想定範囲内で、復旧前に存在していた予定が大量再作成・大量削除になっていない
- `data/caldav_sync_result.json` の `failure_count` が `0` で、`processed_count` と `ignored_count` の内訳が読み取れる
- ログに `component=sync_state` / `component=caldav` の異常系が追加で出ていない
- 復元した `data/sync_state.json` がそのまま読み込めており、必要なら `python -m src.sync_state_backup backup` で復旧後の安定点を再退避しておく

### dry-run warning が出たときの確認手順

- `restore --validate` 直後なら、いま読み込んでいる `data/sync_state.json` が意図したバックアップ内容かを再確認する
- `data/sync_plan.json` を開き、`create` / `delete` の対象 event が想定外に偏っていないかを見る
- 代表予定を 3 件以上テスト用カレンダーで確認し、重複作成や想定外削除が起きそうでないことを目視で確かめる
- 問題がなければ、先にテスト用カレンダーで `CALDAV_DRY_RUN=false python -m src.main` を 1 回だけ流してから本番へ進む

### テスト用カレンダーで確認する項目

- 代表的な予定 3 件以上を選び、タイトル、開始終了時刻、終日予定、説明、場所が意図どおり見える
- 復旧対象に含まれていた更新予定が `update` として見えており、不要な重複予定が作られていない
- Garoon 側で削除済みの予定がテスト用カレンダーに残り続けていない
- 直近で触った recurring event や長時間予定があるなら、それも 1 件は目視確認する
- `CALDAV_DRY_RUN=false` をテスト用カレンダーで 1 回実行しても、`data/sync_state.json` の更新内容が想定どおりである

### 本番カレンダーへ戻す条件

- テスト用カレンダーで `CALDAV_DRY_RUN=true` の差分が想定どおり
- テスト用カレンダーで `CALDAV_DRY_RUN=false` を実行しても重複作成・想定外削除・validation error が出ない
- `data/caldav_sync_result.json` の失敗件数が `0` で、確認した代表予定が期待どおり反映されている
- 直前の `sync_state.json` をバックアップ済みで、必要ならすぐに `restore --validate` へ戻せる

### backup / prune の基本運用例

作業前の退避:

```bash
source .venv/bin/activate
python -m src.sync_state_backup backup
```

保有状況の確認:

```bash
source .venv/bin/activate
python -m src.sync_state_backup list
```

削除前の確認:

```bash
source .venv/bin/activate
python -m src.sync_state_backup prune --keep 10 --dry-run
```

実際の整理:

```bash
source .venv/bin/activate
python -m src.sync_state_backup prune --keep 10
```

`prune` は `data/backups/` 配下の `sync_state-*.json` だけを対象にし、現在使用中の `data/sync_state.json` は触りません。まず `--dry-run` で削除対象を確認し、問題なければ同じ `--keep` 値で本実行してください。`--keep` は 1 以上のみ受け付けるため、誤って全削除しにくい形にしています。個人運用では、直近の復旧ポイントを残しやすい `10` から `20` 世代を目安に保つ運用を推奨します。

### ログで確認すべき項目

- `component`: どの層の失敗か (`sync_state` / `sync_plan` / `caldav`)
- `phase`: どの段階で失敗または warning したか (`load` / `build` / `save` / `deliver` / `dry_run_review`)
- `error_kind`: `validation_failed` / `json_decode_failed` / `io_failed` / `etag_mismatch` / `anomalous_change_warning` などの分類
- `event_id` / `ics_uid` / `action`: CalDAV 失敗や state validation で影響範囲を特定するためのキー
- `path`: `sync_state.json` / `sync_plan.json` の対象パス、または build source 名
- `status_code` / `conflict_kind` / `resolution_strategy` / `resource_url`: CalDAV delivery failure の深掘りに使う追加情報
- `create_conflict_resource_exists` / `create_conflict_uid_match_found` / `create_conflict_uid_lookup_attempted` / `create_conflict_uid_lookup_candidates` / `create_conflict_uid_lookup_method` / `create_conflict_remote_uid_confirmed` / `create_conflict_state_drift_suspected` / `create_conflict_existing_resource_url`: `create 412` で remote existing / state drift と UID lookup 不足を切り分ける追加情報
- `phase=discovery`: iCloud 系サーバー向けの CalDAV discovery ログです。`root_url` / `principal_url` / `calendar_home_url` / `calendar_url` / `calendar_name` を見ると、どこまで解決できたかを追えます
- `phase=sync`: CalDAV 全体処理が途中中断したログです。`processed_count` / `remaining_count` / `total_count` / `action_index` を見ると、「どこまで進んで、どの action で止まったか」を追いやすくなります

PoC 第14段階の state 更新ルールは次の通りです。

- `dry-run=true`: `sync_state.json` は更新しません
- `dry-run=false`: `create` / `update` は最終的に CalDAV 実送信が成功したイベントだけ更新します
- `delete` は最終的に削除完了が確認できたイベントだけ `events` から外して `tombstones` に保存します
- `event_id` が `tombstones` に残ったまま Garoon 側で再出現した場合は、tombstone の `ics_uid` を再利用して `create` として扱います
- 再出現 `create` 成功時は `tombstones` から削除し、`events` に戻します
- 再出現 `create` 失敗時は tombstone を残し、active event は復元しません
- 送信失敗したイベントは更新しません
- `412` / `409` などの競合結果でも更新しません
- `412 etag_mismatch` で recovery 後の 1 回限り自動再送が成功した場合は、通常の成功送信と同じ扱いで更新します
- ただし read path recovery が成功した失敗結果については、既存 state の `resource_url` / `etag` だけを補正できます
- recovery 補正では `updated_at` / `content_hash` / `sequence` / `last_synced_at` / `last_delivery_status` / `last_delivery_at` は進めません
- `delete` 失敗時は tombstone を確定せず、既存 `events` の通常情報を残します
- `skip` は更新しません
- `resource_url` / `etag` は CalDAV 応答から取得できた場合のみ保存します
- `create` 成功時の `sequence` は `0` を維持します
- `update` 送信時の payload は `sync_state.json` の現在値を基準に `+1` した `sequence` を使います
- `update` 成功時は、送信 payload に使った `sequence` をそのまま `sync_state.json` に保存します
- `update` 失敗時、`dry-run=true`、recovery-only 成功時は `sequence` を増やしません
- `etag_mismatch` からの自動再送に成功した場合でも、同じ update payload を再送するだけなので `sequence` の増分は 1 回分です

`SEQUENCE` ルールの要点:

- 初回 `create` は常に `0`
- `update` の payload は前回成功値を基準に `+1`
- `sync_plan.json` の `sequence` と `calendar.ics` の `SEQUENCE` は送信予定 payload の値です
- `caldav_sync_result.json` には互換用の `sequence` に加えて `payload_sequence` を保存します
- `sync_state.json` には「最終的に成功した payload の `sequence`」だけを保存します

初回の実送信成功後から `data/sync_state.json` に active event と tombstone が蓄積されます。2 回目以降は `event_id` ごとに前回 state と比較し、次の分類ができる状態になります。

- 新規イベント
- tombstone からの再出現イベント（`event_id` 一致）
- 更新イベント（`content_hash` または `updated_at` が変化）
- 変更なし
- 削除候補（前回 state にはあるが今回取得結果にないイベント）

この差分は `data/sync_plan.json` で次の同期アクションに変換されます。

- 新規イベント -> `create`
- tombstone からの再出現イベント -> `create`
- 更新イベント -> `update`
- 変更なし -> `skip`
- 削除候補 -> `delete`

`action_reason` には `new_event` / `reappeared_from_tombstone` / `content_changed` / `updated_at_changed` / `content_and_updated_at_changed` / `missing_from_current_fetch` などを保存し、なぜそのアクションになったかを追跡できるようにしています。再出現 `create` では `reappeared_from_tombstone=true` と `tombstone_deleted_at` も保存されます。

将来 iCloud 連携を追加する際に Garoon の生レスポンスに依存しすぎないよう、イベントモデルを一度正規化してから JSON / ICS に変換する構成にしています。

## CalDAV 同期

PoC 第14段階の CalDAV 同期は次の方針です。

PoC 第30段階では、`CALDAV_URL` をそのまま calendar collection として使わず、実送信時に次の discovery を 1 回だけ実行してから対象カレンダーを解決します。

1. `CALDAV_URL` に対して `current-user-principal` / `principal-URL` を問い合わせる
2. principal に対して `calendar-home-set` を問い合わせる
3. calendar-home 配下の calendar collection を列挙し、`CALDAV_CALENDAR_NAME` に一致する `displayname` を選ぶ

iCloud のように root 直下への単純な `PROPFIND` だけでは calendar collection を得られないサーバーでも、この discovery 経路で解決した URL に対して `create` / `update` / `delete` を行います。`dry-run=true` の挙動と state 更新ルールは変わりません。

iCloud で `create` が `412 Precondition Failed` になったときは、まず ICS 内容不正よりも remote existing / state drift を疑ってください。PoC 第37段階では、失敗した `resource_name` の URL 存在確認に加え、`ics_uid` の remote 検索を `UID` 条件付き `REPORT` と calendar collection 全体の remote ICS 比較まで広げ、その結果を `data/caldav_sync_result.json` と structured log に残します。PoC 第38段階では、さらに `create 412` かつ `create_conflict_existing_resource_url` がある result に対して、remote existing resource を read-only で `GET` し、local payload と remote resource の `UID` / `SUMMARY` / `DTSTART` / `DTEND` / `DESCRIPTION` 有無 / `LOCATION` 有無 / `SEQUENCE` / `LAST-MODIFIED` の比較レポートを `data/reports/` に JSON 保存します。PoC 第40段階では、この `caldav_sync_result.json` を read-only で集計し、`drift_report_status` ごとの件数、`drift_diff_count` ごとの件数、`drift_diff_fields` の組み合わせ頻度、代表 `event_id` 例、`remote_fetch_failed` 件数を `python -m src.caldav_sync_result_summary --result-path data/caldav_sync_result.json` で一覧できるようにしました。PoC 第41段階ではさらに、`SUMMARY` / `DESCRIPTION` / `LOCATION` / `DTSTART` / `DTEND` / `SEQUENCE` / `LAST-MODIFIED` を含む単体フィールド差分頻度も見られるようにしています。PoC 第44段階では、iCloud の `REPORT` multi-status に collection root と event resource href が混在しても、`create_conflict_existing_resource_url` は calendar collection root ではなく既存 event resource の href を指すように補正しました。相対 href が返るサーバーでも絶対 URL 化して保存するので、drift report の remote fetch はその実 resource URL に対して行われます。iCloud カレンダー上では既存イベントが見えているのに `create_conflict_uid_match_found=false` のままなら、いったん payload 差分よりも UID lookup を疑ってください。`python -m src.sync_plan_inspect --action create --result-path data/caldav_sync_result.json --only failed --conflict state-drift` で state drift 疑いだけを絞り込み、`drift_report_status` / `drift_report_path` 列から比較レポートを開くと確認しやすいです。

`create 412` の診断項目:

- `create_conflict_resource_exists`: 送信先 `resource_name` の URL に同名 resource がすでに存在したか
- `create_conflict_uid_match_found`: 同じ `ics_uid` を持つ event が remote に見つかったか
- `create_conflict_uid_lookup_attempted`: `create 412` 診断で UID lookup まで実行できたか
- `create_conflict_uid_lookup_candidates`: UID lookup で見えた candidate resource 数。`0` なら lookup miss、`1` 以上なら remote 側の見え方をさらに確認してください
- `create_conflict_uid_lookup_method`: 実行した lookup 手順。`calendar_query_uid_calendar_data` は UID 条件付き `REPORT`、`calendar_collection_scan_calendar_data` は calendar collection 内の remote ICS 直接比較を表します
- `create_conflict_uid_lookup_raw_candidates`: raw candidate 一覧。各要素に `href`、可能なら `parsed_remote_uid` / `summary` / `dtstart` / `dtend`、および `found_via` が入ります
- `create_conflict_uid_query_raw_path`: UID 条件付き `REPORT` の raw response 保存先。iCloud の multi-status 差異確認用です
- `create_conflict_collection_scan_raw_path`: collection scan `REPORT` の raw response 保存先。UID 条件付き `REPORT` と見比べる前提の read-only 診断です
- `create_conflict_remote_uid_confirmed`: 取得した remote ICS の `UID` を直接比較して一致確認できたか
- `create_conflict_state_drift_suspected`: 上のどちらかが真で、remote 実在に対してローカル state が `create` 扱いだった疑いがあるか
- `create_conflict_existing_resource_url`: 見つかった既存 resource の URL。`UID` 検索で見つかった event resource href を優先し、相対 href は絶対 URL 化して保存します。calendar collection root 自体は existing resource として扱いません
- `create_conflict_selected_candidate_reason`: `existing_resource_url` をどの根拠で選んだか。たとえば `confirmed_uid_match_from_calendar_query_uid_calendar_data` や `first_candidate_from_calendar_collection_scan_calendar_data`
- `create_conflict_selected_candidate_index`: `create_conflict_uid_lookup_raw_candidates` 内で selected candidate が何番目だったか。`0` 始まり
- `create_conflict_candidate_ranking`: summary 完全一致 / `DTSTART` 一致 / `DTEND` 一致 / summary 部分一致を score 化した read-only 補助ランキング。`create_conflict_remote_uid_confirmed=false` の間は自動補正根拠に使いません
- `create_conflict_uid_lookup_diagnostics_path`: raw candidate 一覧を別 JSON 保存した場合のパス
- `create_conflict_state_drift_report_path`: `data/reports/` に保存した read-only 比較レポートの JSON パス
- `create_conflict_state_drift_report_status`: レポート生成結果。`generated` なら比較済み、`remote_fetch_failed` なら remote existing resource の読取失敗、`report_write_failed` ならファイル保存失敗
- `create_conflict_remote_fetch_error`: remote existing resource を読めなかった場合のエラー
- `request_method` / `request_url`: 実際に送ろうとした `PUT` と対象 URL
- `request_headers`: `If-None-Match` / `If-Match` / `Content-Type` / `Content-Length` の要約
- `response_headers`: `ETag` / `Content-Type` / `Content-Length` / `Location` の要約
- `response_body_excerpt`: 応答本文がある場合の短い抜粋

生成される state drift レポート例:

```json
{
  "kind": "create_conflict_state_drift_report",
  "event_id": "evt-create",
  "ics_uid": "uid-create",
  "existing_resource_url": "https://caldav.example.com/calendars/tomo/poc/uid-create.ics",
  "remote_fetch": {
    "success": true,
    "error": null,
    "etag": "\"etag-existing\""
  },
  "local_event": {
    "uid": "uid-create",
    "summary": "Subject evt-create",
    "dtstart": "20260312T010000Z",
    "dtend": "20260312T020000Z",
    "has_description": true,
    "has_location": false,
    "sequence": "0",
    "last_modified": "20260312T000000Z"
  },
  "remote_event": {
    "uid": "uid-create",
    "summary": "Remote Subject",
    "dtstart": "20260312T010000Z",
    "dtend": "20260312T020000Z",
    "has_description": false,
    "has_location": true,
    "sequence": "4",
    "last_modified": "20260311T150000Z"
  },
  "comparison": {
    "SUMMARY": {
      "local": "Subject evt-create",
      "remote": "Remote Subject",
      "equal": false
    },
    "DESCRIPTION": {
      "local_present": true,
      "remote_present": false,
      "equal": false
    }
  }
}
```

`create 412` の次の確認観点:

- `request_headers.If-None-Match` が `*` になっているか。意図しない `If-Match` が付いていないか
- iCloud UI では既存イベントが見えるのに `create_conflict_uid_lookup_candidates=0` なら、まず UID lookup の不足や server 応答差を疑う
- `create_conflict_uid_lookup_candidates>0` かつ `create_conflict_remote_uid_confirmed=false` なら、候補は見えているが remote ICS 比較まで確定できていない
- `create_conflict_uid_query_raw_path` と `create_conflict_collection_scan_raw_path` が両方ある場合は、同じ event がどちらの `REPORT` でどう見えているかを raw XML 同士で比較する
- `create_conflict_candidate_ranking` は「今の selected を上書きするため」ではなく、「近傍候補が本当に別にいるか」を読むための補助情報です
- 複数 event の `create_conflict_existing_resource_url` が同じ `.ics` に寄っているなら、その drift report は比較相手自体を誤っている可能性があります。`create_conflict_uid_lookup_raw_candidates` と `create_conflict_selected_candidate_reason` を先に確認してください
- `request_headers.Content-Type` と `request_headers.Content-Length` が成功ケースと同じ傾向か
- `response_headers.ETag` / `response_headers.Location` / `response_body_excerpt` に、resource 競合や precondition のヒントが出ていないか

- `create`: 新規イベントを `UID` ベースの `.ics` オブジェクトとして登録します
- tombstone からの再出現でも CalDAV へは `create` を送ります。削除済み `resource_url` は再利用しません
- `update`: まず `sync_state.json` の `resource_url` を使って更新し、無い場合だけ `ics_uid` で既存イベントを検索して更新します
- `delete`: まず `sync_state.json` の `resource_url` を使って削除し、無い場合だけ `ics_uid` で既存イベントを検索して削除します
- `skip`: 送信しません
- `dry-run=true`: 実送信も state 更新も行いません
- `dry-run=false`: `create` / `update` / `delete` を実送信し、成功したイベントだけ state を更新します
- `update` / `delete` 失敗時は read path を使って最新 `resource_url` / `etag` の回復を試みます
- `update` / `delete` の `etag_mismatch` では、recovery で最新 `resource_url` / `etag` が取れた場合だけ、同一実行内で 1 回だけ条件付き自動再送します

`update` の解決順序:

1. `sync_state.json` の `resource_url`
2. `ics_uid` による CalDAV `REPORT` 検索

`delete` の解決順序も同じです。

1. `sync_state.json` の `resource_url`
2. `ics_uid` による CalDAV `REPORT` 検索

`update` の recovery read path:

1. 保存済み `resource_url` があれば、その URL に `HEAD`（必要なら `GET`）して最新 `etag` を取得します
2. `404` / `410` / stale URL 疑いの `412 precondition_failed` などで同じ URL から読めない場合は、`ics_uid` で `REPORT` 検索して最新 `resource_url` を再解決します
3. recovery 結果は `data/caldav_sync_result.json` の `recovery_*` / `refreshed_*` に保存します
4. `etag_mismatch` かつ最新 `resource_url` / `etag` を回復できたときだけ、その metadata を使って 1 回だけ条件付き自動再送します
5. `409 Conflict` は自動再送しません

`delete` の補足:

- `resource_url` がある delete では、まずその URL に `DELETE` します
- `etag` がある場合は、その値を `If-Match` に使う条件付き delete を行います
- 保存済み `resource_url` への delete が `404` / `410` になった場合は、同じ URL の read path の後に `UID` 検索へ再フォールバックします
- `resource_url` が無い場合だけ、最初から `UID` 検索します
- `UID` 検索で resource が見つからない場合は、「CalDAV 上ですでに存在しないことを確認できた idempotent delete 成功」とみなし、`DELETE` 実送信なしで tombstone を保存します
- stale な `resource_url` への delete が `404` / `410` で、`UID` 検索でも何も見つからない場合も同じく already absent 扱いで tombstone を保存します
- `delete` 失敗時は tombstone を確定しません。recovery が成功した場合だけ、active event 側の `resource_url` / `etag` を補正できます

`update` / `delete` 共通の補足:

- `resource_url` がある update では、まずその URL に `PUT` します
- `etag` がある場合は、その値を `If-Match` に使う条件付き update を行います
- 保存済み `etag` を使った条件付き送信が `412 Precondition Failed` になった場合は `etag_mismatch` として扱い、同じ URL への read path で最新 `etag` の回復を試みます
- `etag_mismatch` の自動再送条件は `action=update|delete` / `dry-run=false` / `recovery_succeeded=true` / `refreshed_resource_url!=null` / `refreshed_etag!=null` です
- 自動再送は recovery で取得した `refreshed_resource_url` / `refreshed_etag` を使う条件付き `PUT` / `DELETE` を 1 回だけ行います
- `409 Conflict` は競合系レスポンスとして扱いますが、`412` とは別の `conflict_kind` で結果に残し、read path で最新状態の回復を試みます
- 保存済み `resource_url` への update が `404` / `410`、または保存済み `etag` が無い状態での `412` になった場合は、同じ URL の read path の後に `UID` 検索へ再フォールバックします
- `dry-run` の挙動と `create` の挙動は変わりません
- `delete` の `payload_sequence` は `null` です

discovery 成功時の structured log 例:

```text
caldav discovery resolved component=caldav phase=discovery root_url=https://caldav.icloud.com/ principal_url=https://caldav.icloud.com/123456789/principal/ calendar_home_url=https://caldav.icloud.com/123456789/calendars/ calendar_url=https://caldav.icloud.com/123456789/calendars/poc/ calendar_name="PoC Calendar"
```

`data/sync_state.json` の tombstone を含む例:

```json
{
  "version": 3,
  "events": {
    "evt-1001": {
      "event_id": "evt-1001",
      "ics_uid": "garoon-event-evt-1001-abc123def456@garoon-icloud-sync.local",
      "updated_at": "2026-03-12T01:23:45Z",
      "content_hash": "5d872d7ef6d8f7d850f9df2ee6d4d4f0cae7e3da6fecc0a80259828e0c5d1b4b",
      "sequence": 1,
      "is_deleted": false,
      "last_synced_at": "2026-03-12T01:24:10+00:00",
      "resource_url": "https://caldav.example.com/calendars/tomo/poc/garoon-event-evt-1001.ics",
      "etag": "\"7b9230f1\"",
      "last_delivery_status": "success",
      "last_delivery_at": "2026-03-12T01:24:10+00:00"
    }
  },
  "tombstones": {
    "evt-2002": {
      "event_id": "evt-2002",
      "ics_uid": "garoon-event-evt-2002-fedcba654321@garoon-icloud-sync.local",
      "deleted_at": "2026-03-12T02:00:00+00:00",
      "last_delivery_status": "success",
      "resource_url": "https://caldav.example.com/calendars/tomo/poc/garoon-event-evt-2002.ics",
      "etag": "\"etag-delete\"",
      "last_delivery_at": "2026-03-12T02:00:00+00:00"
    }
  }
}
```

`data/caldav_sync_result.json` の delete 成功例:

```json
{
  "action": "delete",
  "event_id": "evt-2002",
  "ics_uid": "garoon-event-evt-2002-fedcba654321@garoon-icloud-sync.local",
  "sequence": 4,
  "payload_sequence": null,
  "success": true,
  "sent": true,
  "resource_url": "https://caldav.example.com/calendars/tomo/poc/garoon-event-evt-2002.ics",
  "etag": "\"etag-delete\"",
  "resolution_strategy": "sync_state_resource_url",
  "used_stored_resource_url": true,
  "uid_lookup_performed": false,
  "used_stored_etag": true,
  "attempted_conditional_update": true,
  "status_code": 204
}
```

`data/caldav_sync_result.json` の delete 失敗例:

```json
{
  "action": "delete",
  "event_id": "evt-2002",
  "ics_uid": "garoon-event-evt-2002-fedcba654321@garoon-icloud-sync.local",
  "sequence": 4,
  "payload_sequence": null,
  "success": false,
  "sent": false,
  "resource_url": "https://caldav.example.com/calendars/tomo/poc/garoon-event-evt-2002.ics",
  "etag": null,
  "resolution_strategy": "sync_state_resource_url",
  "used_stored_resource_url": true,
  "uid_lookup_performed": false,
  "used_stored_etag": true,
  "attempted_conditional_update": true,
  "recovery_attempted": true,
  "recovery_succeeded": true,
  "refreshed_resource_url": "https://caldav.example.com/calendars/tomo/poc/garoon-event-evt-2002.ics",
  "refreshed_etag": "\"etag-live-delete\"",
  "initial_resource_url": "https://caldav.example.com/calendars/tomo/poc/garoon-event-evt-2002.ics",
  "initial_etag": "\"etag-delete\"",
  "conflict_kind": "conflict",
  "retryable": true,
  "status_code": 409,
  "error": "DELETE https://caldav.example.com/calendars/tomo/poc/garoon-event-evt-2002.ics failed with 409: simulated failure"
}
```

`data/sync_plan.json` / `data/caldav_sync_result.json` / `data/sync_state.json` の再出現イベント例:

```json
{
  "sync_plan": {
    "action": "create",
    "event_id": "evt-3003",
    "ics_uid": "garoon-event-evt-3003-112233445566@garoon-icloud-sync.local",
    "sequence": 0,
    "action_reason": "reappeared_from_tombstone",
    "reappeared_from_tombstone": true,
    "tombstone_deleted_at": "2026-03-12T03:00:00+00:00"
  },
  "caldav_sync_result": {
    "action": "create",
    "event_id": "evt-3003",
    "ics_uid": "garoon-event-evt-3003-112233445566@garoon-icloud-sync.local",
    "sequence": 0,
    "payload_sequence": 0,
    "success": true,
    "sent": true,
    "resolution_strategy": "create_resource_name",
    "reappeared_from_tombstone": true,
    "tombstone_deleted_at": "2026-03-12T03:00:00+00:00",
    "resource_url": "https://caldav.example.com/calendars/tomo/poc/garoon-event-evt-3003.ics"
  },
  "sync_state": {
    "events": {
      "evt-3003": {
        "event_id": "evt-3003",
        "ics_uid": "garoon-event-evt-3003-112233445566@garoon-icloud-sync.local",
        "sequence": 0,
        "resource_url": "https://caldav.example.com/calendars/tomo/poc/garoon-event-evt-3003.ics",
        "etag": "\"etag-recreated\""
      }
    },
    "tombstones": {}
  }
}
```

## 制約

- `repeatInfo` は保持しますが、RRULE への完全変換はまだ実装していません
- 説明と運用は本番用カレンダーではなくテスト用カレンダー前提です

## 今後の拡張

- 繰り返し予定の削除粒度を `RECURRENCE-ID` 単位まで拡張する

## TODO

- Garoon 環境ごとの差異を踏まえた認証方式の確定
- 必要に応じた Garoon API パラメータ追加とページング戦略の見直し
- `GAROON_TARGET_CALENDAR` を使う取得パスの仕様確認
- CalDAV 経由の iCloud 連携
- Garoon の `repeatInfo` から RRULE / RECURRENCE-ID をどう構成するかの確定
- `isStartOnly` とタイムゾーン差分がある予定の扱い確認
- `content_hash` の対象項目と CalDAV 差分判定条件の最終確定

今回の PoC では、Garoon REST API のスケジュール取得エンドポイントを前提に、パスワード認証ヘッダーを利用する実装にしています。環境によっては追加仕様の確認が必要です。
