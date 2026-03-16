from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import src.sync_state_backup as sync_state_backup_module


def test_backup_command_creates_timestamped_copy(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    state_path = tmp_path / "data" / "sync_state.json"
    _write_sync_state(state_path, event_id="evt-current")
    monkeypatch.setattr(
        sync_state_backup_module,
        "_now",
        lambda: datetime(2026, 3, 13, 9, 15, 30, tzinfo=timezone.utc),
    )

    exit_code = sync_state_backup_module.main(
        ["backup", "--state-path", str(state_path)]
    )

    captured = capsys.readouterr()
    backup_path = state_path.parent / "backups" / "sync_state-20260313-091530.json"
    assert exit_code == 0
    assert backup_path.exists()
    assert backup_path.read_text(encoding="utf-8") == state_path.read_text(encoding="utf-8")
    assert captured.err == ""
    assert f"backup: {backup_path}" in captured.out


def test_list_command_shows_available_backups(tmp_path: Path, capsys) -> None:
    state_path = tmp_path / "data" / "sync_state.json"
    backup_dir = state_path.parent / "backups"
    backup_dir.mkdir(parents=True)
    (backup_dir / "sync_state-20260313-101500.json").write_text("{}", encoding="utf-8")
    (backup_dir / "sync_state-20260312-101500.json").write_text("{}", encoding="utf-8")

    exit_code = sync_state_backup_module.main(
        ["list", "--state-path", str(state_path)]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Available sync_state backups" in captured.out
    assert "count: 2" in captured.out
    assert "latest: sync_state-20260313-101500.json" in captured.out
    assert "oldest: sync_state-20260312-101500.json" in captured.out
    assert "sync_state-20260313-101500.json" in captured.out
    assert "sync_state-20260312-101500.json" in captured.out
    assert captured.err == ""


def test_list_command_prints_clear_message_when_no_backups_exist(
    tmp_path: Path,
    capsys,
) -> None:
    state_path = tmp_path / "data" / "sync_state.json"

    exit_code = sync_state_backup_module.main(
        ["list", "--state-path", str(state_path)]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "No sync_state backups found." in captured.out
    assert "python -m src.sync_state_backup backup" in captured.out
    assert captured.err == ""


def test_restore_command_restores_backup_and_rebacks_up_current(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    state_path = tmp_path / "data" / "sync_state.json"
    backup_dir = state_path.parent / "backups"
    backup_dir.mkdir(parents=True)
    _write_sync_state(state_path, event_id="evt-current")
    backup_path = backup_dir / "sync_state-20260313-080000.json"
    _write_sync_state(backup_path, event_id="evt-restored")
    monkeypatch.setattr(
        sync_state_backup_module,
        "_now",
        lambda: datetime(2026, 3, 13, 9, 15, 30, tzinfo=timezone.utc),
    )

    exit_code = sync_state_backup_module.main(
        [
            "restore",
            "sync_state-20260313-080000.json",
            "--state-path",
            str(state_path),
            "--validate",
        ]
    )

    captured = capsys.readouterr()
    current_backup_path = backup_dir / "sync_state-pre-restore-20260313-091530.json"
    assert exit_code == 0
    assert json.loads(state_path.read_text(encoding="utf-8"))["events"]["evt-restored"]["event_id"] == "evt-restored"
    assert json.loads(current_backup_path.read_text(encoding="utf-8"))["events"]["evt-current"]["event_id"] == "evt-current"
    assert f"previous_current_backup: {current_backup_path}" in captured.out
    assert captured.err == ""


def test_prune_command_keeps_newest_backups_and_deletes_older_ones(
    tmp_path: Path,
    capsys,
) -> None:
    state_path = tmp_path / "data" / "sync_state.json"
    backup_dir = state_path.parent / "backups"
    backup_dir.mkdir(parents=True)
    newest = backup_dir / "sync_state-20260313-103000.json"
    middle = backup_dir / "sync_state-20260313-093000.json"
    oldest = backup_dir / "sync_state-20260313-083000.json"
    for path in (newest, middle, oldest):
        path.write_text("{}", encoding="utf-8")
    unrelated = backup_dir / "notes.txt"
    unrelated.write_text("keep me", encoding="utf-8")

    exit_code = sync_state_backup_module.main(
        ["prune", "--state-path", str(state_path), "--keep", "2"]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert newest.exists()
    assert middle.exists()
    assert not oldest.exists()
    assert unrelated.exists()
    assert "Pruned sync_state backups" in captured.out
    assert "total_found: 3" in captured.out
    assert "keep: 2" in captured.out
    assert "kept_count: 2" in captured.out
    assert "deleted_count: 1" in captured.out
    assert "deleted: sync_state-20260313-083000.json" in captured.out
    assert captured.err == ""


def test_prune_command_dry_run_does_not_delete_files(
    tmp_path: Path,
    capsys,
) -> None:
    state_path = tmp_path / "data" / "sync_state.json"
    backup_dir = state_path.parent / "backups"
    backup_dir.mkdir(parents=True)
    newest = backup_dir / "sync_state-20260313-103000.json"
    oldest = backup_dir / "sync_state-20260313-083000.json"
    newest.write_text("{}", encoding="utf-8")
    oldest.write_text("{}", encoding="utf-8")

    exit_code = sync_state_backup_module.main(
        ["prune", "--state-path", str(state_path), "--keep", "1", "--dry-run"]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert newest.exists()
    assert oldest.exists()
    assert "Dry run: would prune sync_state backups" in captured.out
    assert "deleted_count: 1" in captured.out
    assert "would_delete: sync_state-20260313-083000.json" in captured.out
    assert captured.err == ""


def test_list_and_prune_use_embedded_timestamp_order_even_with_pre_restore_names(
    tmp_path: Path,
    capsys,
) -> None:
    state_path = tmp_path / "data" / "sync_state.json"
    backup_dir = state_path.parent / "backups"
    backup_dir.mkdir(parents=True)
    old_pre_restore = backup_dir / "sync_state-pre-restore-20260313-073000.json"
    newest = backup_dir / "sync_state-20260313-093000.json"
    middle = backup_dir / "sync_state-20260313-083000.json"
    for path in (old_pre_restore, newest, middle):
        path.write_text("{}", encoding="utf-8")

    list_exit_code = sync_state_backup_module.main(
        ["list", "--state-path", str(state_path)]
    )
    list_output = capsys.readouterr()

    prune_exit_code = sync_state_backup_module.main(
        ["prune", "--state-path", str(state_path), "--keep", "2"]
    )
    prune_output = capsys.readouterr()

    assert list_exit_code == 0
    assert "latest: sync_state-20260313-093000.json" in list_output.out
    assert "oldest: sync_state-pre-restore-20260313-073000.json" in list_output.out
    assert prune_exit_code == 0
    assert newest.exists()
    assert middle.exists()
    assert not old_pre_restore.exists()
    assert "deleted: sync_state-pre-restore-20260313-073000.json" in prune_output.out
    assert prune_output.err == ""


def test_backup_command_fails_when_sync_state_file_is_missing(
    tmp_path: Path,
    capsys,
) -> None:
    state_path = tmp_path / "data" / "sync_state.json"

    exit_code = sync_state_backup_module.main(
        ["backup", "--state-path", str(state_path)]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert f"sync_state file was not found: {state_path}" in captured.err


def test_resolve_sync_state_path_uses_profile_env_runtime_relative_to_working_directory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    env_path = tmp_path / "runtime" / "profiles" / "tomoya" / ".env"
    env_path.parent.mkdir(parents=True)
    env_path.write_text(
        "\n".join(
            [
                "PROFILE_NAME=tomoya",
                "APP_DATA_DIR=runtime/profiles/tomoya",
                "GAROON_BASE_URL=https://example.cybozu.com/g",
                "GAROON_USERNAME=test-user",
                "GAROON_PASSWORD=test-pass",
                "GAROON_START_DAYS_OFFSET=0",
                "GAROON_END_DAYS_OFFSET=92",
                "LOG_LEVEL=INFO",
                "CALDAV_URL=https://caldav.example.com/",
                "CALDAV_USERNAME=calendar-user",
                "CALDAV_PASSWORD=calendar-pass",
                "CALDAV_CALENDAR_NAME=PoC Calendar",
            ]
        ),
        encoding="utf-8",
    )

    state_path = sync_state_backup_module.resolve_sync_state_path(
        state_path_arg=None,
        env_path=str(env_path),
    )

    assert state_path == (tmp_path / "runtime" / "profiles" / "tomoya" / "data" / "sync_state.json").resolve()


def _write_sync_state(path: Path, *, event_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": 3,
                "events": {
                    event_id: {
                        "event_id": event_id,
                        "ics_uid": f"{event_id}@example.com",
                        "updated_at": "2026-03-12T10:00:00Z",
                        "content_hash": f"hash-{event_id}",
                        "sequence": 1,
                        "is_deleted": False,
                        "last_synced_at": "2026-03-12T10:05:00+00:00",
                    }
                },
                "tombstones": {},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
