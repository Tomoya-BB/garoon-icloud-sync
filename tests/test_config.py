from __future__ import annotations

from pathlib import Path

import pytest

from src.config import DEFAULT_PROFILE_NAME, ConfigError, load_config


def test_load_config_resolves_relative_output_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "GAROON_BASE_URL=https://example.cybozu.com/g",
                "GAROON_USERNAME=test-user",
                "GAROON_PASSWORD=test-pass",
                "GAROON_START_DAYS_OFFSET=-1",
                "GAROON_END_DAYS_OFFSET=3",
                "OUTPUT_JSON_PATH=data/events.json",
                "LOG_LEVEL=debug",
                "CALDAV_URL=https://caldav.example.com/",
                "CALDAV_USERNAME=calendar-user",
                "CALDAV_PASSWORD=calendar-pass",
                "CALDAV_CALENDAR_NAME=PoC Calendar",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("OUTPUT_JSON_PATH", raising=False)
    monkeypatch.delenv("DRY_RUN_WARN_CREATE_COUNT", raising=False)
    monkeypatch.delenv("DRY_RUN_WARN_DELETE_COUNT", raising=False)

    config = load_config(env_path)

    assert config.garoon_base_url == "https://example.cybozu.com/g"
    assert config.garoon_start_days_offset == -1
    assert config.garoon_end_days_offset == 3
    assert config.output_json_path == (tmp_path / "data" / "events.json").resolve()
    assert config.log_level == "DEBUG"
    assert config.caldav_url == "https://caldav.example.com"
    assert config.caldav_calendar_name == "PoC Calendar"
    assert config.caldav_dry_run is True
    assert config.dry_run_warn_create_count == 10
    assert config.dry_run_warn_delete_count == 10
    assert config.caldav_diagnostic_dump_failed_ics is False
    assert config.caldav_diagnostic_dump_success_ics is False
    assert config.caldav_diagnostic_dump_uid_lookup_json is False


def test_load_config_raises_for_missing_required_values(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("GAROON_BASE_URL=https://example.cybozu.com/g\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="Missing required environment variables"):
        load_config(env_path)


def test_load_config_reads_dry_run_warning_thresholds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "GAROON_BASE_URL=https://example.cybozu.com/g",
                "GAROON_USERNAME=test-user",
                "GAROON_PASSWORD=test-pass",
                "GAROON_START_DAYS_OFFSET=-1",
                "GAROON_END_DAYS_OFFSET=3",
                "OUTPUT_JSON_PATH=data/events.json",
                "LOG_LEVEL=info",
                "CALDAV_URL=https://caldav.example.com/",
                "CALDAV_USERNAME=calendar-user",
                "CALDAV_PASSWORD=calendar-pass",
                "CALDAV_CALENDAR_NAME=PoC Calendar",
                "DRY_RUN_WARN_CREATE_COUNT=4",
                "DRY_RUN_WARN_DELETE_COUNT=7",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.delenv("DRY_RUN_WARN_CREATE_COUNT", raising=False)
    monkeypatch.delenv("DRY_RUN_WARN_DELETE_COUNT", raising=False)

    config = load_config(env_path)

    assert config.dry_run_warn_create_count == 4
    assert config.dry_run_warn_delete_count == 7


def test_load_config_rejects_non_positive_dry_run_warning_threshold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "GAROON_BASE_URL=https://example.cybozu.com/g",
                "GAROON_USERNAME=test-user",
                "GAROON_PASSWORD=test-pass",
                "GAROON_START_DAYS_OFFSET=-1",
                "GAROON_END_DAYS_OFFSET=3",
                "OUTPUT_JSON_PATH=data/events.json",
                "LOG_LEVEL=info",
                "CALDAV_URL=https://caldav.example.com/",
                "CALDAV_USERNAME=calendar-user",
                "CALDAV_PASSWORD=calendar-pass",
                "CALDAV_CALENDAR_NAME=PoC Calendar",
                "DRY_RUN_WARN_CREATE_COUNT=0",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.delenv("DRY_RUN_WARN_CREATE_COUNT", raising=False)
    monkeypatch.delenv("DRY_RUN_WARN_DELETE_COUNT", raising=False)

    with pytest.raises(ConfigError, match="DRY_RUN_WARN_CREATE_COUNT must be greater than or equal to 1"):
        load_config(env_path)


def test_load_config_reads_caldav_diagnostic_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "GAROON_BASE_URL=https://example.cybozu.com/g",
                "GAROON_USERNAME=test-user",
                "GAROON_PASSWORD=test-pass",
                "GAROON_START_DAYS_OFFSET=-1",
                "GAROON_END_DAYS_OFFSET=3",
                "OUTPUT_JSON_PATH=data/events.json",
                "LOG_LEVEL=info",
                "CALDAV_URL=https://caldav.example.com/",
                "CALDAV_USERNAME=calendar-user",
                "CALDAV_PASSWORD=calendar-pass",
                "CALDAV_CALENDAR_NAME=PoC Calendar",
                "CALDAV_DIAGNOSTIC_DUMP_FAILED_ICS=true",
                "CALDAV_DIAGNOSTIC_DUMP_SUCCESS_ICS=yes",
                "CALDAV_DIAGNOSTIC_DUMP_UID_LOOKUP_JSON=on",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.delenv("CALDAV_DIAGNOSTIC_DUMP_FAILED_ICS", raising=False)
    monkeypatch.delenv("CALDAV_DIAGNOSTIC_DUMP_SUCCESS_ICS", raising=False)
    monkeypatch.delenv("CALDAV_DIAGNOSTIC_DUMP_UID_LOOKUP_JSON", raising=False)

    config = load_config(env_path)

    assert config.caldav_diagnostic_dump_failed_ics is True
    assert config.caldav_diagnostic_dump_success_ics is True
    assert config.caldav_diagnostic_dump_uid_lookup_json is True


def test_load_config_builds_profile_runtime_paths_when_output_path_is_omitted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    env_path = tmp_path / ".env"
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
                "LOG_LEVEL=info",
                "CALDAV_URL=https://caldav.example.com/",
                "CALDAV_USERNAME=calendar-user",
                "CALDAV_PASSWORD=calendar-pass",
                "CALDAV_CALENDAR_NAME=PoC Calendar",
                "CALDAV_DRY_RUN=false",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("OUTPUT_JSON_PATH", raising=False)
    monkeypatch.delenv("APP_DATA_DIR", raising=False)
    monkeypatch.delenv("PROFILE_NAME", raising=False)

    config = load_config(env_path)

    runtime_dir = (tmp_path / "runtime" / "profiles" / "tomoya").resolve()
    data_dir = runtime_dir / "data"
    assert config.profile_name == "tomoya"
    assert config.app_data_dir == runtime_dir
    assert config.output_json_path == data_dir / "events.json"
    assert config.sync_state_path == data_dir / "sync_state.json"
    assert config.sync_plan_path == data_dir / "sync_plan.json"
    assert config.caldav_sync_result_path == data_dir / "caldav_sync_result.json"
    assert config.ics_path == data_dir / "calendar.ics"
    assert config.diagnostics_dir == data_dir / "diagnostics"
    assert config.reports_dir == data_dir / "reports"
    assert config.backups_dir == data_dir / "backups"
    assert config.logs_dir == runtime_dir / "logs"
    assert config.run_summary_path == data_dir / "run_summary.json"


def test_load_config_defaults_profile_name_when_unspecified(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "GAROON_BASE_URL=https://example.cybozu.com/g",
                "GAROON_USERNAME=test-user",
                "GAROON_PASSWORD=test-pass",
                "GAROON_START_DAYS_OFFSET=0",
                "GAROON_END_DAYS_OFFSET=92",
                "LOG_LEVEL=info",
                "CALDAV_URL=https://caldav.example.com/",
                "CALDAV_USERNAME=calendar-user",
                "CALDAV_PASSWORD=calendar-pass",
                "CALDAV_CALENDAR_NAME=PoC Calendar",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("PROFILE_NAME", raising=False)
    monkeypatch.delenv("APP_DATA_DIR", raising=False)
    monkeypatch.delenv("OUTPUT_JSON_PATH", raising=False)

    config = load_config(env_path)

    assert config.profile_name == DEFAULT_PROFILE_NAME
    assert config.output_json_path == (tmp_path / "data" / "events.json").resolve()


def test_load_config_resolves_profile_runtime_relative_to_working_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
                "LOG_LEVEL=info",
                "CALDAV_URL=https://caldav.example.com/",
                "CALDAV_USERNAME=calendar-user",
                "CALDAV_PASSWORD=calendar-pass",
                "CALDAV_CALENDAR_NAME=PoC Calendar",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("APP_DATA_DIR", raising=False)
    monkeypatch.delenv("PROFILE_NAME", raising=False)
    monkeypatch.delenv("OUTPUT_JSON_PATH", raising=False)

    config = load_config(env_path)

    runtime_dir = (tmp_path / "runtime" / "profiles" / "tomoya").resolve()
    assert config.app_data_dir == runtime_dir
    assert config.sync_state_path == runtime_dir / "data" / "sync_state.json"
