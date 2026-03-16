from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
import re

from dotenv import dotenv_values

DEFAULT_PROFILE_NAME = "default"
_PROFILE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class ConfigError(ValueError):
    """Raised when application configuration is invalid."""


@dataclass(frozen=True, slots=True)
class AppConfig:
    garoon_base_url: str
    garoon_username: str
    garoon_password: str
    garoon_start_days_offset: int
    garoon_end_days_offset: int
    output_json_path: Path
    log_level: str
    caldav_url: str
    caldav_username: str
    caldav_password: str
    caldav_calendar_name: str
    caldav_dry_run: bool
    dry_run_warn_create_count: int = 10
    dry_run_warn_delete_count: int = 10
    caldav_diagnostic_dump_failed_ics: bool = False
    caldav_diagnostic_dump_success_ics: bool = False
    caldav_diagnostic_dump_uid_lookup_json: bool = False
    garoon_target_user: str | None = None
    garoon_target_calendar: str | None = None
    profile_name: str = DEFAULT_PROFILE_NAME
    app_data_dir: Path | None = None
    sync_state_path: Path | None = None
    sync_plan_path: Path | None = None
    caldav_sync_result_path: Path | None = None
    ics_path: Path | None = None
    diagnostics_dir: Path | None = None
    reports_dir: Path | None = None
    backups_dir: Path | None = None
    logs_dir: Path | None = None
    log_file_path: Path | None = None
    run_summary_path: Path | None = None


def load_config(env_path: str | Path = ".env") -> AppConfig:
    resolved_env_path = Path(env_path).expanduser().resolve()
    file_values = dotenv_values(resolved_env_path)
    values = {**file_values, **os.environ}
    working_dir = Path.cwd().resolve()

    required_keys = (
        "GAROON_BASE_URL",
        "GAROON_USERNAME",
        "GAROON_PASSWORD",
        "GAROON_START_DAYS_OFFSET",
        "GAROON_END_DAYS_OFFSET",
        "LOG_LEVEL",
        "CALDAV_URL",
        "CALDAV_USERNAME",
        "CALDAV_PASSWORD",
        "CALDAV_CALENDAR_NAME",
    )
    missing = [key for key in required_keys if not values.get(key)]
    if missing:
        joined = ", ".join(missing)
        raise ConfigError(f"Missing required environment variables: {joined}")

    start_days_offset = _parse_int(
        "GAROON_START_DAYS_OFFSET",
        values["GAROON_START_DAYS_OFFSET"],
    )
    end_days_offset = _parse_int(
        "GAROON_END_DAYS_OFFSET",
        values["GAROON_END_DAYS_OFFSET"],
    )
    if start_days_offset > end_days_offset:
        raise ConfigError(
            "GAROON_START_DAYS_OFFSET must be less than or equal to "
            "GAROON_END_DAYS_OFFSET."
        )

    profile_name = _normalize_profile_name(values.get("PROFILE_NAME"))
    app_data_dir = _resolve_optional_path(values.get("APP_DATA_DIR"), working_dir=working_dir)
    output_json_path = _resolve_output_json_path(
        values.get("OUTPUT_JSON_PATH"),
        app_data_dir=app_data_dir,
        working_dir=working_dir,
    )
    data_dir = output_json_path.parent
    runtime_dir = app_data_dir or _infer_runtime_dir(data_dir)
    sync_state_path = data_dir / "sync_state.json"
    sync_plan_path = data_dir / "sync_plan.json"
    caldav_sync_result_path = data_dir / "caldav_sync_result.json"
    ics_path = data_dir / "calendar.ics"
    diagnostics_dir = data_dir / "diagnostics"
    reports_dir = data_dir / "reports"
    backups_dir = data_dir / "backups"
    logs_dir = runtime_dir / "logs"
    log_file_path = logs_dir / "garoon-icloud-sync.log"
    run_summary_path = data_dir / "run_summary.json"

    return AppConfig(
        garoon_base_url=_normalize_url("GAROON_BASE_URL", values["GAROON_BASE_URL"]),
        garoon_username=str(values["GAROON_USERNAME"]),
        garoon_password=str(values["GAROON_PASSWORD"]),
        garoon_start_days_offset=start_days_offset,
        garoon_end_days_offset=end_days_offset,
        output_json_path=output_json_path,
        log_level=str(values["LOG_LEVEL"]).upper(),
        caldav_url=_normalize_url("CALDAV_URL", values["CALDAV_URL"]),
        caldav_username=str(values["CALDAV_USERNAME"]),
        caldav_password=str(values["CALDAV_PASSWORD"]),
        caldav_calendar_name=_normalize_non_empty("CALDAV_CALENDAR_NAME", values["CALDAV_CALENDAR_NAME"]),
        caldav_dry_run=_parse_bool("CALDAV_DRY_RUN", values.get("CALDAV_DRY_RUN", "true")),
        dry_run_warn_create_count=_parse_threshold_count(
            "DRY_RUN_WARN_CREATE_COUNT",
            values,
            default=10,
        ),
        dry_run_warn_delete_count=_parse_threshold_count(
            "DRY_RUN_WARN_DELETE_COUNT",
            values,
            default=10,
        ),
        caldav_diagnostic_dump_failed_ics=_parse_bool(
            "CALDAV_DIAGNOSTIC_DUMP_FAILED_ICS",
            values.get("CALDAV_DIAGNOSTIC_DUMP_FAILED_ICS", "false"),
        ),
        caldav_diagnostic_dump_success_ics=_parse_bool(
            "CALDAV_DIAGNOSTIC_DUMP_SUCCESS_ICS",
            values.get("CALDAV_DIAGNOSTIC_DUMP_SUCCESS_ICS", "false"),
        ),
        caldav_diagnostic_dump_uid_lookup_json=_parse_bool(
            "CALDAV_DIAGNOSTIC_DUMP_UID_LOOKUP_JSON",
            values.get("CALDAV_DIAGNOSTIC_DUMP_UID_LOOKUP_JSON", "false"),
        ),
        garoon_target_user=_empty_to_none(values.get("GAROON_TARGET_USER")),
        garoon_target_calendar=_empty_to_none(values.get("GAROON_TARGET_CALENDAR")),
        profile_name=profile_name,
        app_data_dir=runtime_dir,
        sync_state_path=sync_state_path,
        sync_plan_path=sync_plan_path,
        caldav_sync_result_path=caldav_sync_result_path,
        ics_path=ics_path,
        diagnostics_dir=diagnostics_dir,
        reports_dir=reports_dir,
        backups_dir=backups_dir,
        logs_dir=logs_dir,
        log_file_path=log_file_path,
        run_summary_path=run_summary_path,
    )


def _parse_int(name: str, raw_value: object) -> int:
    try:
        return int(str(raw_value))
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be an integer.") from exc


def _normalize_url(name: str, raw_url: object) -> str:
    value = str(raw_url).strip().rstrip("/")
    if not value:
        raise ConfigError(f"{name} must not be empty.")
    return value


def _normalize_non_empty(name: str, value: object) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ConfigError(f"{name} must not be empty.")
    return normalized


def _parse_bool(name: str, raw_value: object) -> bool:
    normalized = str(raw_value).strip().lower()
    truthy = {"1", "true", "yes", "on"}
    falsy = {"0", "false", "no", "off"}
    if normalized in truthy:
        return True
    if normalized in falsy:
        return False
    raise ConfigError(f"{name} must be a boolean value.")


def _parse_threshold_count(
    name: str,
    values: Mapping[str, object],
    *,
    default: int,
) -> int:
    raw_value = values.get(name, default)
    value = _parse_int(name, raw_value)
    if value < 1:
        raise ConfigError(f"{name} must be greater than or equal to 1.")
    return value


def _empty_to_none(value: object) -> str | None:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped or None


def _resolve_output_json_path(
    raw_value: object,
    *,
    app_data_dir: Path | None,
    working_dir: Path,
) -> Path:
    if raw_value is not None and str(raw_value).strip():
        return _resolve_path(str(raw_value), working_dir=working_dir)
    base_dir = app_data_dir or working_dir
    return (base_dir / "data" / "events.json").resolve()


def _resolve_optional_path(raw_value: object, *, working_dir: Path) -> Path | None:
    if raw_value is None:
        return None
    normalized = str(raw_value).strip()
    if not normalized:
        return None
    return _resolve_path(normalized, working_dir=working_dir)


def _resolve_path(raw_value: str, *, working_dir: Path) -> Path:
    path = Path(raw_value).expanduser()
    if not path.is_absolute():
        path = working_dir / path
    return path.resolve()


def _infer_runtime_dir(data_dir: Path) -> Path:
    if data_dir.name == "data":
        return data_dir.parent.resolve()
    return data_dir.resolve()


def _normalize_profile_name(raw_value: object) -> str:
    if raw_value is None:
        return DEFAULT_PROFILE_NAME
    normalized = str(raw_value).strip()
    if not normalized:
        return DEFAULT_PROFILE_NAME
    if not _PROFILE_NAME_PATTERN.fullmatch(normalized):
        raise ConfigError("PROFILE_NAME must match ^[A-Za-z0-9][A-Za-z0-9._-]*$.")
    return normalized
