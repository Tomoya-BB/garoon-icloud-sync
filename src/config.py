from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values


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


def load_config(env_path: str | Path = ".env") -> AppConfig:
    resolved_env_path = Path(env_path).expanduser().resolve()
    env_dir = resolved_env_path.parent
    file_values = dotenv_values(resolved_env_path)
    values = {**file_values, **os.environ}

    required_keys = (
        "GAROON_BASE_URL",
        "GAROON_USERNAME",
        "GAROON_PASSWORD",
        "GAROON_START_DAYS_OFFSET",
        "GAROON_END_DAYS_OFFSET",
        "OUTPUT_JSON_PATH",
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

    output_json_path = Path(values["OUTPUT_JSON_PATH"]).expanduser()
    if not output_json_path.is_absolute():
        output_json_path = (env_dir / output_json_path).resolve()

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
