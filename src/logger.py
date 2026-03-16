from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Mapping

_SAFE_LOG_VALUE_RE = re.compile(r"^[A-Za-z0-9_./:@+-]+$")


class _ProfileNameFilter(logging.Filter):
    def __init__(self, profile_name: str):
        super().__init__()
        self._profile_name = profile_name

    def filter(self, record: logging.LogRecord) -> bool:
        record.profile_name = self._profile_name
        return True


def configure_logging(
    level: str,
    *,
    profile_name: str = "default",
    log_file_path: Path | None = None,
) -> None:
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    profile_filter = _ProfileNameFilter(profile_name)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [profile=%(profile_name)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(profile_filter)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    if log_file_path is not None:
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
        file_handler.addFilter(profile_filter)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))


def classify_exception_error_kind(exc: BaseException) -> str:
    if isinstance(exc, OSError):
        return "io_failed"
    return "other"


def format_structured_log_fields(fields: Mapping[str, Any]) -> str:
    return " ".join(
        f"{key}={format_structured_log_value(value)}"
        for key, value in fields.items()
        if value is not None
    )


def format_structured_log_value(value: Any) -> str:
    if isinstance(value, str):
        if _SAFE_LOG_VALUE_RE.fullmatch(value):
            return value
        return json.dumps(value, ensure_ascii=False)

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, int | float):
        return str(value)

    return json.dumps(value, ensure_ascii=False)


def log_structured_error(
    logger: logging.Logger,
    message: str,
    *,
    fields: Mapping[str, Any],
) -> None:
    logger.error("%s %s", message, format_structured_log_fields(fields))


def log_structured_info(
    logger: logging.Logger,
    message: str,
    *,
    fields: Mapping[str, Any],
) -> None:
    logger.info("%s %s", message, format_structured_log_fields(fields))


def log_structured_warning(
    logger: logging.Logger,
    message: str,
    *,
    fields: Mapping[str, Any],
) -> None:
    logger.warning("%s %s", message, format_structured_log_fields(fields))
