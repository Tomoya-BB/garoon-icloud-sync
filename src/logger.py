from __future__ import annotations

import json
import logging
import re
import sys
from typing import Any, Mapping

_SAFE_LOG_VALUE_RE = re.compile(r"^[A-Za-z0-9_./:@+-]+$")


def configure_logging(level: str) -> None:
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root_logger.addHandler(handler)
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
