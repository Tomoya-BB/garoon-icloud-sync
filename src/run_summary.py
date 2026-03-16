from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

NORMAL_START_DAYS_OFFSET = 0
NORMAL_END_DAYS_OFFSET = 92


@dataclass(frozen=True, slots=True)
class RunWarning:
    code: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RunCounts:
    create: int = 0
    update: int = 0
    delete: int = 0
    skip: int = 0

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RunFetchWindow:
    start_days_offset: int
    end_days_offset: int
    start: str
    end: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RunSummary:
    profile: str
    started_at: str
    finished_at: str
    mode: str
    dry_run: bool
    fetch_window: RunFetchWindow
    result: str
    counts: RunCounts = field(default_factory=RunCounts)
    warnings: list[RunWarning] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "mode": self.mode,
            "dry_run": self.dry_run,
            "fetch_window": self.fetch_window.to_dict(),
            "result": self.result,
            "counts": self.counts.to_dict(),
            "warnings": [warning.to_dict() for warning in self.warnings],
            "error": self.error,
        }


def determine_run_mode(start_days_offset: int, end_days_offset: int) -> str:
    if start_days_offset < NORMAL_START_DAYS_OFFSET or end_days_offset > NORMAL_END_DAYS_OFFSET:
        return "backfill"
    return "normal"


def build_delete_warning(delete_count: int) -> RunWarning | None:
    if delete_count < 1:
        return None
    return RunWarning(
        code="DELETE_DETECTED",
        message=f"Delete actions detected: {delete_count}. Review sync_plan.json before relying on this run.",
    )


def build_window_warning(start_days_offset: int, end_days_offset: int) -> RunWarning | None:
    if determine_run_mode(start_days_offset, end_days_offset) != "backfill":
        return None
    return RunWarning(
        code="BACKFILL_WINDOW",
        message=(
            "Fetch window is outside the recommended normal range (0..92 days). "
            "After backfill, restore GAROON_START_DAYS_OFFSET=0 and GAROON_END_DAYS_OFFSET=92."
        ),
    )


def build_summary_history_path(history_dir: Path, *, recorded_at: datetime | None = None) -> Path:
    timestamp = (recorded_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return history_dir / f"run_summary-{timestamp.strftime('%Y%m%dT%H%M%SZ')}.json"


def save_run_summary(path: Path, summary: RunSummary) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(summary.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def save_run_summary_history(path: Path, summary: RunSummary) -> None:
    save_run_summary(path, summary)
