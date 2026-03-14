from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

_REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CALDAV_SYNC_RESULT_PATH = Path(__file__).resolve().parent.parent / "data" / "caldav_sync_result.json"

CREATE_CONFLICT_CHOICES = (
    "state-drift",
    "uid-match",
    "resource-exists",
)
CREATE_CONFLICT_FIELD_BY_FILTER = {
    "state-drift": "create_conflict_state_drift_suspected",
    "uid-match": "create_conflict_uid_match_found",
    "resource-exists": "create_conflict_resource_exists",
}


@dataclass(frozen=True)
class CreateConflictSummary:
    create_total: int
    create_failed: int
    create_failed_412: int
    state_drift_suspected: int
    uid_match_found: int
    resource_exists: int
    existing_resource_url: int
    state_drift_uid_match_only: int
    state_drift_resource_exists_only: int
    state_drift_both: int

    def as_rows(self) -> tuple[tuple[str, int], ...]:
        return (
            ("create_total", self.create_total),
            ("create_failed", self.create_failed),
            ("create_failed_412", self.create_failed_412),
            ("state_drift_suspected", self.state_drift_suspected),
            ("uid_match_found", self.uid_match_found),
            ("resource_exists", self.resource_exists),
            ("existing_resource_url", self.existing_resource_url),
            ("state_drift_uid_match_only", self.state_drift_uid_match_only),
            ("state_drift_resource_exists_only", self.state_drift_resource_exists_only),
            ("state_drift_both", self.state_drift_both),
        )


@dataclass(frozen=True)
class DriftSummaryBucket:
    label: str
    count: int
    sample_event_ids: tuple[str, ...]


@dataclass(frozen=True)
class CreateConflictDriftSummary:
    total_with_remote_existing: int
    remote_fetch_failed: int
    status_buckets: tuple[DriftSummaryBucket, ...]
    diff_count_buckets: tuple[DriftSummaryBucket, ...]
    diff_field_buckets: tuple[DriftSummaryBucket, ...]
    individual_diff_field_buckets: tuple[DriftSummaryBucket, ...]


def build_summary_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize create_conflict diagnostics from caldav_sync_result.json.",
    )
    parser.add_argument(
        "--result-path",
        default=str(DEFAULT_CALDAV_SYNC_RESULT_PATH),
        help="Path to caldav_sync_result.json. Defaults to data/caldav_sync_result.json.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_summary_parser()
    args = parser.parse_args(argv)

    try:
        result_path = Path(args.result_path).expanduser().resolve()
        payload = load_caldav_sync_result(result_path)
    except FileNotFoundError as exc:
        print(f"inspection input file was not found: {exc.filename}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(f"Failed to summarize CalDAV sync result: {exc}", file=sys.stderr)
        return 1

    raw_results = _extract_results(payload)
    _print_summary_report(result_path, summarize_create_conflicts(raw_results))
    _print_drift_summary_report(summarize_create_conflict_drift(raw_results))
    return 0


def load_caldav_sync_result(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"caldav_sync_result.json is invalid JSON at line {exc.lineno} column {exc.colno}"
        ) from exc

    if not isinstance(payload, dict):
        raise ValueError("caldav_sync_result.json must be a JSON object")

    raw_results = _extract_results(payload)
    for raw_result in raw_results:
        _enrich_result_with_state_drift_summary(raw_result)
    return payload


def build_result_index(payload: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
    indexed: dict[tuple[str, str, str], dict[str, Any]] = {}
    for index, raw_result in enumerate(_extract_results(payload)):
        location = f"caldav_sync_result.json.results[{index}]"
        if not isinstance(raw_result, dict):
            raise ValueError(f"{location} must be a JSON object")
        action = _require_non_empty_string(raw_result.get("action"), location=f"{location}.action")
        event_id = _require_non_empty_string(raw_result.get("event_id"), location=f"{location}.event_id")
        ics_uid = _require_non_empty_string(raw_result.get("ics_uid"), location=f"{location}.ics_uid")
        indexed[(action, event_id, ics_uid)] = raw_result
    return indexed


def summarize_create_conflicts(raw_results: Iterable[dict[str, Any]]) -> CreateConflictSummary:
    create_total = 0
    create_failed = 0
    create_failed_412 = 0
    state_drift_suspected = 0
    uid_match_found = 0
    resource_exists = 0
    existing_resource_url = 0
    state_drift_uid_match_only = 0
    state_drift_resource_exists_only = 0
    state_drift_both = 0

    for raw_result in raw_results:
        if raw_result.get("action") != "create":
            continue
        create_total += 1

        if raw_result.get("success") is False:
            create_failed += 1
        if raw_result.get("status_code") == 412:
            create_failed_412 += 1

        has_uid_match = raw_result.get("create_conflict_uid_match_found") is True
        has_resource_exists = raw_result.get("create_conflict_resource_exists") is True
        has_state_drift = raw_result.get("create_conflict_state_drift_suspected") is True
        has_existing_resource_url = isinstance(raw_result.get("create_conflict_existing_resource_url"), str) and bool(
            raw_result.get("create_conflict_existing_resource_url")
        )

        if has_uid_match:
            uid_match_found += 1
        if has_resource_exists:
            resource_exists += 1
        if has_state_drift:
            state_drift_suspected += 1
        if has_existing_resource_url:
            existing_resource_url += 1

        if has_state_drift and has_uid_match and has_resource_exists:
            state_drift_both += 1
        elif has_state_drift and has_uid_match:
            state_drift_uid_match_only += 1
        elif has_state_drift and has_resource_exists:
            state_drift_resource_exists_only += 1

    return CreateConflictSummary(
        create_total=create_total,
        create_failed=create_failed,
        create_failed_412=create_failed_412,
        state_drift_suspected=state_drift_suspected,
        uid_match_found=uid_match_found,
        resource_exists=resource_exists,
        existing_resource_url=existing_resource_url,
        state_drift_uid_match_only=state_drift_uid_match_only,
        state_drift_resource_exists_only=state_drift_resource_exists_only,
        state_drift_both=state_drift_both,
    )


def summarize_create_conflict_drift(
    raw_results: Iterable[dict[str, Any]],
    *,
    sample_size: int = 3,
) -> CreateConflictDriftSummary:
    target_results = [raw_result for raw_result in raw_results if _is_create_412_with_remote_existing(raw_result)]

    status_counts: dict[str, int] = {}
    diff_count_counts: dict[str, int] = {}
    diff_field_counts: dict[str, int] = {}
    individual_diff_field_counts: dict[str, int] = {}
    status_samples: dict[str, list[str]] = {}
    diff_count_samples: dict[str, list[str]] = {}
    diff_field_samples: dict[str, list[str]] = {}
    individual_diff_field_samples: dict[str, list[str]] = {}
    remote_fetch_failed = 0

    for raw_result in target_results:
        event_id = _get_summary_event_id(raw_result)

        status_label = _get_drift_report_status_label(raw_result)
        _increment_bucket_count(status_counts, status_label)
        _record_bucket_sample(status_samples, status_label, event_id, sample_size=sample_size)
        if status_label == "remote_fetch_failed":
            remote_fetch_failed += 1

        diff_count_label = _get_drift_diff_count_label(raw_result.get("drift_diff_count"))
        _increment_bucket_count(diff_count_counts, diff_count_label)
        _record_bucket_sample(diff_count_samples, diff_count_label, event_id, sample_size=sample_size)

        normalized_diff_fields = _normalize_drift_diff_fields(raw_result.get("drift_diff_fields"))
        diff_field_label = _get_drift_diff_fields_label(normalized_diff_fields)
        _increment_bucket_count(diff_field_counts, diff_field_label)
        _record_bucket_sample(diff_field_samples, diff_field_label, event_id, sample_size=sample_size)
        for field_name in normalized_diff_fields:
            _increment_bucket_count(individual_diff_field_counts, field_name)
            _record_bucket_sample(
                individual_diff_field_samples,
                field_name,
                event_id,
                sample_size=sample_size,
            )

    return CreateConflictDriftSummary(
        total_with_remote_existing=len(target_results),
        remote_fetch_failed=remote_fetch_failed,
        status_buckets=_build_sorted_buckets(status_counts, status_samples, sort_mode="count_desc"),
        diff_count_buckets=_build_sorted_buckets(diff_count_counts, diff_count_samples, sort_mode="diff_count"),
        diff_field_buckets=_build_sorted_buckets(diff_field_counts, diff_field_samples, sort_mode="count_desc"),
        individual_diff_field_buckets=_build_sorted_buckets(
            individual_diff_field_counts,
            individual_diff_field_samples,
            sort_mode="count_desc",
        ),
    )


def matches_delivery_filter(raw_result: dict[str, Any], only: str | None) -> bool:
    if only is None:
        return True
    if only == "failed":
        return raw_result.get("success") is False
    raise ValueError(f"Unsupported delivery filter: {only}")


def matches_conflict_filters(raw_result: dict[str, Any], conflict_filters: set[str]) -> bool:
    if not conflict_filters:
        return True
    return any(
        raw_result.get(CREATE_CONFLICT_FIELD_BY_FILTER[conflict_filter]) is True
        for conflict_filter in conflict_filters
    )


def summarize_state_drift_comparison(comparison: Any) -> tuple[int | None, list[str]]:
    if not isinstance(comparison, dict):
        return None, []

    comparable_fields = 0
    diff_fields: list[str] = []
    for field_name, raw_field in comparison.items():
        if not isinstance(field_name, str) or not isinstance(raw_field, dict):
            continue
        equal = raw_field.get("equal")
        if not isinstance(equal, bool):
            continue
        comparable_fields += 1
        if equal is False:
            diff_fields.append(field_name)

    if comparable_fields == 0:
        return None, []
    return len(diff_fields), diff_fields


def _extract_results(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw_results = payload.get("results")
    if not isinstance(raw_results, list):
        raise ValueError("caldav_sync_result.json.results must be a list")
    for index, raw_result in enumerate(raw_results):
        if not isinstance(raw_result, dict):
            raise ValueError(f"caldav_sync_result.json.results[{index}] must be a JSON object")
    return raw_results


def _print_summary_report(result_path: Path, summary: CreateConflictSummary) -> None:
    print("CalDAV sync result summary")
    print(f"result_path: {result_path}")
    for label, value in summary.as_rows():
        print(f"{label}: {value}")


def _print_drift_summary_report(summary: CreateConflictDriftSummary) -> None:
    print()
    print(f"total create 412 with drift reports: {summary.total_with_remote_existing}")
    print(f"remote_fetch_failed: {summary.remote_fetch_failed}")

    print()
    print("drift_report_status summary")
    _print_drift_buckets(summary.status_buckets)

    print()
    print("drift_diff_count summary")
    _print_drift_buckets(summary.diff_count_buckets)

    print()
    print("drift_diff_fields combination summary")
    _print_drift_buckets(summary.diff_field_buckets)

    print()
    print("individual drift field frequency")
    _print_drift_buckets(summary.individual_diff_field_buckets)

    print()
    print("sample event_ids")
    if not summary.individual_diff_field_buckets:
        print("- none")
        return
    for bucket in summary.individual_diff_field_buckets:
        print(f"- {bucket.label}: {_format_sample_event_ids(bucket.sample_event_ids)}")


def _enrich_result_with_state_drift_summary(raw_result: dict[str, Any]) -> None:
    report_status = raw_result.get("drift_report_status")
    if not isinstance(report_status, str) or not report_status:
        legacy_status = raw_result.get("create_conflict_state_drift_report_status")
        if isinstance(legacy_status, str) and legacy_status:
            raw_result["drift_report_status"] = legacy_status

    has_diff_count = "drift_diff_count" in raw_result and (
        isinstance(raw_result.get("drift_diff_count"), int)
        or raw_result.get("drift_diff_count") is None
    )
    has_diff_fields = "drift_diff_fields" in raw_result and isinstance(raw_result.get("drift_diff_fields"), list)
    if has_diff_count and has_diff_fields:
        return

    comparison = _load_state_drift_comparison_from_report(
        raw_result.get("create_conflict_state_drift_report_path")
    )
    if comparison is None:
        return

    diff_count, diff_fields = summarize_state_drift_comparison(comparison)
    raw_result["drift_diff_count"] = diff_count
    raw_result["drift_diff_fields"] = diff_fields


def _load_state_drift_comparison_from_report(report_path_value: Any) -> dict[str, Any] | None:
    report_path = _resolve_state_drift_report_path(report_path_value)
    if report_path is None:
        return None

    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None

    comparison = payload.get("comparison")
    return comparison if isinstance(comparison, dict) else None


def _resolve_state_drift_report_path(report_path_value: Any) -> Path | None:
    if not isinstance(report_path_value, str) or not report_path_value:
        return None

    report_path = Path(report_path_value).expanduser()
    candidates = [report_path]
    if not report_path.is_absolute():
        candidates.insert(0, (_REPO_ROOT / report_path).resolve())

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _require_non_empty_string(value: Any, *, location: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{location} must be a non-empty string")
    return value


def _is_create_412_with_remote_existing(raw_result: dict[str, Any]) -> bool:
    existing_resource_url = raw_result.get("create_conflict_existing_resource_url")
    return (
        raw_result.get("action") == "create"
        and raw_result.get("status_code") == 412
        and isinstance(existing_resource_url, str)
        and bool(existing_resource_url)
    )


def _get_summary_event_id(raw_result: dict[str, Any]) -> str:
    event_id = raw_result.get("event_id")
    if isinstance(event_id, str) and event_id:
        return event_id
    return "(missing event_id)"


def _get_drift_report_status_label(raw_result: dict[str, Any]) -> str:
    status = raw_result.get("drift_report_status") or raw_result.get("create_conflict_state_drift_report_status")
    if isinstance(status, str) and status:
        return status
    return "(missing)"


def _get_drift_diff_count_label(diff_count: Any) -> str:
    if isinstance(diff_count, int):
        return str(diff_count)
    if diff_count is None:
        return "null"
    return "(invalid)"


def _get_drift_diff_fields_label(diff_fields: Any) -> str:
    normalized_fields = _normalize_drift_diff_fields(diff_fields)
    if not normalized_fields:
        return "(no diff fields)"
    return ", ".join(normalized_fields)


def _normalize_drift_diff_fields(diff_fields: Any) -> list[str]:
    if not isinstance(diff_fields, list):
        return []
    return sorted({field for field in diff_fields if isinstance(field, str) and field})


def _record_bucket_sample(
    buckets: dict[str, list[str]],
    label: str,
    event_id: str,
    *,
    sample_size: int,
) -> None:
    samples = buckets.setdefault(label, [])
    if len(samples) >= sample_size or event_id in samples:
        return
    samples.append(event_id)


def _increment_bucket_count(counts: dict[str, int], label: str) -> None:
    counts[label] = counts.get(label, 0) + 1


def _build_sorted_buckets(
    counts: dict[str, int],
    samples_by_label: dict[str, list[str]],
    *,
    sort_mode: str,
) -> tuple[DriftSummaryBucket, ...]:
    items = list(counts.items())
    if sort_mode == "count_desc":
        items.sort(key=lambda item: (-item[1], item[0]))
    elif sort_mode == "diff_count":
        items.sort(key=lambda item: _diff_count_sort_key(item[0]))
    else:
        raise ValueError(f"Unsupported sort mode: {sort_mode}")

    return tuple(
        DriftSummaryBucket(
            label=label,
            count=count,
            sample_event_ids=tuple(samples_by_label.get(label, [])),
        )
        for label, count in items
    )


def _diff_count_sort_key(label: str) -> tuple[int, int, str]:
    if label.isdigit():
        return (0, int(label), label)
    if label == "null":
        return (1, 0, label)
    return (2, 0, label)


def _print_drift_buckets(buckets: Sequence[DriftSummaryBucket]) -> None:
    if not buckets:
        print("- none")
        return
    for bucket in buckets:
        print(
            f"- {bucket.label}: {bucket.count} "
            f"(sample_event_ids: {_format_sample_event_ids(bucket.sample_event_ids)})"
        )


def _format_sample_event_ids(sample_event_ids: Sequence[str]) -> str:
    if not sample_event_ids:
        return "-"
    return ", ".join(sample_event_ids)


if __name__ == "__main__":
    raise SystemExit(main())
