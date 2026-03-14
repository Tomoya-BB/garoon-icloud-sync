from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Sequence

from src.caldav_result_helpers import (
    CREATE_CONFLICT_CHOICES,
    DEFAULT_CALDAV_SYNC_RESULT_PATH,
    build_result_index,
    load_caldav_sync_result,
    matches_conflict_filters,
    matches_delivery_filter,
    summarize_create_conflicts,
)
from src.sync_plan import DEFAULT_SYNC_PLAN_PATH, SyncActionType, SyncPlan, load_sync_plan

DEFAULT_ACTION_FILTERS = (
    SyncActionType.CREATE,
    SyncActionType.DELETE,
)
DISPLAY_ACTION_ORDER = (
    SyncActionType.CREATE,
    SyncActionType.DELETE,
    SyncActionType.UPDATE,
    SyncActionType.SKIP,
)
ALL_ACTION_CHOICES = [action.value for action in DISPLAY_ACTION_ORDER]
SORT_CHOICES = (
    "default",
    "drift-diff-count",
)
DRIFT_STATUS_CHOICES = (
    "generated",
    "remote_fetch_failed",
    "none",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect create/delete/update actions from sync_plan.json.",
    )
    parser.add_argument(
        "--plan-path",
        default=str(DEFAULT_SYNC_PLAN_PATH),
        help="Path to sync_plan.json. Defaults to data/sync_plan.json.",
    )
    parser.add_argument(
        "--action",
        action="append",
        choices=ALL_ACTION_CHOICES,
        help=(
            "Action type to show. Repeat to include multiple actions. "
            "Defaults to create and delete."
        ),
    )
    parser.add_argument(
        "--result-path",
        default=None,
        help=(
            "Optional path to caldav_sync_result.json. "
            f"When set, delivery success/failed and create diagnostics are shown. "
            f"Example: {DEFAULT_CALDAV_SYNC_RESULT_PATH}."
        ),
    )
    parser.add_argument(
        "--only",
        choices=("failed",),
        help="Only show matching delivery results from caldav_sync_result.json.",
    )
    parser.add_argument(
        "--conflict",
        action="append",
        choices=CREATE_CONFLICT_CHOICES,
        help=(
            "Only show rows whose create diagnostics match the requested conflict kind. "
            "Repeat to include multiple kinds."
        ),
    )
    parser.add_argument(
        "--drift-status",
        choices=DRIFT_STATUS_CHOICES,
        help=(
            "Only show rows whose drift report status matches the requested value. "
            "'none' matches results without drift_report_status."
        ),
    )
    parser.add_argument(
        "--sort",
        choices=SORT_CHOICES,
        default="default",
        help=(
            "Row sort order. "
            "'drift-diff-count' prioritizes create rows with larger drift_diff_count first."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        plan_path = Path(args.plan_path).expanduser().resolve()
        sync_plan = load_sync_plan(plan_path)
        requested_actions = _resolve_requested_actions(args.action)
        result_path = (
            Path(args.result_path).expanduser().resolve()
            if args.result_path is not None
            else None
        )
        requested_conflicts = set(args.conflict or [])
        _validate_result_filters(
            result_path=result_path,
            delivery_filter=args.only,
            conflict_filters=requested_conflicts,
            drift_status_filter=args.drift_status,
        )
        result_payload = load_caldav_sync_result(result_path) if result_path is not None else None
        result_index = build_result_index(result_payload) if result_payload is not None else None
    except FileNotFoundError as exc:
        print(f"inspection input file was not found: {exc.filename}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(f"Failed to inspect sync plan: {exc}", file=sys.stderr)
        return 1

    _print_report(
        sync_plan,
        plan_path=plan_path,
        requested_actions=requested_actions,
        result_path=result_path,
        delivery_filter=args.only,
        requested_conflicts=requested_conflicts,
        requested_drift_status=args.drift_status,
        sort_mode=args.sort,
        result_payload=result_payload,
        result_index=result_index,
    )
    return 0


def _resolve_requested_actions(raw_actions: list[str] | None) -> list[SyncActionType]:
    if not raw_actions:
        return list(DEFAULT_ACTION_FILTERS)

    requested = {SyncActionType(raw_action) for raw_action in raw_actions}
    return [
        action
        for action in DISPLAY_ACTION_ORDER
        if action in requested
    ]


def _print_report(
    sync_plan: SyncPlan,
    *,
    plan_path: Path,
    requested_actions: list[SyncActionType],
    result_path: Path | None,
    delivery_filter: str | None,
    requested_conflicts: set[str],
    requested_drift_status: str | None,
    sort_mode: str,
    result_payload: dict[str, Any] | None,
    result_index: dict[tuple[str, str, str], dict[str, Any]] | None,
) -> None:
    print("Sync plan inspection")
    print(f"plan_path: {plan_path}")
    if result_path is not None:
        print(f"result_path: {result_path}")
    print(f"generated_at: {sync_plan.generated_at}")
    print(f"shown_actions: {', '.join(action.value for action in requested_actions)}")
    print(f"total_actions: {len(sync_plan.actions)}")
    if delivery_filter is not None:
        print(f"only: {delivery_filter}")
    if requested_conflicts:
        print(f"conflict_filters: {', '.join(sorted(requested_conflicts))}")
    if requested_drift_status is not None:
        print(f"drift_status: {requested_drift_status}")
    if sort_mode != "default":
        print(f"sort: {sort_mode}")
    if result_payload is not None:
        _print_create_conflict_summary(result_payload)

    for action in requested_actions:
        matching_actions = [
            item
            for item in sync_plan.actions
            if item.action is action
        ]
        rows = [
            _build_row(
                item,
                result_index=result_index,
                delivery_filter=delivery_filter,
                requested_conflicts=requested_conflicts,
                requested_drift_status=requested_drift_status,
            )
            for item in matching_actions
        ]
        rows = [row for row in rows if row is not None]
        rows = _sort_rows(rows, action=action, sort_mode=sort_mode)
        print()
        print(f"[{action.value}] count={len(rows)}")
        if not rows:
            print("(none)")
            continue
        _print_table(rows, columns=_resolve_columns(result_index=result_index))


def _build_row(
    item,
    *,
    result_index: dict[tuple[str, str, str], dict[str, Any]] | None,
    delivery_filter: str | None,
    requested_conflicts: set[str],
    requested_drift_status: str | None,
) -> dict[str, Any] | None:
    row: dict[str, Any] = {
        "action": item.action.value,
        "event_id": item.event_id,
        "ics_uid": item.ics_uid,
        "action_reason": item.action_reason,
        "summary": item.summary or "(not available)",
    }
    if result_index is None:
        return row

    raw_result = result_index.get((item.action.value, item.event_id, item.ics_uid))
    if delivery_filter is not None and raw_result is None:
        return None
    if requested_conflicts and raw_result is None:
        return None
    if requested_drift_status is not None and raw_result is None:
        return None
    if raw_result is not None and not matches_delivery_filter(raw_result, delivery_filter):
        return None
    if raw_result is not None and not matches_conflict_filters(raw_result, requested_conflicts):
        return None
    if raw_result is not None and not _matches_drift_status_filter(raw_result, requested_drift_status):
        return None
    result = _build_result_row(raw_result) if raw_result is not None else None
    if result is None:
        row.update(
            {
                "delivery": "(not delivered)",
                "status_code": "-",
                "error_kind": "-",
                "state_drift": "-",
                "uid_match": "-",
                "resource_exists": "-",
                "existing_resource_url": "-",
                "selected_candidate_index": "-",
                "selected_candidate_reason": "-",
                "drift_report_status": "-",
                "drift_diff_count": "-",
                "drift_diff_fields": "-",
                "drift_report_path": "-",
                "all_day": "-",
                "desc": "-",
                "loc": "-",
                "payload_bytes": "-",
                "resource_name": "-",
            }
        )
        return row

    row.update(result)
    if row["summary"] == "(not available)" and result.get("summary") not in {None, "-", ""}:
        row["summary"] = result["summary"]
    return row


def _resolve_columns(
    *,
    result_index: dict[tuple[str, str, str], dict[str, Any]] | None,
) -> tuple[str, ...]:
    if result_index is None:
        return ("action", "event_id", "ics_uid", "action_reason", "summary")
    return (
        "delivery",
        "status_code",
        "error_kind",
        "state_drift",
        "uid_match",
        "resource_exists",
        "existing_resource_url",
        "selected_candidate_index",
        "selected_candidate_reason",
        "drift_report_status",
        "drift_diff_count",
        "drift_diff_fields",
        "drift_report_path",
        "action",
        "event_id",
        "ics_uid",
        "action_reason",
        "summary",
        "all_day",
        "desc",
        "loc",
        "payload_bytes",
        "resource_name",
    )


def _print_table(rows: list[dict[str, Any]], *, columns: tuple[str, ...]) -> None:
    widths = {
        column: max(len(column), *(len(row[column]) for row in rows))
        for column in columns
    }

    header = "  ".join(column.ljust(widths[column]) for column in columns)
    print(header)
    print("  ".join("-" * widths[column] for column in columns))
    for row in rows:
        print("  ".join(row[column].ljust(widths[column]) for column in columns))


def _build_result_row(raw_result: dict[str, Any]) -> dict[str, Any]:
    payload_summary = raw_result.get("payload_summary")
    if not isinstance(payload_summary, dict):
        payload_summary = {}

    success = raw_result.get("success")
    delivery = (
        "success"
        if success is True
        else "failed"
        if success is False
        else "(unknown)"
    )
    error_kind = _string_or_dash(raw_result.get("error_kind"))
    if error_kind == "-":
        error_kind = _string_or_dash(raw_result.get("conflict_kind"))
    drift_diff_count = raw_result.get("drift_diff_count")
    drift_report_status = _get_drift_report_status(raw_result)
    return {
        "delivery": delivery,
        "status_code": _int_or_dash(raw_result.get("status_code")),
        "error_kind": error_kind,
        "state_drift": _bool_or_dash(raw_result.get("create_conflict_state_drift_suspected")),
        "uid_match": _bool_or_dash(raw_result.get("create_conflict_uid_match_found")),
        "resource_exists": _bool_or_dash(raw_result.get("create_conflict_resource_exists")),
        "existing_resource_url": _string_or_dash(raw_result.get("create_conflict_existing_resource_url")),
        "selected_candidate_index": _int_or_dash(
            raw_result.get("create_conflict_selected_candidate_index")
        ),
        "selected_candidate_reason": _string_or_dash(
            raw_result.get("create_conflict_selected_candidate_reason")
        ),
        "drift_report_status": _string_or_dash(drift_report_status),
        "drift_diff_count": _int_or_dash(drift_diff_count),
        "drift_diff_fields": _drift_diff_fields_or_dash(raw_result.get("drift_diff_fields")),
        "drift_report_path": _string_or_dash(raw_result.get("create_conflict_state_drift_report_path")),
        "summary": _string_or_dash(payload_summary.get("summary")),
        "all_day": _bool_or_dash(payload_summary.get("is_all_day")),
        "desc": _bool_or_dash(payload_summary.get("has_description")),
        "loc": _bool_or_dash(payload_summary.get("has_location")),
        "payload_bytes": _int_or_dash(raw_result.get("payload_bytes")),
        "resource_name": _string_or_dash(raw_result.get("resource_name")),
        "_drift_diff_count_value": drift_diff_count if isinstance(drift_diff_count, int) else None,
    }


def _print_create_conflict_summary(result_payload: dict[str, Any]) -> None:
    summary = summarize_create_conflicts(build_result_index(result_payload).values())
    print("create_conflict_summary:")
    for label, value in summary.as_rows():
        print(f"  {label}: {value}")


def _validate_result_filters(
    *,
    result_path: Path | None,
    delivery_filter: str | None,
    conflict_filters: set[str],
    drift_status_filter: str | None,
) -> None:
    if result_path is not None:
        return
    if delivery_filter is not None or conflict_filters or drift_status_filter is not None:
        raise ValueError("--only, --conflict, and --drift-status require --result-path")


def _sort_rows(rows: list[dict[str, Any]], *, action: SyncActionType, sort_mode: str) -> list[dict[str, Any]]:
    if action is not SyncActionType.CREATE:
        return rows
    if sort_mode == "drift-diff-count":
        return sorted(rows, key=_create_row_sort_key_by_drift_diff_count)
    return sorted(rows, key=_create_row_sort_key)


def _create_row_sort_key(row: dict[str, Any]) -> tuple[int, int, int, int, str]:
    delivery_rank = 2
    if row.get("delivery") == "failed":
        delivery_rank = 0
    elif row.get("delivery") == "(not delivered)":
        delivery_rank = 1

    existing_resource_rank = 0 if row.get("existing_resource_url") not in {None, "-", ""} else 1
    diff_count = row.get("_drift_diff_count_value")
    diff_count_rank = 0 if isinstance(diff_count, int) else 1
    return (
        delivery_rank,
        existing_resource_rank,
        diff_count_rank,
        -(diff_count if isinstance(diff_count, int) else -1),
        row["event_id"],
    )


def _create_row_sort_key_by_drift_diff_count(row: dict[str, Any]) -> tuple[int, int, int, int, str]:
    delivery_rank = 0 if row.get("delivery") == "failed" else 1
    diff_count = row.get("_drift_diff_count_value")
    diff_count_rank = 0 if isinstance(diff_count, int) else 1
    existing_resource_rank = 0 if row.get("existing_resource_url") not in {None, "-", ""} else 1
    return (
        delivery_rank,
        diff_count_rank,
        -(diff_count if isinstance(diff_count, int) else -1),
        existing_resource_rank,
        row["event_id"],
    )


def _string_or_dash(value: Any) -> str:
    return value if isinstance(value, str) and value else "-"


def _int_or_dash(value: Any) -> str:
    return str(value) if isinstance(value, int) else "-"


def _bool_or_dash(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return "-"


def _string_list_or_dash(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "-"
    items = [item for item in value if isinstance(item, str) and item]
    if not items:
        return "-"
    return ",".join(items)


def _drift_diff_fields_or_dash(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "-"
    items = [item for item in value if isinstance(item, str) and item]
    if not items:
        return "-"
    if len(items) <= 3:
        return ",".join(items)
    return f"{','.join(items[:3])},+{len(items) - 3}"


def _get_drift_report_status(raw_result: dict[str, Any]) -> str | None:
    status = raw_result.get("drift_report_status")
    if isinstance(status, str) and status:
        return status
    legacy_status = raw_result.get("create_conflict_state_drift_report_status")
    if isinstance(legacy_status, str) and legacy_status:
        return legacy_status
    return None


def _matches_drift_status_filter(raw_result: dict[str, Any], requested_drift_status: str | None) -> bool:
    if requested_drift_status is None:
        return True
    actual_status = _get_drift_report_status(raw_result)
    if requested_drift_status == "none":
        return actual_status is None
    return actual_status == requested_drift_status


if __name__ == "__main__":
    raise SystemExit(main())
