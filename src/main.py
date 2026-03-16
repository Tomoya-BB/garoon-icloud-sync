from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path

from src.caldav_client import (
    DEFAULT_CALDAV_SYNC_RESULT_PATH,
    CalDAVActionResult,
    CalDAVClient,
    CalDAVConnectionSettings,
    save_caldav_sync_report,
)
from src.config import AppConfig, ConfigError, DEFAULT_PROFILE_NAME, load_config
from src.ics_writer import DEFAULT_ICS_PATH, write_calendar
from src.garoon_client import (
    GaroonAuthenticationError,
    GaroonClient,
    GaroonClientError,
    PasswordAuthStrategy,
)
from src.logger import (
    classify_exception_error_kind,
    configure_logging,
    log_structured_error,
    log_structured_info,
    log_structured_warning,
)
from src.models import DateRange, EventSnapshot
from src.run_summary import (
    RunCounts,
    RunFetchWindow,
    RunSummary,
    RunWarning,
    build_delete_warning,
    build_summary_history_path,
    build_window_warning,
    determine_run_mode,
    save_run_summary,
    save_run_summary_history,
)
from src.sync_plan import (
    DEFAULT_SYNC_PLAN_PATH,
    SyncPlanActionSummary,
    build_sequence_by_event_id,
    build_sync_plan,
    build_uid_by_event_id,
    save_sync_plan,
    summarize_sync_plan_actions,
)
from src.sync_state import (
    DEFAULT_SYNC_STATE_PATH,
    SyncStateJsonDecodeError,
    SyncState,
    SyncStateValidationError,
    build_next_sync_state_from_delivery,
    diff_events,
    load_sync_state,
    save_sync_state,
)

_SYNC_STATE_EVENT_ID_PATTERNS = (
    re.compile(r"event_id '([^']+)'"),
    re.compile(r"(?:events|tombstones)\['([^']+)'\]"),
)
_SYNC_STATE_ICS_UID_PATTERN = re.compile(r"ics_uid '([^']+)'")


@dataclass(frozen=True, slots=True)
class DryRunAnomalousChangeWarning:
    create_count: int
    delete_count: int
    total_count: int
    create_threshold: int
    delete_threshold: int

    @property
    def triggered(self) -> bool:
        return (
            self.create_count >= self.create_threshold
            or self.delete_count >= self.delete_threshold
        )


def main() -> int:
    started_at = datetime.now().astimezone()
    finished_at = started_at
    config: AppConfig | None = None
    logger: logging.Logger | None = None
    date_range: DateRange | None = None
    mode = "normal"
    action_summary: SyncPlanActionSummary | None = None
    warnings: list[RunWarning] = []
    exit_code = 1
    error_message: str | None = None
    sync_state_saved = False
    caldav_report = None

    try:
        config = load_config()
        configure_app_logging(config)
        logger = logging.getLogger(__name__)
        sync_state_path = config.sync_state_path or DEFAULT_SYNC_STATE_PATH
        sync_plan_path = config.sync_plan_path or DEFAULT_SYNC_PLAN_PATH
        caldav_sync_result_path = config.caldav_sync_result_path or DEFAULT_CALDAV_SYNC_RESULT_PATH
        ics_path = config.ics_path or DEFAULT_ICS_PATH
        diagnostics_dir = config.diagnostics_dir
        reports_dir = config.reports_dir
        date_range = build_date_range(
            config.garoon_start_days_offset,
            config.garoon_end_days_offset,
        )
        mode = determine_run_mode(
            config.garoon_start_days_offset,
            config.garoon_end_days_offset,
        )
        log_run_start(
            logger,
            config=config,
            date_range=date_range,
            mode=mode,
        )
        window_warning = build_window_warning(
            config.garoon_start_days_offset,
            config.garoon_end_days_offset,
        )
        if window_warning is not None:
            warnings.append(window_warning)
            print_run_warning(window_warning)
            log_run_warning(logger, warning=window_warning, mode=mode)

        client = GaroonClient(
            base_url=config.garoon_base_url,
            auth_strategy=PasswordAuthStrategy(
                username=config.garoon_username,
                password=config.garoon_password,
            ),
            logger=logger,
        )
        events = client.fetch_events(
            date_range=date_range,
            target_user=config.garoon_target_user,
            target_calendar=config.garoon_target_calendar,
        )
        snapshot = EventSnapshot.build(
            fetched_at=datetime.now().astimezone(),
            date_range=date_range,
            events=events,
        )
        try:
            sync_state = load_profiled_sync_state(
                sync_state_path,
                create_if_missing=not config.caldav_dry_run,
                profile_name=config.profile_name,
            )
        except SyncStateValidationError as exc:
            print_sync_state_validation_failure(
                "load",
                exc,
                path=sync_state_path,
            )
            error_message = str(exc)
            return exit_code
        except SyncStateJsonDecodeError as exc:
            print_sync_state_json_decode_failure(
                "load",
                exc,
                path=sync_state_path,
            )
            error_message = str(exc)
            return exit_code
        except (OSError, ValueError) as exc:
            log_sync_state_failure(
                "load",
                classify_exception_error_kind(exc),
                exc,
                path=sync_state_path,
            )
            print(f"Failed to load sync state from {sync_state_path}: {exc}")
            error_message = str(exc)
            return exit_code
        sync_state = ensure_profile_bound_sync_state(sync_state, profile_name=config.profile_name)
        synced_at = datetime.now().astimezone()
        diff = diff_events(
            events,
            sync_state,
            synced_at=synced_at,
            fetch_window=date_range,
        )
        try:
            sync_plan = build_sync_plan(diff, generated_at=synced_at)
        except (OSError, ValueError) as exc:
            log_sync_plan_failure(
                "build",
                classify_exception_error_kind(exc),
                context="build_sync_plan",
            )
            print(f"Failed to build sync plan: {exc}")
            error_message = str(exc)
            return exit_code

        save_snapshot(config.output_json_path, snapshot.to_dict())
        write_calendar(
            ics_path,
            events,
            sequence_by_event_id=build_sequence_by_event_id(sync_plan),
            uid_by_event_id=build_uid_by_event_id(sync_plan),
        )
        try:
            save_sync_plan(sync_plan_path, sync_plan)
        except (OSError, ValueError) as exc:
            log_sync_plan_failure(
                "save",
                classify_exception_error_kind(exc),
                path=sync_plan_path,
            )
            print(f"Failed to save sync plan to {sync_plan_path}: {exc}")
            error_message = str(exc)
            return exit_code
        action_summary = summarize_sync_plan_actions(sync_plan)
        delete_warning = build_delete_warning(action_summary.delete_count)
        if delete_warning is not None:
            warnings.append(delete_warning)
            print_run_warning(delete_warning)
            log_run_warning(logger, warning=delete_warning, mode=mode)
        dry_run_warning = maybe_build_dry_run_anomalous_change_warning(
            sync_plan,
            dry_run=config.caldav_dry_run,
            create_threshold=config.dry_run_warn_create_count,
            delete_threshold=config.dry_run_warn_delete_count,
        )
        if dry_run_warning is not None:
            print_dry_run_anomalous_change_warning(dry_run_warning)
            log_dry_run_anomalous_change_warning(dry_run_warning)
            warnings.append(
                RunWarning(
                    code="DRY_RUN_ANOMALOUS_CHANGE",
                    message=build_dry_run_anomalous_change_warning_message(dry_run_warning),
                )
            )
        caldav_report = CalDAVClient(
            CalDAVConnectionSettings(
                url=config.caldav_url,
                username=config.caldav_username,
                password=config.caldav_password,
                calendar_name=config.caldav_calendar_name,
                dry_run=config.caldav_dry_run,
                diagnostic_dump_failed_ics=config.caldav_diagnostic_dump_failed_ics,
                diagnostic_dump_success_ics=config.caldav_diagnostic_dump_success_ics,
                diagnostic_dump_uid_lookup_json=config.caldav_diagnostic_dump_uid_lookup_json,
                diagnostic_dir=diagnostics_dir,
                report_dir=reports_dir,
            ),
            logger=logger,
        ).sync(
            sync_plan,
            events,
            generated_at=synced_at,
            previous_sync_state=sync_state.events,
        )
        log_caldav_delivery_failures(caldav_report.results)
        save_caldav_sync_report(caldav_sync_result_path, caldav_report)

        if not config.caldav_dry_run:
            try:
                next_sync_state = build_next_sync_state_from_delivery(
                    events,
                    sync_state,
                    caldav_report.results,
                    synced_at=synced_at,
                    fetch_window=date_range,
                )
            except SyncStateValidationError as exc:
                print_sync_state_validation_failure(
                    "build",
                    exc,
                    context="build_next_sync_state_from_delivery",
                )
                error_message = str(exc)
                return exit_code
            except (OSError, ValueError) as exc:
                log_sync_state_failure(
                    "build",
                    classify_exception_error_kind(exc),
                    exc,
                    context="build_next_sync_state_from_delivery",
                )
                print(f"Failed to build next sync state from delivery results: {exc}")
                error_message = str(exc)
                return exit_code
            if next_sync_state != sync_state:
                try:
                    save_sync_state(sync_state_path, next_sync_state)
                except SyncStateValidationError as exc:
                    print_sync_state_validation_failure(
                        "save",
                        exc,
                        path=sync_state_path,
                    )
                    error_message = str(exc)
                    return exit_code
                except (OSError, ValueError) as exc:
                    log_sync_state_failure(
                        "save",
                        classify_exception_error_kind(exc),
                        exc,
                        path=sync_state_path,
                    )
                    print(f"Failed to save sync state to {sync_state_path}: {exc}")
                    error_message = str(exc)
                    return exit_code
                sync_state_saved = True

        print(f"Fetched {len(events)} events")
        print(f"Saved JSON to {config.output_json_path}")
        print(f"Saved ICS to {ics_path}")
        print(
            "Sync plan summary: "
            f"{action_summary.create_count} create, "
            f"{action_summary.update_count} update, "
            f"{action_summary.skip_count} skip, "
            f"{action_summary.delete_count} delete"
        )
        print(
            "CalDAV sync summary: "
            f"{caldav_report.processed_count} processed, "
            f"{caldav_report.success_count} succeeded, "
            f"{caldav_report.failure_count} failed, "
            f"dry_run={caldav_report.dry_run}"
        )
        print(f"Saved sync plan to {sync_plan_path}")
        print(f"Saved CalDAV sync result to {caldav_sync_result_path}")
        if config.caldav_dry_run:
            print(f"Skipped sync state update because dry_run=true: {sync_state_path}")
        elif sync_state_saved:
            print(f"Saved sync state to {sync_state_path}")
        else:
            print(
                "Sync state unchanged because no create/update/delete actions succeeded: "
                f"{sync_state_path}"
            )
        exit_code = 0 if caldav_report.failure_count == 0 else 1
        if exit_code != 0:
            error_message = f"CalDAV delivery failures detected: {caldav_report.failure_count}"
        return exit_code
    except ConfigError as exc:
        error_message = f"Configuration error: {exc}"
        print(error_message)
    except GaroonAuthenticationError as exc:
        error_message = f"Authentication error: {exc}"
        print(error_message)
    except GaroonClientError as exc:
        error_message = f"Garoon API error: {exc}"
        print(error_message)
    except OSError as exc:
        error_message = f"File output error: {exc}"
        print(error_message)
    except ValueError as exc:
        error_message = f"Sync state error: {exc}"
        print(error_message)
    finally:
        finished_at = datetime.now().astimezone()
        if config is not None and date_range is not None:
            save_run_artifacts(
                config=config,
                started_at=started_at,
                finished_at=finished_at,
                date_range=date_range,
                mode=mode,
                action_summary=action_summary,
                warnings=warnings,
                error_message=error_message,
                result="success" if exit_code == 0 else "failed",
            )
            if logger is not None:
                log_run_finished(
                    logger,
                    config=config,
                    date_range=date_range,
                    mode=mode,
                    action_summary=action_summary,
                    warnings=warnings,
                    error_message=error_message,
                    result="success" if exit_code == 0 else "failed",
                )
    return exit_code


def ensure_profile_bound_sync_state(sync_state: SyncState, *, profile_name: str) -> SyncState:
    if sync_state.profile == profile_name:
        return sync_state
    return SyncState(
        version=sync_state.version,
        profile=profile_name,
        events=dict(sync_state.events),
        tombstones=dict(sync_state.tombstones),
    )


def configure_app_logging(config: AppConfig) -> None:
    try:
        configure_logging(
            config.log_level,
            profile_name=config.profile_name,
            log_file_path=config.log_file_path,
        )
    except TypeError:
        configure_logging(config.log_level)


def load_profiled_sync_state(
    sync_state_path: Path,
    *,
    create_if_missing: bool,
    profile_name: str,
) -> SyncState:
    try:
        return load_sync_state(
            sync_state_path,
            create_if_missing=create_if_missing,
            expected_profile=profile_name,
        )
    except TypeError:
        return load_sync_state(
            sync_state_path,
            create_if_missing=create_if_missing,
        )


def save_run_artifacts(
    *,
    config: AppConfig,
    started_at: datetime,
    finished_at: datetime,
    date_range: DateRange,
    mode: str,
    action_summary: SyncPlanActionSummary | None,
    warnings: list[RunWarning],
    error_message: str | None,
    result: str,
) -> None:
    run_summary_path = config.run_summary_path
    diagnostics_dir = config.diagnostics_dir
    if run_summary_path is None or diagnostics_dir is None:
        return

    summary = RunSummary(
        profile=config.profile_name,
        started_at=started_at.isoformat(timespec="seconds"),
        finished_at=finished_at.isoformat(timespec="seconds"),
        mode=mode,
        dry_run=config.caldav_dry_run,
        fetch_window=build_run_fetch_window(config=config, date_range=date_range),
        result=result,
        counts=build_run_counts(action_summary),
        warnings=list(warnings),
        error=error_message,
    )
    save_run_summary(run_summary_path, summary)
    history_path = build_summary_history_path(
        diagnostics_dir / "run_summaries",
        recorded_at=finished_at,
    )
    save_run_summary_history(history_path, summary)
    print(f"Saved run summary to {run_summary_path}")
    print(f"Saved run summary history to {history_path}")


def build_run_fetch_window(*, config: AppConfig, date_range: DateRange):
    return RunFetchWindow(
        start_days_offset=config.garoon_start_days_offset,
        end_days_offset=config.garoon_end_days_offset,
        start=date_range.start.isoformat(timespec="seconds"),
        end=date_range.end.isoformat(timespec="seconds"),
    )


def build_run_counts(action_summary: SyncPlanActionSummary | None) -> RunCounts:
    if action_summary is None:
        return RunCounts()
    return RunCounts(
        create=action_summary.create_count,
        update=action_summary.update_count,
        delete=action_summary.delete_count,
        skip=action_summary.skip_count,
    )


def log_run_start(
    logger: logging.Logger,
    *,
    config: AppConfig,
    date_range: DateRange,
    mode: str,
) -> None:
    log_structured_info(
        logger,
        "sync run started",
        fields={
            "component": "runner",
            "phase": "start",
            "profile": config.profile_name,
            "dry_run": config.caldav_dry_run,
            "mode": mode,
            "start_days_offset": config.garoon_start_days_offset,
            "end_days_offset": config.garoon_end_days_offset,
            "fetch_window_start": date_range.start.isoformat(timespec="seconds"),
            "fetch_window_end": date_range.end.isoformat(timespec="seconds"),
        },
    )


def log_run_finished(
    logger: logging.Logger,
    *,
    config: AppConfig,
    date_range: DateRange,
    mode: str,
    action_summary: SyncPlanActionSummary | None,
    warnings: list[RunWarning],
    error_message: str | None,
    result: str,
) -> None:
    counts = build_run_counts(action_summary)
    log_structured_info(
        logger,
        "sync run finished",
        fields={
            "component": "runner",
            "phase": "finish",
            "profile": config.profile_name,
            "dry_run": config.caldav_dry_run,
            "mode": mode,
            "start_days_offset": config.garoon_start_days_offset,
            "end_days_offset": config.garoon_end_days_offset,
            "fetch_window_start": date_range.start.isoformat(timespec="seconds"),
            "fetch_window_end": date_range.end.isoformat(timespec="seconds"),
            "result": result,
            "create_count": counts.create,
            "update_count": counts.update,
            "delete_count": counts.delete,
            "skip_count": counts.skip,
            "warning_count": len(warnings),
            "error": error_message,
        },
    )


def log_run_warning(logger: logging.Logger, *, warning: RunWarning, mode: str) -> None:
    log_structured_warning(
        logger,
        "sync run warning",
        fields={
            "component": "runner",
            "phase": "warning",
            "mode": mode,
            "warning_code": warning.code,
            "warning": warning.message,
        },
    )


def print_run_warning(warning: RunWarning) -> None:
    print(f"WARNING[{warning.code}]: {warning.message}")


def build_dry_run_anomalous_change_warning_message(
    warning: DryRunAnomalousChangeWarning,
) -> str:
    return (
        "Dry run detected unusually large pending changes. "
        f"create={warning.create_count} delete={warning.delete_count} total={warning.total_count}. "
        "Review sync_plan.json and sync_state.json before proceeding."
    )


def build_date_range(start_days_offset: int, end_days_offset: int) -> DateRange:
    now = datetime.now().astimezone()
    start_date = now.date() + timedelta(days=start_days_offset)
    end_date = now.date() + timedelta(days=end_days_offset)
    tzinfo = now.tzinfo
    start = datetime.combine(start_date, time(hour=0, minute=0, second=0), tzinfo=tzinfo)
    end = datetime.combine(end_date, time(hour=23, minute=59, second=59), tzinfo=tzinfo)
    return DateRange(start=start, end=end)


def save_snapshot(output_path: Path, payload: dict[str, object]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def maybe_build_dry_run_anomalous_change_warning(
    sync_plan,
    *,
    dry_run: bool,
    create_threshold: int,
    delete_threshold: int,
) -> DryRunAnomalousChangeWarning | None:
    if not dry_run:
        return None
    summary = summarize_sync_plan_actions(sync_plan)
    warning = DryRunAnomalousChangeWarning(
        create_count=summary.create_count,
        delete_count=summary.delete_count,
        total_count=summary.total_count,
        create_threshold=create_threshold,
        delete_threshold=delete_threshold,
    )
    return warning if warning.triggered else None


def print_dry_run_anomalous_change_warning(warning: DryRunAnomalousChangeWarning) -> None:
    print(
        "WARNING: dry-run detected unusually large pending changes.\n"
        f"- create: {warning.create_count} (threshold: {warning.create_threshold})\n"
        f"- delete: {warning.delete_count} (threshold: {warning.delete_threshold})\n"
        f"- total actions: {warning.total_count}\n"
        "- inspect examples: python -m src.sync_plan_inspect --action create | "
        "python -m src.sync_plan_inspect --action delete\n"
        "Review sync_state.json and sync_plan.json for the active profile, then verify representative "
        "events on a test calendar before proceeding to production."
    )


def log_dry_run_anomalous_change_warning(warning: DryRunAnomalousChangeWarning) -> None:
    fields = {
        "component": "sync_plan",
        "phase": "dry_run_review",
        "error_kind": "anomalous_change_warning",
        "create_count": warning.create_count,
        "delete_count": warning.delete_count,
        "total_count": warning.total_count,
        "create_threshold": warning.create_threshold,
        "delete_threshold": warning.delete_threshold,
    }
    log_structured_error(
        logging.getLogger(__name__),
        "dry-run anomalous change warning",
        fields=fields,
    )


def print_sync_state_validation_failure(
    stage: str,
    exc: SyncStateValidationError,
    *,
    path: Path | None = None,
    context: str | None = None,
) -> None:
    log_sync_state_failure(
        stage,
        "validation_failed",
        exc,
        path=path,
        context=context,
    )
    print_sync_state_failure(
        stage,
        "validation failed",
        exc,
        path=path,
        context=context,
    )


def print_sync_state_json_decode_failure(
    stage: str,
    exc: SyncStateJsonDecodeError,
    *,
    path: Path | None = None,
    context: str | None = None,
) -> None:
    log_sync_state_failure(
        stage,
        "json_decode_failed",
        exc,
        path=path,
        context=context,
    )
    print_sync_state_failure(
        stage,
        "json decode failed",
        exc,
        path=path,
        context=context,
    )


def print_sync_state_failure(
    stage: str,
    reason: str,
    exc: ValueError,
    *,
    path: Path | None = None,
    context: str | None = None,
) -> None:
    location = build_sync_state_failure_location(path=path, context=context)
    prefix = f"[sync_state:{stage}] {reason}"
    if location:
        prefix = f"{prefix} [{location}]"
    print(f"{prefix}:\n{extract_sync_state_failure_details(exc)}")


def extract_sync_state_failure_details(exc: ValueError) -> str:
    message = str(exc)
    _, separator, details = message.partition(":\n")
    return details if separator else message


def log_sync_state_failure(
    stage: str,
    error_kind: str,
    exc: Exception,
    *,
    path: Path | None = None,
    context: str | None = None,
) -> None:
    resolved_path = build_sync_state_failure_path(path=path, context=context)
    event_id, ics_uid = extract_sync_state_failure_identifiers(exc)
    fields = {
        "component": "sync_state",
        "phase": stage,
        "error_kind": error_kind,
        "path": resolved_path,
    }
    if event_id is not None:
        fields["event_id"] = event_id
    if ics_uid is not None:
        fields["ics_uid"] = ics_uid
    log_structured_error(
        logging.getLogger(__name__),
        "sync_state failure",
        fields=fields,
    )


def log_sync_plan_failure(
    stage: str,
    error_kind: str,
    *,
    path: Path | None = None,
    context: str | None = None,
) -> None:
    fields = {
        "component": "sync_plan",
        "phase": stage,
        "error_kind": error_kind,
        "path": build_sync_state_failure_path(path=path, context=context),
    }
    log_structured_error(
        logging.getLogger(__name__),
        "sync_plan failure",
        fields=fields,
    )


def log_caldav_delivery_failures(results: list[object]) -> None:
    logger = logging.getLogger(__name__)
    for result in results:
        if not isinstance(result, CalDAVActionResult):
            continue
        if result.success:
            continue
        fields = {
            "component": "caldav",
            "phase": "deliver",
            "error_kind": classify_caldav_delivery_error_kind(result),
            "event_id": result.event_id,
            "ics_uid": result.ics_uid,
            "action": result.action,
            "resource_url": result.resource_url,
            "resolution_strategy": result.resolution_strategy,
            "conflict_kind": result.conflict_kind,
            "status_code": result.status_code,
            "retryable": result.retryable,
            "create_conflict_resource_exists": result.create_conflict_resource_exists,
            "create_conflict_uid_match_found": result.create_conflict_uid_match_found,
            "create_conflict_uid_lookup_attempted": result.create_conflict_uid_lookup_attempted,
            "create_conflict_uid_lookup_candidates": result.create_conflict_uid_lookup_candidates,
            "create_conflict_uid_lookup_method": result.create_conflict_uid_lookup_method,
            "create_conflict_remote_uid_confirmed": result.create_conflict_remote_uid_confirmed,
            "create_conflict_state_drift_suspected": result.create_conflict_state_drift_suspected,
            "create_conflict_existing_resource_url": result.create_conflict_existing_resource_url,
            "create_conflict_selected_candidate_reason": (
                result.create_conflict_selected_candidate_reason
            ),
            "create_conflict_selected_candidate_index": (
                result.create_conflict_selected_candidate_index
            ),
            "create_conflict_uid_lookup_candidate_hrefs": _extract_uid_lookup_candidate_hrefs(result),
            "create_conflict_uid_lookup_diagnostics_path": (
                result.create_conflict_uid_lookup_diagnostics_path
            ),
            "create_conflict_uid_query_raw_path": result.create_conflict_uid_query_raw_path,
            "create_conflict_collection_scan_raw_path": (
                result.create_conflict_collection_scan_raw_path
            ),
            "error": result.error,
        }
        fields.update(_build_create_412_precondition_log_fields(result))
        log_structured_error(
            logger,
            "caldav delivery failure",
            fields=fields,
        )


def classify_caldav_delivery_error_kind(result: CalDAVActionResult) -> str:
    if result.error_kind is not None:
        return result.error_kind
    if result.conflict_kind is not None:
        return result.conflict_kind
    if result.status_code is not None:
        return "http_failed"
    return "other"


def _build_create_412_precondition_log_fields(result: CalDAVActionResult) -> dict[str, object]:
    if result.action != "create" or result.status_code != 412:
        return {}

    request_headers = result.request_headers or {}
    response_headers = result.response_headers or {}
    return {
        "request_method": result.request_method,
        "request_url": result.request_url,
        "request_if_none_match": request_headers.get("If-None-Match"),
        "request_if_match": request_headers.get("If-Match"),
        "request_content_type": request_headers.get("Content-Type"),
        "request_content_length": request_headers.get("Content-Length"),
        "create_conflict_uid_lookup_attempted": result.create_conflict_uid_lookup_attempted,
        "create_conflict_uid_lookup_candidates": result.create_conflict_uid_lookup_candidates,
        "create_conflict_uid_lookup_method": result.create_conflict_uid_lookup_method,
        "create_conflict_remote_uid_confirmed": result.create_conflict_remote_uid_confirmed,
        "create_conflict_selected_candidate_reason": result.create_conflict_selected_candidate_reason,
        "create_conflict_selected_candidate_index": result.create_conflict_selected_candidate_index,
        "create_conflict_uid_lookup_candidate_hrefs": _extract_uid_lookup_candidate_hrefs(result),
        "create_conflict_uid_lookup_diagnostics_path": result.create_conflict_uid_lookup_diagnostics_path,
        "create_conflict_uid_query_raw_path": result.create_conflict_uid_query_raw_path,
        "create_conflict_collection_scan_raw_path": result.create_conflict_collection_scan_raw_path,
        "response_etag": response_headers.get("ETag"),
        "response_content_type": response_headers.get("Content-Type"),
        "response_content_length": response_headers.get("Content-Length"),
        "response_location": response_headers.get("Location"),
        "response_body_excerpt": result.response_body_excerpt,
    }


def build_sync_state_failure_location(
    *,
    path: Path | None = None,
    context: str | None = None,
) -> str | None:
    location = context
    if path is not None:
        location = f"{location} ({path})" if location else str(path)
    return location


def _extract_uid_lookup_candidate_hrefs(result: CalDAVActionResult) -> list[str]:
    return [
        str(candidate.get("href"))
        for candidate in result.create_conflict_uid_lookup_raw_candidates
        if isinstance(candidate, dict) and isinstance(candidate.get("href"), str)
    ]


def build_sync_state_failure_path(
    *,
    path: Path | None = None,
    context: str | None = None,
) -> str:
    if path is not None:
        return str(path)
    if context is not None:
        return context
    return "unknown"


def extract_sync_state_failure_identifiers(exc: Exception) -> tuple[str | None, str | None]:
    if not isinstance(exc, ValueError):
        return None, None
    details = extract_sync_state_failure_details(exc)
    event_id = find_first_sync_state_failure_match(details, _SYNC_STATE_EVENT_ID_PATTERNS)
    ics_uid = find_first_sync_state_failure_match(details, (_SYNC_STATE_ICS_UID_PATTERN,))
    return event_id, ics_uid


def find_first_sync_state_failure_match(
    text: str,
    patterns: tuple[re.Pattern[str], ...],
) -> str | None:
    for pattern in patterns:
        match = pattern.search(text)
        if match is not None:
            return match.group(1)
    return None


if __name__ == "__main__":
    raise SystemExit(main())
