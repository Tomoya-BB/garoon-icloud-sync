from __future__ import annotations

import json
import logging
from pathlib import Path

from src.caldav_client import CalDAVActionResult, CalDAVSyncReport
from src.config import AppConfig
from src.models import DateRange, EventDateTime, EventRecord
from src.sync_state import (
    EventSyncState,
    SyncStateJsonDecodeError,
    SyncState,
    SyncStateValidationError,
    TombstoneSyncState,
    build_event_content_hash,
)
import src.main as main_module


def test_main_dry_run_does_not_save_sync_state(monkeypatch, tmp_path: Path) -> None:
    config = AppConfig(
        garoon_base_url="https://garoon.example.com/g",
        garoon_username="user",
        garoon_password="pass",
        garoon_start_days_offset=0,
        garoon_end_days_offset=1,
        output_json_path=tmp_path / "events.json",
        log_level="INFO",
        caldav_url="https://caldav.example.com/principals/tomo",
        caldav_username="caldav-user",
        caldav_password="caldav-pass",
        caldav_calendar_name="PoC Calendar",
        caldav_dry_run=True,
    )
    events = [_build_event("evt-1")]
    load_calls: list[tuple[Path, bool]] = []
    save_calls: list[tuple[Path, SyncState]] = []

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return events

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            assert sync_events == events
            assert previous_sync_state == {}
            return CalDAVSyncReport(
                generated_at="2026-03-12T00:00:00+00:00",
                dry_run=True,
                calendar_name=self._settings.calendar_name,
                source_url=self._settings.url,
                processed_count=1,
                ignored_count=0,
                results=[
                    CalDAVActionResult(
                        action="create",
                        event_id="evt-1",
                        ics_uid="uid-1",
                        sequence=0,
                        dry_run=True,
                        success=True,
                        sent=False,
                        action_reason="new_event",
                        resource_name="uid-1.ics",
                        resource_url=None,
                        etag=None,
                        updated_at="2026-03-12T00:00:00Z",
                        delivered_at=None,
                        payload_summary={"subject": "Subject evt-1"},
                        payload_bytes=123,
                    )
                ],
            )

    def fake_load_sync_state(path: Path, *, create_if_missing: bool = True) -> SyncState:
        load_calls.append((path, create_if_missing))
        return SyncState.empty()

    def fake_save_sync_state(path: Path, state: SyncState) -> None:
        save_calls.append((path, state))

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", fake_load_sync_state)
    monkeypatch.setattr(main_module, "save_sync_state", fake_save_sync_state)
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    exit_code = main_module.main()

    assert exit_code == 0
    assert load_calls == [(tmp_path / "sync_state.json", False)]
    assert save_calls == []


def test_main_dry_run_does_not_plan_delete_for_events_outside_current_fetch_window(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = _build_app_config(
        tmp_path,
        dry_run=True,
        start_days_offset=0,
        end_days_offset=31,
    )
    normal_window = DateRange(
        start=EventDateTime("2026-03-15T00:00:00+09:00").as_datetime(),
        end=EventDateTime("2026-04-15T23:59:59+09:00").as_datetime(),
    )
    previous_state = SyncState(
        events={
            "evt-in-range": EventSyncState(
                event_id="evt-in-range",
                ics_uid="uid-in-range",
                updated_at="2026-03-12T00:00:00Z",
                content_hash=build_event_content_hash(_build_event("evt-in-range")),
                sequence=1,
                is_deleted=False,
                last_synced_at="2026-03-10T00:00:00+00:00",
                last_seen_window_start="2025-03-15T00:00:00+00:00",
                last_seen_window_end="2026-09-14T23:59:59+00:00",
            ),
            "evt-out-of-range": EventSyncState(
                event_id="evt-out-of-range",
                ics_uid="uid-out-of-range",
                updated_at="2026-03-10T00:00:00Z",
                content_hash=build_event_content_hash(_build_event("evt-out-of-range")),
                sequence=2,
                is_deleted=False,
                last_synced_at="2026-03-10T00:00:00+00:00",
                last_seen_window_start="2025-03-15T00:00:00+00:00",
                last_seen_window_end="2026-09-14T23:59:59+00:00",
            ),
        }
    )
    build_date_range_calls: list[tuple[int, int]] = []
    saved_plans = []

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)
    monkeypatch.setattr(
        main_module,
        "build_date_range",
        lambda start_days_offset, end_days_offset: (
            build_date_range_calls.append((start_days_offset, end_days_offset)) or normal_window
        ),
    )

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            assert kwargs["date_range"] == normal_window
            return [_build_event("evt-in-range")]

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            assert sync_events == [_build_event("evt-in-range")]
            assert previous_sync_state == previous_state.events
            assert all(action.action.value != "delete" for action in sync_plan.actions)
            return CalDAVSyncReport(
                generated_at="2026-03-15T00:00:00+00:00",
                dry_run=True,
                calendar_name=self._settings.calendar_name,
                source_url=self._settings.url,
                processed_count=0,
                ignored_count=len(sync_plan.actions),
                results=[],
            )

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: previous_state)
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda path, plan: saved_plans.append(plan))
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    exit_code = main_module.main()

    assert exit_code == 0
    assert build_date_range_calls == [(0, 31)]
    assert len(saved_plans) == 1
    assert [action.action.value for action in saved_plans[0].actions] == ["skip"]


def test_main_dry_run_does_not_warn_when_create_count_is_below_threshold(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = _build_app_config(
        tmp_path,
        dry_run=True,
        create_warn_count=2,
        delete_warn_count=2,
    )
    events = [_build_event("evt-1")]

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return events

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            return CalDAVSyncReport(
                generated_at="2026-03-12T00:00:00+00:00",
                dry_run=True,
                calendar_name=self._settings.calendar_name,
                source_url=self._settings.url,
                processed_count=len(sync_plan.actions),
                ignored_count=0,
                results=[],
            )

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: SyncState.empty())
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "WARNING: dry-run detected unusually large pending changes." not in captured.out
    assert not _find_log_messages(caplog, "dry-run anomalous change warning")


def test_main_dry_run_warns_when_create_count_reaches_threshold(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = _build_app_config(
        tmp_path,
        dry_run=True,
        create_warn_count=1,
        delete_warn_count=5,
    )
    events = [_build_event("evt-1")]

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return events

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            return CalDAVSyncReport(
                generated_at="2026-03-12T00:00:00+00:00",
                dry_run=True,
                calendar_name=self._settings.calendar_name,
                source_url=self._settings.url,
                processed_count=len(sync_plan.actions),
                ignored_count=0,
                results=[],
            )

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: SyncState.empty())
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    _assert_dry_run_warning_output(
        captured.out,
        create_count=1,
        delete_count=0,
        total_count=1,
        create_threshold=1,
        delete_threshold=5,
    )
    _assert_structured_log(
        caplog,
        message_prefix="dry-run anomalous change warning",
        component="sync_plan",
        phase="dry_run_review",
        error_kind="anomalous_change_warning",
        create_count="1",
        delete_count="0",
        total_count="1",
    )


def test_main_dry_run_warns_when_delete_count_reaches_threshold(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = _build_app_config(
        tmp_path,
        dry_run=True,
        create_warn_count=5,
        delete_warn_count=1,
    )
    previous_state = SyncState(
        events={
            "evt-deleted": EventSyncState(
                event_id="evt-deleted",
                ics_uid="uid-deleted",
                updated_at="2026-03-11T00:00:00Z",
                content_hash="hash-deleted",
                sequence=3,
                is_deleted=False,
                last_synced_at="2026-03-11T00:00:00+00:00",
                last_seen_window_start="2026-03-11T00:00:00+00:00",
                last_seen_window_end="2026-03-12T23:59:59+00:00",
            )
        }
    )
    fetch_window = DateRange(
        start=EventDateTime("2026-03-11T00:00:00+00:00").as_datetime(),
        end=EventDateTime("2026-03-12T23:59:59+00:00").as_datetime(),
    )

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)
    monkeypatch.setattr(main_module, "build_date_range", lambda *_args: fetch_window)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return []

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            return CalDAVSyncReport(
                generated_at="2026-03-12T00:00:00+00:00",
                dry_run=True,
                calendar_name=self._settings.calendar_name,
                source_url=self._settings.url,
                processed_count=len(sync_plan.actions),
                ignored_count=0,
                results=[],
            )

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: previous_state)
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    _assert_dry_run_warning_output(
        captured.out,
        create_count=0,
        delete_count=1,
        total_count=1,
        create_threshold=5,
        delete_threshold=1,
    )
    _assert_structured_log(
        caplog,
        message_prefix="dry-run anomalous change warning",
        component="sync_plan",
        phase="dry_run_review",
        error_kind="anomalous_change_warning",
        create_count="0",
        delete_count="1",
        total_count="1",
    )


def test_main_does_not_warn_for_non_dry_run_even_when_threshold_is_reached(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = _build_app_config(
        tmp_path,
        dry_run=False,
        create_warn_count=1,
        delete_warn_count=1,
    )
    events = [_build_event("evt-1")]

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return events

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            return CalDAVSyncReport(
                generated_at="2026-03-12T00:00:00+00:00",
                dry_run=False,
                calendar_name=self._settings.calendar_name,
                source_url=self._settings.url,
                processed_count=len(sync_plan.actions),
                ignored_count=0,
                results=[],
            )

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: SyncState.empty())
    monkeypatch.setattr(main_module, "build_next_sync_state_from_delivery", lambda *args, **kwargs: SyncState.empty())
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "WARNING: dry-run detected unusually large pending changes." not in captured.out
    assert not _find_log_messages(caplog, "dry-run anomalous change warning")


def test_main_saves_recovered_resource_metadata_without_marking_delivery_success(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = AppConfig(
        garoon_base_url="https://garoon.example.com/g",
        garoon_username="user",
        garoon_password="pass",
        garoon_start_days_offset=0,
        garoon_end_days_offset=1,
        output_json_path=tmp_path / "events.json",
        log_level="INFO",
        caldav_url="https://caldav.example.com/principals/tomo",
        caldav_username="caldav-user",
        caldav_password="caldav-pass",
        caldav_calendar_name="PoC Calendar",
        caldav_dry_run=False,
    )
    events = [_build_event("evt-1")]
    previous_state = SyncState(
        events={
            "evt-1": EventSyncState(
                event_id="evt-1",
                ics_uid="uid-1",
                updated_at="2026-03-11T00:00:00Z",
                content_hash=build_event_content_hash(_build_event("evt-1", subject="Before")),
                sequence=3,
                is_deleted=False,
                last_synced_at="2026-03-11T00:00:00+00:00",
                resource_url="https://caldav.example.com/calendars/tomo/old.ics",
                etag="\"old-etag\"",
                last_delivery_status="success",
                last_delivery_at="2026-03-11T00:00:00+00:00",
            )
        }
    )
    save_calls: list[tuple[Path, SyncState]] = []

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return events

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            assert previous_sync_state == previous_state.events
            return CalDAVSyncReport(
                generated_at="2026-03-12T00:00:00+00:00",
                dry_run=False,
                calendar_name=self._settings.calendar_name,
                source_url=self._settings.url,
                processed_count=1,
                ignored_count=0,
                results=[
                    CalDAVActionResult(
                        action="update",
                        event_id="evt-1",
                        ics_uid="uid-1",
                        sequence=3,
                        dry_run=False,
                        success=False,
                        sent=False,
                        action_reason="content_changed",
                        resource_name="uid-1.ics",
                        resource_url="https://caldav.example.com/calendars/tomo/old.ics",
                        etag=None,
                        updated_at="2026-03-12T00:00:00Z",
                        delivered_at=None,
                        payload_summary={"subject": "Subject evt-1"},
                        payload_bytes=123,
                        recovery_attempted=True,
                        recovery_succeeded=True,
                        refreshed_resource_url="https://caldav.example.com/calendars/tomo/current.ics",
                        refreshed_etag="\"fresh-etag\"",
                        status_code=412,
                        conflict_kind="etag_mismatch",
                        retryable=True,
                        etag_mismatch=True,
                        attempted_conditional_update=True,
                    )
                ],
            )

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: previous_state)
    monkeypatch.setattr(main_module, "save_sync_state", lambda path, state: save_calls.append((path, state)))
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    exit_code = main_module.main()

    assert exit_code == 1
    assert len(save_calls) == 1
    saved_state = save_calls[0][1]
    assert saved_state.events["evt-1"].resource_url == "https://caldav.example.com/calendars/tomo/current.ics"
    assert saved_state.events["evt-1"].etag == "\"fresh-etag\""
    assert saved_state.events["evt-1"].updated_at == "2026-03-11T00:00:00Z"
    assert saved_state.events["evt-1"].last_delivery_at == "2026-03-11T00:00:00+00:00"


def test_main_prints_clear_message_when_sync_state_load_fails(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = AppConfig(
        garoon_base_url="https://garoon.example.com/g",
        garoon_username="user",
        garoon_password="pass",
        garoon_start_days_offset=0,
        garoon_end_days_offset=1,
        output_json_path=tmp_path / "events.json",
        log_level="INFO",
        caldav_url="https://caldav.example.com/principals/tomo",
        caldav_username="caldav-user",
        caldav_password="caldav-pass",
        caldav_calendar_name="PoC Calendar",
        caldav_dry_run=False,
    )

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return []

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(
        main_module,
        "load_sync_state",
        lambda path, create_if_missing=True: (_ for _ in ()).throw(
            SyncStateValidationError(
                "Invalid sync state while loading:\n"
                "- event_id 'evt-1' exists in both events and tombstones"
            )
        ),
    )
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 1
    _assert_sync_state_validation_output(
        captured.out,
        stage="load",
        detail="event_id 'evt-1' exists in both events and tombstones",
        location=str(tmp_path / "sync_state.json"),
    )
    _assert_sync_state_structured_log(
        caplog,
        phase="load",
        error_kind="validation_failed",
        path=str(tmp_path / "sync_state.json"),
        event_id="evt-1",
    )


def test_main_prints_clear_message_when_sync_state_json_is_broken(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = AppConfig(
        garoon_base_url="https://garoon.example.com/g",
        garoon_username="user",
        garoon_password="pass",
        garoon_start_days_offset=0,
        garoon_end_days_offset=1,
        output_json_path=tmp_path / "events.json",
        log_level="INFO",
        caldav_url="https://caldav.example.com/principals/tomo",
        caldav_username="caldav-user",
        caldav_password="caldav-pass",
        caldav_calendar_name="PoC Calendar",
        caldav_dry_run=False,
    )
    state_path = tmp_path / "sync_state.json"
    decode_error = json.JSONDecodeError(
        "Expecting ',' delimiter",
        '{"version": 3\n"events": {}}',
        14,
    )

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return []

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(
        main_module,
        "load_sync_state",
        lambda path, create_if_missing=True: (_ for _ in ()).throw(
            SyncStateJsonDecodeError(state_path, decode_error)
        ),
    )
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", state_path)

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 1
    _assert_sync_state_failure_output(
        captured.out,
        stage="load",
        reason="json decode failed",
        detail="invalid JSON at line 2, column 1 (char 14): Expecting ',' delimiter",
        location=str(state_path),
    )
    _assert_sync_state_structured_log(
        caplog,
        phase="load",
        error_kind="json_decode_failed",
        path=str(state_path),
    )


def test_main_prints_clear_message_when_sync_state_save_fails(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = AppConfig(
        garoon_base_url="https://garoon.example.com/g",
        garoon_username="user",
        garoon_password="pass",
        garoon_start_days_offset=0,
        garoon_end_days_offset=1,
        output_json_path=tmp_path / "events.json",
        log_level="INFO",
        caldav_url="https://caldav.example.com/principals/tomo",
        caldav_username="caldav-user",
        caldav_password="caldav-pass",
        caldav_calendar_name="PoC Calendar",
        caldav_dry_run=False,
    )
    events = [_build_event("evt-1")]

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return events

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            return CalDAVSyncReport(
                generated_at="2026-03-12T00:00:00+00:00",
                dry_run=False,
                calendar_name=self._settings.calendar_name,
                source_url=self._settings.url,
                processed_count=1,
                ignored_count=0,
                results=[
                    CalDAVActionResult(
                        action="create",
                        event_id="evt-1",
                        ics_uid="uid-1",
                        sequence=0,
                        dry_run=False,
                        success=True,
                        sent=True,
                        action_reason="new_event",
                        resource_name="uid-1.ics",
                        resource_url="https://caldav.example.com/calendars/tomo/uid-1.ics",
                        etag="\"etag-1\"",
                        updated_at="2026-03-12T00:00:00Z",
                        delivered_at="2026-03-12T00:00:00+00:00",
                        payload_summary={"subject": "Subject evt-1"},
                        payload_bytes=123,
                    )
                ],
            )

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: SyncState.empty())
    monkeypatch.setattr(
        main_module,
        "build_next_sync_state_from_delivery",
        lambda *args, **kwargs: SyncState(
            events={
                "evt-1": EventSyncState(
                    event_id="evt-1",
                    ics_uid="uid-1",
                    updated_at="2026-03-12T00:00:00Z",
                    content_hash="hash-1",
                    sequence=0,
                    is_deleted=False,
                    last_synced_at="2026-03-12T00:00:00+00:00",
                )
            },
            tombstones={
                "evt-1": TombstoneSyncState(
                    event_id="evt-1",
                    ics_uid="uid-deleted",
                    deleted_at="2026-03-12T00:00:00+00:00",
                    last_delivery_status="success",
                )
            },
        ),
    )
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 1
    _assert_sync_state_validation_output(
        captured.out,
        stage="save",
        detail="event_id 'evt-1' exists in both events and tombstones",
        location=str(tmp_path / "sync_state.json"),
    )
    _assert_sync_state_structured_log(
        caplog,
        phase="save",
        error_kind="validation_failed",
        path=str(tmp_path / "sync_state.json"),
        event_id="evt-1",
    )
    assert not (tmp_path / "sync_state.json").exists()


def test_main_prints_clear_message_when_sync_state_build_fails(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = AppConfig(
        garoon_base_url="https://garoon.example.com/g",
        garoon_username="user",
        garoon_password="pass",
        garoon_start_days_offset=0,
        garoon_end_days_offset=1,
        output_json_path=tmp_path / "events.json",
        log_level="INFO",
        caldav_url="https://caldav.example.com/principals/tomo",
        caldav_username="caldav-user",
        caldav_password="caldav-pass",
        caldav_calendar_name="PoC Calendar",
        caldav_dry_run=False,
    )
    events = [_build_event("evt-1")]
    save_calls: list[tuple[Path, SyncState]] = []

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return events

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            return CalDAVSyncReport(
                generated_at="2026-03-12T00:00:00+00:00",
                dry_run=False,
                calendar_name=self._settings.calendar_name,
                source_url=self._settings.url,
                processed_count=1,
                ignored_count=0,
                results=[
                    CalDAVActionResult(
                        action="create",
                        event_id="evt-1",
                        ics_uid="uid-1",
                        sequence=0,
                        dry_run=False,
                        success=True,
                        sent=True,
                        action_reason="new_event",
                        resource_name="uid-1.ics",
                        resource_url="https://caldav.example.com/calendars/tomo/uid-1.ics",
                        etag="\"etag-1\"",
                        updated_at="2026-03-12T00:00:00Z",
                        delivered_at="2026-03-12T00:00:00+00:00",
                        payload_summary={"subject": "Subject evt-1"},
                        payload_bytes=123,
                    )
                ],
            )

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: SyncState.empty())
    monkeypatch.setattr(
        main_module,
        "build_next_sync_state_from_delivery",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            SyncStateValidationError(
                "Refusing to return invalid sync state from build_next_sync_state_from_delivery:\n"
                "- ics_uid 'uid-1' is duplicated across events['evt-1'], tombstones['evt-1']"
            )
        ),
    )
    monkeypatch.setattr(main_module, "save_sync_state", lambda path, state: save_calls.append((path, state)))
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert save_calls == []
    _assert_sync_state_validation_output(
        captured.out,
        stage="build",
        detail="ics_uid 'uid-1' is duplicated across events['evt-1'], tombstones['evt-1']",
        location="build_next_sync_state_from_delivery",
    )
    _assert_sync_state_structured_log(
        caplog,
        phase="build",
        error_kind="validation_failed",
        path="build_next_sync_state_from_delivery",
        event_id="evt-1",
        ics_uid="uid-1",
    )


def test_main_logs_sync_state_load_io_failure_as_structured_log(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = _build_app_config(tmp_path)

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return []

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(
        main_module,
        "load_sync_state",
        lambda path, create_if_missing=True: (_ for _ in ()).throw(OSError("disk read failed")),
    )
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == f"Failed to load sync state from {tmp_path / 'sync_state.json'}: disk read failed\n"
    _assert_structured_log(
        caplog,
        message_prefix="sync_state failure",
        component="sync_state",
        phase="load",
        error_kind="io_failed",
        path=str(tmp_path / "sync_state.json"),
    )


def test_main_logs_sync_state_load_other_failure_as_structured_log(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = _build_app_config(tmp_path)

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return []

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(
        main_module,
        "load_sync_state",
        lambda path, create_if_missing=True: (_ for _ in ()).throw(ValueError("unexpected load failure")),
    )
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == (
        f"Failed to load sync state from {tmp_path / 'sync_state.json'}: unexpected load failure\n"
    )
    _assert_structured_log(
        caplog,
        message_prefix="sync_state failure",
        component="sync_state",
        phase="load",
        error_kind="other",
        path=str(tmp_path / "sync_state.json"),
    )


def test_main_logs_sync_state_save_io_failure_as_structured_log(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = _build_app_config(tmp_path)
    events = [_build_event("evt-1")]

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return events

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            return _build_successful_caldav_report(self._settings)

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: SyncState.empty())
    monkeypatch.setattr(
        main_module,
        "build_next_sync_state_from_delivery",
        lambda *args, **kwargs: _build_next_sync_state("evt-1"),
    )
    monkeypatch.setattr(
        main_module,
        "save_sync_state",
        lambda path, state: (_ for _ in ()).throw(OSError("disk write failed")),
    )
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out.endswith(
        f"Failed to save sync state to {tmp_path / 'sync_state.json'}: disk write failed\n"
    )
    _assert_structured_log(
        caplog,
        message_prefix="sync_state failure",
        component="sync_state",
        phase="save",
        error_kind="io_failed",
        path=str(tmp_path / "sync_state.json"),
    )


def test_main_logs_sync_state_save_other_failure_as_structured_log(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = _build_app_config(tmp_path)
    events = [_build_event("evt-1")]

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return events

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            return _build_successful_caldav_report(self._settings)

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: SyncState.empty())
    monkeypatch.setattr(
        main_module,
        "build_next_sync_state_from_delivery",
        lambda *args, **kwargs: _build_next_sync_state("evt-1"),
    )
    monkeypatch.setattr(
        main_module,
        "save_sync_state",
        lambda path, state: (_ for _ in ()).throw(ValueError("unexpected save failure")),
    )
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out.endswith(
        f"Failed to save sync state to {tmp_path / 'sync_state.json'}: unexpected save failure\n"
    )
    _assert_structured_log(
        caplog,
        message_prefix="sync_state failure",
        component="sync_state",
        phase="save",
        error_kind="other",
        path=str(tmp_path / "sync_state.json"),
    )


def test_main_logs_sync_plan_save_failure_as_structured_log(
    monkeypatch,
    tmp_path: Path,
    capsys,
    caplog,
) -> None:
    config = _build_app_config(tmp_path, dry_run=True)
    events = [_build_event("evt-1")]

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return events

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: SyncState.empty())
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        main_module,
        "save_sync_plan",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("permission denied")),
    )
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out.endswith(
        f"Failed to save sync plan to {tmp_path / 'sync_plan.json'}: permission denied\n"
    )
    _assert_structured_log(
        caplog,
        message_prefix="sync_plan failure",
        component="sync_plan",
        phase="save",
        error_kind="io_failed",
        path=str(tmp_path / "sync_plan.json"),
    )


def test_main_logs_caldav_delivery_failure_as_structured_log(
    monkeypatch,
    tmp_path: Path,
    caplog,
) -> None:
    config = _build_app_config(tmp_path)
    events = [_build_event("evt-1")]

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return events

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            return CalDAVSyncReport(
                generated_at="2026-03-12T00:00:00+00:00",
                dry_run=False,
                calendar_name=self._settings.calendar_name,
                source_url=self._settings.url,
                processed_count=1,
                ignored_count=0,
                results=[
                    CalDAVActionResult(
                        action="update",
                        event_id="evt-1",
                        ics_uid="uid-1",
                        sequence=7,
                        dry_run=False,
                        success=False,
                        sent=False,
                        action_reason="content_changed",
                        resource_name="uid-1.ics",
                        resource_url="https://caldav.example.com/calendars/tomo/current.ics",
                        etag=None,
                        updated_at="2026-03-12T00:00:00Z",
                        delivered_at=None,
                        payload_summary={"subject": "Subject evt-1"},
                        payload_bytes=123,
                        resolution_strategy="sync_state_resource_url",
                        conflict_kind="etag_mismatch",
                        retryable=True,
                        status_code=412,
                        error="PUT failed with 412",
                    )
                ],
            )

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: SyncState.empty())
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        main_module,
        "build_next_sync_state_from_delivery",
        lambda *args, **kwargs: SyncState.empty(),
    )
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()

    assert exit_code == 1
    _assert_structured_log(
        caplog,
        message_prefix="caldav delivery failure",
        component="caldav",
        phase="deliver",
        error_kind="etag_mismatch",
        event_id="evt-1",
        ics_uid="uid-1",
        action="update",
        conflict_kind="etag_mismatch",
        status_code="412",
        resource_url="https://caldav.example.com/calendars/tomo/current.ics",
    )


def test_main_logs_create_412_diagnostics_in_structured_error(
    monkeypatch,
    tmp_path: Path,
    caplog,
) -> None:
    config = _build_app_config(tmp_path)
    events = [_build_event("evt-1")]

    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(main_module, "configure_logging", lambda level: None)

    class FakeGaroonClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_events(self, **kwargs) -> list[EventRecord]:
            return events

    class FakeCalDAVClient:
        def __init__(self, settings, *, logger=None) -> None:
            self._settings = settings

        def sync(self, sync_plan, sync_events, *, generated_at=None, previous_sync_state=None) -> CalDAVSyncReport:
            return CalDAVSyncReport(
                generated_at="2026-03-12T00:00:00+00:00",
                dry_run=False,
                calendar_name=self._settings.calendar_name,
                source_url=self._settings.url,
                processed_count=1,
                ignored_count=0,
                results=[
                    CalDAVActionResult(
                        action="create",
                        event_id="evt-1",
                        ics_uid="uid-1",
                        sequence=0,
                        dry_run=False,
                        success=False,
                        sent=False,
                        action_reason="new_event",
                        resource_name="uid-1.ics",
                        resource_url="https://caldav.example.com/calendars/tomo/poc/uid-1.ics",
                        etag=None,
                        updated_at="2026-03-12T00:00:00Z",
                        delivered_at=None,
                        payload_summary={"subject": "Subject evt-1"},
                        payload_bytes=123,
                        resolution_strategy="create_resource_name",
                        conflict_kind="precondition_failed",
                        retryable=True,
                        status_code=412,
                        create_conflict_resource_exists=False,
                        create_conflict_uid_match_found=True,
                        create_conflict_uid_lookup_attempted=True,
                        create_conflict_uid_lookup_candidates=7,
                        create_conflict_uid_lookup_method=(
                            "calendar_query_uid_calendar_data+calendar_collection_scan_calendar_data"
                        ),
                        create_conflict_remote_uid_confirmed=True,
                        create_conflict_state_drift_suspected=True,
                        create_conflict_existing_resource_url="https://caldav.example.com/calendars/tomo/poc/existing.ics",
                        create_conflict_selected_candidate_reason=(
                            "confirmed_uid_match_from_calendar_collection_scan_calendar_data"
                        ),
                        create_conflict_selected_candidate_index=1,
                        create_conflict_uid_lookup_raw_candidates=[
                            {
                                "href": "https://caldav.example.com/calendars/tomo/poc/a.ics",
                            },
                            {
                                "href": "https://caldav.example.com/calendars/tomo/poc/existing.ics",
                            },
                        ],
                        create_conflict_uid_lookup_diagnostics_path=(
                            str(tmp_path / "diagnostics" / "uid_lookup.json")
                        ),
                        request_method="PUT",
                        request_url="https://caldav.example.com/calendars/tomo/poc/uid-1.ics",
                        request_headers={
                            "If-None-Match": "*",
                            "If-Match": None,
                            "Content-Type": "text/calendar; charset=utf-8",
                            "Content-Length": 123,
                        },
                        response_headers={
                            "ETag": None,
                            "Content-Type": "text/plain",
                            "Content-Length": "0",
                            "Location": "https://caldav.example.com/calendars/tomo/poc/uid-1.ics",
                        },
                        response_body_excerpt="precondition failed",
                        error="PUT failed with 412",
                    )
                ],
            )

    monkeypatch.setattr(main_module, "GaroonClient", FakeGaroonClient)
    monkeypatch.setattr(main_module, "CalDAVClient", FakeCalDAVClient)
    monkeypatch.setattr(main_module, "load_sync_state", lambda path, create_if_missing=True: SyncState.empty())
    monkeypatch.setattr(main_module, "save_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "write_calendar", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_sync_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "save_caldav_sync_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        main_module,
        "build_next_sync_state_from_delivery",
        lambda *args, **kwargs: SyncState.empty(),
    )
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_STATE_PATH", tmp_path / "sync_state.json")
    monkeypatch.setattr(main_module, "DEFAULT_SYNC_PLAN_PATH", tmp_path / "sync_plan.json")
    monkeypatch.setattr(main_module, "DEFAULT_CALDAV_SYNC_RESULT_PATH", tmp_path / "caldav_sync_result.json")
    monkeypatch.setattr(main_module, "DEFAULT_ICS_PATH", tmp_path / "calendar.ics")

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        exit_code = main_module.main()

    assert exit_code == 1
    _assert_structured_log(
        caplog,
        message_prefix="caldav delivery failure",
        component="caldav",
        phase="deliver",
        error_kind="precondition_failed",
        event_id="evt-1",
        ics_uid="uid-1",
        action="create",
        conflict_kind="precondition_failed",
        status_code="412",
        resource_url="https://caldav.example.com/calendars/tomo/poc/uid-1.ics",
        create_conflict_resource_exists="false",
        create_conflict_uid_match_found="true",
        create_conflict_uid_lookup_attempted="true",
        create_conflict_uid_lookup_candidates="7",
        create_conflict_uid_lookup_method=(
            "calendar_query_uid_calendar_data+calendar_collection_scan_calendar_data"
        ),
        create_conflict_remote_uid_confirmed="true",
        create_conflict_state_drift_suspected="true",
        create_conflict_existing_resource_url="https://caldav.example.com/calendars/tomo/poc/existing.ics",
        create_conflict_selected_candidate_reason=(
            "confirmed_uid_match_from_calendar_collection_scan_calendar_data"
        ),
        create_conflict_selected_candidate_index="1",
        create_conflict_uid_lookup_candidate_hrefs=json.dumps(
            [
                "https://caldav.example.com/calendars/tomo/poc/a.ics",
                "https://caldav.example.com/calendars/tomo/poc/existing.ics",
            ]
        ),
        create_conflict_uid_lookup_diagnostics_path=str(
            tmp_path / "diagnostics" / "uid_lookup.json"
        ),
        request_method="PUT",
        request_url="https://caldav.example.com/calendars/tomo/poc/uid-1.ics",
        request_if_none_match=json.dumps("*"),
        request_content_type=json.dumps("text/calendar; charset=utf-8"),
        request_content_length="123",
        response_content_type="text/plain",
        response_content_length="0",
        response_location="https://caldav.example.com/calendars/tomo/poc/uid-1.ics",
        response_body_excerpt="precondition failed",
    )


def _build_app_config(
    tmp_path: Path,
    *,
    dry_run: bool = False,
    create_warn_count: int = 10,
    delete_warn_count: int = 10,
    start_days_offset: int = 0,
    end_days_offset: int = 1,
) -> AppConfig:
    return AppConfig(
        garoon_base_url="https://garoon.example.com/g",
        garoon_username="user",
        garoon_password="pass",
        garoon_start_days_offset=start_days_offset,
        garoon_end_days_offset=end_days_offset,
        output_json_path=tmp_path / "events.json",
        log_level="INFO",
        caldav_url="https://caldav.example.com/principals/tomo",
        caldav_username="caldav-user",
        caldav_password="caldav-pass",
        caldav_calendar_name="PoC Calendar",
        caldav_dry_run=dry_run,
        dry_run_warn_create_count=create_warn_count,
        dry_run_warn_delete_count=delete_warn_count,
    )


def _build_successful_caldav_report(settings) -> CalDAVSyncReport:
    return CalDAVSyncReport(
        generated_at="2026-03-12T00:00:00+00:00",
        dry_run=False,
        calendar_name=settings.calendar_name,
        source_url=settings.url,
        processed_count=1,
        ignored_count=0,
        results=[
            CalDAVActionResult(
                action="create",
                event_id="evt-1",
                ics_uid="uid-1",
                sequence=0,
                dry_run=False,
                success=True,
                sent=True,
                action_reason="new_event",
                resource_name="uid-1.ics",
                resource_url="https://caldav.example.com/calendars/tomo/uid-1.ics",
                etag="\"etag-1\"",
                updated_at="2026-03-12T00:00:00Z",
                delivered_at="2026-03-12T00:00:00+00:00",
                payload_summary={"subject": "Subject evt-1"},
                payload_bytes=123,
            )
        ],
    )


def _build_next_sync_state(event_id: str) -> SyncState:
    return SyncState(
        events={
            event_id: EventSyncState(
                event_id=event_id,
                ics_uid="uid-1",
                updated_at="2026-03-12T00:00:00Z",
                content_hash="hash-1",
                sequence=0,
                is_deleted=False,
                last_synced_at="2026-03-12T00:00:00+00:00",
            )
        }
    )


def _build_event(event_id: str, *, subject: str | None = None) -> EventRecord:
    return EventRecord(
        event_id=event_id,
        subject=subject or f"Subject {event_id}",
        start=EventDateTime(date_time="2026-03-12T10:00:00+09:00", time_zone="Asia/Tokyo"),
        end=EventDateTime(date_time="2026-03-12T11:00:00+09:00", time_zone="Asia/Tokyo"),
        is_all_day=False,
        is_start_only=False,
        event_type="normal",
        event_menu="MTG",
        visibility_type="public",
        notes="Agenda",
        created_at="2026-03-11T00:00:00Z",
        updated_at="2026-03-12T00:00:00Z",
        original_start_time_zone="Asia/Tokyo",
        original_end_time_zone="Asia/Tokyo",
        repeat_id=None,
        repeat_info=None,
        attendees=[],
        facilities=[],
    )


def _assert_sync_state_validation_output(
    output: str,
    *,
    stage: str,
    detail: str,
    location: str,
) -> None:
    _assert_sync_state_failure_output(
        output,
        stage=stage,
        reason="validation failed",
        detail=detail,
        location=location,
    )


def _assert_sync_state_failure_output(
    output: str,
    *,
    stage: str,
    reason: str,
    detail: str,
    location: str,
) -> None:
    assert output.startswith(f"[sync_state:{stage}] {reason} [{location}]:")
    assert f"- {detail}" in output


def _assert_dry_run_warning_output(
    output: str,
    *,
    create_count: int,
    delete_count: int,
    total_count: int,
    create_threshold: int,
    delete_threshold: int,
) -> None:
    assert "WARNING: dry-run detected unusually large pending changes." in output
    assert f"- create: {create_count} (threshold: {create_threshold})" in output
    assert f"- delete: {delete_count} (threshold: {delete_threshold})" in output
    assert f"- total actions: {total_count}" in output
    assert "python -m src.sync_plan_inspect --action create" in output
    assert "python -m src.sync_plan_inspect --action delete" in output
    assert "verify representative events on a test calendar before proceeding to production" in output


def _assert_sync_state_structured_log(
    caplog,
    *,
    phase: str,
    error_kind: str,
    path: str,
    event_id: str | None = None,
    ics_uid: str | None = None,
) -> None:
    _assert_structured_log(
        caplog,
        message_prefix="sync_state failure",
        component="sync_state",
        phase=phase,
        error_kind=error_kind,
        path=path,
        event_id=event_id,
        ics_uid=ics_uid,
    )


def _assert_structured_log(
    caplog,
    *,
    message_prefix: str,
    component: str,
    phase: str,
    error_kind: str,
    path: str | None = None,
    event_id: str | None = None,
    ics_uid: str | None = None,
    action: str | None = None,
    conflict_kind: str | None = None,
    status_code: str | None = None,
    resource_url: str | None = None,
    create_conflict_resource_exists: str | None = None,
    create_conflict_uid_match_found: str | None = None,
    create_conflict_uid_lookup_attempted: str | None = None,
    create_conflict_uid_lookup_candidates: str | None = None,
    create_conflict_uid_lookup_method: str | None = None,
    create_conflict_remote_uid_confirmed: str | None = None,
    create_conflict_state_drift_suspected: str | None = None,
    create_conflict_existing_resource_url: str | None = None,
    create_conflict_selected_candidate_reason: str | None = None,
    create_conflict_selected_candidate_index: str | None = None,
    create_conflict_uid_lookup_candidate_hrefs: str | None = None,
    create_conflict_uid_lookup_diagnostics_path: str | None = None,
    request_method: str | None = None,
    request_url: str | None = None,
    request_if_none_match: str | None = None,
    request_if_match: str | None = None,
    request_content_type: str | None = None,
    request_content_length: str | None = None,
    response_etag: str | None = None,
    response_content_type: str | None = None,
    response_content_length: str | None = None,
    response_location: str | None = None,
    response_body_excerpt: str | None = None,
    create_count: str | None = None,
    delete_count: str | None = None,
    total_count: str | None = None,
) -> None:
    messages = _find_log_messages(caplog, message_prefix)
    assert messages
    message = messages[-1]
    assert message_prefix in message
    assert f"component={component}" in message
    assert f"phase={phase}" in message
    assert f"error_kind={error_kind}" in message
    if path is not None:
        assert f"path={path}" in message
    if event_id is not None:
        assert f"event_id={event_id}" in message
    if ics_uid is not None:
        assert f"ics_uid={ics_uid}" in message
    if action is not None:
        assert f"action={action}" in message
    if conflict_kind is not None:
        assert f"conflict_kind={conflict_kind}" in message
    if status_code is not None:
        assert f"status_code={status_code}" in message
    if resource_url is not None:
        assert f"resource_url={resource_url}" in message
    if create_conflict_resource_exists is not None:
        assert f"create_conflict_resource_exists={create_conflict_resource_exists}" in message
    if create_conflict_uid_match_found is not None:
        assert f"create_conflict_uid_match_found={create_conflict_uid_match_found}" in message
    if create_conflict_uid_lookup_attempted is not None:
        assert (
            f"create_conflict_uid_lookup_attempted={create_conflict_uid_lookup_attempted}"
            in message
        )
    if create_conflict_uid_lookup_candidates is not None:
        assert (
            f"create_conflict_uid_lookup_candidates={create_conflict_uid_lookup_candidates}"
            in message
        )
    if create_conflict_uid_lookup_method is not None:
        assert f"create_conflict_uid_lookup_method={create_conflict_uid_lookup_method}" in message
    if create_conflict_remote_uid_confirmed is not None:
        assert (
            f"create_conflict_remote_uid_confirmed={create_conflict_remote_uid_confirmed}"
            in message
        )
    if create_conflict_state_drift_suspected is not None:
        assert f"create_conflict_state_drift_suspected={create_conflict_state_drift_suspected}" in message
    if create_conflict_existing_resource_url is not None:
        assert (
            f"create_conflict_existing_resource_url={create_conflict_existing_resource_url}"
            in message
        )
    if create_conflict_selected_candidate_reason is not None:
        assert (
            "create_conflict_selected_candidate_reason="
            f"{create_conflict_selected_candidate_reason}"
            in message
        )
    if create_conflict_selected_candidate_index is not None:
        assert (
            "create_conflict_selected_candidate_index="
            f"{create_conflict_selected_candidate_index}"
            in message
        )
    if create_conflict_uid_lookup_candidate_hrefs is not None:
        assert (
            "create_conflict_uid_lookup_candidate_hrefs="
            f"{create_conflict_uid_lookup_candidate_hrefs}"
            in message
        )
    if create_conflict_uid_lookup_diagnostics_path is not None:
        assert (
            "create_conflict_uid_lookup_diagnostics_path="
            f"{create_conflict_uid_lookup_diagnostics_path}"
            in message
        )
    if request_method is not None:
        assert f"request_method={request_method}" in message
    if request_url is not None:
        assert f"request_url={request_url}" in message
    if request_if_none_match is not None:
        assert f"request_if_none_match={request_if_none_match}" in message
    if request_if_match is not None:
        assert f"request_if_match={request_if_match}" in message
    if request_content_type is not None:
        assert f"request_content_type={request_content_type}" in message
    if request_content_length is not None:
        assert f"request_content_length={request_content_length}" in message
    if response_etag is not None:
        assert f"response_etag={response_etag}" in message
    if response_content_type is not None:
        assert f"response_content_type={response_content_type}" in message
    if response_content_length is not None:
        assert f"response_content_length={response_content_length}" in message
    if response_location is not None:
        assert f"response_location={response_location}" in message
    if response_body_excerpt is not None:
        assert f"response_body_excerpt={json.dumps(response_body_excerpt)}" in message
    if create_count is not None:
        assert f"create_count={create_count}" in message
    if delete_count is not None:
        assert f"delete_count={delete_count}" in message
    if total_count is not None:
        assert f"total_count={total_count}" in message


def _find_log_messages(caplog, message_prefix: str) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == main_module.__name__ and message_prefix in record.getMessage()
    ]
