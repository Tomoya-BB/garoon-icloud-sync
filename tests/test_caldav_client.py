from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
from pathlib import Path

import pytest

import src.caldav_client as caldav_client_module
from src.caldav_client import (
    CalDAVCalendarObject,
    CalDAVClient,
    CalDAVClientError,
    CalDAVDiscoveryError,
    CalDAVDeleteResult,
    CalDAVHTTPError,
    CalDAVConnectionSettings,
    CalDAVPutResult,
    CalDAVRequestResponseDiagnostics,
    CalDAVResourceState,
    CalDAVUIDLookupCandidate,
    CalDAVUIDLookupDiagnostics,
    RequestsCalDAVTransport,
    save_caldav_sync_report,
    build_caldav_actions,
)
from src.models import EventDateTime, EventRecord
from src.sync_plan import SyncActionType, SyncPlan, SyncPlanAction
from src.sync_state import EventSyncState


@pytest.fixture(autouse=True)
def isolate_state_drift_reports_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(caldav_client_module, "DEFAULT_CALDAV_REPORTS_DIR", tmp_path / "reports")


def test_dry_run_does_not_send_create_or_update() -> None:
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
        _build_action(SyncActionType.UPDATE, event_id="evt-update", ics_uid="uid-update", sequence=4),
        _build_action(SyncActionType.DELETE, event_id="evt-delete", ics_uid="uid-delete", sequence=4),
    ])
    transport = FakeTransport()
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=True,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-create"), _build_event("evt-update")],
        generated_at=_generated_at(),
        previous_sync_state={
            "evt-delete": _build_previous_state(
                "evt-delete",
                resource_url="https://caldav.example.com/calendars/tomo/poc/delete.ics",
                etag="\"etag-delete\"",
            )
        },
    )

    assert report.processed_count == 3
    assert report.success_count == 3
    assert report.failure_count == 0
    assert transport.resolve_calls == 0
    assert transport.find_calls == []
    assert transport.put_calls == []
    assert transport.delete_calls == []
    assert all(result.sent is False for result in report.results)
    assert [result.sequence for result in report.results] == [0, 4, 4]
    assert [result.payload_sequence for result in report.results] == [0, 4, None]
    assert all(result.retry_attempted is False for result in report.results)
    assert all(result.retry_succeeded is False for result in report.results)
    assert all(result.retry_count == 0 for result in report.results)


def test_sync_result_preserves_reappeared_tombstone_diagnostics() -> None:
    plan = _build_sync_plan([
        _build_action(
            SyncActionType.CREATE,
            event_id="evt-reappeared",
            ics_uid="uid-reappeared",
            sequence=0,
            reappeared_from_tombstone=True,
            tombstone_deleted_at="2026-03-11T00:00:00+00:00",
        ),
    ])
    transport = FakeTransport(
        calendar_url="https://caldav.example.com/calendars/tomo/poc/",
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-reappeared")],
        generated_at=_generated_at(),
    )

    assert report.success_count == 1
    assert report.results[0].action == "create"
    assert report.results[0].reappeared_from_tombstone is True
    assert report.results[0].tombstone_deleted_at == "2026-03-11T00:00:00+00:00"
    assert report.results[0].resource_url.endswith("uid-reappeared.ics")


def test_sync_logs_structured_error_when_unexpected_exception_interrupts_action(caplog) -> None:
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
        _build_action(SyncActionType.UPDATE, event_id="evt-update", ics_uid="uid-update", sequence=7),
    ])
    transport = FakeTransport(
        fail_with_runtime_error_on_put={"https://caldav.example.com/calendars/tomo/poc/uid-create.ics"},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    with caplog.at_level(logging.ERROR, logger=caldav_client_module.__name__):
        with pytest.raises(RuntimeError, match="simulated runtime failure"):
            client.sync(
                plan,
                [_build_event("evt-create"), _build_event("evt-update")],
                generated_at=_generated_at(),
            )

    message = _last_caldav_client_log(caplog)
    assert "caldav sync failure" in message
    assert "component=caldav" in message
    assert "phase=sync" in message
    assert "error_kind=other" in message
    assert "action=create" in message
    assert "event_id=evt-create" in message
    assert "ics_uid=uid-create" in message
    assert "resource_url=https://caldav.example.com/calendars/tomo/poc/uid-create.ics" in message
    assert "processed_count=0" in message
    assert "remaining_count=1" in message
    assert "total_count=2" in message
    assert "action_index=1" in message


def test_sync_logs_structured_error_with_action_context_when_building_actions_fails(caplog) -> None:
    plan = _build_sync_plan([
        _build_action(SyncActionType.UPDATE, event_id="evt-missing", ics_uid="uid-missing", sequence=7),
    ])
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=FakeTransport(),
    )

    with caplog.at_level(logging.ERROR, logger=caldav_client_module.__name__):
        with pytest.raises(ValueError, match=r"Event 'evt-missing' referenced by sync_plan was not found."):
            client.sync(
                plan,
                [],
                generated_at=_generated_at(),
            )

    message = _last_caldav_client_log(caplog)
    assert "caldav sync failure" in message
    assert "component=caldav" in message
    assert "phase=sync" in message
    assert "error_kind=other" in message
    assert "action=update" in message
    assert "event_id=evt-missing" in message
    assert "ics_uid=uid-missing" in message
    assert "processed_count=0" in message
    assert "remaining_count=0" in message
    assert "total_count=1" in message
    assert "action_index=1" in message


def test_failed_create_does_not_dump_diagnostic_ics_when_flag_is_off(tmp_path: Path) -> None:
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
    ])
    transport = FakeTransport(
        calendar_url="https://caldav.example.com/calendars/tomo/poc/",
        put_errors_by_resource_url={
            "https://caldav.example.com/calendars/tomo/poc/uid-create.ics": [412],
        },
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
            diagnostic_dir=tmp_path / "diagnostics",
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-create")],
        generated_at=_generated_at(),
    )

    assert report.failure_count == 1
    result = report.results[0]
    assert result.diagnostic_payload_path is None
    assert result.diagnostic_request_response_path is None
    assert result.request_method == "PUT"
    assert result.request_url == "https://caldav.example.com/calendars/tomo/poc/uid-create.ics"
    assert result.request_headers == {
        "If-None-Match": "*",
        "If-Match": None,
        "Content-Type": "text/calendar; charset=utf-8",
        "Content-Length": result.payload_bytes,
    }
    assert result.response_headers == {
        "ETag": "\"etag-create\"",
        "Content-Type": "text/plain",
        "Content-Length": "0",
        "Location": "https://caldav.example.com/calendars/tomo/poc/uid-create.ics",
    }
    assert result.response_body_excerpt == "simulated failure"
    assert list((tmp_path / "diagnostics").glob("*")) == []


def test_failed_create_dumps_diagnostic_ics_and_serializes_result_path(tmp_path: Path) -> None:
    diagnostics_dir = tmp_path / "diagnostics"
    result_path = tmp_path / "caldav_sync_result.json"
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt:create/1", ics_uid="uid-create", sequence=0),
    ])
    transport = FakeTransport(
        calendar_url="https://caldav.example.com/calendars/tomo/poc/",
        put_errors_by_resource_url={
            "https://caldav.example.com/calendars/tomo/poc/uid-create.ics": [412],
        },
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
            diagnostic_dump_failed_ics=True,
            diagnostic_dir=diagnostics_dir,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt:create/1")],
        generated_at=_generated_at(),
    )

    assert report.failure_count == 1
    diagnostic_payload_path = report.results[0].diagnostic_payload_path
    assert diagnostic_payload_path is not None
    diagnostic_file = Path(diagnostic_payload_path)
    assert diagnostic_file.is_absolute()
    assert diagnostic_file.exists()
    assert "create_failed" in diagnostic_file.name
    assert "event_evt-create-1" in diagnostic_file.name
    assert "resource_uid-create.ics" in diagnostic_file.name
    saved_payload = diagnostic_file.read_text(encoding="utf-8")
    assert saved_payload.startswith("BEGIN:VCALENDAR")
    assert "UID:uid-create" in saved_payload
    diagnostic_request_response_path = report.results[0].diagnostic_request_response_path
    assert diagnostic_request_response_path is not None
    diagnostic_http_file = Path(diagnostic_request_response_path)
    assert diagnostic_http_file.is_absolute()
    assert diagnostic_http_file.exists()
    assert diagnostic_http_file.name.endswith("__http.json")
    saved_http = json.loads(diagnostic_http_file.read_text(encoding="utf-8"))
    assert saved_http["request_method"] == "PUT"
    assert saved_http["request_url"] == "https://caldav.example.com/calendars/tomo/poc/uid-create.ics"
    assert saved_http["request_headers"]["If-None-Match"] == "*"
    assert saved_http["request_headers"]["If-Match"] is None
    assert saved_http["request_headers"]["Content-Type"] == "text/calendar; charset=utf-8"
    assert saved_http["request_headers"]["Content-Length"] == report.results[0].payload_bytes
    assert saved_http["response_body_excerpt"] == "simulated failure"

    save_caldav_sync_report(result_path, report)
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    assert payload["results"][0]["diagnostic_payload_path"] == str(diagnostic_file)
    assert payload["results"][0]["diagnostic_request_response_path"] == str(diagnostic_http_file)


def test_successful_create_can_dump_diagnostic_ics_during_dry_run(tmp_path: Path) -> None:
    diagnostics_dir = tmp_path / "diagnostics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
    ])
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=True,
            diagnostic_dump_success_ics=True,
            diagnostic_dir=diagnostics_dir,
        ),
        transport=FakeTransport(),
    )

    report = client.sync(
        plan,
        [_build_event("evt-create")],
        generated_at=_generated_at(),
    )

    assert report.success_count == 1
    diagnostic_payload_path = report.results[0].diagnostic_payload_path
    assert diagnostic_payload_path is not None
    diagnostic_file = Path(diagnostic_payload_path)
    assert diagnostic_file.exists()
    assert diagnostic_file.read_text(encoding="utf-8").startswith("BEGIN:VCALENDAR")


def test_create_412_diagnoses_existing_remote_resource() -> None:
    attempted_resource_url = "https://caldav.example.com/calendars/tomo/poc/uid-create.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
    ])
    transport = FakeTransport(
        calendar_url="https://caldav.example.com/calendars/tomo/poc/",
        put_errors_by_resource_url={attempted_resource_url: [412]},
        etags_by_resource_url={attempted_resource_url: "\"etag-existing\""},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-create")],
        generated_at=_generated_at(),
    )

    assert report.failure_count == 1
    result = report.results[0]
    assert result.success is False
    assert result.status_code == 412
    assert result.conflict_kind == "precondition_failed"
    assert result.resource_url == attempted_resource_url
    assert result.create_conflict_resource_exists is True
    assert result.create_conflict_uid_match_found is False
    assert result.create_conflict_uid_lookup_attempted is True
    assert result.create_conflict_uid_lookup_candidates == 0
    assert (
        result.create_conflict_uid_lookup_method
        == "calendar_query_uid_calendar_data+calendar_collection_scan_calendar_data"
    )
    assert result.create_conflict_remote_uid_confirmed is False
    assert result.create_conflict_state_drift_suspected is True
    assert result.create_conflict_existing_resource_url == attempted_resource_url
    assert result.create_conflict_selected_candidate_reason is None
    assert result.create_conflict_selected_candidate_index is None
    assert result.create_conflict_uid_lookup_raw_candidates == []
    assert result.create_conflict_uid_lookup_diagnostics_path is None
    assert transport.get_calls == [attempted_resource_url]
    assert transport.find_calls == ["uid-create"]


def test_create_412_diagnoses_existing_remote_uid_match() -> None:
    attempted_resource_url = "https://caldav.example.com/calendars/tomo/poc/uid-create.ics"
    matched_resource_url = "https://caldav.example.com/calendars/tomo/poc/existing-elsewhere.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
    ])
    transport = FakeTransport(
        calendar_url="https://caldav.example.com/calendars/tomo/poc/",
        existing_urls={"uid-create": matched_resource_url},
        etags_by_resource_url={matched_resource_url: "\"etag-existing\""},
        put_errors_by_resource_url={attempted_resource_url: [412]},
        get_errors_by_resource_url={attempted_resource_url: [404]},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-create")],
        generated_at=_generated_at(),
    )

    assert report.failure_count == 1
    result = report.results[0]
    assert result.success is False
    assert result.status_code == 412
    assert result.create_conflict_resource_exists is False
    assert result.create_conflict_uid_match_found is True
    assert result.create_conflict_uid_lookup_attempted is True
    assert result.create_conflict_uid_lookup_candidates == 1
    assert result.create_conflict_uid_lookup_method == "calendar_query_uid_calendar_data"
    assert result.create_conflict_remote_uid_confirmed is True
    assert result.create_conflict_state_drift_suspected is True
    assert result.create_conflict_existing_resource_url == matched_resource_url
    assert result.create_conflict_selected_candidate_reason == (
        "confirmed_uid_match_from_calendar_query_uid_calendar_data"
    )
    assert result.create_conflict_selected_candidate_index == 0
    assert result.create_conflict_uid_lookup_raw_candidates == [
        {
            "href": matched_resource_url,
            "etag": "\"etag-existing\"",
            "parsed_remote_uid": None,
            "summary": None,
            "dtstart": None,
            "dtend": None,
            "found_via": [],
        }
    ]
    assert transport.get_calls == [attempted_resource_url]
    assert transport.find_calls == ["uid-create"]


def test_create_412_does_not_suspect_state_drift_when_remote_event_is_absent() -> None:
    attempted_resource_url = "https://caldav.example.com/calendars/tomo/poc/uid-create.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
    ])
    transport = FakeTransport(
        calendar_url="https://caldav.example.com/calendars/tomo/poc/",
        put_errors_by_resource_url={attempted_resource_url: [412]},
        get_errors_by_resource_url={attempted_resource_url: [404]},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-create")],
        generated_at=_generated_at(),
    )

    assert report.failure_count == 1
    result = report.results[0]
    assert result.success is False
    assert result.status_code == 412
    assert result.create_conflict_resource_exists is False
    assert result.create_conflict_uid_match_found is False
    assert result.create_conflict_uid_lookup_attempted is True
    assert result.create_conflict_uid_lookup_candidates == 0
    assert (
        result.create_conflict_uid_lookup_method
        == "calendar_query_uid_calendar_data+calendar_collection_scan_calendar_data"
    )
    assert result.create_conflict_remote_uid_confirmed is False
    assert result.create_conflict_state_drift_suspected is False
    assert result.create_conflict_existing_resource_url is None
    assert result.create_conflict_selected_candidate_reason is None
    assert result.create_conflict_selected_candidate_index is None
    assert result.create_conflict_uid_lookup_raw_candidates == []
    assert transport.get_calls == [attempted_resource_url]
    assert transport.find_calls == ["uid-create"]


def test_create_412_records_uid_lookup_candidates_found_via_collection_scan() -> None:
    attempted_resource_url = "https://caldav.example.com/calendars/tomo/poc/uid-create.ics"
    matched_resource_url = "https://caldav.example.com/calendars/tomo/poc/existing-collection.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
    ])
    transport = FakeTransport(
        calendar_url="https://caldav.example.com/calendars/tomo/poc/",
        put_errors_by_resource_url={attempted_resource_url: [412]},
        get_errors_by_resource_url={attempted_resource_url: [404]},
        uid_lookup_diagnostics_by_uid={
            "uid-create": CalDAVUIDLookupDiagnostics(
                attempted=True,
                candidate_count=5,
                method="calendar_query_uid_calendar_data+calendar_collection_scan_calendar_data",
                matched_resource_url=matched_resource_url,
                matched_resource_etag="\"etag-existing\"",
                remote_uid_confirmed=True,
            )
        },
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-create")],
        generated_at=_generated_at(),
    )

    result = report.results[0]
    assert result.create_conflict_uid_match_found is True
    assert result.create_conflict_uid_lookup_attempted is True
    assert result.create_conflict_uid_lookup_candidates == 5
    assert (
        result.create_conflict_uid_lookup_method
        == "calendar_query_uid_calendar_data+calendar_collection_scan_calendar_data"
    )
    assert result.create_conflict_remote_uid_confirmed is True
    assert result.create_conflict_existing_resource_url == matched_resource_url


def test_create_412_records_raw_uid_lookup_candidates_and_selected_reason(
    tmp_path: Path,
) -> None:
    attempted_resource_url = "https://caldav.example.com/calendars/tomo/poc/uid-create.ics"
    matched_resource_url = "https://caldav.example.com/calendars/tomo/poc/matched.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
    ])
    transport = FakeTransport(
        calendar_url="https://caldav.example.com/calendars/tomo/poc/",
        put_errors_by_resource_url={attempted_resource_url: [412]},
        get_errors_by_resource_url={attempted_resource_url: [404]},
        uid_lookup_diagnostics_by_uid={
            "uid-create": CalDAVUIDLookupDiagnostics(
                attempted=True,
                candidate_count=3,
                method="calendar_query_uid_calendar_data+calendar_collection_scan_calendar_data",
                matched_resource_url=matched_resource_url,
                matched_resource_etag="\"etag-match\"",
                remote_uid_confirmed=True,
                selected_candidate_reason="confirmed_uid_match_from_calendar_collection_scan_calendar_data",
                selected_candidate_index=1,
                uid_query_raw_response=(
                    "<d:multistatus xmlns:d=\"DAV:\" xmlns:c=\"urn:ietf:params:xml:ns:caldav\">"
                    "<d:response><d:href>/home/poc/other.ics</d:href></d:response>"
                    "</d:multistatus>"
                ),
                collection_scan_raw_response=(
                    "<d:multistatus xmlns:d=\"DAV:\" xmlns:c=\"urn:ietf:params:xml:ns:caldav\">"
                    "<d:response><d:href>/home/poc/matched.ics</d:href></d:response>"
                    "</d:multistatus>"
                ),
                candidates=[
                    CalDAVUIDLookupCandidate(
                        resource_url="https://caldav.example.com/calendars/tomo/poc/other.ics",
                        etag="\"etag-other\"",
                        remote_uid="uid-other",
                        summary="Other Event",
                        dtstart="20260312T030000Z",
                        dtend="20260312T040000Z",
                        found_via=("calendar_query_uid_calendar_data",),
                    ),
                    CalDAVUIDLookupCandidate(
                        resource_url=matched_resource_url,
                        etag="\"etag-match\"",
                        remote_uid="uid-create",
                        summary="Matched Event",
                        dtstart="20260312T010000Z",
                        dtend="20260312T020000Z",
                        found_via=("calendar_collection_scan_calendar_data",),
                    ),
                    CalDAVUIDLookupCandidate(
                        resource_url="https://caldav.example.com/calendars/tomo/poc/third.ics",
                        etag="\"etag-third\"",
                        remote_uid=None,
                        summary=None,
                        dtstart=None,
                        dtend=None,
                        found_via=("calendar_collection_scan_calendar_data",),
                    ),
                ],
            )
        },
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
            diagnostic_dump_uid_lookup_json=True,
            diagnostic_dir=tmp_path / "diagnostics",
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-create")],
        generated_at=_generated_at(),
    )

    result = report.results[0]
    assert result.create_conflict_uid_lookup_candidates == 3
    assert result.create_conflict_selected_candidate_reason == (
        "confirmed_uid_match_from_calendar_collection_scan_calendar_data"
    )
    assert result.create_conflict_selected_candidate_index == 1
    assert result.create_conflict_uid_lookup_raw_candidates == [
        {
            "href": "https://caldav.example.com/calendars/tomo/poc/other.ics",
            "etag": "\"etag-other\"",
            "parsed_remote_uid": "uid-other",
            "summary": "Other Event",
            "dtstart": "20260312T030000Z",
            "dtend": "20260312T040000Z",
            "found_via": ["calendar_query_uid_calendar_data"],
        },
        {
            "href": matched_resource_url,
            "etag": "\"etag-match\"",
            "parsed_remote_uid": "uid-create",
            "summary": "Matched Event",
            "dtstart": "20260312T010000Z",
            "dtend": "20260312T020000Z",
            "found_via": ["calendar_collection_scan_calendar_data"],
        },
        {
            "href": "https://caldav.example.com/calendars/tomo/poc/third.ics",
            "etag": "\"etag-third\"",
            "parsed_remote_uid": None,
            "summary": None,
            "dtstart": None,
            "dtend": None,
            "found_via": ["calendar_collection_scan_calendar_data"],
        },
    ]
    assert result.create_conflict_uid_lookup_diagnostics_path is not None
    diagnostics_path = Path(result.create_conflict_uid_lookup_diagnostics_path)
    payload = json.loads(diagnostics_path.read_text(encoding="utf-8"))
    assert payload["selected_candidate_reason"] == (
        "confirmed_uid_match_from_calendar_collection_scan_calendar_data"
    )
    assert payload["selected_candidate_index"] == 1
    assert [item["href"] for item in payload["candidates"]] == [
        "https://caldav.example.com/calendars/tomo/poc/other.ics",
        matched_resource_url,
        "https://caldav.example.com/calendars/tomo/poc/third.ics",
    ]
    assert result.create_conflict_uid_query_raw_path is not None
    assert result.create_conflict_collection_scan_raw_path is not None
    uid_query_raw_path = Path(result.create_conflict_uid_query_raw_path)
    collection_scan_raw_path = Path(result.create_conflict_collection_scan_raw_path)
    assert uid_query_raw_path.read_text(encoding="utf-8").startswith("<d:multistatus")
    assert "other.ics" in uid_query_raw_path.read_text(encoding="utf-8")
    assert "matched.ics" in collection_scan_raw_path.read_text(encoding="utf-8")
    assert result.create_conflict_candidate_ranking == [
        {
            "rank": 1,
            "candidate_index": 1,
            "href": matched_resource_url,
            "etag": "\"etag-match\"",
            "parsed_remote_uid": "uid-create",
            "summary": "Matched Event",
            "dtstart": "20260312T010000Z",
            "dtend": "20260312T020000Z",
            "found_via": ["calendar_collection_scan_calendar_data"],
            "summary_exact_match": False,
            "dtstart_match": True,
            "dtend_match": True,
            "summary_partial_match": False,
            "score": 60,
        },
        {
            "rank": 2,
            "candidate_index": 0,
            "href": "https://caldav.example.com/calendars/tomo/poc/other.ics",
            "etag": "\"etag-other\"",
            "parsed_remote_uid": "uid-other",
            "summary": "Other Event",
            "dtstart": "20260312T030000Z",
            "dtend": "20260312T040000Z",
            "found_via": ["calendar_query_uid_calendar_data"],
            "summary_exact_match": False,
            "dtstart_match": False,
            "dtend_match": False,
            "summary_partial_match": False,
            "score": 0,
        },
        {
            "rank": 3,
            "candidate_index": 2,
            "href": "https://caldav.example.com/calendars/tomo/poc/third.ics",
            "etag": "\"etag-third\"",
            "parsed_remote_uid": None,
            "summary": None,
            "dtstart": None,
            "dtend": None,
            "found_via": ["calendar_collection_scan_calendar_data"],
            "summary_exact_match": False,
            "dtstart_match": False,
            "dtend_match": False,
            "summary_partial_match": False,
            "score": 0,
        },
    ]
    assert payload["uid_query_raw_path"] == result.create_conflict_uid_query_raw_path
    assert payload["collection_scan_raw_path"] == result.create_conflict_collection_scan_raw_path
    assert payload["candidate_ranking"] == result.create_conflict_candidate_ranking


def test_create_412_keeps_existing_selection_but_exposes_read_only_candidate_ranking() -> None:
    attempted_resource_url = "https://caldav.example.com/calendars/tomo/poc/uid-create.ics"
    wrong_resource_url = "https://caldav.example.com/calendars/tomo/poc/wrong-first.ics"
    better_resource_url = "https://caldav.example.com/calendars/tomo/poc/better-neighbor.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
    ])
    transport = FakeTransport(
        calendar_url="https://caldav.example.com/calendars/tomo/poc/",
        put_errors_by_resource_url={attempted_resource_url: [412]},
        get_errors_by_resource_url={attempted_resource_url: [404]},
        uid_lookup_diagnostics_by_uid={
            "uid-create": CalDAVUIDLookupDiagnostics(
                attempted=True,
                candidate_count=2,
                method="calendar_query_uid_calendar_data+calendar_collection_scan_calendar_data",
                matched_resource_url=wrong_resource_url,
                matched_resource_etag="\"etag-wrong\"",
                remote_uid_confirmed=False,
                selected_candidate_reason="first_candidate_from_calendar_collection_scan_calendar_data",
                selected_candidate_index=0,
                candidates=[
                    CalDAVUIDLookupCandidate(
                        resource_url=wrong_resource_url,
                        etag="\"etag-wrong\"",
                        remote_uid="uid-other",
                        summary="Completely Different",
                        dtstart="20260312T050000Z",
                        dtend="20260312T060000Z",
                        found_via=("calendar_collection_scan_calendar_data",),
                    ),
                    CalDAVUIDLookupCandidate(
                        resource_url=better_resource_url,
                        etag="\"etag-better\"",
                        remote_uid="uid-nearby",
                        summary="Subject evt-create",
                        dtstart="20260312T010000Z",
                        dtend="20260312T020000Z",
                        found_via=("calendar_collection_scan_calendar_data",),
                    ),
                ],
            )
        },
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-create")],
        generated_at=_generated_at(),
    )

    result = report.results[0]
    assert result.create_conflict_uid_match_found is True
    assert result.create_conflict_remote_uid_confirmed is False
    assert result.create_conflict_existing_resource_url == wrong_resource_url
    assert result.create_conflict_selected_candidate_reason == (
        "first_candidate_from_calendar_collection_scan_calendar_data"
    )
    assert result.create_conflict_selected_candidate_index == 0
    assert [item["href"] for item in result.create_conflict_candidate_ranking] == [
        better_resource_url,
        wrong_resource_url,
    ]
    assert result.create_conflict_candidate_ranking[0]["score"] == 160
    assert result.create_conflict_candidate_ranking[0]["summary_exact_match"] is True
    assert result.create_conflict_candidate_ranking[0]["dtstart_match"] is True
    assert result.create_conflict_candidate_ranking[0]["dtend_match"] is True
    assert result.create_conflict_candidate_ranking[1]["score"] == 0


def test_create_412_generates_state_drift_report_for_existing_resource(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted_resource_url = "https://caldav.example.com/calendars/tomo/poc/uid-create.ics"
    monkeypatch.setattr(caldav_client_module, "DEFAULT_CALDAV_REPORTS_DIR", tmp_path / "reports")
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=FakeTransport(
            calendar_url="https://caldav.example.com/calendars/tomo/poc/",
            put_errors_by_resource_url={attempted_resource_url: [412]},
            etags_by_resource_url={attempted_resource_url: "\"etag-existing\""},
            remote_calendar_data_by_resource_url={
                attempted_resource_url: (
                    "BEGIN:VCALENDAR\r\n"
                    "BEGIN:VEVENT\r\n"
                    "UID:uid-create\r\n"
                    "SUMMARY:Remote Subject\r\n"
                    "DTSTART:20260312T010000Z\r\n"
                    "DTEND:20260312T020000Z\r\n"
                    "LOCATION:Remote Room\r\n"
                    "SEQUENCE:4\r\n"
                    "LAST-MODIFIED:20260311T150000Z\r\n"
                    "END:VEVENT\r\n"
                    "END:VCALENDAR\r\n"
                )
            },
        ),
    )
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
    ])

    report = client.sync(
        plan,
        [_build_event("evt-create")],
        generated_at=_generated_at(),
    )

    result = report.results[0]
    assert result.create_conflict_state_drift_report_status == "generated"
    assert result.drift_report_status == "generated"
    assert result.drift_diff_count == 5
    assert result.drift_diff_fields == [
        "SUMMARY",
        "DESCRIPTION",
        "LOCATION",
        "SEQUENCE",
        "LAST-MODIFIED",
    ]
    assert result.create_conflict_remote_fetch_error is None
    assert result.create_conflict_state_drift_report_path is not None
    report_path = Path(result.create_conflict_state_drift_report_path)
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["kind"] == "create_conflict_state_drift_report"
    assert payload["event_id"] == "evt-create"
    assert payload["existing_resource_url"] == attempted_resource_url
    assert payload["remote_fetch"] == {
        "success": True,
        "error": None,
        "etag": "\"etag-existing\"",
    }
    assert payload["local_event"] == {
        "uid": "uid-create",
        "summary": "Subject evt-create",
        "dtstart": "20260312T010000Z",
        "dtend": "20260312T020000Z",
        "has_description": True,
        "has_location": False,
        "sequence": "0",
        "last_modified": "20260312T000000Z",
    }
    assert payload["remote_event"] == {
        "uid": "uid-create",
        "summary": "Remote Subject",
        "dtstart": "20260312T010000Z",
        "dtend": "20260312T020000Z",
        "has_description": False,
        "has_location": True,
        "sequence": "4",
        "last_modified": "20260311T150000Z",
    }
    assert payload["comparison"]["UID"]["equal"] is True
    assert payload["comparison"]["SUMMARY"]["equal"] is False
    assert payload["comparison"]["DESCRIPTION"]["local_present"] is True
    assert payload["comparison"]["DESCRIPTION"]["remote_present"] is False
    assert payload["comparison"]["DESCRIPTION"]["equal"] is False
    assert payload["comparison"]["LOCATION"]["local_present"] is False
    assert payload["comparison"]["LOCATION"]["remote_present"] is True
    assert payload["comparison"]["SEQUENCE"]["equal"] is False


def test_create_412_generates_state_drift_report_for_uid_lookup_resource_href() -> None:
    attempted_resource_url = "https://caldav.example.com/calendars/tomo/poc/uid-create.ics"
    matched_resource_url = "https://caldav.example.com/calendars/tomo/poc/existing-remote.ics"
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=FakeTransport(
            calendar_url="https://caldav.example.com/calendars/tomo/poc/",
            put_errors_by_resource_url={attempted_resource_url: [412]},
            get_errors_by_resource_url={attempted_resource_url: [404]},
            uid_lookup_diagnostics_by_uid={
                "uid-create": CalDAVUIDLookupDiagnostics(
                    attempted=True,
                    candidate_count=1,
                    method="calendar_query_uid_calendar_data",
                    matched_resource_url=matched_resource_url,
                    matched_resource_etag="\"etag-existing\"",
                    remote_uid_confirmed=False,
                )
            },
            remote_calendar_data_by_resource_url={
                matched_resource_url: (
                    "BEGIN:VCALENDAR\r\n"
                    "BEGIN:VEVENT\r\n"
                    "UID:uid-create\r\n"
                    "SUMMARY:Remote Subject\r\n"
                    "DTSTART:20260312T010000Z\r\n"
                    "DTEND:20260312T020000Z\r\n"
                    "END:VEVENT\r\n"
                    "END:VCALENDAR\r\n"
                )
            },
            etags_by_resource_url={matched_resource_url: "\"etag-existing\""},
        ),
    )
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
    ])

    report = client.sync(
        plan,
        [_build_event("evt-create")],
        generated_at=_generated_at(),
    )

    result = report.results[0]
    assert result.create_conflict_uid_match_found is True
    assert result.create_conflict_remote_uid_confirmed is False
    assert result.create_conflict_existing_resource_url == matched_resource_url
    assert result.create_conflict_state_drift_report_status == "generated"
    assert result.drift_report_status == "generated"
    assert result.create_conflict_remote_fetch_error is None
    assert result.create_conflict_state_drift_report_path is not None
    report_path = Path(result.create_conflict_state_drift_report_path)
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["attempted_resource_url"] == attempted_resource_url
    assert payload["existing_resource_url"] == matched_resource_url
    assert payload["remote_fetch"] == {
        "success": True,
        "error": None,
        "etag": "\"etag-existing\"",
    }
    assert client._transport.get_data_calls == [matched_resource_url]


def test_create_412_records_remote_fetch_failure_in_state_drift_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted_resource_url = "https://caldav.example.com/calendars/tomo/poc/uid-create.ics"
    monkeypatch.setattr(caldav_client_module, "DEFAULT_CALDAV_REPORTS_DIR", tmp_path / "reports")
    transport = FakeTransport(
        calendar_url="https://caldav.example.com/calendars/tomo/poc/",
        put_errors_by_resource_url={attempted_resource_url: [412]},
        etags_by_resource_url={attempted_resource_url: "\"etag-existing\""},
        get_data_errors_by_resource_url={attempted_resource_url: [503]},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
    ])

    report = client.sync(
        plan,
        [_build_event("evt-create")],
        generated_at=_generated_at(),
    )

    result = report.results[0]
    assert result.create_conflict_state_drift_report_status == "remote_fetch_failed"
    assert result.drift_report_status == "remote_fetch_failed"
    assert result.drift_diff_count is None
    assert result.drift_diff_fields == []
    assert (
        result.create_conflict_remote_fetch_error
        == f"GET {attempted_resource_url} failed with 503: simulated fetch failure"
    )
    assert result.create_conflict_state_drift_report_path is not None
    report_path = Path(result.create_conflict_state_drift_report_path)
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["remote_fetch"]["success"] is False
    assert payload["remote_fetch"]["etag"] is None
    assert payload["remote_fetch"]["error"] == result.create_conflict_remote_fetch_error
    assert payload["remote_event"] is None
    assert payload["comparison"]["UID"]["equal"] is None
    assert transport.get_data_calls == [attempted_resource_url]


def test_sync_builds_create_and_update_requests_without_stored_resource_url() -> None:
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
        _build_action(SyncActionType.UPDATE, event_id="evt-update", ics_uid="uid-update", sequence=7),
    ])
    transport = FakeTransport(
        calendar_url="https://caldav.example.com/calendars/tomo/poc/",
        existing_urls={"uid-update": "https://caldav.example.com/calendars/tomo/poc/existing.ics"},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-create"), _build_event("evt-update")],
        generated_at=_generated_at(),
    )

    assert report.success_count == 2
    assert transport.resolve_calls == 1
    assert transport.find_calls == ["uid-update"]
    assert len(transport.put_calls) == 2
    assert report.results[0].etag == "\"etag-create\""
    assert report.results[1].etag == "\"etag-update\""
    assert report.results[0].delivered_at == "2026-03-12T00:00:00+00:00"
    assert report.results[0].payload_summary == {
        "summary": "Subject evt-create",
        "subject": "Subject evt-create",
        "start": "2026-03-12T10:00:00+09:00",
        "end": "2026-03-12T11:00:00+09:00",
        "is_all_day": False,
        "has_description": True,
        "has_location": False,
    }

    create_call = transport.put_calls[0]
    assert create_call["resource_url"].endswith("uid-create.ics")
    assert create_call["overwrite"] is False
    assert "BEGIN:VCALENDAR" in create_call["ics_payload"]
    assert "UID:uid-create" in create_call["ics_payload"]

    update_call = transport.put_calls[1]
    assert update_call["resource_url"] == "https://caldav.example.com/calendars/tomo/poc/existing.ics"
    assert update_call["overwrite"] is True
    assert update_call["etag"] is None
    assert "UID:uid-update" in update_call["ics_payload"]
    assert "SEQUENCE:7" in update_call["ics_payload"]
    assert report.results[0].payload_sequence == 0
    assert report.results[1].payload_sequence == 7
    assert report.results[1].resolution_strategy == "uid_lookup"
    assert report.results[1].used_stored_resource_url is False
    assert report.results[1].uid_lookup_performed is True
    assert report.results[1].used_stored_etag is False
    assert report.results[1].attempted_conditional_update is False
    assert report.results[1].recovery_attempted is False
    assert report.results[1].recovery_succeeded is False


def test_update_prefers_stored_resource_url_and_skips_uid_lookup() -> None:
    stored_resource_url = "https://caldav.example.com/calendars/tomo/poc/stored.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.UPDATE, event_id="evt-update", ics_uid="uid-update", sequence=7),
    ])
    transport = FakeTransport(
        existing_urls={"uid-update": "https://caldav.example.com/calendars/tomo/poc/existing.ics"},
        etags_by_resource_url={stored_resource_url: "\"etag-update-new\""},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-update")],
        generated_at=_generated_at(),
        previous_sync_state={
            "evt-update": _build_previous_state(
                "evt-update",
                resource_url=stored_resource_url,
                etag="\"etag-stored\"",
            )
        },
    )

    assert report.success_count == 1
    assert transport.resolve_calls == 0
    assert transport.find_calls == []
    assert len(transport.put_calls) == 1
    assert transport.put_calls[0]["resource_url"] == stored_resource_url
    assert transport.put_calls[0]["overwrite"] is True
    assert transport.put_calls[0]["etag"] == "\"etag-stored\""
    assert report.results[0].resource_url == stored_resource_url
    assert report.results[0].etag == "\"etag-update-new\""
    assert report.results[0].resolution_strategy == "sync_state_resource_url"
    assert report.results[0].used_stored_resource_url is True
    assert report.results[0].uid_lookup_performed is False
    assert report.results[0].used_stored_etag is True
    assert report.results[0].attempted_conditional_update is True
    assert report.results[0].conflict_kind is None
    assert report.results[0].retryable is False
    assert report.results[0].etag_mismatch is False
    assert report.results[0].recovery_attempted is False
    assert report.results[0].recovery_succeeded is False
    assert report.results[0].initial_resource_url == stored_resource_url
    assert report.results[0].initial_etag == "\"etag-stored\""
    assert report.results[0].retry_attempted is False
    assert report.results[0].retry_succeeded is False
    assert report.results[0].retry_count == 0


def test_update_falls_back_to_uid_lookup_after_stored_resource_url_not_found() -> None:
    stale_resource_url = "https://caldav.example.com/calendars/tomo/poc/stale.ics"
    current_resource_url = "https://caldav.example.com/calendars/tomo/poc/current.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.UPDATE, event_id="evt-update", ics_uid="uid-update", sequence=7),
    ])
    transport = FakeTransport(
        existing_urls={"uid-update": current_resource_url},
        etags_by_resource_url={current_resource_url: "\"etag-current\""},
        put_errors_by_resource_url={stale_resource_url: [404]},
        get_errors_by_resource_url={stale_resource_url: [404]},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-update")],
        generated_at=_generated_at(),
        previous_sync_state={
            "evt-update": _build_previous_state(
                "evt-update",
                resource_url=stale_resource_url,
                etag="\"etag-stale\"",
            )
        },
    )

    assert report.success_count == 1
    assert transport.resolve_calls == 1
    assert transport.find_calls == ["uid-update"]
    assert len(transport.put_calls) == 2
    assert transport.put_calls[0]["resource_url"] == stale_resource_url
    assert transport.put_calls[0]["overwrite"] is True
    assert transport.put_calls[0]["etag"] == "\"etag-stale\""
    assert transport.put_calls[1]["resource_url"] == current_resource_url
    assert transport.put_calls[1]["overwrite"] is True
    assert transport.put_calls[1]["etag"] is None
    assert report.results[0].resource_url == current_resource_url
    assert report.results[0].etag == "\"etag-current\""
    assert report.results[0].resolution_strategy == "sync_state_resource_url_then_uid_lookup"
    assert report.results[0].used_stored_resource_url is True
    assert report.results[0].uid_lookup_performed is True
    assert report.results[0].used_stored_etag is True
    assert report.results[0].attempted_conditional_update is True
    assert report.results[0].recovery_attempted is True
    assert report.results[0].recovery_succeeded is True
    assert report.results[0].refreshed_resource_url == current_resource_url
    assert report.results[0].refreshed_etag == "\"etag-current\""
    assert report.results[0].initial_resource_url == stale_resource_url
    assert report.results[0].initial_etag == "\"etag-stale\""
    assert report.results[0].retry_attempted is False
    assert report.results[0].retry_succeeded is False
    assert report.results[0].retry_count == 0
    assert transport.get_calls == [stale_resource_url]


def test_update_retries_once_after_etag_mismatch_and_returns_success_on_retry_success() -> None:
    stored_resource_url = "https://caldav.example.com/calendars/tomo/poc/stored.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.UPDATE, event_id="evt-update", ics_uid="uid-update", sequence=7),
    ])
    transport = FakeTransport(
        etags_by_resource_url={stored_resource_url: "\"etag-live\""},
        put_errors_by_resource_url={stored_resource_url: [412]},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-update")],
        generated_at=_generated_at(),
        previous_sync_state={
            "evt-update": _build_previous_state(
                "evt-update",
                resource_url=stored_resource_url,
                etag="\"etag-stored\"",
            )
        },
    )

    assert report.success_count == 1
    assert report.failure_count == 0
    assert transport.resolve_calls == 0
    assert transport.find_calls == []
    assert transport.get_calls == [stored_resource_url]
    assert len(transport.put_calls) == 2
    result = report.results[0]
    assert result.success is True
    assert result.sent is True
    assert result.resource_url == stored_resource_url
    assert result.etag == "\"etag-live\""
    assert result.status_code == 204
    assert result.payload_sequence == 7
    assert result.resolution_strategy == "sync_state_resource_url"
    assert result.used_stored_resource_url is True
    assert result.uid_lookup_performed is False
    assert result.used_stored_etag is True
    assert result.attempted_conditional_update is True
    assert result.conflict_kind is None
    assert result.retryable is False
    assert result.etag_mismatch is False
    assert result.recovery_attempted is True
    assert result.recovery_succeeded is True
    assert result.refreshed_resource_url == stored_resource_url
    assert result.refreshed_etag == "\"etag-live\""
    assert result.initial_resource_url == stored_resource_url
    assert result.initial_etag == "\"etag-stored\""
    assert result.retry_attempted is True
    assert result.retry_succeeded is True
    assert result.retry_count == 1
    assert result.retry_resource_url == stored_resource_url
    assert result.retry_etag == "\"etag-live\""
    assert transport.put_calls[0]["etag"] == "\"etag-stored\""
    assert transport.put_calls[1]["etag"] == "\"etag-live\""


def test_update_returns_conflict_on_409_without_uid_lookup_retry() -> None:
    stored_resource_url = "https://caldav.example.com/calendars/tomo/poc/stored.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.UPDATE, event_id="evt-update", ics_uid="uid-update", sequence=7),
    ])
    transport = FakeTransport(
        existing_urls={"uid-update": "https://caldav.example.com/calendars/tomo/poc/current.ics"},
        etags_by_resource_url={stored_resource_url: "\"etag-live\""},
        put_errors_by_resource_url={stored_resource_url: [409]},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-update")],
        generated_at=_generated_at(),
        previous_sync_state={
            "evt-update": _build_previous_state(
                "evt-update",
                resource_url=stored_resource_url,
                etag="\"etag-stored\"",
            )
        },
    )

    assert report.success_count == 0
    assert report.failure_count == 1
    assert transport.resolve_calls == 0
    assert transport.find_calls == []
    assert transport.get_calls == [stored_resource_url]
    assert len(transport.put_calls) == 1
    result = report.results[0]
    assert result.success is False
    assert result.resource_url == stored_resource_url
    assert result.status_code == 409
    assert result.resolution_strategy == "sync_state_resource_url"
    assert result.used_stored_resource_url is True
    assert result.uid_lookup_performed is False
    assert result.used_stored_etag is True
    assert result.attempted_conditional_update is True
    assert result.conflict_kind == "conflict"
    assert result.retryable is True
    assert result.etag_mismatch is False
    assert result.recovery_attempted is True
    assert result.recovery_succeeded is True
    assert result.refreshed_resource_url == stored_resource_url
    assert result.refreshed_etag == "\"etag-live\""
    assert result.initial_resource_url == stored_resource_url
    assert result.initial_etag == "\"etag-stored\""
    assert result.retry_attempted is False
    assert result.retry_succeeded is False
    assert result.retry_count == 0


def test_update_returns_recovery_result_after_stale_resource_url_404() -> None:
    stale_resource_url = "https://caldav.example.com/calendars/tomo/poc/stale.ics"
    current_resource_url = "https://caldav.example.com/calendars/tomo/poc/current.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.UPDATE, event_id="evt-update", ics_uid="uid-update", sequence=7),
    ])
    transport = FakeTransport(
        existing_urls={"uid-update": current_resource_url},
        etags_by_resource_url={current_resource_url: "\"etag-current\""},
        put_errors_by_resource_url={
            stale_resource_url: [404],
            current_resource_url: [409],
        },
        get_errors_by_resource_url={stale_resource_url: [404]},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-update")],
        generated_at=_generated_at(),
        previous_sync_state={
            "evt-update": _build_previous_state(
                "evt-update",
                resource_url=stale_resource_url,
                etag="\"etag-stale\"",
            )
        },
    )

    assert report.success_count == 0
    assert report.failure_count == 1
    assert transport.find_calls == ["uid-update"]
    assert transport.get_calls == [stale_resource_url, current_resource_url]
    result = report.results[0]
    assert result.success is False
    assert result.status_code == 409
    assert result.resolution_strategy == "sync_state_resource_url_then_uid_lookup"
    assert result.recovery_attempted is True
    assert result.recovery_succeeded is True
    assert result.refreshed_resource_url == current_resource_url
    assert result.refreshed_etag == "\"etag-current\""
    assert result.initial_resource_url == stale_resource_url
    assert result.initial_etag == "\"etag-stale\""
    assert result.retry_attempted is False
    assert result.retry_succeeded is False
    assert result.retry_count == 0


def test_update_retries_only_once_after_etag_mismatch_and_returns_failure_on_retry_failure() -> None:
    stored_resource_url = "https://caldav.example.com/calendars/tomo/poc/stored.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.UPDATE, event_id="evt-update", ics_uid="uid-update", sequence=7),
    ])
    transport = FakeTransport(
        etags_by_resource_url={stored_resource_url: "\"etag-live\""},
        put_errors_by_resource_url={stored_resource_url: [412, 412]},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-update")],
        generated_at=_generated_at(),
        previous_sync_state={
            "evt-update": _build_previous_state(
                "evt-update",
                resource_url=stored_resource_url,
                etag="\"etag-stored\"",
            )
        },
    )

    assert report.success_count == 0
    assert report.failure_count == 1
    assert transport.get_calls == [stored_resource_url]
    assert len(transport.put_calls) == 2
    result = report.results[0]
    assert result.success is False
    assert result.status_code == 412
    assert result.payload_sequence == 7
    assert result.recovery_attempted is True
    assert result.recovery_succeeded is True
    assert result.refreshed_resource_url == stored_resource_url
    assert result.refreshed_etag == "\"etag-live\""
    assert result.conflict_kind == "etag_mismatch"
    assert result.retryable is True
    assert result.etag_mismatch is True
    assert result.initial_resource_url == stored_resource_url
    assert result.initial_etag == "\"etag-stored\""
    assert result.retry_attempted is True
    assert result.retry_succeeded is False
    assert result.retry_count == 1
    assert result.retry_resource_url == stored_resource_url
    assert result.retry_etag == "\"etag-live\""
    assert transport.put_calls[0]["etag"] == "\"etag-stored\""
    assert transport.put_calls[1]["etag"] == "\"etag-live\""


def test_update_returns_failed_recovery_when_read_path_cannot_find_current_resource() -> None:
    stored_resource_url = "https://caldav.example.com/calendars/tomo/poc/stored.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.UPDATE, event_id="evt-update", ics_uid="uid-update", sequence=7),
    ])
    transport = FakeTransport(
        put_errors_by_resource_url={stored_resource_url: [412]},
        get_errors_by_resource_url={stored_resource_url: [404]},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-update")],
        generated_at=_generated_at(),
        previous_sync_state={
            "evt-update": _build_previous_state(
                "evt-update",
                resource_url=stored_resource_url,
                etag="\"etag-stored\"",
            )
        },
    )

    assert report.success_count == 0
    assert report.failure_count == 1
    result = report.results[0]
    assert result.success is False
    assert result.status_code == 412
    assert result.recovery_attempted is True
    assert result.recovery_succeeded is False
    assert result.refreshed_resource_url is None
    assert result.refreshed_etag is None
    assert result.retry_attempted is False
    assert result.retry_succeeded is False
    assert result.retry_count == 0


def test_delete_uses_stored_resource_url_and_etag() -> None:
    stored_resource_url = "https://caldav.example.com/calendars/tomo/poc/stored-delete.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.DELETE, event_id="evt-delete", ics_uid="uid-delete", sequence=6),
    ])
    transport = FakeTransport()
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [],
        generated_at=_generated_at(),
        previous_sync_state={
            "evt-delete": _build_previous_state(
                "evt-delete",
                resource_url=stored_resource_url,
                etag="\"etag-delete\"",
            )
        },
    )

    assert report.success_count == 1
    assert transport.resolve_calls == 0
    assert transport.find_calls == []
    assert transport.delete_calls == [{"resource_url": stored_resource_url, "etag": "\"etag-delete\""}]
    result = report.results[0]
    assert result.action == "delete"
    assert result.success is True
    assert result.sent is True
    assert result.resource_url == stored_resource_url
    assert result.etag == "\"etag-delete\""
    assert result.resolution_strategy == "sync_state_resource_url"
    assert result.used_stored_resource_url is True
    assert result.used_stored_etag is True
    assert result.attempted_conditional_update is True
    assert result.payload_sequence is None


def test_delete_falls_back_to_uid_lookup_when_resource_url_is_missing() -> None:
    current_resource_url = "https://caldav.example.com/calendars/tomo/poc/current-delete.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.DELETE, event_id="evt-delete", ics_uid="uid-delete", sequence=6),
    ])
    transport = FakeTransport(
        existing_urls={"uid-delete": current_resource_url},
        etags_by_resource_url={current_resource_url: "\"etag-current-delete\""},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [],
        generated_at=_generated_at(),
    )

    assert report.success_count == 1
    assert transport.resolve_calls == 1
    assert transport.find_calls == ["uid-delete"]
    assert transport.delete_calls == [{"resource_url": current_resource_url, "etag": "\"etag-current-delete\""}]
    result = report.results[0]
    assert result.success is True
    assert result.sent is True
    assert result.resource_url == current_resource_url
    assert result.etag == "\"etag-current-delete\""
    assert result.resolution_strategy == "uid_lookup"
    assert result.uid_lookup_performed is True
    assert result.used_stored_resource_url is False
    assert result.used_stored_etag is True


def test_delete_without_resource_is_treated_as_already_absent() -> None:
    plan = _build_sync_plan([
        _build_action(SyncActionType.DELETE, event_id="evt-delete", ics_uid="uid-delete", sequence=6),
    ])
    transport = FakeTransport()
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [],
        generated_at=_generated_at(),
    )

    assert report.success_count == 1
    assert transport.resolve_calls == 1
    assert transport.find_calls == ["uid-delete"]
    assert transport.delete_calls == []
    result = report.results[0]
    assert result.success is True
    assert result.sent is False
    assert result.resource_url is None
    assert result.etag is None
    assert result.resolution_strategy == "uid_lookup_absent"
    assert result.uid_lookup_performed is True


def test_delete_failure_does_not_become_success() -> None:
    stored_resource_url = "https://caldav.example.com/calendars/tomo/poc/delete-fail.ics"
    plan = _build_sync_plan([
        _build_action(SyncActionType.DELETE, event_id="evt-delete", ics_uid="uid-delete", sequence=6),
    ])
    transport = FakeTransport(
        delete_errors_by_resource_url={stored_resource_url: [409]},
        etags_by_resource_url={stored_resource_url: "\"etag-live-delete\""},
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [],
        generated_at=_generated_at(),
        previous_sync_state={
            "evt-delete": _build_previous_state(
                "evt-delete",
                resource_url=stored_resource_url,
                etag="\"etag-delete\"",
            )
        },
    )

    assert report.success_count == 0
    assert report.failure_count == 1
    result = report.results[0]
    assert result.success is False
    assert result.status_code == 409
    assert result.resource_url == stored_resource_url
    assert result.resolution_strategy == "sync_state_resource_url"
    assert result.recovery_attempted is True
    assert result.recovery_succeeded is True


def test_skip_is_the_only_ignored_action() -> None:
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
        _build_action(SyncActionType.SKIP, event_id="evt-skip", ics_uid="uid-skip", sequence=1),
        _build_action(SyncActionType.DELETE, event_id="evt-delete", ics_uid="uid-delete", sequence=2),
        _build_action(SyncActionType.UPDATE, event_id="evt-update", ics_uid="uid-update", sequence=3),
    ])
    prepared = build_caldav_actions(
        plan,
        [_build_event("evt-create"), _build_event("evt-update")],
        generated_at=_generated_at(),
        previous_sync_state={
            "evt-delete": _build_previous_state(
                "evt-delete",
                resource_url="https://caldav.example.com/calendars/tomo/poc/delete.ics",
            )
        },
    )

    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.example.com/principals/tomo/",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=True,
        ),
        transport=FakeTransport(),
    )
    report = client.sync(
        plan,
        [_build_event("evt-create"), _build_event("evt-update")],
        generated_at=_generated_at(),
        previous_sync_state={
            "evt-delete": _build_previous_state(
                "evt-delete",
                resource_url="https://caldav.example.com/calendars/tomo/poc/delete.ics",
            )
        },
    )

    assert [item.action for item in prepared] == [SyncActionType.CREATE, SyncActionType.DELETE, SyncActionType.UPDATE]
    assert report.processed_count == 3
    assert report.ignored_count == 1
    assert [item.action for item in report.ignored_actions] == ["skip"]


def test_requests_transport_discovers_calendar_collection_via_principal_and_logs_result(caplog) -> None:
    settings = CalDAVConnectionSettings(
        url="https://caldav.icloud.com",
        username="user",
        password="pass",
        calendar_name="PoC Calendar",
        dry_run=False,
    )
    session = FakeRequestsSession(
        responses=[
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:">
                  <d:response>
                    <d:href>/</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:current-user-principal>
                          <d:href>/123456789/principal/</d:href>
                        </d:current-user-principal>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
                  <d:response>
                    <d:href>/123456789/principal/</d:href>
                    <d:propstat>
                      <d:prop>
                        <c:calendar-home-set>
                          <d:href>/123456789/calendars/</d:href>
                        </c:calendar-home-set>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
                  <d:response>
                    <d:href>/123456789/calendars/</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:displayname>Calendars</d:displayname>
                        <d:resourcetype><d:collection /></d:resourcetype>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                  <d:response>
                    <d:href>/123456789/calendars/home/</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:displayname>Home</d:displayname>
                        <d:resourcetype><d:collection /><c:calendar /></d:resourcetype>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                  <d:response>
                    <d:href>/123456789/calendars/poc/</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:displayname>PoC Calendar</d:displayname>
                        <d:resourcetype><d:collection /><c:calendar /></d:resourcetype>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
        ]
    )
    transport = RequestsCalDAVTransport(
        settings,
        logger=logging.getLogger(caldav_client_module.__name__),
        session=session,
    )

    with caplog.at_level(logging.INFO, logger=caldav_client_module.__name__):
        calendar_url = transport.resolve_calendar_url(settings)

    assert calendar_url == "https://caldav.icloud.com/123456789/calendars/poc/"
    assert [call["url"] for call in session.calls] == [
        "https://caldav.icloud.com/",
        "https://caldav.icloud.com/123456789/principal/",
        "https://caldav.icloud.com/123456789/calendars/",
    ]
    assert [call["headers"]["Depth"] for call in session.calls] == ["0", "0", "1"]
    assert "current-user-principal" in session.calls[0]["data"]
    assert "calendar-home-set" in session.calls[1]["data"]
    assert "displayname" in session.calls[2]["data"]

    message = _last_caldav_client_log(caplog)
    assert "caldav discovery resolved" in message
    assert "component=caldav" in message
    assert "phase=discovery" in message
    assert "root_url=https://caldav.icloud.com/" in message
    assert "principal_url=https://caldav.icloud.com/123456789/principal/" in message
    assert "calendar_home_url=https://caldav.icloud.com/123456789/calendars/" in message
    assert "calendar_url=https://caldav.icloud.com/123456789/calendars/poc/" in message
    assert "calendar_name=\"PoC Calendar\"" in message


def test_requests_transport_chooses_exact_calendar_name_match() -> None:
    settings = CalDAVConnectionSettings(
        url="https://caldav.icloud.com",
        username="user",
        password="pass",
        calendar_name="PoC Calendar",
        dry_run=False,
    )
    session = FakeRequestsSession(
        responses=[
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:">
                  <d:response>
                    <d:href>/</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:current-user-principal>
                          <d:href>/principal/</d:href>
                        </d:current-user-principal>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
                  <d:response>
                    <d:href>/principal/</d:href>
                    <d:propstat>
                      <d:prop>
                        <c:calendar-home-set>
                          <d:href>/home/</d:href>
                        </c:calendar-home-set>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
                  <d:response>
                    <d:href>/home/poc-candidate/</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:displayname>PoC Calendar Archive</d:displayname>
                        <d:resourcetype><d:collection /><c:calendar /></d:resourcetype>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                  <d:response>
                    <d:href>/home/poc/</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:displayname>PoC Calendar</d:displayname>
                        <d:resourcetype><d:collection /><c:calendar /></d:resourcetype>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
        ]
    )
    transport = RequestsCalDAVTransport(settings, session=session)

    calendar_url = transport.resolve_calendar_url(settings)

    assert calendar_url == "https://caldav.icloud.com/home/poc/"


def test_requests_transport_put_create_returns_request_response_diagnostics() -> None:
    settings = CalDAVConnectionSettings(
        url="https://caldav.example.com/principals/tomo/",
        username="user",
        password="pass",
        calendar_name="PoC Calendar",
        dry_run=False,
    )
    session = FakeRequestsSession(
        responses=[
            FakeRequestsResponse(
                201,
                "",
                headers={
                    "ETag": "\"etag-create\"",
                    "Content-Type": "text/plain",
                    "Content-Length": "0",
                    "Location": "https://caldav.example.com/calendars/tomo/poc/uid-create.ics",
                },
            )
        ]
    )
    transport = RequestsCalDAVTransport(settings, session=session)

    result = transport.put_calendar_object(
        "https://caldav.example.com/calendars/tomo/poc/uid-create.ics",
        "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n",
        overwrite=False,
    )

    diagnostics = result.request_response_diagnostics
    assert diagnostics is not None
    assert diagnostics.request_method == "PUT"
    assert diagnostics.request_url == "https://caldav.example.com/calendars/tomo/poc/uid-create.ics"
    assert diagnostics.request_headers["If-None-Match"] == "*"
    assert diagnostics.request_headers["If-Match"] is None
    assert diagnostics.request_headers["Content-Type"] == "text/calendar; charset=utf-8"
    assert diagnostics.request_headers["Content-Length"] == len(
        "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n".encode("utf-8")
    )
    assert diagnostics.response_headers == {
        "ETag": "\"etag-create\"",
        "Content-Type": "text/plain",
        "Content-Length": "0",
        "Location": "https://caldav.example.com/calendars/tomo/poc/uid-create.ics",
    }
    assert diagnostics.response_body_excerpt is None


def test_requests_transport_put_create_error_includes_request_response_diagnostics() -> None:
    settings = CalDAVConnectionSettings(
        url="https://caldav.example.com/principals/tomo/",
        username="user",
        password="pass",
        calendar_name="PoC Calendar",
        dry_run=False,
    )
    session = FakeRequestsSession(
        responses=[
            FakeRequestsResponse(
                412,
                "precondition failed",
                headers={
                    "Content-Type": "text/plain",
                    "Content-Length": "19",
                },
            )
        ]
    )
    transport = RequestsCalDAVTransport(settings, session=session)

    with pytest.raises(CalDAVHTTPError) as exc_info:
        transport.put_calendar_object(
            "https://caldav.example.com/calendars/tomo/poc/uid-create.ics",
            "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n",
            overwrite=False,
        )

    diagnostics = exc_info.value.request_response_diagnostics
    assert diagnostics is not None
    assert diagnostics.request_method == "PUT"
    assert diagnostics.request_headers["If-None-Match"] == "*"
    assert diagnostics.request_headers["If-Match"] is None
    assert diagnostics.request_headers["Content-Type"] == "text/calendar; charset=utf-8"
    assert diagnostics.response_headers == {
        "ETag": None,
        "Content-Type": "text/plain",
        "Content-Length": "19",
        "Location": None,
    }
    assert diagnostics.response_body_excerpt == "precondition failed"


def test_requests_transport_diagnose_uid_lookup_falls_back_to_collection_scan() -> None:
    settings = CalDAVConnectionSettings(
        url="https://caldav.icloud.com",
        username="user",
        password="pass",
        calendar_name="PoC Calendar",
        dry_run=False,
    )
    session = FakeRequestsSession(
        responses=[
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
                </d:multistatus>
                """,
            ),
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
                  <d:response>
                    <d:href>/home/poc/a.ics</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:getetag>"etag-a"</d:getetag>
                        <c:calendar-data>BEGIN:VCALENDAR\r
BEGIN:VEVENT\r
UID:uid-other\r
END:VEVENT\r
END:VCALENDAR\r
</c:calendar-data>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                  <d:response>
                    <d:href>/home/poc/matched.ics</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:getetag>"etag-match"</d:getetag>
                        <c:calendar-data>BEGIN:VCALENDAR\r
BEGIN:VEVENT\r
UID:uid-create\r
END:VEVENT\r
END:VCALENDAR\r
</c:calendar-data>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
        ]
    )
    transport = RequestsCalDAVTransport(settings, session=session)

    diagnostics = transport.diagnose_uid_lookup(
        "https://caldav.icloud.com/home/poc/",
        "uid-create",
    )

    assert diagnostics.attempted is True
    assert diagnostics.candidate_count == 2
    assert (
        diagnostics.method
        == "calendar_query_uid_calendar_data+calendar_collection_scan_calendar_data"
    )
    assert diagnostics.matched_resource_url == "https://caldav.icloud.com/home/poc/matched.ics"
    assert diagnostics.matched_resource_etag == "\"etag-match\""
    assert diagnostics.remote_uid_confirmed is True
    assert diagnostics.selected_candidate_reason == (
        "confirmed_uid_match_from_calendar_collection_scan_calendar_data"
    )
    assert diagnostics.selected_candidate_index == 1
    assert [candidate.resource_url for candidate in diagnostics.candidates] == [
        "https://caldav.icloud.com/home/poc/a.ics",
        "https://caldav.icloud.com/home/poc/matched.ics",
    ]
    assert diagnostics.candidates[0].summary is None
    assert diagnostics.candidates[1].remote_uid == "uid-create"
    assert diagnostics.uid_query_raw_response is not None
    assert diagnostics.collection_scan_raw_response is not None
    assert "/home/poc/a.ics" in diagnostics.collection_scan_raw_response
    assert [call["method"] for call in session.calls] == ["REPORT", "REPORT"]


def test_requests_transport_find_event_resource_by_uid_ignores_collection_root_href() -> None:
    settings = CalDAVConnectionSettings(
        url="https://caldav.icloud.com",
        username="user",
        password="pass",
        calendar_name="PoC Calendar",
        dry_run=False,
    )
    session = FakeRequestsSession(
        responses=[
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:">
                  <d:response>
                    <d:href>/home/poc/</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:getetag>"etag-root"</d:getetag>
                      </d:prop>
                      <d:status>HTTP/1.1 200 OK</d:status>
                    </d:propstat>
                  </d:response>
                  <d:response>
                    <d:href>matched.ics</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:getetag>"etag-match"</d:getetag>
                      </d:prop>
                      <d:status>HTTP/1.1 200 OK</d:status>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
        ]
    )
    transport = RequestsCalDAVTransport(settings, session=session)

    resource = transport.find_event_resource_by_uid(
        "https://caldav.icloud.com/home/poc/",
        "uid-create",
    )

    assert resource is not None
    assert resource.resource_url == "https://caldav.icloud.com/home/poc/matched.ics"
    assert resource.etag == "\"etag-match\""
    assert [call["method"] for call in session.calls] == ["REPORT"]


def test_requests_transport_diagnose_uid_lookup_prefers_uid_report_resource_href_without_calendar_data() -> None:
    settings = CalDAVConnectionSettings(
        url="https://caldav.icloud.com",
        username="user",
        password="pass",
        calendar_name="PoC Calendar",
        dry_run=False,
    )
    session = FakeRequestsSession(
        responses=[
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
                  <d:response>
                    <d:href>/home/poc/</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:getetag>"etag-root"</d:getetag>
                      </d:prop>
                      <d:status>HTTP/1.1 200 OK</d:status>
                    </d:propstat>
                  </d:response>
                  <d:response>
                    <d:href>matched.ics</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:getetag>"etag-match"</d:getetag>
                      </d:prop>
                      <d:status>HTTP/1.1 200 OK</d:status>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
                  <d:response>
                    <d:href>/home/poc/</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:getetag>"etag-root"</d:getetag>
                      </d:prop>
                      <d:status>HTTP/1.1 200 OK</d:status>
                    </d:propstat>
                  </d:response>
                  <d:response>
                    <d:href>other.ics</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:getetag>"etag-other"</d:getetag>
                      </d:prop>
                      <d:status>HTTP/1.1 200 OK</d:status>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
        ]
    )
    transport = RequestsCalDAVTransport(settings, session=session)

    diagnostics = transport.diagnose_uid_lookup(
        "https://caldav.icloud.com/home/poc/",
        "uid-create",
    )

    assert diagnostics.attempted is True
    assert diagnostics.candidate_count == 2
    assert (
        diagnostics.method
        == "calendar_query_uid_calendar_data+calendar_collection_scan_calendar_data"
    )
    assert diagnostics.matched_resource_url == "https://caldav.icloud.com/home/poc/matched.ics"
    assert diagnostics.matched_resource_etag == "\"etag-match\""
    assert diagnostics.remote_uid_confirmed is False
    assert diagnostics.selected_candidate_reason == "first_candidate_from_calendar_query_uid_calendar_data"
    assert diagnostics.selected_candidate_index == 0
    assert [candidate.resource_url for candidate in diagnostics.candidates] == [
        "https://caldav.icloud.com/home/poc/matched.ics",
        "https://caldav.icloud.com/home/poc/other.ics",
    ]
    assert diagnostics.uid_query_raw_response is not None
    assert diagnostics.collection_scan_raw_response is not None
    assert "matched.ics" in diagnostics.uid_query_raw_response
    assert "other.ics" in diagnostics.collection_scan_raw_response
    assert [call["method"] for call in session.calls] == ["REPORT", "REPORT"]


def test_sync_returns_discovery_error_kind_when_calendar_resolution_fails() -> None:
    plan = _build_sync_plan([
        _build_action(SyncActionType.CREATE, event_id="evt-create", ics_uid="uid-create", sequence=0),
    ])
    transport = FakeTransport(
        resolve_error=CalDAVDiscoveryError(
            "Calendar 'PoC Calendar' was not found below https://caldav.icloud.com/home/.",
            error_kind="discovery_calendar_not_found",
            url="https://caldav.icloud.com/home/",
            root_url="https://caldav.icloud.com/",
            principal_url="https://caldav.icloud.com/principal/",
            calendar_home_url="https://caldav.icloud.com/home/",
            calendar_name="PoC Calendar",
        ),
    )
    client = CalDAVClient(
        CalDAVConnectionSettings(
            url="https://caldav.icloud.com",
            username="user",
            password="pass",
            calendar_name="PoC Calendar",
            dry_run=False,
        ),
        transport=transport,
    )

    report = client.sync(
        plan,
        [_build_event("evt-create")],
        generated_at=_generated_at(),
    )

    assert report.success_count == 0
    assert report.failure_count == 1
    assert report.results[0].error_kind == "discovery_calendar_not_found"
    assert report.results[0].status_code is None
    assert report.results[0].resource_url is None
    assert "PoC Calendar" in report.results[0].error


def test_requests_transport_raises_discovery_error_when_calendar_is_missing(caplog) -> None:
    settings = CalDAVConnectionSettings(
        url="https://caldav.icloud.com",
        username="user",
        password="pass",
        calendar_name="Missing Calendar",
        dry_run=False,
    )
    session = FakeRequestsSession(
        responses=[
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:">
                  <d:response>
                    <d:href>/</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:current-user-principal>
                          <d:href>/principal/</d:href>
                        </d:current-user-principal>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
                  <d:response>
                    <d:href>/principal/</d:href>
                    <d:propstat>
                      <d:prop>
                        <c:calendar-home-set>
                          <d:href>/home/</d:href>
                        </c:calendar-home-set>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
            FakeRequestsResponse(
                207,
                """
                <d:multistatus xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
                  <d:response>
                    <d:href>/home/work/</d:href>
                    <d:propstat>
                      <d:prop>
                        <d:displayname>Work</d:displayname>
                        <d:resourcetype><d:collection /><c:calendar /></d:resourcetype>
                      </d:prop>
                    </d:propstat>
                  </d:response>
                </d:multistatus>
                """,
            ),
        ]
    )
    transport = RequestsCalDAVTransport(
        settings,
        logger=logging.getLogger(caldav_client_module.__name__),
        session=session,
    )

    with caplog.at_level(logging.ERROR, logger=caldav_client_module.__name__):
        with pytest.raises(CalDAVDiscoveryError) as exc_info:
            transport.resolve_calendar_url(settings)

    assert exc_info.value.error_kind == "discovery_calendar_not_found"
    message = _last_caldav_client_log(caplog)
    assert "caldav discovery failure" in message
    assert "component=caldav" in message
    assert "phase=discovery" in message
    assert "error_kind=discovery_calendar_not_found" in message
    assert "calendar_name=\"Missing Calendar\"" in message
    assert "calendar_home_url=https://caldav.icloud.com/home/" in message


@dataclass
class FakeTransport:
    calendar_url: str = "https://caldav.example.com/calendars/tomo/poc/"
    resolve_error: CalDAVClientError | None = None
    existing_urls: dict[str, str] = field(default_factory=dict)
    uid_lookup_diagnostics_by_uid: dict[str, CalDAVUIDLookupDiagnostics] = field(default_factory=dict)
    etags_by_resource_url: dict[str, str] = field(default_factory=dict)
    remote_calendar_data_by_resource_url: dict[str, str] = field(default_factory=dict)
    put_errors_by_resource_url: dict[str, list[int]] = field(default_factory=dict)
    delete_errors_by_resource_url: dict[str, list[int]] = field(default_factory=dict)
    get_errors_by_resource_url: dict[str, list[int]] = field(default_factory=dict)
    get_data_errors_by_resource_url: dict[str, list[int]] = field(default_factory=dict)
    fail_with_runtime_error_on_put: set[str] = field(default_factory=set)
    resolve_calls: int = 0
    find_calls: list[str] = field(default_factory=list)
    put_calls: list[dict[str, object]] = field(default_factory=list)
    delete_calls: list[dict[str, object]] = field(default_factory=list)
    get_calls: list[str] = field(default_factory=list)
    get_data_calls: list[str] = field(default_factory=list)

    def resolve_calendar_url(self, settings: CalDAVConnectionSettings) -> str:
        self.resolve_calls += 1
        if self.resolve_error is not None:
            raise self.resolve_error
        return self.calendar_url

    def find_event_resource_by_uid(self, calendar_url: str, uid: str) -> CalDAVResourceState | None:
        self.find_calls.append(uid)
        resource_url = self.existing_urls.get(uid)
        if resource_url is None:
            return None
        return CalDAVResourceState(
            resource_url=resource_url,
            etag=self.etags_by_resource_url.get(resource_url),
        )

    def diagnose_uid_lookup(self, calendar_url: str, uid: str) -> CalDAVUIDLookupDiagnostics:
        self.find_calls.append(uid)
        configured = self.uid_lookup_diagnostics_by_uid.get(uid)
        if configured is not None:
            return configured

        resource_url = self.existing_urls.get(uid)
        if resource_url is None:
            return CalDAVUIDLookupDiagnostics(
                attempted=True,
                candidate_count=0,
                method="calendar_query_uid_calendar_data+calendar_collection_scan_calendar_data",
                matched_resource_url=None,
                matched_resource_etag=None,
                remote_uid_confirmed=False,
                selected_candidate_reason=None,
                selected_candidate_index=None,
                candidates=[],
            )
        return CalDAVUIDLookupDiagnostics(
            attempted=True,
            candidate_count=1,
            method="calendar_query_uid_calendar_data",
            matched_resource_url=resource_url,
            matched_resource_etag=self.etags_by_resource_url.get(resource_url),
            remote_uid_confirmed=True,
            selected_candidate_reason="confirmed_uid_match_from_calendar_query_uid_calendar_data",
            selected_candidate_index=0,
            candidates=[
                CalDAVUIDLookupCandidate(
                    resource_url=resource_url,
                    etag=self.etags_by_resource_url.get(resource_url),
                )
            ],
        )

    def get_calendar_object(self, resource_url: str) -> CalDAVResourceState:
        self.get_calls.append(resource_url)
        status_codes = self.get_errors_by_resource_url.get(resource_url)
        if status_codes:
            status_code = status_codes.pop(0)
            raise CalDAVHTTPError("HEAD", resource_url, status_code, "simulated failure")
        return CalDAVResourceState(
            resource_url=resource_url,
            etag=self.etags_by_resource_url.get(resource_url),
        )

    def get_calendar_object_data(self, resource_url: str) -> CalDAVCalendarObject:
        self.get_data_calls.append(resource_url)
        status_codes = self.get_data_errors_by_resource_url.get(resource_url)
        if status_codes:
            status_code = status_codes.pop(0)
            raise CalDAVHTTPError("GET", resource_url, status_code, "simulated fetch failure")
        return CalDAVCalendarObject(
            resource_url=resource_url,
            etag=self.etags_by_resource_url.get(resource_url),
            calendar_data=self.remote_calendar_data_by_resource_url.get(resource_url),
        )

    def put_calendar_object(
        self,
        resource_url: str,
        ics_payload: str,
        *,
        overwrite: bool,
        etag: str | None = None,
    ) -> CalDAVPutResult:
        self.put_calls.append({
            "resource_url": resource_url,
            "ics_payload": ics_payload,
            "overwrite": overwrite,
            "etag": etag,
        })
        if resource_url in self.fail_with_runtime_error_on_put:
            raise RuntimeError("simulated runtime failure")
        request_response_diagnostics = _build_fake_put_request_response_diagnostics(
            resource_url,
            ics_payload,
            overwrite=overwrite,
            etag=etag,
            response_headers=self._build_put_response_headers(resource_url, overwrite=overwrite),
            response_body_excerpt=None,
        )
        status_codes = self.put_errors_by_resource_url.get(resource_url)
        if status_codes:
            status_code = status_codes.pop(0)
            raise CalDAVHTTPError(
                "PUT",
                resource_url,
                status_code,
                "simulated failure",
                request_response_diagnostics=_build_fake_put_request_response_diagnostics(
                    resource_url,
                    ics_payload,
                    overwrite=overwrite,
                    etag=etag,
                    response_headers=self._build_put_response_headers(resource_url, overwrite=overwrite),
                    response_body_excerpt="simulated failure",
                ),
            )
        default_etag = "\"etag-update\"" if overwrite else "\"etag-create\""
        return CalDAVPutResult(
            status_code=201 if not overwrite else 204,
            resource_url=resource_url,
            etag=self.etags_by_resource_url.get(resource_url, default_etag),
            request_response_diagnostics=request_response_diagnostics,
        )

    def delete_calendar_object(
        self,
        resource_url: str,
        *,
        etag: str | None = None,
    ) -> CalDAVDeleteResult:
        self.delete_calls.append({
            "resource_url": resource_url,
            "etag": etag,
        })
        status_codes = self.delete_errors_by_resource_url.get(resource_url)
        if status_codes:
            status_code = status_codes.pop(0)
            raise CalDAVHTTPError("DELETE", resource_url, status_code, "simulated failure")
        return CalDAVDeleteResult(
            status_code=204,
            resource_url=resource_url,
            etag=self.etags_by_resource_url.get(resource_url, etag),
            sent=True,
        )

    def _build_put_response_headers(
        self,
        resource_url: str,
        *,
        overwrite: bool,
    ) -> dict[str, str]:
        default_etag = "\"etag-update\"" if overwrite else "\"etag-create\""
        return {
            "ETag": self.etags_by_resource_url.get(resource_url, default_etag),
            "Content-Type": "text/plain",
            "Content-Length": "0",
            "Location": resource_url,
        }


@dataclass
class FakeRequestsResponse:
    status_code: int
    text: str
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class FakeRequestsSession:
    responses: list[FakeRequestsResponse]
    calls: list[dict[str, object]] = field(default_factory=list)
    auth: tuple[str, str] | None = None
    headers: dict[str, str] = field(default_factory=dict)

    def request(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        data,
        timeout: float,
    ) -> FakeRequestsResponse:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "data": data,
                "timeout": timeout,
            }
        )
        assert self.responses, "No fake response queued."
        return self.responses.pop(0)


def _build_fake_put_request_response_diagnostics(
    resource_url: str,
    ics_payload: str,
    *,
    overwrite: bool,
    etag: str | None,
    response_headers: dict[str, str],
    response_body_excerpt: str | None,
) -> CalDAVRequestResponseDiagnostics:
    request_headers = {
        "If-None-Match": None if overwrite else "*",
        "If-Match": (etag or "*") if overwrite else None,
        "Content-Type": "text/calendar; charset=utf-8",
        "Content-Length": len(ics_payload.encode("utf-8")),
    }
    return CalDAVRequestResponseDiagnostics(
        request_method="PUT",
        request_url=resource_url,
        request_headers=request_headers,
        response_headers=dict(response_headers),
        response_body_excerpt=response_body_excerpt,
    )


def _build_sync_plan(actions: list[SyncPlanAction]) -> SyncPlan:
    return SyncPlan(
        generated_at="2026-03-12T00:00:00+00:00",
        actions=actions,
    )


def _build_action(
    action: SyncActionType,
    *,
    event_id: str,
    ics_uid: str,
    sequence: int,
    reappeared_from_tombstone: bool = False,
    tombstone_deleted_at: str | None = None,
) -> SyncPlanAction:
    return SyncPlanAction(
        action=action,
        event_id=event_id,
        ics_uid=ics_uid,
        sequence=sequence,
        content_hash=f"hash-{event_id}",
        updated_at="2026-03-12T00:00:00Z",
        action_reason="test",
        reappeared_from_tombstone=reappeared_from_tombstone,
        tombstone_deleted_at=tombstone_deleted_at,
    )


def _build_event(event_id: str) -> EventRecord:
    return EventRecord(
        event_id=event_id,
        subject=f"Subject {event_id}",
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


def _generated_at() -> datetime:
    return datetime(2026, 3, 12, 0, 0, 0, tzinfo=timezone.utc)


def _last_caldav_client_log(caplog) -> str:
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == caldav_client_module.__name__
    ]
    assert messages
    return messages[-1]


def _build_previous_state(
    event_id: str,
    *,
    resource_url: str | None = None,
    etag: str | None = None,
) -> EventSyncState:
    return EventSyncState(
        event_id=event_id,
        ics_uid=f"uid-{event_id.removeprefix('evt-')}",
        updated_at="2026-03-11T00:00:00Z",
        content_hash=f"hash-{event_id}",
        sequence=6,
        is_deleted=False,
        last_synced_at="2026-03-11T00:00:00+00:00",
        resource_url=resource_url,
        etag=etag,
        last_delivery_status="success",
        last_delivery_at="2026-03-11T00:00:00+00:00",
    )
