from __future__ import annotations

import json
from pathlib import Path

import pytest

import src.sync_plan_inspect as sync_plan_inspect_module


def test_inspect_defaults_to_create_and_delete_sections(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-create",
                "ics_uid": "uid-create",
                "sequence": 0,
                "content_hash": "hash-create",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "Create Event",
            },
            {
                "action": "update",
                "event_id": "evt-update",
                "ics_uid": "uid-update",
                "sequence": 3,
                "content_hash": "hash-update",
                "updated_at": "2026-03-12T11:00:00Z",
                "action_reason": "content_changed",
                "summary": "Updated Event",
            },
            {
                "action": "delete",
                "event_id": "evt-delete",
                "ics_uid": "uid-delete",
                "sequence": 5,
                "content_hash": "hash-delete",
                "updated_at": "2026-03-11T09:00:00Z",
                "action_reason": "missing_from_current_fetch",
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(["--plan-path", str(plan_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "shown_actions: create, delete" in captured.out
    assert "[create] count=1" in captured.out
    assert "[delete] count=1" in captured.out
    assert "[update]" not in captured.out
    assert "evt-create" in captured.out
    assert "Create Event" in captured.out
    assert "evt-delete" in captured.out
    assert "(not available)" in captured.out
    assert captured.err == ""


def test_inspect_can_filter_to_single_action(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-create",
                "ics_uid": "uid-create",
                "sequence": 0,
                "content_hash": "hash-create",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "Create Event",
            },
            {
                "action": "update",
                "event_id": "evt-update",
                "ics_uid": "uid-update",
                "sequence": 3,
                "content_hash": "hash-update",
                "updated_at": "2026-03-12T11:00:00Z",
                "action_reason": "content_changed",
                "summary": "Updated Event",
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        ["--plan-path", str(plan_path), "--action", "update"]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "shown_actions: update" in captured.out
    assert "[update] count=1" in captured.out
    assert "[create]" not in captured.out
    assert "evt-update" in captured.out
    assert "Updated Event" in captured.out
    assert captured.err == ""


def test_inspect_reports_missing_file(tmp_path: Path, capsys) -> None:
    missing_path = tmp_path / "missing-sync-plan.json"

    exit_code = sync_plan_inspect_module.main(["--plan-path", str(missing_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert f"inspection input file was not found: {missing_path}" in captured.err


def test_inspect_reports_invalid_payload(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    plan_path.write_text(
        json.dumps({"version": 1, "generated_at": "2026-03-12T00:00:00+00:00", "actions": {}}),
        encoding="utf-8",
    )

    exit_code = sync_plan_inspect_module.main(["--plan-path", str(plan_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "Failed to inspect sync plan: sync_plan.actions must be a list" in captured.err


def test_inspect_can_overlay_caldav_result_diagnostics(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    result_path = tmp_path / "caldav_sync_result.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-success",
                "ics_uid": "uid-success",
                "sequence": 0,
                "content_hash": "hash-success",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "Success Event",
            },
            {
                "action": "create",
                "event_id": "evt-failed",
                "ics_uid": "uid-failed",
                "sequence": 0,
                "content_hash": "hash-failed",
                "updated_at": "2026-03-12T11:00:00Z",
                "action_reason": "new_event",
                "summary": "Failed Event",
            },
        ],
    )
    _write_caldav_result(
        result_path,
        results=[
            {
                "action": "create",
                "event_id": "evt-success",
                "ics_uid": "uid-success",
                "success": True,
                "status_code": 201,
                "error_kind": None,
                "resource_name": "uid-success.ics",
                "payload_bytes": 321,
                "payload_summary": {
                    "summary": "Success Event",
                    "is_all_day": False,
                    "has_description": True,
                    "has_location": False,
                },
            },
            {
                "action": "create",
                "event_id": "evt-failed",
                "ics_uid": "uid-failed",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "resource_name": "uid-failed.ics",
                "payload_bytes": 654,
                "payload_summary": {
                    "summary": "Failed Event",
                    "is_all_day": True,
                    "has_description": False,
                    "has_location": True,
                },
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        [
            "--plan-path",
            str(plan_path),
            "--result-path",
            str(result_path),
            "--action",
            "create",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert f"result_path: {result_path.resolve()}" in captured.out
    assert "delivery" in captured.out
    assert "status_code" in captured.out
    assert "error_kind" in captured.out
    assert "state_drift" in captured.out
    assert "existing_resource_url" in captured.out
    assert "selected_candidate_index" in captured.out
    assert "selected_candidate_reason" in captured.out
    assert "drift_report_status" in captured.out
    assert "drift_diff_count" in captured.out
    assert "drift_diff_fields" in captured.out
    assert "drift_report_path" in captured.out
    assert "payload_bytes" in captured.out
    assert "resource_name" in captured.out
    assert "success" in captured.out
    assert "failed" in captured.out
    assert "201" in captured.out
    assert "412" in captured.out
    assert "http_failed" in captured.out
    assert "false" in captured.out
    assert "true" in captured.out
    assert "uid-success.ics" in captured.out
    assert "uid-failed.ics" in captured.out
    assert captured.err == ""


def test_inspect_prints_state_drift_report_columns(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    result_path = tmp_path / "caldav_sync_result.json"
    drift_report_path = tmp_path / "reports" / "create_state_drift__evt-create.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-create",
                "ics_uid": "uid-create",
                "sequence": 0,
                "content_hash": "hash-create",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "Create Event",
            },
        ],
    )
    _write_caldav_result(
        result_path,
        results=[
            {
                "action": "create",
                "event_id": "evt-create",
                "ics_uid": "uid-create",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "resource_name": "uid-create.ics",
                "payload_bytes": 321,
                "payload_summary": {"summary": "Create Event"},
                "create_conflict_state_drift_suspected": True,
                "create_conflict_existing_resource_url": "https://caldav.example.com/uid-create.ics",
                "create_conflict_selected_candidate_index": 2,
                "create_conflict_selected_candidate_reason": (
                    "confirmed_uid_match_from_calendar_collection_scan_calendar_data"
                ),
                "create_conflict_state_drift_report_status": "generated",
                "drift_diff_count": 4,
                "drift_diff_fields": ["SUMMARY", "DESCRIPTION", "LOCATION", "SEQUENCE"],
                "create_conflict_state_drift_report_path": str(drift_report_path),
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        [
            "--plan-path",
            str(plan_path),
            "--result-path",
            str(result_path),
            "--action",
            "create",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "2" in captured.out
    assert "confirmed_uid_match_from_calendar_collection_scan_calendar_data" in captured.out
    assert "generated" in captured.out
    assert "4" in captured.out
    assert "SUMMARY,DESCRIPTION,LOCATION,+1" in captured.out
    assert str(drift_report_path) in captured.out
    assert captured.err == ""


def test_inspect_can_filter_to_failed_results(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    result_path = tmp_path / "caldav_sync_result.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-success",
                "ics_uid": "uid-success",
                "sequence": 0,
                "content_hash": "hash-success",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "Success Event",
            },
            {
                "action": "create",
                "event_id": "evt-failed",
                "ics_uid": "uid-failed",
                "sequence": 0,
                "content_hash": "hash-failed",
                "updated_at": "2026-03-12T11:00:00Z",
                "action_reason": "new_event",
                "summary": "Failed Event",
            },
        ],
    )
    _write_caldav_result(
        result_path,
        results=[
            {
                "action": "create",
                "event_id": "evt-success",
                "ics_uid": "uid-success",
                "success": True,
                "status_code": 201,
                "error_kind": None,
                "resource_name": "uid-success.ics",
                "payload_bytes": 321,
                "payload_summary": {"summary": "Success Event"},
            },
            {
                "action": "create",
                "event_id": "evt-failed",
                "ics_uid": "uid-failed",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "resource_name": "uid-failed.ics",
                "payload_bytes": 654,
                "payload_summary": {"summary": "Failed Event"},
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        [
            "--plan-path",
            str(plan_path),
            "--result-path",
            str(result_path),
            "--action",
            "create",
            "--only",
            "failed",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "only: failed" in captured.out
    assert "[create] count=1" in captured.out
    assert "evt-failed" in captured.out
    assert "Failed Event" in captured.out
    assert "evt-success" not in captured.out
    assert "Success Event" not in captured.out
    assert captured.err == ""


def test_inspect_can_filter_to_generated_drift_status_with_legacy_field(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    result_path = tmp_path / "caldav_sync_result.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-generated",
                "ics_uid": "uid-generated",
                "sequence": 0,
                "content_hash": "hash-generated",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "Generated Event",
            },
            {
                "action": "create",
                "event_id": "evt-remote-failed",
                "ics_uid": "uid-remote-failed",
                "sequence": 0,
                "content_hash": "hash-remote-failed",
                "updated_at": "2026-03-12T11:00:00Z",
                "action_reason": "new_event",
                "summary": "Remote Failed Event",
            },
            {
                "action": "create",
                "event_id": "evt-none",
                "ics_uid": "uid-none",
                "sequence": 0,
                "content_hash": "hash-none",
                "updated_at": "2026-03-12T12:00:00Z",
                "action_reason": "new_event",
                "summary": "None Event",
            },
        ],
    )
    _write_caldav_result(
        result_path,
        results=[
            {
                "action": "create",
                "event_id": "evt-generated",
                "ics_uid": "uid-generated",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "payload_summary": {"summary": "Generated Event"},
                "create_conflict_state_drift_report_status": "generated",
            },
            {
                "action": "create",
                "event_id": "evt-remote-failed",
                "ics_uid": "uid-remote-failed",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "payload_summary": {"summary": "Remote Failed Event"},
                "drift_report_status": "remote_fetch_failed",
            },
            {
                "action": "create",
                "event_id": "evt-none",
                "ics_uid": "uid-none",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "payload_summary": {"summary": "None Event"},
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        [
            "--plan-path",
            str(plan_path),
            "--result-path",
            str(result_path),
            "--action",
            "create",
            "--drift-status",
            "generated",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "drift_status: generated" in captured.out
    assert "[create] count=1" in captured.out
    assert "evt-generated" in captured.out
    assert "evt-remote-failed" not in captured.out
    assert "evt-none" not in captured.out
    assert captured.err == ""


def test_inspect_can_filter_to_remote_fetch_failed_drift_status(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    result_path = tmp_path / "caldav_sync_result.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-generated",
                "ics_uid": "uid-generated",
                "sequence": 0,
                "content_hash": "hash-generated",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "Generated Event",
            },
            {
                "action": "create",
                "event_id": "evt-remote-failed",
                "ics_uid": "uid-remote-failed",
                "sequence": 0,
                "content_hash": "hash-remote-failed",
                "updated_at": "2026-03-12T11:00:00Z",
                "action_reason": "new_event",
                "summary": "Remote Failed Event",
            },
        ],
    )
    _write_caldav_result(
        result_path,
        results=[
            {
                "action": "create",
                "event_id": "evt-generated",
                "ics_uid": "uid-generated",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "payload_summary": {"summary": "Generated Event"},
                "drift_report_status": "generated",
            },
            {
                "action": "create",
                "event_id": "evt-remote-failed",
                "ics_uid": "uid-remote-failed",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "payload_summary": {"summary": "Remote Failed Event"},
                "drift_report_status": "remote_fetch_failed",
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        [
            "--plan-path",
            str(plan_path),
            "--result-path",
            str(result_path),
            "--action",
            "create",
            "--drift-status",
            "remote_fetch_failed",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "drift_status: remote_fetch_failed" in captured.out
    assert "[create] count=1" in captured.out
    assert "evt-remote-failed" in captured.out
    assert "evt-generated" not in captured.out
    assert captured.err == ""


def test_inspect_can_filter_to_none_drift_status(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    result_path = tmp_path / "caldav_sync_result.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-none",
                "ics_uid": "uid-none",
                "sequence": 0,
                "content_hash": "hash-none",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "None Event",
            },
            {
                "action": "create",
                "event_id": "evt-generated",
                "ics_uid": "uid-generated",
                "sequence": 0,
                "content_hash": "hash-generated",
                "updated_at": "2026-03-12T11:00:00Z",
                "action_reason": "new_event",
                "summary": "Generated Event",
            },
        ],
    )
    _write_caldav_result(
        result_path,
        results=[
            {
                "action": "create",
                "event_id": "evt-none",
                "ics_uid": "uid-none",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "payload_summary": {"summary": "None Event"},
            },
            {
                "action": "create",
                "event_id": "evt-generated",
                "ics_uid": "uid-generated",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "payload_summary": {"summary": "Generated Event"},
                "drift_report_status": "generated",
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        [
            "--plan-path",
            str(plan_path),
            "--result-path",
            str(result_path),
            "--action",
            "create",
            "--drift-status",
            "none",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "drift_status: none" in captured.out
    assert "[create] count=1" in captured.out
    assert "evt-none" in captured.out
    assert "evt-generated" not in captured.out
    assert captured.err == ""


def test_inspect_sorts_failed_create_rows_by_drift_diff_count(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    result_path = tmp_path / "caldav_sync_result.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-low",
                "ics_uid": "uid-low",
                "sequence": 0,
                "content_hash": "hash-low",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "Low Diff Event",
            },
            {
                "action": "create",
                "event_id": "evt-high",
                "ics_uid": "uid-high",
                "sequence": 0,
                "content_hash": "hash-high",
                "updated_at": "2026-03-12T11:00:00Z",
                "action_reason": "new_event",
                "summary": "High Diff Event",
            },
        ],
    )
    _write_caldav_result(
        result_path,
        results=[
            {
                "action": "create",
                "event_id": "evt-low",
                "ics_uid": "uid-low",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "resource_name": "uid-low.ics",
                "payload_bytes": 123,
                "payload_summary": {"summary": "Low Diff Event"},
                "create_conflict_state_drift_suspected": True,
                "create_conflict_existing_resource_url": "https://caldav.example.com/uid-low.ics",
                "drift_report_status": "generated",
                "drift_diff_count": 1,
                "drift_diff_fields": ["SUMMARY"],
                "create_conflict_state_drift_report_path": str(tmp_path / "reports" / "low.json"),
            },
            {
                "action": "create",
                "event_id": "evt-high",
                "ics_uid": "uid-high",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "resource_name": "uid-high.ics",
                "payload_bytes": 456,
                "payload_summary": {"summary": "High Diff Event"},
                "create_conflict_state_drift_suspected": True,
                "create_conflict_existing_resource_url": "https://caldav.example.com/uid-high.ics",
                "drift_report_status": "generated",
                "drift_diff_count": 4,
                "drift_diff_fields": ["SUMMARY", "DESCRIPTION", "LOCATION", "SEQUENCE"],
                "create_conflict_state_drift_report_path": str(tmp_path / "reports" / "high.json"),
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        [
            "--plan-path",
            str(plan_path),
            "--result-path",
            str(result_path),
            "--action",
            "create",
            "--only",
            "failed",
            "--sort",
            "drift-diff-count",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "sort: drift-diff-count" in captured.out
    assert "drift_diff_count" in captured.out
    assert "drift_diff_fields" in captured.out
    assert "SUMMARY,DESCRIPTION,LOCATION,+1" in captured.out
    assert captured.out.index("evt-high") < captured.out.index("evt-low")
    assert captured.err == ""


def test_inspect_drift_sort_handles_rows_without_drift_report(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    result_path = tmp_path / "caldav_sync_result.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-nodrift",
                "ics_uid": "uid-nodrift",
                "sequence": 0,
                "content_hash": "hash-nodrift",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "No Drift Event",
            },
            {
                "action": "create",
                "event_id": "evt-drift",
                "ics_uid": "uid-drift",
                "sequence": 0,
                "content_hash": "hash-drift",
                "updated_at": "2026-03-12T11:00:00Z",
                "action_reason": "new_event",
                "summary": "Drift Event",
            },
        ],
    )
    _write_caldav_result(
        result_path,
        results=[
            {
                "action": "create",
                "event_id": "evt-nodrift",
                "ics_uid": "uid-nodrift",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "resource_name": "uid-nodrift.ics",
                "payload_bytes": 123,
                "payload_summary": {"summary": "No Drift Event"},
                "create_conflict_existing_resource_url": "https://caldav.example.com/uid-nodrift.ics",
            },
            {
                "action": "create",
                "event_id": "evt-drift",
                "ics_uid": "uid-drift",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "resource_name": "uid-drift.ics",
                "payload_bytes": 456,
                "payload_summary": {"summary": "Drift Event"},
                "create_conflict_existing_resource_url": "https://caldav.example.com/uid-drift.ics",
                "drift_report_status": "generated",
                "drift_diff_count": 2,
                "drift_diff_fields": ["SUMMARY", "DTSTART"],
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        [
            "--plan-path",
            str(plan_path),
            "--result-path",
            str(result_path),
            "--action",
            "create",
            "--only",
            "failed",
            "--sort",
            "drift-diff-count",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "generated" in captured.out
    assert "-" in captured.out
    assert captured.out.index("evt-drift") < captured.out.index("evt-nodrift")
    assert captured.err == ""


def test_inspect_can_combine_drift_status_with_existing_filters_and_sort(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    result_path = tmp_path / "caldav_sync_result.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-high-generated",
                "ics_uid": "uid-high-generated",
                "sequence": 0,
                "content_hash": "hash-high-generated",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "High Generated Event",
            },
            {
                "action": "create",
                "event_id": "evt-low-generated",
                "ics_uid": "uid-low-generated",
                "sequence": 0,
                "content_hash": "hash-low-generated",
                "updated_at": "2026-03-12T11:00:00Z",
                "action_reason": "new_event",
                "summary": "Low Generated Event",
            },
            {
                "action": "create",
                "event_id": "evt-remote-failed",
                "ics_uid": "uid-remote-failed",
                "sequence": 0,
                "content_hash": "hash-remote-failed",
                "updated_at": "2026-03-12T12:00:00Z",
                "action_reason": "new_event",
                "summary": "Remote Failed Event",
            },
            {
                "action": "create",
                "event_id": "evt-success-generated",
                "ics_uid": "uid-success-generated",
                "sequence": 0,
                "content_hash": "hash-success-generated",
                "updated_at": "2026-03-12T13:00:00Z",
                "action_reason": "new_event",
                "summary": "Success Generated Event",
            },
        ],
    )
    _write_caldav_result(
        result_path,
        results=[
            {
                "action": "create",
                "event_id": "evt-high-generated",
                "ics_uid": "uid-high-generated",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "payload_summary": {"summary": "High Generated Event"},
                "create_conflict_existing_resource_url": "https://caldav.example.com/high.ics",
                "drift_report_status": "generated",
                "drift_diff_count": 5,
            },
            {
                "action": "create",
                "event_id": "evt-low-generated",
                "ics_uid": "uid-low-generated",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "payload_summary": {"summary": "Low Generated Event"},
                "create_conflict_existing_resource_url": "https://caldav.example.com/low.ics",
                "drift_report_status": "generated",
                "drift_diff_count": 2,
            },
            {
                "action": "create",
                "event_id": "evt-remote-failed",
                "ics_uid": "uid-remote-failed",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "payload_summary": {"summary": "Remote Failed Event"},
                "create_conflict_existing_resource_url": "https://caldav.example.com/remote.ics",
                "drift_report_status": "remote_fetch_failed",
            },
            {
                "action": "create",
                "event_id": "evt-success-generated",
                "ics_uid": "uid-success-generated",
                "success": True,
                "status_code": 201,
                "error_kind": None,
                "payload_summary": {"summary": "Success Generated Event"},
                "drift_report_status": "generated",
                "drift_diff_count": 9,
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        [
            "--plan-path",
            str(plan_path),
            "--result-path",
            str(result_path),
            "--action",
            "create",
            "--only",
            "failed",
            "--drift-status",
            "generated",
            "--sort",
            "drift-diff-count",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "only: failed" in captured.out
    assert "drift_status: generated" in captured.out
    assert "sort: drift-diff-count" in captured.out
    assert "[create] count=2" in captured.out
    assert "evt-high-generated" in captured.out
    assert "evt-low-generated" in captured.out
    assert "evt-remote-failed" not in captured.out
    assert "evt-success-generated" not in captured.out
    assert captured.out.index("evt-high-generated") < captured.out.index("evt-low-generated")
    assert captured.err == ""


@pytest.mark.parametrize(
    ("conflict_kind", "expected_event_id"),
    [
        ("state-drift", "evt-state-drift"),
        ("uid-match", "evt-uid-match"),
        ("resource-exists", "evt-resource-exists"),
    ],
)
def test_inspect_can_filter_by_conflict_kind(
    tmp_path: Path,
    capsys,
    conflict_kind: str,
    expected_event_id: str,
) -> None:
    plan_path = tmp_path / "sync_plan.json"
    result_path = tmp_path / "caldav_sync_result.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-state-drift",
                "ics_uid": "uid-state-drift",
                "sequence": 0,
                "content_hash": "hash-state-drift",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "State Drift Event",
            },
            {
                "action": "create",
                "event_id": "evt-uid-match",
                "ics_uid": "uid-uid-match",
                "sequence": 0,
                "content_hash": "hash-uid-match",
                "updated_at": "2026-03-12T11:00:00Z",
                "action_reason": "new_event",
                "summary": "UID Match Event",
            },
            {
                "action": "create",
                "event_id": "evt-resource-exists",
                "ics_uid": "uid-resource-exists",
                "sequence": 0,
                "content_hash": "hash-resource-exists",
                "updated_at": "2026-03-12T12:00:00Z",
                "action_reason": "new_event",
                "summary": "Resource Exists Event",
            },
        ],
    )
    _write_caldav_result(
        result_path,
        results=[
            {
                "action": "create",
                "event_id": "evt-state-drift",
                "ics_uid": "uid-state-drift",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "resource_name": "uid-state-drift.ics",
                "payload_bytes": 321,
                "payload_summary": {"summary": "State Drift Event"},
                "create_conflict_state_drift_suspected": True,
                "create_conflict_existing_resource_url": "https://caldav.example.com/state-drift.ics",
            },
            {
                "action": "create",
                "event_id": "evt-uid-match",
                "ics_uid": "uid-uid-match",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "resource_name": "uid-uid-match.ics",
                "payload_bytes": 222,
                "payload_summary": {"summary": "UID Match Event"},
                "create_conflict_uid_match_found": True,
                "create_conflict_existing_resource_url": "https://caldav.example.com/uid-match.ics",
            },
            {
                "action": "create",
                "event_id": "evt-resource-exists",
                "ics_uid": "uid-resource-exists",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "resource_name": "uid-resource-exists.ics",
                "payload_bytes": 111,
                "payload_summary": {"summary": "Resource Exists Event"},
                "create_conflict_resource_exists": True,
                "create_conflict_existing_resource_url": "https://caldav.example.com/resource-exists.ics",
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        [
            "--plan-path",
            str(plan_path),
            "--result-path",
            str(result_path),
            "--action",
            "create",
            "--only",
            "failed",
            "--conflict",
            conflict_kind,
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert f"conflict_filters: {conflict_kind}" in captured.out
    assert "[create] count=1" in captured.out
    assert expected_event_id in captured.out
    assert "https://caldav.example.com/" in captured.out
    for other_event_id in {
        "evt-state-drift",
        "evt-uid-match",
        "evt-resource-exists",
    } - {expected_event_id}:
        assert other_event_id not in captured.out
    assert captured.err == ""


def test_inspect_prints_create_conflict_summary(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    result_path = tmp_path / "caldav_sync_result.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-a",
                "ics_uid": "uid-a",
                "sequence": 0,
                "content_hash": "hash-a",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "Event A",
            },
            {
                "action": "create",
                "event_id": "evt-b",
                "ics_uid": "uid-b",
                "sequence": 0,
                "content_hash": "hash-b",
                "updated_at": "2026-03-12T11:00:00Z",
                "action_reason": "new_event",
                "summary": "Event B",
            },
            {
                "action": "create",
                "event_id": "evt-c",
                "ics_uid": "uid-c",
                "sequence": 0,
                "content_hash": "hash-c",
                "updated_at": "2026-03-12T12:00:00Z",
                "action_reason": "new_event",
                "summary": "Event C",
            },
        ],
    )
    _write_caldav_result(
        result_path,
        results=[
            {
                "action": "create",
                "event_id": "evt-a",
                "ics_uid": "uid-a",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "payload_summary": {"summary": "Event A"},
                "create_conflict_uid_match_found": True,
                "create_conflict_state_drift_suspected": True,
                "create_conflict_existing_resource_url": "https://caldav.example.com/a.ics",
            },
            {
                "action": "create",
                "event_id": "evt-b",
                "ics_uid": "uid-b",
                "success": False,
                "status_code": 412,
                "error_kind": "http_failed",
                "payload_summary": {"summary": "Event B"},
                "create_conflict_resource_exists": True,
                "create_conflict_state_drift_suspected": True,
                "create_conflict_existing_resource_url": "https://caldav.example.com/b.ics",
            },
            {
                "action": "create",
                "event_id": "evt-c",
                "ics_uid": "uid-c",
                "success": True,
                "status_code": 201,
                "error_kind": None,
                "payload_summary": {"summary": "Event C"},
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        [
            "--plan-path",
            str(plan_path),
            "--result-path",
            str(result_path),
            "--action",
            "create",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "create_conflict_summary:" in captured.out
    assert "create_total: 3" in captured.out
    assert "create_failed: 2" in captured.out
    assert "create_failed_412: 2" in captured.out
    assert "state_drift_suspected: 2" in captured.out
    assert "uid_match_found: 1" in captured.out
    assert "resource_exists: 1" in captured.out
    assert "existing_resource_url: 2" in captured.out
    assert "state_drift_uid_match_only: 1" in captured.out
    assert "state_drift_resource_exists_only: 1" in captured.out
    assert "state_drift_both: 0" in captured.out
    assert captured.err == ""


def test_inspect_requires_result_path_for_result_filters(tmp_path: Path, capsys) -> None:
    plan_path = tmp_path / "sync_plan.json"
    _write_sync_plan(
        plan_path,
        actions=[
            {
                "action": "create",
                "event_id": "evt-create",
                "ics_uid": "uid-create",
                "sequence": 0,
                "content_hash": "hash-create",
                "updated_at": "2026-03-12T10:00:00Z",
                "action_reason": "new_event",
                "summary": "Create Event",
            },
        ],
    )

    exit_code = sync_plan_inspect_module.main(
        ["--plan-path", str(plan_path), "--drift-status", "generated"]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "Failed to inspect sync plan: --only, --conflict, and --drift-status require --result-path" in captured.err


def _write_sync_plan(path: Path, *, actions: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "generated_at": "2026-03-12T12:00:00+00:00",
                "actions": actions,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_caldav_result(path: Path, *, results: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": "2026-03-12T12:00:00+00:00",
                "dry_run": False,
                "calendar_name": "PoC Calendar",
                "source_url": "https://caldav.example.com",
                "processed_count": len(results),
                "ignored_count": 0,
                "success_count": sum(1 for item in results if item.get("success") is True),
                "failure_count": sum(1 for item in results if item.get("success") is False),
                "results": results,
                "ignored_actions": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
