from __future__ import annotations

import json
from pathlib import Path

import src.caldav_sync_result_summary as caldav_sync_result_summary_module


def test_summary_cli_prints_state_drift_counts(tmp_path: Path, capsys) -> None:
    result_path = tmp_path / "caldav_sync_result.json"
    result_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-03-12T12:00:00+00:00",
                "dry_run": False,
                "calendar_name": "PoC Calendar",
                "source_url": "https://caldav.example.com",
                "processed_count": 4,
                "ignored_count": 0,
                "success_count": 1,
                "failure_count": 3,
                "results": [
                    {
                        "action": "create",
                        "event_id": "evt-a",
                        "ics_uid": "uid-a",
                        "success": False,
                        "status_code": 412,
                        "create_conflict_uid_match_found": True,
                        "create_conflict_state_drift_suspected": True,
                        "create_conflict_existing_resource_url": "https://caldav.example.com/a.ics",
                        "drift_report_status": "generated",
                        "drift_diff_count": 3,
                        "drift_diff_fields": ["SUMMARY", "SEQUENCE", "DTSTART"],
                    },
                    {
                        "action": "create",
                        "event_id": "evt-b",
                        "ics_uid": "uid-b",
                        "success": False,
                        "status_code": 412,
                        "create_conflict_resource_exists": True,
                        "create_conflict_state_drift_suspected": True,
                        "create_conflict_existing_resource_url": "https://caldav.example.com/b.ics",
                        "drift_report_status": "remote_fetch_failed",
                        "drift_diff_count": None,
                        "drift_diff_fields": [],
                    },
                    {
                        "action": "create",
                        "event_id": "evt-e",
                        "ics_uid": "uid-e",
                        "success": False,
                        "status_code": 412,
                        "create_conflict_resource_exists": True,
                        "create_conflict_state_drift_suspected": True,
                        "create_conflict_existing_resource_url": "https://caldav.example.com/e.ics",
                        "drift_report_status": "generated",
                        "drift_diff_count": 5,
                        "drift_diff_fields": [
                            "DESCRIPTION",
                            "LOCATION",
                            "DTEND",
                            "LAST-MODIFIED",
                            "SUMMARY",
                        ],
                    },
                    {
                        "action": "create",
                        "event_id": "evt-c",
                        "ics_uid": "uid-c",
                        "success": True,
                        "status_code": 201,
                    },
                    {
                        "action": "update",
                        "event_id": "evt-d",
                        "ics_uid": "uid-d",
                        "success": False,
                        "status_code": 409,
                    },
                ],
                "ignored_actions": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    exit_code = caldav_sync_result_summary_module.main(["--result-path", str(result_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "CalDAV sync result summary" in captured.out
    assert "create_total: 4" in captured.out
    assert "create_failed: 3" in captured.out
    assert "create_failed_412: 3" in captured.out
    assert "state_drift_suspected: 3" in captured.out
    assert "uid_match_found: 1" in captured.out
    assert "resource_exists: 2" in captured.out
    assert "existing_resource_url: 3" in captured.out
    assert "total create 412 with drift reports: 3" in captured.out
    assert "remote_fetch_failed: 1" in captured.out
    assert "drift_report_status summary" in captured.out
    assert "- generated: 2 (sample_event_ids: evt-a, evt-e)" in captured.out
    assert "- remote_fetch_failed: 1 (sample_event_ids: evt-b)" in captured.out
    assert "drift_diff_count summary" in captured.out
    assert "- 3: 1 (sample_event_ids: evt-a)" in captured.out
    assert "- 5: 1 (sample_event_ids: evt-e)" in captured.out
    assert "- null: 1 (sample_event_ids: evt-b)" in captured.out
    assert "drift_diff_fields combination summary" in captured.out
    assert "- DTSTART, SEQUENCE, SUMMARY: 1 (sample_event_ids: evt-a)" in captured.out
    assert "- DESCRIPTION, DTEND, LAST-MODIFIED, LOCATION, SUMMARY: 1 (sample_event_ids: evt-e)" in captured.out
    assert "- (no diff fields): 1 (sample_event_ids: evt-b)" in captured.out
    assert "individual drift field frequency" in captured.out
    assert "- SUMMARY: 2 (sample_event_ids: evt-a, evt-e)" in captured.out
    assert "- DESCRIPTION: 1 (sample_event_ids: evt-e)" in captured.out
    assert "- LOCATION: 1 (sample_event_ids: evt-e)" in captured.out
    assert "- DTSTART: 1 (sample_event_ids: evt-a)" in captured.out
    assert "- DTEND: 1 (sample_event_ids: evt-e)" in captured.out
    assert "- SEQUENCE: 1 (sample_event_ids: evt-a)" in captured.out
    assert "- LAST-MODIFIED: 1 (sample_event_ids: evt-e)" in captured.out
    assert "sample event_ids" in captured.out
    assert "- SUMMARY: evt-a, evt-e" in captured.out
    assert captured.err == ""
