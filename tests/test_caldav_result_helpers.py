from __future__ import annotations

import json
from pathlib import Path

from src.caldav_result_helpers import (
    load_caldav_sync_result,
    summarize_create_conflict_drift,
    summarize_state_drift_comparison,
)


def test_summarize_state_drift_comparison_counts_only_diff_fields() -> None:
    diff_count, diff_fields = summarize_state_drift_comparison(
        {
            "UID": {"equal": True},
            "SUMMARY": {"equal": False},
            "DTSTART": {"equal": True},
            "DESCRIPTION": {"equal": False},
            "LAST-MODIFIED": {"equal": None},
        }
    )

    assert diff_count == 2
    assert diff_fields == ["SUMMARY", "DESCRIPTION"]


def test_load_caldav_sync_result_backfills_drift_summary_from_report(tmp_path: Path) -> None:
    report_path = tmp_path / "reports" / "create_state_drift.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "comparison": {
                    "UID": {"equal": True},
                    "SUMMARY": {"equal": False},
                    "SEQUENCE": {"equal": False},
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    result_path = tmp_path / "caldav_sync_result.json"
    result_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-03-12T12:00:00+00:00",
                "dry_run": False,
                "calendar_name": "PoC Calendar",
                "source_url": "https://caldav.example.com",
                "processed_count": 1,
                "ignored_count": 0,
                "success_count": 0,
                "failure_count": 1,
                "results": [
                    {
                        "action": "create",
                        "event_id": "evt-create",
                        "ics_uid": "uid-create",
                        "success": False,
                        "status_code": 412,
                        "create_conflict_state_drift_report_status": "generated",
                        "create_conflict_state_drift_report_path": str(report_path),
                    }
                ],
                "ignored_actions": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    payload = load_caldav_sync_result(result_path)

    assert payload["results"][0]["drift_report_status"] == "generated"
    assert payload["results"][0]["drift_diff_count"] == 2
    assert payload["results"][0]["drift_diff_fields"] == ["SUMMARY", "SEQUENCE"]


def test_summarize_create_conflict_drift_counts_diff_field_combinations_and_remote_fetch_failed() -> None:
    summary = summarize_create_conflict_drift(
        [
            {
                "action": "create",
                "event_id": "evt-a",
                "ics_uid": "uid-a",
                "status_code": 412,
                "create_conflict_existing_resource_url": "https://caldav.example.com/a.ics",
                "drift_report_status": "generated",
                "drift_diff_count": 2,
                "drift_diff_fields": ["SEQUENCE", "SUMMARY"],
            },
            {
                "action": "create",
                "event_id": "evt-b",
                "ics_uid": "uid-b",
                "status_code": 412,
                "create_conflict_existing_resource_url": "https://caldav.example.com/b.ics",
                "drift_report_status": "generated",
                "drift_diff_count": 2,
                "drift_diff_fields": ["SUMMARY", "SEQUENCE"],
            },
            {
                "action": "create",
                "event_id": "evt-c",
                "ics_uid": "uid-c",
                "status_code": 412,
                "create_conflict_existing_resource_url": "https://caldav.example.com/c.ics",
                "drift_report_status": "remote_fetch_failed",
                "drift_diff_count": None,
                "drift_diff_fields": [],
            },
            {
                "action": "create",
                "event_id": "evt-d",
                "ics_uid": "uid-d",
                "status_code": 412,
                "create_conflict_existing_resource_url": "",
                "drift_report_status": "generated",
                "drift_diff_count": 1,
                "drift_diff_fields": ["DESCRIPTION"],
            },
            {
                "action": "update",
                "event_id": "evt-e",
                "ics_uid": "uid-e",
                "status_code": 412,
                "create_conflict_existing_resource_url": "https://caldav.example.com/e.ics",
                "drift_report_status": "generated",
                "drift_diff_count": 3,
                "drift_diff_fields": ["DESCRIPTION", "LOCATION", "SUMMARY"],
            },
        ]
    )

    assert summary.total_with_remote_existing == 3
    assert summary.remote_fetch_failed == 1
    assert summary.status_buckets[0].label == "generated"
    assert summary.status_buckets[0].count == 2
    assert summary.status_buckets[0].sample_event_ids == ("evt-a", "evt-b")
    assert summary.status_buckets[1].label == "remote_fetch_failed"
    assert summary.status_buckets[1].count == 1
    assert summary.diff_count_buckets[0].label == "2"
    assert summary.diff_count_buckets[0].count == 2
    assert summary.diff_count_buckets[1].label == "null"
    assert summary.diff_count_buckets[1].count == 1
    assert summary.diff_field_buckets[0].label == "SEQUENCE, SUMMARY"
    assert summary.diff_field_buckets[0].count == 2
    assert summary.diff_field_buckets[0].sample_event_ids == ("evt-a", "evt-b")
    assert summary.diff_field_buckets[1].label == "(no diff fields)"
    assert summary.diff_field_buckets[1].count == 1
    assert summary.individual_diff_field_buckets[0].label == "SEQUENCE"
    assert summary.individual_diff_field_buckets[0].count == 2
    assert summary.individual_diff_field_buckets[0].sample_event_ids == ("evt-a", "evt-b")
    assert summary.individual_diff_field_buckets[1].label == "SUMMARY"
    assert summary.individual_diff_field_buckets[1].count == 2
    assert summary.individual_diff_field_buckets[1].sample_event_ids == ("evt-a", "evt-b")
