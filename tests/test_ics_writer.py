from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from src.ics_writer import build_calendar, write_calendar
from src.models import EventDateTime, EventRecord, Facility


def test_build_calendar_renders_timed_event_fields_and_stable_uid() -> None:
    event = EventRecord(
        event_id="42",
        subject="Design, Review; A\\B",
        start=EventDateTime(date_time="2026-03-11T10:00:00+09:00", time_zone="Asia/Tokyo"),
        end=EventDateTime(date_time="2026-03-11T11:30:00+09:00", time_zone="Asia/Tokyo"),
        is_all_day=False,
        is_start_only=False,
        event_type="normal",
        event_menu="MTG",
        visibility_type="public",
        notes="Line 1\nLine 2, bring docs; escape \\",
        created_at="2026-03-10T23:00:00Z",
        updated_at="2026-03-10T23:55:00Z",
        original_start_time_zone="Asia/Tokyo",
        original_end_time_zone="Asia/Tokyo",
        repeat_id=None,
        repeat_info=None,
        facilities=[
            Facility(id="1", code="conf-a", name="Conference Room A"),
            Facility(id="2", code="remote", name="Remote Booth"),
        ],
    )
    generated_at = datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc)

    first = build_calendar([event], generated_at=generated_at)
    second = build_calendar([event], generated_at=generated_at)

    first_uid = re.search(r"UID:(.+)\r\n", first)
    second_uid = re.search(r"UID:(.+)\r\n", second)

    assert first.startswith("BEGIN:VCALENDAR\r\n")
    assert first.endswith("END:VCALENDAR\r\n")
    assert first_uid is not None
    assert second_uid is not None
    assert first_uid.group(1) == second_uid.group(1)
    assert "DTSTAMP:20260311T000000Z\r\n" in first
    assert "LAST-MODIFIED:20260310T235500Z\r\n" in first
    assert "SEQUENCE:0\r\n" in first
    assert "DTSTART:20260311T010000Z\r\n" in first
    assert "DTEND:20260311T023000Z\r\n" in first
    assert "SUMMARY:Design\\, Review\\; A\\\\B\r\n" in first
    assert "DESCRIPTION:Line 1\\nLine 2\\, bring docs\\; escape \\\\\r\n" in first
    assert "LOCATION:Conference Room A\\, Remote Booth\r\n" in first


def test_build_calendar_uses_sequence_from_sync_state_when_provided() -> None:
    event = EventRecord(
        event_id="seq-1",
        subject="Sequence Test",
        start=EventDateTime(date_time="2026-03-11T10:00:00+09:00", time_zone="Asia/Tokyo"),
        end=EventDateTime(date_time="2026-03-11T11:30:00+09:00", time_zone="Asia/Tokyo"),
        is_all_day=False,
        is_start_only=False,
        event_type="normal",
        event_menu="MTG",
        visibility_type="public",
        notes=None,
        created_at="2026-03-10T23:00:00Z",
        updated_at="2026-03-10T23:55:00Z",
        original_start_time_zone="Asia/Tokyo",
        original_end_time_zone="Asia/Tokyo",
        repeat_id=None,
        repeat_info=None,
        facilities=[],
    )

    calendar = build_calendar(
        [event],
        generated_at=datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc),
        sequence_by_event_id={"seq-1": 7},
    )

    assert "SEQUENCE:7\r\n" in calendar


def test_build_calendar_renders_all_day_event_as_value_date() -> None:
    event = EventRecord(
        event_id="all-day-1",
        subject="Company Offsite",
        start=EventDateTime(date_time="2026-03-12T00:00:00+09:00", time_zone="Asia/Tokyo"),
        end=EventDateTime(date_time="2026-03-13T23:59:59+09:00", time_zone="Asia/Tokyo"),
        is_all_day=True,
        is_start_only=False,
        event_type="normal",
        event_menu="OUT",
        visibility_type="public",
        notes=None,
        created_at=None,
        updated_at="2026-03-01T08:00:00Z",
        original_start_time_zone="Asia/Tokyo",
        original_end_time_zone="Asia/Tokyo",
        repeat_id=None,
        repeat_info=None,
        facilities=[],
    )

    calendar = build_calendar(
        [event],
        generated_at=datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc),
    )

    assert "DTSTART;VALUE=DATE:20260312\r\n" in calendar
    assert "DTEND;VALUE=DATE:20260314\r\n" in calendar
    assert "LAST-MODIFIED:20260301T080000Z\r\n" in calendar


def test_build_calendar_fills_dtend_for_start_only_event_without_end() -> None:
    event = EventRecord(
        event_id="start-only-1",
        subject="Quick Sync",
        start=EventDateTime(date_time="2026-03-11T10:00:00+09:00", time_zone="Asia/Tokyo"),
        end=None,
        is_all_day=False,
        is_start_only=True,
        event_type="normal",
        event_menu="MTG",
        visibility_type="public",
        notes=None,
        created_at=None,
        updated_at=None,
        original_start_time_zone="Asia/Tokyo",
        original_end_time_zone=None,
        repeat_id=None,
        repeat_info=None,
        facilities=[],
    )

    calendar = build_calendar(
        [event],
        generated_at=datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc),
    )

    assert "DTSTART:20260311T010000Z\r\n" in calendar
    assert "DTEND:20260311T013000Z\r\n" in calendar


def test_build_calendar_does_not_fill_dtend_for_non_start_only_event_without_end() -> None:
    event = EventRecord(
        event_id="timed-no-end",
        subject="No End Yet",
        start=EventDateTime(date_time="2026-03-11T10:00:00+09:00", time_zone="Asia/Tokyo"),
        end=None,
        is_all_day=False,
        is_start_only=False,
        event_type="normal",
        event_menu="MTG",
        visibility_type="public",
        notes=None,
        created_at=None,
        updated_at=None,
        original_start_time_zone="Asia/Tokyo",
        original_end_time_zone=None,
        repeat_id=None,
        repeat_info=None,
        facilities=[],
    )

    calendar = build_calendar(
        [event],
        generated_at=datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc),
    )

    assert "DTSTART:20260311T010000Z\r\n" in calendar
    assert "DTEND:" not in calendar


def test_build_calendar_fills_dtend_for_recurring_start_only_event_without_end() -> None:
    event = EventRecord(
        event_id="series-1:202603180100",
        subject="Recurring Quick Sync",
        start=EventDateTime(date_time="2026-03-18T10:00:00+09:00", time_zone="Asia/Tokyo"),
        end=None,
        is_all_day=False,
        is_start_only=True,
        event_type="normal",
        event_menu="MTG",
        visibility_type="public",
        notes=None,
        created_at=None,
        updated_at=None,
        original_start_time_zone="Asia/Tokyo",
        original_end_time_zone=None,
        repeat_id="202603180100",
        repeat_info={"type": "weekly"},
        facilities=[],
    )

    calendar = build_calendar(
        [event],
        generated_at=datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc),
    )

    assert "DTSTART:20260318T010000Z\r\n" in calendar
    assert "DTEND:20260318T013000Z\r\n" in calendar


def test_build_calendar_summary_prefers_subject_over_event_menu() -> None:
    event = EventRecord(
        event_id="summary-subject",
        subject="定例会議",
        start=EventDateTime(date_time="2026-03-11T09:00:00+09:00", time_zone="Asia/Tokyo"),
        end=EventDateTime(date_time="2026-03-11T10:00:00+09:00", time_zone="Asia/Tokyo"),
        is_all_day=False,
        is_start_only=False,
        event_type="normal",
        event_menu="在宅",
        visibility_type="public",
        notes=None,
        created_at=None,
        updated_at=None,
        original_start_time_zone="Asia/Tokyo",
        original_end_time_zone="Asia/Tokyo",
        repeat_id=None,
        repeat_info=None,
        facilities=[],
    )

    calendar = build_calendar(
        [event],
        generated_at=datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc),
    )

    assert "SUMMARY:定例会議\r\n" in calendar


def test_build_calendar_summary_uses_event_menu_when_subject_is_blank() -> None:
    event = EventRecord(
        event_id="summary-event-menu",
        subject="   ",
        start=EventDateTime(date_time="2026-03-13T00:00:00+09:00", time_zone="Asia/Tokyo"),
        end=EventDateTime(date_time="2026-03-13T23:59:59+09:00", time_zone="Asia/Tokyo"),
        is_all_day=True,
        is_start_only=False,
        event_type="normal",
        event_menu="在宅",
        visibility_type="public",
        notes=None,
        created_at=None,
        updated_at=None,
        original_start_time_zone="Asia/Tokyo",
        original_end_time_zone="Asia/Tokyo",
        repeat_id=None,
        repeat_info=None,
        facilities=[],
    )

    calendar = build_calendar(
        [event],
        generated_at=datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc),
    )

    assert "SUMMARY:在宅\r\n" in calendar


def test_build_calendar_summary_falls_back_to_no_title_when_subject_and_event_menu_are_blank() -> None:
    event = EventRecord(
        event_id="summary-no-title",
        subject="",
        start=EventDateTime(date_time="2026-03-14T09:00:00+09:00", time_zone="Asia/Tokyo"),
        end=EventDateTime(date_time="2026-03-14T10:00:00+09:00", time_zone="Asia/Tokyo"),
        is_all_day=False,
        is_start_only=False,
        event_type="normal",
        event_menu="   ",
        visibility_type="public",
        notes=None,
        created_at=None,
        updated_at=None,
        original_start_time_zone="Asia/Tokyo",
        original_end_time_zone="Asia/Tokyo",
        repeat_id=None,
        repeat_info=None,
        facilities=[],
    )

    calendar = build_calendar(
        [event],
        generated_at=datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc),
    )

    assert "SUMMARY:(no title)\r\n" in calendar


def test_write_calendar_writes_utf8_ics_file(tmp_path: Path) -> None:
    output_path = tmp_path / "calendar.ics"
    event = EventRecord(
        event_id="jp-1",
        subject="定例会議",
        start=EventDateTime(date_time="2026-03-11T09:00:00+09:00", time_zone="Asia/Tokyo"),
        end=EventDateTime(date_time="2026-03-11T10:00:00+09:00", time_zone="Asia/Tokyo"),
        is_all_day=False,
        is_start_only=False,
        event_type="normal",
        event_menu="MTG",
        visibility_type="private",
        notes="議題を確認する",
        created_at=None,
        updated_at=None,
        original_start_time_zone="Asia/Tokyo",
        original_end_time_zone="Asia/Tokyo",
        repeat_id=None,
        repeat_info=None,
        facilities=[Facility(id="3", code="room-b", name="会議室B")],
    )

    write_calendar(
        output_path,
        [event],
        generated_at=datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc),
    )

    raw = output_path.read_bytes()
    content = raw.decode("utf-8")

    assert raw.startswith(b"BEGIN:VCALENDAR\r\n")
    assert "SUMMARY:定例会議\r\n" in content
    assert "DESCRIPTION:議題を確認する\r\n" in content
    assert "LOCATION:会議室B\r\n" in content
    assert "SEQUENCE:0\r\n" in content
    assert "LAST-MODIFIED:" not in content
