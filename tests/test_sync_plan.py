from __future__ import annotations

from datetime import datetime, timezone

from src.models import EventDateTime, EventRecord
from src.sync_plan import SyncActionType, build_sync_plan, summarize_sync_plan_actions
from src.sync_state import (
    EventSyncState,
    SyncState,
    TombstoneSyncState,
    build_event_content_hash,
    diff_events,
)


def test_build_sync_plan_maps_new_event_to_create_action() -> None:
    event = _build_event(event_id="evt-new", updated_at="2026-03-11T01:00:00Z")

    diff = diff_events([event], SyncState.empty(), synced_at=_synced_at())
    plan = build_sync_plan(diff, generated_at=_synced_at())

    assert len(plan.actions) == 1
    assert plan.actions[0].action is SyncActionType.CREATE
    assert plan.actions[0].event_id == "evt-new"
    assert plan.actions[0].sequence == 0
    assert plan.actions[0].action_reason == "new_event"
    assert plan.actions[0].summary == "Planning"
    assert plan.actions[0].reappeared_from_tombstone is False
    assert plan.actions[0].tombstone_deleted_at is None


def test_build_sync_plan_maps_reappeared_tombstone_event_to_create_action() -> None:
    event = _build_event(event_id="evt-returned", updated_at="2026-03-11T01:30:00Z")
    previous_state = SyncState(
        tombstones={
            "evt-returned": TombstoneSyncState(
                event_id="evt-returned",
                ics_uid="uid-returned",
                deleted_at="2026-03-10T23:00:00+00:00",
                last_delivery_status="success",
                resource_url="https://caldav.example.com/calendars/poc/deleted.ics",
                etag="\"etag-old\"",
                last_delivery_at="2026-03-10T23:00:00+00:00",
            )
        }
    )

    diff = diff_events([event], previous_state, synced_at=_synced_at())
    plan = build_sync_plan(diff, generated_at=_synced_at())

    assert len(plan.actions) == 1
    assert plan.actions[0].action is SyncActionType.CREATE
    assert plan.actions[0].event_id == "evt-returned"
    assert plan.actions[0].ics_uid == "uid-returned"
    assert plan.actions[0].sequence == 0
    assert plan.actions[0].action_reason == "reappeared_from_tombstone"
    assert plan.actions[0].summary == "Planning"
    assert plan.actions[0].reappeared_from_tombstone is True
    assert plan.actions[0].tombstone_deleted_at == "2026-03-10T23:00:00+00:00"


def test_build_sync_plan_maps_updated_event_to_update_action_when_content_changed() -> None:
    event = _build_event(
        event_id="evt-updated",
        subject="Updated subject",
        updated_at="2026-03-11T02:00:00Z",
    )
    previous_state = SyncState(
        events={
            "evt-updated": EventSyncState(
                event_id="evt-updated",
                ics_uid="uid-updated",
                updated_at="2026-03-10T02:00:00Z",
                content_hash=build_event_content_hash(_build_event(event_id="evt-updated", subject="Old subject")),
                sequence=3,
                is_deleted=False,
                last_synced_at="2026-03-10T12:00:00+00:00",
            )
        }
    )

    diff = diff_events([event], previous_state, synced_at=_synced_at())
    plan = build_sync_plan(diff, generated_at=_synced_at())

    assert plan.actions[0].action is SyncActionType.UPDATE
    assert plan.actions[0].sequence == 4
    assert plan.actions[0].action_reason == "content_and_updated_at_changed"


def test_build_sync_plan_uses_updated_at_changed_reason_when_only_timestamp_changed() -> None:
    event = _build_event(event_id="evt-time-only", updated_at="2026-03-11T03:00:00Z")
    previous_state = SyncState(
        events={
            "evt-time-only": EventSyncState(
                event_id="evt-time-only",
                ics_uid="uid-time-only",
                updated_at="2026-03-10T03:00:00Z",
                content_hash=build_event_content_hash(event),
                sequence=5,
                is_deleted=False,
                last_synced_at="2026-03-10T12:00:00+00:00",
            )
        }
    )

    diff = diff_events([event], previous_state, synced_at=_synced_at())
    plan = build_sync_plan(diff, generated_at=_synced_at())

    assert plan.actions[0].action is SyncActionType.UPDATE
    assert plan.actions[0].sequence == 6
    assert plan.actions[0].action_reason == "updated_at_changed"
    assert plan.actions[0].content_hash == build_event_content_hash(event)


def test_build_sync_plan_maps_unchanged_event_to_skip_action() -> None:
    event = _build_event(event_id="evt-same", updated_at="2026-03-11T04:00:00Z")
    previous_state = SyncState(
        events={
            "evt-same": EventSyncState(
                event_id="evt-same",
                ics_uid="uid-same",
                updated_at="2026-03-11T04:00:00Z",
                content_hash=build_event_content_hash(event),
                sequence=7,
                is_deleted=False,
                last_synced_at="2026-03-10T12:00:00+00:00",
            )
        }
    )

    diff = diff_events([event], previous_state, synced_at=_synced_at())
    plan = build_sync_plan(diff, generated_at=_synced_at())

    assert plan.actions[0].action is SyncActionType.SKIP
    assert plan.actions[0].sequence == 7
    assert plan.actions[0].action_reason == "no_changes"


def test_build_sync_plan_maps_missing_event_to_delete_action() -> None:
    previous_state = SyncState(
        events={
            "evt-missing": EventSyncState(
                event_id="evt-missing",
                ics_uid="uid-missing",
                updated_at="2026-03-10T05:00:00Z",
                content_hash="stale-hash",
                sequence=9,
                is_deleted=False,
                last_synced_at="2026-03-10T12:00:00+00:00",
            )
        }
    )

    diff = diff_events([], previous_state, synced_at=_synced_at())
    plan = build_sync_plan(diff, generated_at=_synced_at())

    assert plan.actions[0].action is SyncActionType.DELETE
    assert plan.actions[0].event_id == "evt-missing"
    assert plan.actions[0].sequence == 9
    assert plan.actions[0].action_reason == "missing_from_current_fetch"
    assert plan.actions[0].summary is None


def test_build_sync_plan_uses_event_menu_when_subject_is_blank() -> None:
    event = _build_event(
        event_id="evt-menu-only",
        subject="   ",
        updated_at="2026-03-11T05:30:00Z",
    )

    diff = diff_events([event], SyncState.empty(), synced_at=_synced_at())
    plan = build_sync_plan(diff, generated_at=_synced_at())

    assert plan.actions[0].summary == "MTG"


def test_build_sync_plan_creates_distinct_actions_for_recurring_occurrences() -> None:
    first = _build_event(
        event_id="evt-series:202603180100",
        repeat_id="202603180100",
        garoon_event_id="evt-series",
        updated_at="2026-03-11T05:00:00Z",
    )
    second = _build_event(
        event_id="evt-series:202603250100",
        repeat_id="202603250100",
        garoon_event_id="evt-series",
        updated_at="2026-03-11T05:00:00Z",
    )

    diff = diff_events([first, second], SyncState.empty(), synced_at=_synced_at())
    plan = build_sync_plan(diff, generated_at=_synced_at())

    assert [action.event_id for action in plan.actions] == [
        "evt-series:202603180100",
        "evt-series:202603250100",
    ]
    assert all(action.action is SyncActionType.CREATE for action in plan.actions)


def test_summarize_sync_plan_actions_counts_each_action_type() -> None:
    event_new = _build_event(event_id="evt-new", updated_at="2026-03-11T01:00:00Z")
    event_updated = _build_event(event_id="evt-updated", updated_at="2026-03-11T02:00:00Z")
    event_same = _build_event(event_id="evt-same", updated_at="2026-03-11T03:00:00Z")
    previous_state = SyncState(
        events={
            "evt-updated": EventSyncState(
                event_id="evt-updated",
                ics_uid="uid-updated",
                updated_at="2026-03-10T02:00:00Z",
                content_hash=build_event_content_hash(
                    _build_event(event_id="evt-updated", subject="Old subject")
                ),
                sequence=2,
                is_deleted=False,
                last_synced_at="2026-03-10T12:00:00+00:00",
            ),
            "evt-same": EventSyncState(
                event_id="evt-same",
                ics_uid="uid-same",
                updated_at="2026-03-11T03:00:00Z",
                content_hash=build_event_content_hash(event_same),
                sequence=4,
                is_deleted=False,
                last_synced_at="2026-03-10T12:00:00+00:00",
            ),
            "evt-deleted": EventSyncState(
                event_id="evt-deleted",
                ics_uid="uid-deleted",
                updated_at="2026-03-10T05:00:00Z",
                content_hash="stale-hash",
                sequence=9,
                is_deleted=False,
                last_synced_at="2026-03-10T12:00:00+00:00",
            ),
        }
    )

    diff = diff_events(
        [event_new, event_updated, event_same],
        previous_state,
        synced_at=_synced_at(),
    )
    plan = build_sync_plan(diff, generated_at=_synced_at())
    summary = summarize_sync_plan_actions(plan)

    assert summary.create_count == 1
    assert summary.update_count == 1
    assert summary.skip_count == 1
    assert summary.delete_count == 1
    assert summary.total_count == 4


def _build_event(
    event_id: str = "evt-1",
    subject: str = "Planning",
    updated_at: str | None = "2026-03-11T00:00:00Z",
    repeat_id: str | None = None,
    garoon_event_id: str | None = None,
) -> EventRecord:
    return EventRecord(
        event_id=event_id,
        subject=subject,
        start=EventDateTime(date_time="2026-03-11T10:00:00+09:00", time_zone="Asia/Tokyo"),
        end=EventDateTime(date_time="2026-03-11T11:00:00+09:00", time_zone="Asia/Tokyo"),
        is_all_day=False,
        is_start_only=False,
        event_type="normal",
        event_menu="MTG",
        visibility_type="public",
        notes="Agenda",
        created_at="2026-03-10T00:00:00Z",
        updated_at=updated_at,
        original_start_time_zone="Asia/Tokyo",
        original_end_time_zone="Asia/Tokyo",
        repeat_id=repeat_id,
        repeat_info=None,
        attendees=[],
        facilities=[],
        garoon_event_id=garoon_event_id,
    )


def _synced_at() -> datetime:
    return datetime(2026, 3, 11, 12, 0, 0, tzinfo=timezone.utc)
