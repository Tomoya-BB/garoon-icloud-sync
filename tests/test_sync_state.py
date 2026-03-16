from __future__ import annotations

from dataclasses import dataclass
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import src.sync_state as sync_state_module
from src.models import Attendee, DateRange, EventDateTime, EventRecord, Facility
from src.sync_state import (
    EventSyncState,
    SyncState,
    SyncStateJsonDecodeError,
    SyncStatus,
    TombstoneSyncState,
    build_event_content_hash,
    build_next_sync_state,
    build_next_sync_state_from_delivery,
    diff_events,
    load_sync_state,
    save_sync_state,
    validate_sync_state,
)


def test_load_sync_state_creates_new_file_when_missing(tmp_path: Path) -> None:
    state_path = tmp_path / "data" / "sync_state.json"

    state = load_sync_state(state_path)

    assert state == SyncState.empty()
    assert state_path.exists()
    assert json.loads(state_path.read_text(encoding="utf-8")) == {
        "version": 3,
        "events": {},
        "tombstones": {},
    }


def test_save_and_load_sync_state_round_trip(tmp_path: Path) -> None:
    state_path = tmp_path / "sync_state.json"
    state = SyncState(
        events={
            "evt-1": EventSyncState(
                event_id="evt-1",
                ics_uid="uid-1",
                updated_at="2026-03-11T01:00:00Z",
                content_hash="abc123",
                sequence=2,
                is_deleted=False,
                last_synced_at="2026-03-11T02:00:00+00:00",
            )
        }
    )

    save_sync_state(state_path, state)
    loaded = load_sync_state(state_path)

    assert loaded == state


def test_load_sync_state_creates_profiled_file_when_requested(tmp_path: Path) -> None:
    state_path = tmp_path / "data" / "sync_state.json"

    state = load_sync_state(state_path, expected_profile="tomoya")

    assert state == SyncState.empty(profile="tomoya")
    assert json.loads(state_path.read_text(encoding="utf-8")) == {
        "version": 3,
        "profile": "tomoya",
        "events": {},
        "tombstones": {},
    }


def test_load_sync_state_rejects_profile_mismatch(tmp_path: Path) -> None:
    state_path = tmp_path / "sync_state.json"
    state_path.write_text(
        json.dumps(
            {
                "version": 3,
                "profile": "alice",
                "events": {},
                "tombstones": {},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"sync state profile mismatch",
    ):
        load_sync_state(state_path, expected_profile="bob")


def test_save_sync_state_rejects_event_id_present_in_events_and_tombstones(tmp_path: Path) -> None:
    state_path = tmp_path / "sync_state.json"
    state = SyncState(
        events={
            "evt-1": EventSyncState(
                event_id="evt-1",
                ics_uid="uid-active",
                updated_at="2026-03-11T01:00:00Z",
                content_hash="hash-active",
                sequence=1,
                is_deleted=False,
                last_synced_at="2026-03-11T02:00:00+00:00",
            )
        },
        tombstones={
            "evt-1": TombstoneSyncState(
                event_id="evt-1",
                ics_uid="uid-tombstone",
                deleted_at="2026-03-11T03:00:00+00:00",
                last_delivery_status="success",
            )
        },
    )

    with pytest.raises(
        ValueError,
        match=r"(?s)Refusing to save invalid sync state to .*event_id 'evt-1' exists in both events and tombstones",
    ):
        save_sync_state(state_path, state)

    assert not state_path.exists()


def test_save_sync_state_rejects_duplicated_ics_uid(tmp_path: Path) -> None:
    state_path = tmp_path / "sync_state.json"
    state = SyncState(
        events={
            "evt-1": EventSyncState(
                event_id="evt-1",
                ics_uid="uid-shared",
                updated_at="2026-03-11T01:00:00Z",
                content_hash="hash-1",
                sequence=1,
                is_deleted=False,
                last_synced_at="2026-03-11T02:00:00+00:00",
            ),
            "evt-2": EventSyncState(
                event_id="evt-2",
                ics_uid="uid-shared",
                updated_at="2026-03-11T01:30:00Z",
                content_hash="hash-2",
                sequence=2,
                is_deleted=False,
                last_synced_at="2026-03-11T02:30:00+00:00",
            ),
        }
    )

    with pytest.raises(
        ValueError,
        match=r"(?s)Refusing to save invalid sync state to .*ics_uid 'uid-shared' is duplicated across events\['evt-1'\], events\['evt-2'\]",
    ):
        save_sync_state(state_path, state)

    assert not state_path.exists()


def test_save_sync_state_rejects_missing_required_field(tmp_path: Path) -> None:
    state_path = tmp_path / "sync_state.json"
    state = SyncState(
        events={
            "evt-1": BrokenSerializedEventSyncState(
                {
                    "event_id": "evt-1",
                    "ics_uid": "uid-1",
                    "updated_at": "2026-03-11T01:00:00Z",
                    "sequence": 1,
                    "is_deleted": False,
                    "last_synced_at": "2026-03-11T02:00:00+00:00",
                }
            )
        }
    )

    with pytest.raises(
        ValueError,
        match=r"(?s)Refusing to save invalid sync state to .*events\['evt-1'\] is missing required field 'content_hash'",
    ):
        save_sync_state(state_path, state)

    assert not state_path.exists()


def test_load_sync_state_rejects_event_id_present_in_events_and_tombstones(tmp_path: Path) -> None:
    state_path = tmp_path / "sync_state.json"
    state_path.write_text(
        json.dumps(
            {
                "version": 3,
                "events": {
                    "evt-1": {
                        "event_id": "evt-1",
                        "ics_uid": "uid-active",
                        "updated_at": "2026-03-11T01:00:00Z",
                        "content_hash": "hash-active",
                        "sequence": 1,
                        "is_deleted": False,
                        "last_synced_at": "2026-03-11T02:00:00+00:00",
                    }
                },
                "tombstones": {
                    "evt-1": {
                        "event_id": "evt-1",
                        "ics_uid": "uid-tombstone",
                        "deleted_at": "2026-03-11T03:00:00+00:00",
                        "last_delivery_status": "success",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"event_id 'evt-1' exists in both events and tombstones"):
        load_sync_state(state_path)


def test_load_sync_state_rejects_duplicated_ics_uid_across_active_and_tombstone(tmp_path: Path) -> None:
    state_path = tmp_path / "sync_state.json"
    state_path.write_text(
        json.dumps(
            {
                "version": 3,
                "events": {
                    "evt-1": {
                        "event_id": "evt-1",
                        "ics_uid": "uid-shared",
                        "updated_at": "2026-03-11T01:00:00Z",
                        "content_hash": "hash-active",
                        "sequence": 1,
                        "is_deleted": False,
                        "last_synced_at": "2026-03-11T02:00:00+00:00",
                    }
                },
                "tombstones": {
                    "evt-2": {
                        "event_id": "evt-2",
                        "ics_uid": "uid-shared",
                        "deleted_at": "2026-03-11T03:00:00+00:00",
                        "last_delivery_status": "success",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"ics_uid 'uid-shared' is duplicated across events\['evt-1'\], tombstones\['evt-2'\]",
    ):
        load_sync_state(state_path)


def test_load_sync_state_rejects_invalid_version(tmp_path: Path) -> None:
    state_path = tmp_path / "sync_state.json"
    state_path.write_text(
        json.dumps(
            {
                "version": 99,
                "events": {},
                "tombstones": {},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"unsupported sync state version 99; expected 3"):
        load_sync_state(state_path)


def test_load_sync_state_rejects_missing_required_field(tmp_path: Path) -> None:
    state_path = tmp_path / "sync_state.json"
    state_path.write_text(
        json.dumps(
            {
                "version": 3,
                "events": {
                    "evt-1": {
                        "event_id": "evt-1",
                        "ics_uid": "uid-1",
                        "updated_at": "2026-03-11T01:00:00Z",
                        "sequence": 1,
                        "is_deleted": False,
                        "last_synced_at": "2026-03-11T02:00:00+00:00",
                    }
                },
                "tombstones": {},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"events\['evt-1'\] is missing required field 'content_hash'"):
        load_sync_state(state_path)


def test_load_sync_state_wraps_json_decode_error_with_path_and_position(tmp_path: Path) -> None:
    state_path = tmp_path / "sync_state.json"
    state_path.write_text('{"version": 3\n"events": {}}', encoding="utf-8")

    with pytest.raises(
        SyncStateJsonDecodeError,
        match=r"Invalid sync state JSON while loading .*sync_state\.json:\n- invalid JSON at line 2, column 1 \(char 14\): Expecting ',' delimiter",
    ):
        load_sync_state(state_path)


def test_build_event_content_hash_is_stable_for_equivalent_payloads() -> None:
    first = _build_event(
        repeat_info={"until": "2026-04-01", "type": "weekly"},
        attendees=[
            Attendee(id="2", code="beta", name="Bob", type="USER", attendance_status="ACCEPT"),
            Attendee(id="1", code="alpha", name="Alice", type="USER", attendance_status="WAIT"),
        ],
        facilities=[
            Facility(id="2", code="room-b", name="Room B"),
            Facility(id="1", code="room-a", name="Room A"),
        ],
    )
    second = _build_event(
        repeat_info={"type": "weekly", "until": "2026-04-01"},
        attendees=[
            Attendee(id="1", code="alpha", name="Alice", type="USER", attendance_status="WAIT"),
            Attendee(id="2", code="beta", name="Bob", type="USER", attendance_status="ACCEPT"),
        ],
        facilities=[
            Facility(id="1", code="room-a", name="Room A"),
            Facility(id="2", code="room-b", name="Room B"),
        ],
    )

    assert build_event_content_hash(first) == build_event_content_hash(second)


def test_diff_events_classifies_new_updated_unchanged_and_deleted_candidates() -> None:
    synced_at = datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc)
    fetch_window = _build_fetch_window(
        start="2026-03-11T00:00:00+00:00",
        end="2026-03-11T23:59:59+00:00",
    )
    new_event = _build_event(event_id="evt-new", updated_at="2026-03-11T01:00:00Z")
    unchanged_event = _build_event(event_id="evt-same", updated_at="2026-03-11T02:00:00Z")
    updated_event = _build_event(event_id="evt-updated", subject="Updated", updated_at="2026-03-11T03:00:00Z")

    previous_state = SyncState(
        events={
            "evt-same": EventSyncState(
                event_id="evt-same",
                ics_uid="uid-same",
                updated_at="2026-03-11T02:00:00Z",
                content_hash=build_event_content_hash(unchanged_event),
                sequence=4,
                is_deleted=False,
                last_synced_at="2026-03-10T12:00:00+00:00",
            ),
            "evt-updated": EventSyncState(
                event_id="evt-updated",
                ics_uid="uid-updated",
                updated_at="2026-03-10T03:00:00Z",
                content_hash=build_event_content_hash(_build_event(event_id="evt-updated", subject="Old")),
                sequence=5,
                is_deleted=False,
                last_synced_at="2026-03-10T12:00:00+00:00",
            ),
            "evt-missing": EventSyncState(
                event_id="evt-missing",
                ics_uid="uid-missing",
                updated_at="2026-03-10T04:00:00Z",
                content_hash="stale",
                sequence=1,
                is_deleted=False,
                last_synced_at="2026-03-10T12:00:00+00:00",
                last_seen_window_start="2026-03-11T00:00:00+00:00",
                last_seen_window_end="2026-03-11T23:59:59+00:00",
            ),
        }
    )

    diff = diff_events(
        [new_event, unchanged_event, updated_event],
        previous_state,
        synced_at=synced_at,
        fetch_window=fetch_window,
    )

    assert [item.event.event_id for item in diff.new_events] == ["evt-new"]
    assert [item.event.event_id for item in diff.updated_events] == ["evt-updated"]
    assert [item.event.event_id for item in diff.unchanged_events] == ["evt-same"]
    assert [item.event_id for item in diff.deleted_candidates] == ["evt-missing"]
    assert diff.new_events[0].status is SyncStatus.NEW
    assert diff.updated_events[0].status is SyncStatus.UPDATED
    assert diff.unchanged_events[0].status is SyncStatus.UNCHANGED


def test_diff_events_reuses_tombstone_uid_for_reappeared_event() -> None:
    event = _build_event(event_id="evt-returned", updated_at="2026-03-11T05:30:00Z")
    previous_state = SyncState(
        tombstones={
            "evt-returned": TombstoneSyncState(
                event_id="evt-returned",
                ics_uid="uid-returned",
                deleted_at="2026-03-10T05:00:00+00:00",
                last_delivery_status="success",
                resource_url="https://caldav.example.com/calendars/poc/old-returned.ics",
                etag="\"etag-old\"",
                last_delivery_at="2026-03-10T05:00:00+00:00",
            )
        }
    )

    diff = diff_events(
        [event],
        previous_state,
        synced_at=datetime(2026, 3, 11, 5, 30, 0, tzinfo=timezone.utc),
        fetch_window=_build_fetch_window(
            start="2026-03-11T00:00:00+00:00",
            end="2026-03-11T23:59:59+00:00",
        ),
    )

    assert [item.event.event_id for item in diff.new_events] == ["evt-returned"]
    assert diff.new_events[0].reappeared_from_tombstone is True
    assert diff.new_events[0].previous_tombstone == previous_state.tombstones["evt-returned"]
    assert diff.new_events[0].next_state.ics_uid == "uid-returned"
    assert diff.new_events[0].next_state.sequence == 0
    assert diff.new_events[0].next_state.resource_url is None
    assert diff.new_events[0].next_state.etag is None


def test_diff_events_skips_delete_for_legacy_recurring_state_key() -> None:
    recurring_event = _build_event(
        event_id="evt-series:202603180100",
        updated_at="2026-03-11T06:00:00Z",
        repeat_id="202603180100",
        garoon_event_id="evt-series",
    )
    previous_state = SyncState(
        events={
            "evt-series": EventSyncState(
                event_id="evt-series",
                ics_uid="uid-legacy-series",
                updated_at="2026-03-10T06:00:00Z",
                content_hash="legacy-hash",
                sequence=3,
                is_deleted=False,
                last_synced_at="2026-03-10T06:00:00+00:00",
            )
        }
    )

    diff = diff_events(
        [recurring_event],
        previous_state,
        synced_at=datetime(2026, 3, 11, 6, 0, 0, tzinfo=timezone.utc),
        fetch_window=_build_fetch_window(
            start="2026-03-11T00:00:00+00:00",
            end="2026-03-11T23:59:59+00:00",
        ),
    )

    assert [item.event.event_id for item in diff.new_events] == ["evt-series:202603180100"]
    assert diff.deleted_candidates == []


def test_diff_events_does_not_delete_event_when_current_window_is_narrower_than_state_window() -> None:
    broad_window = _build_fetch_window(
        start="2025-03-15T00:00:00+00:00",
        end="2026-09-14T23:59:59+00:00",
    )
    normal_window = _build_fetch_window(
        start="2026-03-15T00:00:00+00:00",
        end="2026-04-15T23:59:59+00:00",
    )
    previous_state = build_next_sync_state(
        [_build_event(event_id="evt-in-range"), _build_event(event_id="evt-out-of-range")],
        SyncState.empty(),
        synced_at=datetime(2026, 3, 15, 0, 0, 0, tzinfo=timezone.utc),
        fetch_window=broad_window,
    )

    diff = diff_events(
        [_build_event(event_id="evt-in-range")],
        previous_state,
        synced_at=datetime(2026, 3, 16, 0, 0, 0, tzinfo=timezone.utc),
        fetch_window=normal_window,
    )

    assert [item.event.event_id for item in diff.unchanged_events] == ["evt-in-range"]
    assert diff.deleted_candidates == []


def test_diff_events_does_not_delete_legacy_state_without_fetch_window_metadata() -> None:
    diff = diff_events(
        [],
        SyncState(events={"evt-legacy": _build_event_state("evt-legacy")}),
        synced_at=datetime(2026, 3, 16, 0, 0, 0, tzinfo=timezone.utc),
        fetch_window=_build_fetch_window(
            start="2026-03-15T00:00:00+00:00",
            end="2026-04-15T23:59:59+00:00",
        ),
    )

    assert diff.deleted_candidates == []


def test_build_next_sync_state_preserves_sequence_and_updates_timestamp() -> None:
    synced_at = datetime(2026, 3, 11, 5, 0, 0, tzinfo=timezone.utc)
    fetch_window = _build_fetch_window(
        start="2026-03-11T00:00:00+00:00",
        end="2026-03-11T23:59:59+00:00",
    )
    event = _build_event(event_id="evt-1", updated_at="2026-03-11T05:00:00Z")
    previous_state = SyncState(
        events={
            "evt-1": EventSyncState(
                event_id="evt-1",
                ics_uid="uid-1",
                updated_at="2026-03-10T05:00:00Z",
                content_hash="old",
                sequence=7,
                is_deleted=False,
                last_synced_at="2026-03-10T05:00:00+00:00",
            ),
            "evt-missing": EventSyncState(
                event_id="evt-missing",
                ics_uid="uid-missing",
                updated_at="2026-03-10T04:00:00Z",
                content_hash="stale",
                sequence=1,
                is_deleted=False,
                last_synced_at="2026-03-10T04:00:00+00:00",
            )
        }
    )

    next_state = build_next_sync_state(
        [event],
        previous_state,
        synced_at=synced_at,
        fetch_window=fetch_window,
    )

    _assert_sync_state_invariants(next_state)
    assert next_state.events["evt-1"].sequence == 7
    assert next_state.events["evt-1"].updated_at == "2026-03-11T05:00:00Z"
    assert next_state.events["evt-1"].last_synced_at == "2026-03-11T05:00:00+00:00"
    assert next_state.events["evt-1"].last_seen_window_start == "2026-03-11T00:00:00+00:00"
    assert next_state.events["evt-1"].last_seen_window_end == "2026-03-11T23:59:59+00:00"
    assert next_state.events["evt-missing"] == previous_state.events["evt-missing"]


def test_build_next_sync_state_raises_when_built_state_breaks_uid_exclusivity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    synced_at = datetime(2026, 3, 11, 5, 0, 0, tzinfo=timezone.utc)
    event = _build_event(event_id="evt-new", updated_at="2026-03-11T05:00:00Z")
    previous_state = SyncState(
        events={
            "evt-existing": _build_event_state(
                event_id="evt-existing",
                updated_at="2026-03-10T05:00:00Z",
                sequence=7,
            )
        }
    )

    def build_invalid_event_state(*args: object, **kwargs: object) -> EventSyncState:
        return EventSyncState(
            event_id="evt-new",
            ics_uid=previous_state.events["evt-existing"].ics_uid,
            updated_at="2026-03-11T05:00:00Z",
            content_hash=build_event_content_hash(event),
            sequence=0,
            is_deleted=False,
            last_synced_at="2026-03-11T05:00:00+00:00",
        )

    monkeypatch.setattr(sync_state_module, "build_event_sync_state", build_invalid_event_state)

    with pytest.raises(
        ValueError,
        match=(
            r"(?s)Refusing to return invalid sync state from build_next_sync_state:.*"
            r"ics_uid 'uid-existing' is duplicated across "
            r"events\['evt-existing'\], events\['evt-new'\]"
        ),
    ):
        build_next_sync_state([event], previous_state, synced_at=synced_at)


def test_build_next_sync_state_from_delivery_updates_only_successful_create() -> None:
    synced_at = datetime(2026, 3, 11, 6, 0, 0, tzinfo=timezone.utc)
    event = _build_event(event_id="evt-create", updated_at="2026-03-11T06:00:00Z")

    next_state = build_next_sync_state_from_delivery(
        [event],
        SyncState.empty(),
        [
            StubDeliveryResult(
                action="create",
                event_id="evt-create",
                sequence=0,
                payload_sequence=0,
                success=True,
                sent=True,
                delivered_at="2026-03-11T06:00:00+00:00",
            )
        ],
        synced_at=synced_at,
        fetch_window=_build_fetch_window(
            start="2026-03-11T00:00:00+00:00",
            end="2026-03-11T23:59:59+00:00",
        ),
    )

    assert next_state.events["evt-create"].updated_at == "2026-03-11T06:00:00Z"
    assert next_state.events["evt-create"].sequence == 0
    assert next_state.events["evt-create"].last_seen_window_start == "2026-03-11T00:00:00+00:00"
    assert next_state.events["evt-create"].last_seen_window_end == "2026-03-11T23:59:59+00:00"
    assert next_state.events["evt-create"].last_delivery_status == "success"
    assert next_state.events["evt-create"].last_delivery_at == "2026-03-11T06:00:00+00:00"


def test_build_next_sync_state_from_delivery_refreshes_fetch_window_for_skipped_event() -> None:
    previous_state = SyncState(
        events={
            "evt-skip": _build_event_state(
                "evt-skip",
                last_seen_window_start="2025-03-15T00:00:00+00:00",
                last_seen_window_end="2026-09-14T23:59:59+00:00",
            )
        }
    )

    next_state = build_next_sync_state_from_delivery(
        [_build_event(event_id="evt-skip")],
        previous_state,
        [],
        synced_at=datetime(2026, 3, 15, 0, 0, 0, tzinfo=timezone.utc),
        fetch_window=_build_fetch_window(
            start="2026-03-15T00:00:00+00:00",
            end="2026-04-15T23:59:59+00:00",
        ),
    )

    assert next_state.events["evt-skip"].last_synced_at == previous_state.events["evt-skip"].last_synced_at
    assert next_state.events["evt-skip"].last_seen_window_start == "2026-03-15T00:00:00+00:00"
    assert next_state.events["evt-skip"].last_seen_window_end == "2026-04-15T23:59:59+00:00"


def test_build_next_sync_state_from_delivery_moves_reappeared_tombstone_to_active_event() -> None:
    event = _build_event(event_id="evt-reappeared", updated_at="2026-03-11T06:30:00Z")
    previous_state = SyncState(
        tombstones={
            "evt-reappeared": TombstoneSyncState(
                event_id="evt-reappeared",
                ics_uid="uid-reappeared",
                deleted_at="2026-03-10T06:00:00+00:00",
                last_delivery_status="success",
                resource_url="https://caldav.example.com/calendars/poc/deleted.ics",
                etag="\"etag-deleted\"",
                last_delivery_at="2026-03-10T06:00:00+00:00",
            )
        }
    )

    next_state = build_next_sync_state_from_delivery(
        [event],
        previous_state,
        [
            StubDeliveryResult(
                action="create",
                event_id="evt-reappeared",
                ics_uid="uid-reappeared",
                sequence=0,
                payload_sequence=0,
                success=True,
                sent=True,
                resource_url="https://caldav.example.com/calendars/poc/new.ics",
                etag="\"etag-new\"",
                delivered_at="2026-03-11T06:30:00+00:00",
            )
        ],
        synced_at=datetime(2026, 3, 11, 6, 30, 0, tzinfo=timezone.utc),
    )

    assert "evt-reappeared" not in next_state.tombstones
    assert next_state.events["evt-reappeared"].ics_uid == "uid-reappeared"
    assert next_state.events["evt-reappeared"].resource_url == "https://caldav.example.com/calendars/poc/new.ics"
    assert next_state.events["evt-reappeared"].etag == "\"etag-new\""
    assert next_state.events["evt-reappeared"].sequence == 0


def test_build_next_sync_state_from_delivery_updates_only_successful_update() -> None:
    synced_at = datetime(2026, 3, 11, 7, 0, 0, tzinfo=timezone.utc)
    updated_event = _build_event(event_id="evt-update", subject="Updated", updated_at="2026-03-11T07:00:00Z")
    previous_state = SyncState(
        events={
            "evt-update": EventSyncState(
                event_id="evt-update",
                ics_uid="uid-update",
                updated_at="2026-03-10T07:00:00Z",
                content_hash=build_event_content_hash(_build_event(event_id="evt-update", subject="Before")),
                sequence=4,
                is_deleted=False,
                last_synced_at="2026-03-10T07:00:00+00:00",
            )
        }
    )

    next_state = build_next_sync_state_from_delivery(
        [updated_event],
        previous_state,
        [
            StubDeliveryResult(
                action="update",
                event_id="evt-update",
                sequence=4,
                payload_sequence=5,
                success=True,
                sent=True,
                delivered_at="2026-03-11T07:00:00+00:00",
            )
        ],
        synced_at=synced_at,
    )

    assert next_state.events["evt-update"].ics_uid == "uid-update"
    assert next_state.events["evt-update"].updated_at == "2026-03-11T07:00:00Z"
    assert next_state.events["evt-update"].content_hash == build_event_content_hash(updated_event)
    assert next_state.events["evt-update"].sequence == 5
    assert next_state.events["evt-update"].last_synced_at == "2026-03-11T07:00:00+00:00"


def test_build_next_sync_state_from_delivery_does_not_update_failed_actions() -> None:
    event = _build_event(event_id="evt-failed", subject="Changed", updated_at="2026-03-11T08:00:00Z")
    previous_state = SyncState(
        events={
            "evt-failed": EventSyncState(
                event_id="evt-failed",
                ics_uid="uid-failed",
                updated_at="2026-03-10T08:00:00Z",
                content_hash=build_event_content_hash(_build_event(event_id="evt-failed", subject="Before")),
                sequence=6,
                is_deleted=False,
                last_synced_at="2026-03-10T08:00:00+00:00",
                resource_url="https://caldav.example.com/calendars/poc/evt-failed.ics",
                etag="\"old-etag\"",
                last_delivery_status="success",
                last_delivery_at="2026-03-10T08:00:00+00:00",
            )
        }
    )

    next_state = build_next_sync_state_from_delivery(
        [event],
        previous_state,
        [
            StubDeliveryResult(
                action="update",
                event_id="evt-failed",
                sequence=6,
                payload_sequence=7,
                success=False,
                sent=False,
            )
        ],
        synced_at=datetime(2026, 3, 11, 8, 0, 0, tzinfo=timezone.utc),
    )

    assert next_state == previous_state


def test_build_next_sync_state_from_delivery_keeps_tombstone_when_reappeared_create_fails() -> None:
    event = _build_event(event_id="evt-reappeared-fail", updated_at="2026-03-11T08:15:00Z")
    previous_state = SyncState(
        tombstones={
            "evt-reappeared-fail": TombstoneSyncState(
                event_id="evt-reappeared-fail",
                ics_uid="uid-reappeared-fail",
                deleted_at="2026-03-10T08:00:00+00:00",
                last_delivery_status="success",
                resource_url="https://caldav.example.com/calendars/poc/deleted-fail.ics",
                etag="\"etag-delete\"",
                last_delivery_at="2026-03-10T08:00:00+00:00",
            )
        }
    )

    next_state = build_next_sync_state_from_delivery(
        [event],
        previous_state,
        [
            StubDeliveryResult(
                action="create",
                event_id="evt-reappeared-fail",
                ics_uid="uid-reappeared-fail",
                sequence=0,
                payload_sequence=0,
                success=False,
                sent=False,
            )
        ],
        synced_at=datetime(2026, 3, 11, 8, 15, 0, tzinfo=timezone.utc),
    )

    assert next_state == previous_state


def test_build_next_sync_state_from_delivery_saves_tombstone_on_successful_delete() -> None:
    previous_state = SyncState(
        events={
            "evt-delete": EventSyncState(
                event_id="evt-delete",
                ics_uid="uid-delete",
                updated_at="2026-03-10T10:00:00Z",
                content_hash="hash-delete",
                sequence=4,
                is_deleted=False,
                last_synced_at="2026-03-10T10:00:00+00:00",
                resource_url="https://caldav.example.com/calendars/poc/evt-delete.ics",
                etag="\"etag-delete\"",
                last_delivery_status="success",
                last_delivery_at="2026-03-10T10:00:00+00:00",
            )
        }
    )

    next_state = build_next_sync_state_from_delivery(
        [],
        previous_state,
        [
            StubDeliveryResult(
                action="delete",
                event_id="evt-delete",
                ics_uid="uid-delete",
                sequence=4,
                success=True,
                sent=True,
                resource_url="https://caldav.example.com/calendars/poc/evt-delete.ics",
                etag="\"etag-delete\"",
                delivered_at="2026-03-11T10:00:00+00:00",
            )
        ],
        synced_at=datetime(2026, 3, 11, 10, 0, 0, tzinfo=timezone.utc),
    )

    assert "evt-delete" not in next_state.events
    assert next_state.tombstones["evt-delete"] == TombstoneSyncState(
        event_id="evt-delete",
        ics_uid="uid-delete",
        deleted_at="2026-03-11T10:00:00+00:00",
        last_delivery_status="success",
        resource_url="https://caldav.example.com/calendars/poc/evt-delete.ics",
        etag="\"etag-delete\"",
        last_delivery_at="2026-03-11T10:00:00+00:00",
    )


def test_build_next_sync_state_from_delivery_does_not_save_tombstone_when_delete_fails() -> None:
    previous_state = SyncState(
        events={
            "evt-delete-fail": EventSyncState(
                event_id="evt-delete-fail",
                ics_uid="uid-delete-fail",
                updated_at="2026-03-10T11:00:00Z",
                content_hash="hash-delete-fail",
                sequence=5,
                is_deleted=False,
                last_synced_at="2026-03-10T11:00:00+00:00",
                resource_url="https://caldav.example.com/calendars/poc/evt-delete-fail.ics",
                etag="\"etag-old\"",
                last_delivery_status="success",
                last_delivery_at="2026-03-10T11:00:00+00:00",
            )
        }
    )

    next_state = build_next_sync_state_from_delivery(
        [],
        previous_state,
        [
            StubDeliveryResult(
                action="delete",
                event_id="evt-delete-fail",
                ics_uid="uid-delete-fail",
                sequence=5,
                success=False,
                sent=True,
                resource_url="https://caldav.example.com/calendars/poc/evt-delete-fail.ics",
                etag="\"etag-new\"",
                recovery_succeeded=True,
                refreshed_resource_url="https://caldav.example.com/calendars/poc/evt-delete-fail.ics",
                refreshed_etag="\"etag-new\"",
            )
        ],
        synced_at=datetime(2026, 3, 11, 11, 0, 0, tzinfo=timezone.utc),
    )

    assert next_state.events["evt-delete-fail"].resource_url == "https://caldav.example.com/calendars/poc/evt-delete-fail.ics"
    assert next_state.events["evt-delete-fail"].etag == "\"etag-new\""
    assert next_state.tombstones == {}


def test_build_next_sync_state_from_delivery_does_not_update_conflicted_actions() -> None:
    event = _build_event(event_id="evt-conflict", subject="Changed", updated_at="2026-03-11T08:30:00Z")
    previous_state = SyncState(
        events={
            "evt-conflict": EventSyncState(
                event_id="evt-conflict",
                ics_uid="uid-conflict",
                updated_at="2026-03-10T08:00:00Z",
                content_hash=build_event_content_hash(_build_event(event_id="evt-conflict", subject="Before")),
                sequence=6,
                is_deleted=False,
                last_synced_at="2026-03-10T08:00:00+00:00",
                resource_url="https://caldav.example.com/calendars/poc/evt-conflict.ics",
                etag="\"old-etag\"",
                last_delivery_status="success",
                last_delivery_at="2026-03-10T08:00:00+00:00",
            )
        }
    )

    next_state = build_next_sync_state_from_delivery(
        [event],
        previous_state,
        [
            StubDeliveryResult(
                action="update",
                event_id="evt-conflict",
                sequence=6,
                payload_sequence=7,
                success=False,
                sent=True,
                resource_url="https://caldav.example.com/calendars/poc/evt-conflict.ics",
                etag="\"new-etag\"",
                delivered_at="2026-03-11T08:30:00+00:00",
            )
        ],
        synced_at=datetime(2026, 3, 11, 8, 30, 0, tzinfo=timezone.utc),
    )

    assert next_state == previous_state


def test_build_next_sync_state_from_delivery_corrects_resource_metadata_after_recovery() -> None:
    event = _build_event(event_id="evt-recover", subject="Changed", updated_at="2026-03-11T08:45:00Z")
    previous_state = SyncState(
        events={
            "evt-recover": EventSyncState(
                event_id="evt-recover",
                ics_uid="uid-recover",
                updated_at="2026-03-10T08:00:00Z",
                content_hash=build_event_content_hash(_build_event(event_id="evt-recover", subject="Before")),
                sequence=6,
                is_deleted=False,
                last_synced_at="2026-03-10T08:00:00+00:00",
                resource_url="https://caldav.example.com/calendars/poc/stale.ics",
                etag="\"old-etag\"",
                last_delivery_status="success",
                last_delivery_at="2026-03-10T08:00:00+00:00",
            )
        }
    )

    next_state = build_next_sync_state_from_delivery(
        [event],
        previous_state,
        [
            StubDeliveryResult(
                action="update",
                event_id="evt-recover",
                sequence=6,
                payload_sequence=7,
                success=False,
                sent=False,
                recovery_succeeded=True,
                refreshed_resource_url="https://caldav.example.com/calendars/poc/current.ics",
                refreshed_etag="\"fresh-etag\"",
            )
        ],
        synced_at=datetime(2026, 3, 11, 8, 45, 0, tzinfo=timezone.utc),
    )

    assert next_state.events["evt-recover"].updated_at == "2026-03-10T08:00:00Z"
    assert next_state.events["evt-recover"].content_hash == previous_state.events["evt-recover"].content_hash
    assert next_state.events["evt-recover"].last_synced_at == "2026-03-10T08:00:00+00:00"
    assert next_state.events["evt-recover"].resource_url == "https://caldav.example.com/calendars/poc/current.ics"
    assert next_state.events["evt-recover"].etag == "\"fresh-etag\""
    assert next_state.events["evt-recover"].last_delivery_at == "2026-03-10T08:00:00+00:00"


def test_build_next_sync_state_from_delivery_saves_resource_url_and_etag_when_available() -> None:
    event = _build_event(event_id="evt-meta", updated_at="2026-03-11T09:00:00Z")

    next_state = build_next_sync_state_from_delivery(
        [event],
        SyncState.empty(),
        [
            StubDeliveryResult(
                action="create",
                event_id="evt-meta",
                sequence=0,
                payload_sequence=0,
                success=True,
                sent=True,
                resource_url="https://caldav.example.com/calendars/poc/evt-meta.ics",
                etag="\"etag-123\"",
                delivered_at="2026-03-11T09:00:00+00:00",
            )
        ],
        synced_at=datetime(2026, 3, 11, 9, 0, 0, tzinfo=timezone.utc),
    )

    assert next_state.events["evt-meta"].resource_url == "https://caldav.example.com/calendars/poc/evt-meta.ics"
    assert next_state.events["evt-meta"].etag == "\"etag-123\""


def test_build_next_sync_state_from_delivery_updates_state_from_final_retry_success() -> None:
    synced_at = datetime(2026, 3, 11, 9, 30, 0, tzinfo=timezone.utc)
    updated_event = _build_event(event_id="evt-retry", subject="Retried", updated_at="2026-03-11T09:30:00Z")
    previous_state = SyncState(
        events={
            "evt-retry": EventSyncState(
                event_id="evt-retry",
                ics_uid="uid-retry",
                updated_at="2026-03-10T09:00:00Z",
                content_hash=build_event_content_hash(_build_event(event_id="evt-retry", subject="Before")),
                sequence=5,
                is_deleted=False,
                last_synced_at="2026-03-10T09:00:00+00:00",
                resource_url="https://caldav.example.com/calendars/poc/stored.ics",
                etag="\"etag-old\"",
                last_delivery_status="success",
                last_delivery_at="2026-03-10T09:00:00+00:00",
            )
        }
    )

    next_state = build_next_sync_state_from_delivery(
        [updated_event],
        previous_state,
        [
            StubDeliveryResult(
                action="update",
                event_id="evt-retry",
                sequence=5,
                payload_sequence=6,
                success=True,
                sent=True,
                resource_url="https://caldav.example.com/calendars/poc/stored.ics",
                etag="\"etag-new\"",
                delivered_at="2026-03-11T09:30:00+00:00",
                recovery_succeeded=True,
                refreshed_resource_url="https://caldav.example.com/calendars/poc/stored.ics",
                refreshed_etag="\"etag-live\"",
                retry_attempted=True,
                retry_succeeded=True,
                retry_count=1,
            )
        ],
        synced_at=synced_at,
    )

    assert next_state.events["evt-retry"].updated_at == "2026-03-11T09:30:00Z"
    assert next_state.events["evt-retry"].content_hash == build_event_content_hash(updated_event)
    assert next_state.events["evt-retry"].sequence == 6
    assert next_state.events["evt-retry"].resource_url == "https://caldav.example.com/calendars/poc/stored.ics"
    assert next_state.events["evt-retry"].etag == "\"etag-new\""
    assert next_state.events["evt-retry"].last_synced_at == "2026-03-11T09:30:00+00:00"
    assert next_state.events["evt-retry"].last_delivery_status == "success"
    assert next_state.events["evt-retry"].last_delivery_at == "2026-03-11T09:30:00+00:00"


@pytest.mark.parametrize(
    ("case_name", "build_events", "build_previous_state", "build_delivery_results", "assertions"),
    [
        (
            "create_success",
            lambda: [_build_event(event_id="evt-create-case", updated_at="2026-03-11T12:00:00Z")],
            SyncState.empty,
            lambda: [
                StubDeliveryResult(
                    action="create",
                    event_id="evt-create-case",
                    ics_uid="uid-create-case",
                    sequence=0,
                    payload_sequence=0,
                    success=True,
                    sent=True,
                    resource_url="https://caldav.example.com/calendars/poc/evt-create-case.ics",
                    etag="\"etag-create-case\"",
                    delivered_at="2026-03-11T12:00:00+00:00",
                )
            ],
            lambda next_state: (
                next_state.events["evt-create-case"].ics_uid == "uid-create-case"
                and next_state.events["evt-create-case"].sequence == 0
                and next_state.events["evt-create-case"].last_delivery_status == "success"
                and "evt-create-case" not in next_state.tombstones
            ),
        ),
        (
            "update_success",
            lambda: [_build_event(event_id="evt-update-case", subject="After", updated_at="2026-03-11T12:05:00Z")],
            lambda: SyncState(
                events={
                    "evt-update-case": _build_event_state(
                        event_id="evt-update-case",
                        subject="Before",
                        updated_at="2026-03-10T12:05:00Z",
                        sequence=3,
                        resource_url="https://caldav.example.com/calendars/poc/evt-update-case.ics",
                        etag="\"etag-old-update-case\"",
                    )
                }
            ),
            lambda: [
                StubDeliveryResult(
                    action="update",
                    event_id="evt-update-case",
                    ics_uid="uid-update-case",
                    sequence=3,
                    payload_sequence=4,
                    success=True,
                    sent=True,
                    resource_url="https://caldav.example.com/calendars/poc/evt-update-case.ics",
                    etag="\"etag-update-case\"",
                    delivered_at="2026-03-11T12:05:00+00:00",
                )
            ],
            lambda next_state: (
                next_state.events["evt-update-case"].updated_at == "2026-03-11T12:05:00Z"
                and next_state.events["evt-update-case"].sequence == 4
                and next_state.events["evt-update-case"].etag == "\"etag-update-case\""
            ),
        ),
        (
            "delete_success",
            lambda: [],
            lambda: SyncState(
                events={
                    "evt-delete-case": _build_event_state(
                        event_id="evt-delete-case",
                        updated_at="2026-03-10T12:10:00Z",
                        sequence=8,
                        resource_url="https://caldav.example.com/calendars/poc/evt-delete-case.ics",
                        etag="\"etag-delete-case\"",
                    )
                }
            ),
            lambda: [
                StubDeliveryResult(
                    action="delete",
                    event_id="evt-delete-case",
                    ics_uid="uid-delete-case",
                    sequence=8,
                    success=True,
                    sent=True,
                    resource_url="https://caldav.example.com/calendars/poc/evt-delete-case.ics",
                    etag="\"etag-delete-case\"",
                    delivered_at="2026-03-11T12:10:00+00:00",
                )
            ],
            lambda next_state: (
                "evt-delete-case" not in next_state.events
                and next_state.tombstones["evt-delete-case"].ics_uid == "uid-delete-case"
                and next_state.tombstones["evt-delete-case"].deleted_at == "2026-03-11T12:10:00+00:00"
            ),
        ),
        (
            "reappear_success",
            lambda: [_build_event(event_id="evt-reappear-case", updated_at="2026-03-11T12:15:00Z")],
            lambda: SyncState(
                tombstones={
                    "evt-reappear-case": _build_tombstone_state(
                        event_id="evt-reappear-case",
                        deleted_at="2026-03-10T12:15:00+00:00",
                        resource_url="https://caldav.example.com/calendars/poc/evt-reappear-case-old.ics",
                        etag="\"etag-reappear-old\"",
                    )
                }
            ),
            lambda: [
                StubDeliveryResult(
                    action="create",
                    event_id="evt-reappear-case",
                    ics_uid="uid-reappear-case",
                    sequence=0,
                    payload_sequence=0,
                    success=True,
                    sent=True,
                    resource_url="https://caldav.example.com/calendars/poc/evt-reappear-case-new.ics",
                    etag="\"etag-reappear-new\"",
                    delivered_at="2026-03-11T12:15:00+00:00",
                )
            ],
            lambda next_state: (
                next_state.events["evt-reappear-case"].ics_uid == "uid-reappear-case"
                and next_state.events["evt-reappear-case"].resource_url
                == "https://caldav.example.com/calendars/poc/evt-reappear-case-new.ics"
                and "evt-reappear-case" not in next_state.tombstones
            ),
        ),
        (
            "failure_keeps_state",
            lambda: [_build_event(event_id="evt-failure-case", subject="After", updated_at="2026-03-11T12:20:00Z")],
            lambda: SyncState(
                events={
                    "evt-failure-case": _build_event_state(
                        event_id="evt-failure-case",
                        subject="Before",
                        updated_at="2026-03-10T12:20:00Z",
                        sequence=2,
                        resource_url="https://caldav.example.com/calendars/poc/evt-failure-case.ics",
                        etag="\"etag-failure-old\"",
                    )
                }
            ),
            lambda: [
                StubDeliveryResult(
                    action="update",
                    event_id="evt-failure-case",
                    ics_uid="uid-failure-case",
                    sequence=2,
                    payload_sequence=3,
                    success=False,
                    sent=True,
                )
            ],
            lambda next_state: next_state.events["evt-failure-case"]
            == _build_event_state(
                event_id="evt-failure-case",
                subject="Before",
                updated_at="2026-03-10T12:20:00Z",
                sequence=2,
                resource_url="https://caldav.example.com/calendars/poc/evt-failure-case.ics",
                etag="\"etag-failure-old\"",
            ),
        ),
        (
            "dry_run_keeps_state",
            lambda: [_build_event(event_id="evt-dry-run-case", updated_at="2026-03-11T12:25:00Z")],
            SyncState.empty,
            lambda: [
                StubDeliveryResult(
                    action="create",
                    event_id="evt-dry-run-case",
                    ics_uid="uid-dry-run-case",
                    sequence=0,
                    payload_sequence=0,
                    success=True,
                    sent=False,
                )
            ],
            lambda next_state: next_state == SyncState.empty(),
        ),
    ],
)
def test_build_next_sync_state_from_delivery_validates_individual_state_transitions(
    case_name: str,
    build_events,
    build_previous_state,
    build_delivery_results,
    assertions,
) -> None:
    events = build_events()
    previous_state = build_previous_state()
    delivery_results = build_delivery_results()
    next_state = build_next_sync_state_from_delivery(
        events,
        previous_state,
        delivery_results,
        synced_at=datetime(2026, 3, 11, 12, 30, 0, tzinfo=timezone.utc),
    )

    assert assertions(next_state), case_name
    _assert_sync_state_invariants(next_state)


def test_build_next_sync_state_from_delivery_preserves_invariants_across_mixed_transitions() -> None:
    events = [
        _build_event(event_id="evt-create-mixed", subject="Create", updated_at="2026-03-11T13:00:00Z"),
        _build_event(event_id="evt-update-mixed", subject="Update After", updated_at="2026-03-11T13:05:00Z"),
        _build_event(event_id="evt-reappear-mixed", subject="Reappear", updated_at="2026-03-11T13:10:00Z"),
    ]
    previous_state = SyncState(
        events={
            "evt-update-mixed": _build_event_state(
                event_id="evt-update-mixed",
                subject="Update Before",
                updated_at="2026-03-10T13:05:00Z",
                sequence=6,
                resource_url="https://caldav.example.com/calendars/poc/evt-update-mixed.ics",
                etag="\"etag-update-mixed-old\"",
            ),
            "evt-delete-mixed": _build_event_state(
                event_id="evt-delete-mixed",
                updated_at="2026-03-10T13:15:00Z",
                sequence=4,
                resource_url="https://caldav.example.com/calendars/poc/evt-delete-mixed.ics",
                etag="\"etag-delete-mixed-old\"",
            ),
            "evt-fail-mixed": _build_event_state(
                event_id="evt-fail-mixed",
                subject="Failure Before",
                updated_at="2026-03-10T13:20:00Z",
                sequence=9,
                resource_url="https://caldav.example.com/calendars/poc/evt-fail-mixed.ics",
                etag="\"etag-fail-mixed-old\"",
            ),
        },
        tombstones={
            "evt-reappear-mixed": _build_tombstone_state(
                event_id="evt-reappear-mixed",
                deleted_at="2026-03-10T13:10:00+00:00",
                resource_url="https://caldav.example.com/calendars/poc/evt-reappear-mixed-old.ics",
                etag="\"etag-reappear-mixed-old\"",
            )
        },
    )

    next_state = build_next_sync_state_from_delivery(
        events,
        previous_state,
        [
            StubDeliveryResult(
                action="create",
                event_id="evt-create-mixed",
                ics_uid="uid-create-mixed",
                sequence=0,
                payload_sequence=0,
                success=True,
                sent=True,
                resource_url="https://caldav.example.com/calendars/poc/evt-create-mixed.ics",
                etag="\"etag-create-mixed\"",
                delivered_at="2026-03-11T13:00:00+00:00",
            ),
            StubDeliveryResult(
                action="update",
                event_id="evt-update-mixed",
                ics_uid="uid-update-mixed",
                sequence=6,
                payload_sequence=7,
                success=True,
                sent=True,
                resource_url="https://caldav.example.com/calendars/poc/evt-update-mixed.ics",
                etag="\"etag-update-mixed\"",
                delivered_at="2026-03-11T13:05:00+00:00",
            ),
            StubDeliveryResult(
                action="delete",
                event_id="evt-delete-mixed",
                ics_uid="uid-delete-mixed",
                sequence=4,
                success=True,
                sent=True,
                resource_url="https://caldav.example.com/calendars/poc/evt-delete-mixed.ics",
                etag="\"etag-delete-mixed\"",
                delivered_at="2026-03-11T13:15:00+00:00",
            ),
            StubDeliveryResult(
                action="create",
                event_id="evt-reappear-mixed",
                ics_uid="uid-reappear-mixed",
                sequence=0,
                payload_sequence=0,
                success=True,
                sent=True,
                resource_url="https://caldav.example.com/calendars/poc/evt-reappear-mixed.ics",
                etag="\"etag-reappear-mixed\"",
                delivered_at="2026-03-11T13:10:00+00:00",
            ),
            StubDeliveryResult(
                action="update",
                event_id="evt-fail-mixed",
                ics_uid="uid-fail-mixed",
                sequence=9,
                payload_sequence=10,
                success=False,
                sent=True,
            ),
        ],
        synced_at=datetime(2026, 3, 11, 13, 30, 0, tzinfo=timezone.utc),
    )

    _assert_sync_state_invariants(next_state)
    assert set(next_state.events) == {
        "evt-create-mixed",
        "evt-update-mixed",
        "evt-reappear-mixed",
        "evt-fail-mixed",
    }
    assert set(next_state.tombstones) == {"evt-delete-mixed"}
    assert next_state.events["evt-create-mixed"].sequence == 0
    assert next_state.events["evt-update-mixed"].sequence == 7
    assert next_state.events["evt-reappear-mixed"].ics_uid == "uid-reappear-mixed"
    assert next_state.events["evt-reappear-mixed"].resource_url == "https://caldav.example.com/calendars/poc/evt-reappear-mixed.ics"
    assert next_state.tombstones["evt-delete-mixed"].ics_uid == "uid-delete-mixed"
    assert next_state.events["evt-fail-mixed"] == previous_state.events["evt-fail-mixed"]


def test_build_next_sync_state_from_delivery_mixed_retry_outcomes_only_advance_successes() -> None:
    events = [
        _build_event(event_id="evt-retry-success", subject="Retry Success", updated_at="2026-03-11T14:00:00Z"),
        _build_event(event_id="evt-retry-failure", subject="Retry Failure", updated_at="2026-03-11T14:05:00Z"),
        _build_event(event_id="evt-retry-delete", updated_at="2026-03-11T14:10:00Z"),
    ]
    previous_state = SyncState(
        events={
            "evt-retry-success": _build_event_state(
                event_id="evt-retry-success",
                subject="Before Success",
                updated_at="2026-03-10T14:00:00Z",
                sequence=10,
                resource_url="https://caldav.example.com/calendars/poc/evt-retry-success.ics",
                etag="\"etag-retry-success-old\"",
            ),
            "evt-retry-failure": _build_event_state(
                event_id="evt-retry-failure",
                subject="Before Failure",
                updated_at="2026-03-10T14:05:00Z",
                sequence=20,
                resource_url="https://caldav.example.com/calendars/poc/evt-retry-failure.ics",
                etag="\"etag-retry-failure-old\"",
            ),
            "evt-retry-delete": _build_event_state(
                event_id="evt-retry-delete",
                updated_at="2026-03-10T14:10:00Z",
                sequence=30,
                resource_url="https://caldav.example.com/calendars/poc/evt-retry-delete.ics",
                etag="\"etag-retry-delete-old\"",
            ),
        }
    )

    next_state = build_next_sync_state_from_delivery(
        events,
        previous_state,
        [
            StubDeliveryResult(
                action="update",
                event_id="evt-retry-success",
                ics_uid="uid-retry-success",
                sequence=10,
                payload_sequence=11,
                success=True,
                sent=True,
                resource_url="https://caldav.example.com/calendars/poc/evt-retry-success.ics",
                etag="\"etag-retry-success-new\"",
                delivered_at="2026-03-11T14:00:00+00:00",
                recovery_succeeded=True,
                refreshed_resource_url="https://caldav.example.com/calendars/poc/evt-retry-success.ics",
                refreshed_etag="\"etag-retry-success-live\"",
                retry_attempted=True,
                retry_succeeded=True,
                retry_count=1,
            ),
            StubDeliveryResult(
                action="update",
                event_id="evt-retry-failure",
                ics_uid="uid-retry-failure",
                sequence=20,
                payload_sequence=21,
                success=False,
                sent=False,
                recovery_succeeded=True,
                refreshed_resource_url="https://caldav.example.com/calendars/poc/evt-retry-failure-current.ics",
                refreshed_etag="\"etag-retry-failure-current\"",
                retry_attempted=True,
                retry_succeeded=False,
                retry_count=1,
            ),
            StubDeliveryResult(
                action="delete",
                event_id="evt-retry-delete",
                ics_uid="uid-retry-delete",
                sequence=30,
                success=False,
                sent=False,
                recovery_succeeded=True,
                refreshed_resource_url="https://caldav.example.com/calendars/poc/evt-retry-delete-current.ics",
                refreshed_etag="\"etag-retry-delete-current\"",
                retry_attempted=True,
                retry_succeeded=False,
                retry_count=1,
            ),
        ],
        synced_at=datetime(2026, 3, 11, 14, 30, 0, tzinfo=timezone.utc),
    )

    _assert_sync_state_invariants(next_state)
    assert next_state.events["evt-retry-success"].updated_at == "2026-03-11T14:00:00Z"
    assert next_state.events["evt-retry-success"].sequence == 11
    assert next_state.events["evt-retry-success"].etag == "\"etag-retry-success-new\""
    assert next_state.events["evt-retry-failure"].updated_at == "2026-03-10T14:05:00Z"
    assert next_state.events["evt-retry-failure"].sequence == 20
    assert next_state.events["evt-retry-failure"].resource_url == "https://caldav.example.com/calendars/poc/evt-retry-failure-current.ics"
    assert next_state.events["evt-retry-failure"].etag == "\"etag-retry-failure-current\""
    assert "evt-retry-delete" in next_state.events
    assert "evt-retry-delete" not in next_state.tombstones
    assert next_state.events["evt-retry-delete"].resource_url == "https://caldav.example.com/calendars/poc/evt-retry-delete-current.ics"
    assert next_state.events["evt-retry-delete"].etag == "\"etag-retry-delete-current\""


def test_build_next_sync_state_from_delivery_raises_when_successful_delivery_breaks_uid_exclusivity() -> None:
    previous_state = SyncState(
        events={
            "evt-existing": _build_event_state(
                event_id="evt-existing",
                updated_at="2026-03-10T15:00:00Z",
                sequence=1,
            )
        }
    )
    events = [
        _build_event(event_id="evt-new-a", updated_at="2026-03-11T15:00:00Z"),
        _build_event(event_id="evt-new-b", updated_at="2026-03-11T15:05:00Z"),
    ]

    with pytest.raises(
        ValueError,
        match=(
            r"(?s)Refusing to return invalid sync state from "
            r"build_next_sync_state_from_delivery:.*"
            r"ics_uid 'uid-duplicated-mixed' is duplicated across "
            r"events\['evt-new-a'\], events\['evt-new-b'\]"
        ),
    ):
        build_next_sync_state_from_delivery(
            events,
            previous_state,
            [
                StubDeliveryResult(
                    action="create",
                    event_id="evt-new-a",
                    ics_uid="uid-duplicated-mixed",
                    sequence=0,
                    payload_sequence=0,
                    success=True,
                    sent=True,
                    delivered_at="2026-03-11T15:00:00+00:00",
                ),
                StubDeliveryResult(
                    action="create",
                    event_id="evt-new-b",
                    ics_uid="uid-duplicated-mixed",
                    sequence=0,
                    payload_sequence=0,
                    success=True,
                    sent=True,
                    delivered_at="2026-03-11T15:05:00+00:00",
                ),
            ],
            synced_at=datetime(2026, 3, 11, 15, 30, 0, tzinfo=timezone.utc),
        )


def _build_event(
    event_id: str = "evt-1",
    subject: str = "Planning",
    updated_at: str | None = "2026-03-11T00:00:00Z",
    repeat_info: dict[str, str] | None = None,
    repeat_id: str | None = None,
    garoon_event_id: str | None = None,
    attendees: list[Attendee] | None = None,
    facilities: list[Facility] | None = None,
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
        repeat_info=repeat_info,
        attendees=attendees or [],
        facilities=facilities or [],
        garoon_event_id=garoon_event_id,
    )


def _build_event_state(
    event_id: str,
    *,
    subject: str = "Planning",
    updated_at: str = "2026-03-10T00:00:00Z",
    sequence: int = 0,
    resource_url: str | None = None,
    etag: str | None = None,
    last_synced_at: str = "2026-03-10T00:00:00+00:00",
    last_seen_window_start: str | None = None,
    last_seen_window_end: str | None = None,
    last_delivery_status: str | None = "success",
    last_delivery_at: str | None = "2026-03-10T00:00:00+00:00",
) -> EventSyncState:
    event = _build_event(event_id=event_id, subject=subject, updated_at=updated_at)
    return EventSyncState(
        event_id=event_id,
        ics_uid=f"uid-{event_id.removeprefix('evt-')}",
        updated_at=updated_at,
        content_hash=build_event_content_hash(event),
        sequence=sequence,
        is_deleted=False,
        last_synced_at=last_synced_at,
        last_seen_window_start=last_seen_window_start,
        last_seen_window_end=last_seen_window_end,
        resource_url=resource_url,
        etag=etag,
        last_delivery_status=last_delivery_status,
        last_delivery_at=last_delivery_at,
    )


def _build_tombstone_state(
    event_id: str,
    *,
    deleted_at: str = "2026-03-10T00:00:00+00:00",
    resource_url: str | None = None,
    etag: str | None = None,
    last_delivery_status: str = "success",
    last_delivery_at: str | None = "2026-03-10T00:00:00+00:00",
) -> TombstoneSyncState:
    return TombstoneSyncState(
        event_id=event_id,
        ics_uid=f"uid-{event_id.removeprefix('evt-')}",
        deleted_at=deleted_at,
        last_delivery_status=last_delivery_status,
        resource_url=resource_url,
        etag=etag,
        last_delivery_at=last_delivery_at,
    )


def _assert_sync_state_invariants(state: SyncState) -> None:
    validate_sync_state(state.to_dict(), operation="save")
    assert set(state.events).isdisjoint(state.tombstones)
    all_uids = [event_state.ics_uid for event_state in state.events.values()]
    all_uids.extend(tombstone.ics_uid for tombstone in state.tombstones.values())
    assert len(all_uids) == len(set(all_uids))


def _build_fetch_window(*, start: str, end: str) -> DateRange:
    return DateRange(
        start=datetime.fromisoformat(start),
        end=datetime.fromisoformat(end),
    )


@dataclass(frozen=True, slots=True)
class StubDeliveryResult:
    action: str
    event_id: str
    sequence: int
    success: bool
    sent: bool
    ics_uid: str = "uid-stub"
    payload_sequence: int | None = None
    resource_url: str | None = None
    etag: str | None = None
    delivered_at: str | None = None
    recovery_succeeded: bool = False
    refreshed_resource_url: str | None = None
    refreshed_etag: str | None = None
    retry_attempted: bool = False
    retry_succeeded: bool = False
    retry_count: int = 0


@dataclass(frozen=True, slots=True)
class BrokenSerializedEventSyncState:
    payload: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return dict(self.payload)
