from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Protocol

from src.ics_writer import build_ics_uid
from src.models import Attendee, EventDateTime, EventRecord, Facility

DEFAULT_SYNC_STATE_PATH = Path(__file__).resolve().parent.parent / "data" / "sync_state.json"
STATE_VERSION = 3


class SyncStateValidationError(ValueError):
    pass


class SyncStateJsonDecodeError(ValueError):
    def __init__(self, path: Path, cause: json.JSONDecodeError):
        self.path = path
        self.cause = cause
        detail = (
            f"invalid JSON at line {cause.lineno}, column {cause.colno} "
            f"(char {cause.pos}): {cause.msg}"
        )
        super().__init__(f"Invalid sync state JSON while loading {path}:\n- {detail}")


class SyncStatus(StrEnum):
    NEW = "new"
    UPDATED = "updated"
    UNCHANGED = "unchanged"


class DeliveryStatus(StrEnum):
    SUCCESS = "success"


class DeliveryResult(Protocol):
    action: str
    event_id: str
    ics_uid: str
    sequence: int
    payload_sequence: int | None
    success: bool
    sent: bool
    resource_url: str | None
    etag: str | None
    delivered_at: str | None
    recovery_succeeded: bool
    refreshed_resource_url: str | None
    refreshed_etag: str | None


@dataclass(frozen=True, slots=True)
class EventSyncState:
    event_id: str
    ics_uid: str
    updated_at: str | None
    content_hash: str
    sequence: int
    is_deleted: bool
    last_synced_at: str
    resource_url: str | None = None
    etag: str | None = None
    last_delivery_status: str | None = None
    last_delivery_at: str | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EventSyncState":
        return cls(
            event_id=str(payload.get("event_id", "")),
            ics_uid=str(payload.get("ics_uid", "")),
            updated_at=_optional_str(payload.get("updated_at")),
            content_hash=str(payload.get("content_hash", "")),
            sequence=_coerce_int(payload.get("sequence")),
            is_deleted=bool(payload.get("is_deleted", False)),
            last_synced_at=str(payload.get("last_synced_at", "")),
            resource_url=_optional_str(payload.get("resource_url")),
            etag=_optional_str(payload.get("etag")),
            last_delivery_status=_optional_str(payload.get("last_delivery_status")),
            last_delivery_at=_optional_str(payload.get("last_delivery_at")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TombstoneSyncState:
    event_id: str
    ics_uid: str
    deleted_at: str
    last_delivery_status: str
    resource_url: str | None = None
    etag: str | None = None
    last_delivery_at: str | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TombstoneSyncState":
        return cls(
            event_id=str(payload.get("event_id", "")),
            ics_uid=str(payload.get("ics_uid", "")),
            deleted_at=str(payload.get("deleted_at", "")),
            last_delivery_status=str(payload.get("last_delivery_status", "")),
            resource_url=_optional_str(payload.get("resource_url")),
            etag=_optional_str(payload.get("etag")),
            last_delivery_at=_optional_str(payload.get("last_delivery_at")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SyncState:
    version: int = STATE_VERSION
    events: dict[str, EventSyncState] = field(default_factory=dict)
    tombstones: dict[str, TombstoneSyncState] = field(default_factory=dict)

    @classmethod
    def empty(cls) -> "SyncState":
        return cls(version=STATE_VERSION, events={})

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SyncState":
        raw_events = payload.get("events")
        if not isinstance(raw_events, dict):
            raw_events = {}
        raw_tombstones = payload.get("tombstones")
        if not isinstance(raw_tombstones, dict):
            raw_tombstones = {}

        events = {
            event_id: EventSyncState.from_dict(event_payload)
            for event_id, event_payload in raw_events.items()
            if isinstance(event_id, str) and isinstance(event_payload, dict)
        }
        tombstones = {
            event_id: TombstoneSyncState.from_dict(event_payload)
            for event_id, event_payload in raw_tombstones.items()
            if isinstance(event_id, str) and isinstance(event_payload, dict)
        }
        version = _coerce_int(payload.get("version"), default=STATE_VERSION)
        return cls(version=version, events=events, tombstones=tombstones)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "events": {
                event_id: event_state.to_dict()
                for event_id, event_state in sorted(self.events.items())
            },
            "tombstones": {
                event_id: tombstone.to_dict()
                for event_id, tombstone in sorted(self.tombstones.items())
            },
        }


@dataclass(frozen=True, slots=True)
class EventDiff:
    status: SyncStatus
    event: EventRecord
    previous_state: EventSyncState | None
    next_state: EventSyncState
    previous_tombstone: TombstoneSyncState | None = None
    reappeared_from_tombstone: bool = False


@dataclass(frozen=True, slots=True)
class SyncDiffResult:
    new_events: list[EventDiff]
    updated_events: list[EventDiff]
    unchanged_events: list[EventDiff]
    deleted_candidates: list[EventSyncState]


def validate_sync_state(
    payload: Any,
    *,
    path: Path | None = None,
    operation: str | None = None,
    source: str | None = None,
) -> None:
    errors: list[str] = []

    if not isinstance(payload, dict):
        _raise_sync_state_validation_error(
            ["sync state file must contain a JSON object"],
            path=path,
            operation=operation,
            source=source,
        )

    raw_events = payload.get("events")
    raw_tombstones = payload.get("tombstones")

    if "version" not in payload:
        errors.append("missing required top-level field 'version'")
    elif not _is_int_value(payload["version"]):
        errors.append(
            f"top-level field 'version' must be an integer equal to {STATE_VERSION}"
        )
    elif int(payload["version"]) != STATE_VERSION:
        errors.append(
            f"unsupported sync state version {payload['version']}; expected {STATE_VERSION}"
        )

    if "events" not in payload:
        errors.append("missing required top-level field 'events'")
    elif not isinstance(raw_events, dict):
        errors.append("top-level field 'events' must be a JSON object")

    if "tombstones" not in payload:
        errors.append("missing required top-level field 'tombstones'")
    elif not isinstance(raw_tombstones, dict):
        errors.append("top-level field 'tombstones' must be a JSON object")

    uid_references: dict[str, list[str]] = {}

    if isinstance(raw_events, dict):
        for entry_event_id, entry_payload in sorted(raw_events.items()):
            location = f"events[{entry_event_id!r}]"
            uid = _validate_event_state_entry(
                entry_event_id,
                entry_payload,
                location=location,
                errors=errors,
            )
            if uid is not None:
                uid_references.setdefault(uid, []).append(location)

    if isinstance(raw_tombstones, dict):
        for entry_event_id, entry_payload in sorted(raw_tombstones.items()):
            location = f"tombstones[{entry_event_id!r}]"
            uid = _validate_tombstone_state_entry(
                entry_event_id,
                entry_payload,
                location=location,
                errors=errors,
            )
            if uid is not None:
                uid_references.setdefault(uid, []).append(location)

    if isinstance(raw_events, dict) and isinstance(raw_tombstones, dict):
        duplicated_event_ids = sorted(set(raw_events) & set(raw_tombstones))
        for duplicated_event_id in duplicated_event_ids:
            errors.append(
                "event_id "
                f"{duplicated_event_id!r} exists in both events and tombstones"
            )

    for ics_uid, locations in sorted(uid_references.items()):
        unique_locations = sorted(set(locations))
        if len(unique_locations) > 1:
            errors.append(
                f"ics_uid {ics_uid!r} is duplicated across {', '.join(unique_locations)}"
            )

    if errors:
        _raise_sync_state_validation_error(
            errors,
            path=path,
            operation=operation,
            source=source,
        )


def load_sync_state(
    path: Path = DEFAULT_SYNC_STATE_PATH,
    *,
    create_if_missing: bool = True,
) -> SyncState:
    if not path.exists():
        state = SyncState.empty()
        if create_if_missing:
            save_sync_state(path, state)
        return state

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SyncStateJsonDecodeError(path, exc) from exc
    validate_sync_state(payload, path=path, operation="load")
    return SyncState.from_dict(payload)


def save_sync_state(path: Path, state: SyncState) -> None:
    payload = _serialize_validated_sync_state(
        state,
        path=path,
        operation="save",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_event_content_hash(event: EventRecord) -> str:
    canonical_payload = {
        "event_id": event.event_id,
        "subject": event.subject,
        "start": _normalize_event_datetime(event.start),
        "end": _normalize_event_datetime(event.end),
        "is_all_day": event.is_all_day,
        "is_start_only": event.is_start_only,
        "event_type": event.event_type,
        "event_menu": event.event_menu,
        "visibility_type": event.visibility_type,
        "notes": event.notes,
        "original_start_time_zone": event.original_start_time_zone,
        "original_end_time_zone": event.original_end_time_zone,
        "repeat_id": event.repeat_id,
        "repeat_info": _normalize_json_like(event.repeat_info),
        "attendees": [
            _normalize_attendee(attendee)
            for attendee in sorted(event.attendees, key=_attendee_sort_key)
        ],
        "facilities": [
            _normalize_facility(facility)
            for facility in sorted(event.facilities, key=_facility_sort_key)
        ],
    }
    serialized = json.dumps(
        canonical_payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return sha256(serialized.encode("utf-8")).hexdigest()


def get_event_sync_status(event: EventRecord, previous_state: EventSyncState | None) -> SyncStatus:
    if previous_state is None:
        return SyncStatus.NEW

    if (
        previous_state.content_hash != build_event_content_hash(event)
        or previous_state.updated_at != event.updated_at
    ):
        return SyncStatus.UPDATED
    return SyncStatus.UNCHANGED


def build_event_sync_state(
    event: EventRecord,
    previous_state: EventSyncState | None = None,
    previous_tombstone: TombstoneSyncState | None = None,
    synced_at: datetime | None = None,
    *,
    ics_uid: str | None = None,
    sequence: int | None = None,
    resource_url: str | None = None,
    etag: str | None = None,
    last_delivery_status: str | None = None,
    last_delivery_at: str | None = None,
) -> EventSyncState:
    timestamp = _format_timestamp(synced_at or datetime.now(timezone.utc))
    return EventSyncState(
        event_id=event.event_id,
        ics_uid=_resolve_ics_uid(
            event.event_id,
            previous_state=previous_state,
            previous_tombstone=previous_tombstone,
            ics_uid=ics_uid,
        ),
        updated_at=event.updated_at,
        content_hash=build_event_content_hash(event),
        sequence=sequence if sequence is not None else resolve_sequence(previous_state),
        is_deleted=False,
        last_synced_at=timestamp,
        resource_url=_coalesce_optional_str(resource_url, previous_state, "resource_url"),
        etag=_coalesce_optional_str(etag, previous_state, "etag"),
        last_delivery_status=_coalesce_optional_str(
            last_delivery_status,
            previous_state,
            "last_delivery_status",
        ),
        last_delivery_at=_coalesce_optional_str(last_delivery_at, previous_state, "last_delivery_at"),
    )


def diff_events(
    events: list[EventRecord],
    previous_state: SyncState,
    synced_at: datetime | None = None,
) -> SyncDiffResult:
    new_events: list[EventDiff] = []
    updated_events: list[EventDiff] = []
    unchanged_events: list[EventDiff] = []
    current_event_ids = {event.event_id for event in events}
    legacy_recurring_event_ids = _build_legacy_recurring_event_ids(events, previous_state)

    for event in events:
        prior = previous_state.events.get(event.event_id)
        prior_tombstone = previous_state.tombstones.get(event.event_id) if prior is None else None
        status = get_event_sync_status(event, prior)
        next_state = build_event_sync_state(
            event,
            previous_state=prior,
            previous_tombstone=prior_tombstone,
            synced_at=synced_at,
            sequence=resolve_sequence(prior, status),
        )
        diff = EventDiff(
            status=status,
            event=event,
            previous_state=prior,
            next_state=next_state,
            previous_tombstone=prior_tombstone,
            reappeared_from_tombstone=prior is None and prior_tombstone is not None,
        )
        if diff.status is SyncStatus.NEW:
            new_events.append(diff)
        elif diff.status is SyncStatus.UPDATED:
            updated_events.append(diff)
        else:
            unchanged_events.append(diff)

    deleted_candidates = [
        event_state
        for event_id, event_state in sorted(previous_state.events.items())
        if (
            event_id not in current_event_ids
            and event_id not in legacy_recurring_event_ids
            and not event_state.is_deleted
        )
    ]

    return SyncDiffResult(
        new_events=new_events,
        updated_events=updated_events,
        unchanged_events=unchanged_events,
        deleted_candidates=deleted_candidates,
    )


def build_next_sync_state(
    events: list[EventRecord],
    previous_state: SyncState,
    synced_at: datetime | None = None,
) -> SyncState:
    current_event_ids = {event.event_id for event in events}
    event_states = {
        event_id: event_state
        for event_id, event_state in previous_state.events.items()
        if event_id not in current_event_ids
    }
    tombstones = {
        event_id: tombstone
        for event_id, tombstone in previous_state.tombstones.items()
        if event_id not in current_event_ids
    }
    event_states.update(
        {
            event.event_id: build_event_sync_state(
                event,
                previous_state=previous_state.events.get(event.event_id),
                previous_tombstone=(
                    previous_state.tombstones.get(event.event_id)
                    if event.event_id not in previous_state.events
                    else None
                ),
                synced_at=synced_at,
            )
            for event in events
        }
    )
    return _build_validated_sync_state(
        events=event_states,
        tombstones=tombstones,
        source="build_next_sync_state",
    )


def build_next_sync_state_from_delivery(
    events: list[EventRecord],
    previous_state: SyncState,
    delivery_results: Iterable[DeliveryResult],
    synced_at: datetime | None = None,
) -> SyncState:
    event_by_id = {event.event_id: event for event in events}
    event_states = dict(previous_state.events)
    tombstones = dict(previous_state.tombstones)
    updated = False
    fallback_timestamp = synced_at or datetime.now(timezone.utc)

    for result in delivery_results:
        if result.action in {"create", "update"} and result.success and result.sent:
            event = event_by_id.get(result.event_id)
            if event is None:
                raise ValueError(
                    f"Delivery result references unknown event_id '{result.event_id}'."
                )

            prior = previous_state.events.get(result.event_id)
            prior_tombstone = previous_state.tombstones.get(result.event_id) if prior is None else None
            delivery_timestamp = (
                _parse_timestamp(result.delivered_at)
                if result.delivered_at
                else fallback_timestamp
            )
            event_states[result.event_id] = build_event_sync_state(
                event,
                previous_state=prior,
                previous_tombstone=prior_tombstone,
                synced_at=delivery_timestamp,
                ics_uid=result.ics_uid if prior is None else None,
                sequence=_resolve_delivery_sequence(result),
                resource_url=result.resource_url,
                etag=result.etag,
                last_delivery_status=DeliveryStatus.SUCCESS.value,
                last_delivery_at=_format_timestamp(delivery_timestamp),
            )
            tombstones.pop(result.event_id, None)
            updated = True
            continue

        if result.action == "delete":
            if result.success:
                deletion_timestamp = (
                    _parse_timestamp(result.delivered_at)
                    if result.delivered_at
                    else fallback_timestamp
                )
                prior = previous_state.events.get(result.event_id)
                tombstones[result.event_id] = build_tombstone_sync_state(
                    result,
                    previous_state=prior,
                    deleted_at=deletion_timestamp,
                )
                if result.event_id in event_states:
                    del event_states[result.event_id]
                updated = True
                continue

            prior = event_states.get(result.event_id)
            corrected_state = _apply_recovered_resource_metadata(prior, result)
            if corrected_state is not None and corrected_state != prior:
                event_states[result.event_id] = corrected_state
                updated = True
            continue

        prior = event_states.get(result.event_id)
        corrected_state = _apply_recovered_resource_metadata(prior, result)
        if corrected_state is None or corrected_state == prior:
            continue
        event_states[result.event_id] = corrected_state
        updated = True

    if not updated:
        return previous_state

    return _build_validated_sync_state(
        events=event_states,
        tombstones=tombstones,
        source="build_next_sync_state_from_delivery",
    )


def resolve_sequence(
    previous_state: EventSyncState | None,
    status: SyncStatus | None = None,
) -> int:
    if previous_state is None:
        return 0

    if status is SyncStatus.UPDATED:
        return previous_state.sequence
    return previous_state.sequence


def _resolve_delivery_sequence(result: DeliveryResult) -> int:
    payload_sequence = getattr(result, "payload_sequence", None)
    if payload_sequence is not None:
        return int(payload_sequence)
    return result.sequence


def build_tombstone_sync_state(
    result: DeliveryResult,
    *,
    previous_state: EventSyncState | None,
    deleted_at: datetime,
) -> TombstoneSyncState:
    timestamp = _format_timestamp(deleted_at)
    return TombstoneSyncState(
        event_id=result.event_id,
        ics_uid=previous_state.ics_uid if previous_state is not None else result.ics_uid,
        deleted_at=timestamp,
        last_delivery_status=DeliveryStatus.SUCCESS.value,
        resource_url=result.resource_url if result.resource_url is not None else previous_state.resource_url if previous_state is not None else None,
        etag=result.etag if result.etag is not None else previous_state.etag if previous_state is not None else None,
        last_delivery_at=timestamp,
    )


def _resolve_ics_uid(
    event_id: str,
    *,
    previous_state: EventSyncState | None,
    previous_tombstone: TombstoneSyncState | None,
    ics_uid: str | None,
) -> str:
    if ics_uid:
        return ics_uid
    if previous_state is not None and previous_state.ics_uid:
        return previous_state.ics_uid
    if previous_tombstone is not None and previous_tombstone.ics_uid:
        return previous_tombstone.ics_uid
    return build_ics_uid(event_id)


def _build_legacy_recurring_event_ids(
    events: list[EventRecord],
    previous_state: SyncState,
) -> set[str]:
    legacy_ids: set[str] = set()
    previous_event_ids = set(previous_state.events)

    for event in events:
        if not event.repeat_id:
            continue
        if not event.garoon_event_id:
            continue
        if event.garoon_event_id == event.event_id:
            continue
        if event.garoon_event_id in previous_event_ids:
            legacy_ids.add(event.garoon_event_id)

    return legacy_ids


def _normalize_event_datetime(value: EventDateTime | None) -> dict[str, str | None] | None:
    if value is None:
        return None
    return {
        "date_time": value.date_time,
        "time_zone": value.time_zone,
    }


def _normalize_attendee(value: Attendee) -> dict[str, str | None]:
    return {
        "id": value.id,
        "code": value.code,
        "name": value.name,
        "type": value.type,
        "attendance_status": value.attendance_status,
    }


def _normalize_facility(value: Facility) -> dict[str, str | None]:
    return {
        "id": value.id,
        "code": value.code,
        "name": value.name,
    }


def _normalize_json_like(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _normalize_json_like(nested_value)
            for key, nested_value in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, list):
        return [_normalize_json_like(item) for item in value]
    return value


def _attendee_sort_key(value: Attendee) -> tuple[str, str, str, str, str]:
    return (
        value.id or "",
        value.code or "",
        value.name,
        value.type or "",
        value.attendance_status or "",
    )


def _facility_sort_key(value: Facility) -> tuple[str, str, str]:
    return (
        value.id or "",
        value.code or "",
        value.name,
    )


def _validate_event_state_entry(
    entry_event_id: Any,
    payload: Any,
    *,
    location: str,
    errors: list[str],
) -> str | None:
    if not _is_non_empty_str(entry_event_id):
        errors.append(f"{location} must use a non-empty event_id key")
        return None
    if not isinstance(payload, dict):
        errors.append(f"{location} must be a JSON object")
        return None

    _validate_matching_event_id(
        entry_event_id,
        payload,
        location=location,
        errors=errors,
    )
    ics_uid = _validate_required_string_field(payload, "ics_uid", location=location, errors=errors)
    _validate_optional_string_field(
        payload,
        "updated_at",
        location=location,
        errors=errors,
        required=True,
    )
    _validate_required_string_field(payload, "content_hash", location=location, errors=errors)
    _validate_required_int_field(payload, "sequence", location=location, errors=errors)
    is_deleted = _validate_required_bool_field(payload, "is_deleted", location=location, errors=errors)
    _validate_required_string_field(payload, "last_synced_at", location=location, errors=errors)
    _validate_optional_string_field(payload, "resource_url", location=location, errors=errors)
    _validate_optional_string_field(payload, "etag", location=location, errors=errors)
    _validate_optional_string_field(payload, "last_delivery_status", location=location, errors=errors)
    _validate_optional_string_field(payload, "last_delivery_at", location=location, errors=errors)

    if is_deleted is True:
        errors.append(
            f"{location}.is_deleted must be false; deleted entries belong in tombstones"
        )
    return ics_uid


def _validate_tombstone_state_entry(
    entry_event_id: Any,
    payload: Any,
    *,
    location: str,
    errors: list[str],
) -> str | None:
    if not _is_non_empty_str(entry_event_id):
        errors.append(f"{location} must use a non-empty event_id key")
        return None
    if not isinstance(payload, dict):
        errors.append(f"{location} must be a JSON object")
        return None

    _validate_matching_event_id(
        entry_event_id,
        payload,
        location=location,
        errors=errors,
    )
    ics_uid = _validate_required_string_field(payload, "ics_uid", location=location, errors=errors)
    _validate_required_string_field(payload, "deleted_at", location=location, errors=errors)
    _validate_required_string_field(payload, "last_delivery_status", location=location, errors=errors)
    _validate_optional_string_field(payload, "resource_url", location=location, errors=errors)
    _validate_optional_string_field(payload, "etag", location=location, errors=errors)
    _validate_optional_string_field(payload, "last_delivery_at", location=location, errors=errors)
    return ics_uid


def _validate_matching_event_id(
    entry_event_id: str,
    payload: dict[str, Any],
    *,
    location: str,
    errors: list[str],
) -> None:
    payload_event_id = _validate_required_string_field(
        payload,
        "event_id",
        location=location,
        errors=errors,
    )
    if payload_event_id is None:
        return
    if payload_event_id != entry_event_id:
        errors.append(
            f"{location}.event_id must match its object key {entry_event_id!r}"
        )


def _validate_required_string_field(
    payload: dict[str, Any],
    field_name: str,
    *,
    location: str,
    errors: list[str],
) -> str | None:
    if field_name not in payload:
        errors.append(f"{location} is missing required field {field_name!r}")
        return None
    value = payload[field_name]
    if not _is_non_empty_str(value):
        errors.append(f"{location}.{field_name} must be a non-empty string")
        return None
    return value


def _validate_optional_string_field(
    payload: dict[str, Any],
    field_name: str,
    *,
    location: str,
    errors: list[str],
    required: bool = False,
) -> str | None:
    if field_name not in payload:
        if required:
            errors.append(f"{location} is missing required field {field_name!r}")
        return None

    value = payload[field_name]
    if value is None:
        return None
    if not _is_non_empty_str(value):
        errors.append(f"{location}.{field_name} must be a non-empty string or null")
        return None
    return value


def _validate_required_int_field(
    payload: dict[str, Any],
    field_name: str,
    *,
    location: str,
    errors: list[str],
) -> int | None:
    if field_name not in payload:
        errors.append(f"{location} is missing required field {field_name!r}")
        return None
    value = payload[field_name]
    if not _is_int_value(value):
        errors.append(f"{location}.{field_name} must be an integer")
        return None
    return int(value)


def _validate_required_bool_field(
    payload: dict[str, Any],
    field_name: str,
    *,
    location: str,
    errors: list[str],
) -> bool | None:
    if field_name not in payload:
        errors.append(f"{location} is missing required field {field_name!r}")
        return None
    value = payload[field_name]
    if not isinstance(value, bool):
        errors.append(f"{location}.{field_name} must be a boolean")
        return None
    return value


def _raise_sync_state_validation_error(
    errors: list[str],
    *,
    path: Path | None,
    operation: str | None = None,
    source: str | None = None,
) -> None:
    if operation == "load":
        prefix = (
            f"Invalid sync state while loading {path}"
            if path is not None
            else "Invalid sync state while loading"
        )
    elif operation == "save":
        prefix = (
            f"Refusing to save invalid sync state to {path}"
            if path is not None
            else "Refusing to save invalid sync state"
        )
    elif operation == "build":
        source_name = source or "build API"
        prefix = f"Refusing to return invalid sync state from {source_name}"
    else:
        prefix = f"Invalid sync state in {path}" if path is not None else "Invalid sync state"
    details = "\n".join(f"- {error}" for error in errors)
    raise SyncStateValidationError(f"{prefix}:\n{details}")


def _is_int_value(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and value != ""


def _coerce_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _coalesce_optional_str(
    value: str | None,
    previous_state: EventSyncState | None,
    attribute_name: str,
) -> str | None:
    if value is not None:
        return value
    if previous_state is None:
        return None
    return getattr(previous_state, attribute_name)


def _serialize_validated_sync_state(
    state: SyncState,
    *,
    path: Path | None = None,
    operation: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    payload = state.to_dict()
    validate_sync_state(payload, path=path, operation=operation, source=source)
    return payload


def _build_validated_sync_state(
    *,
    events: dict[str, EventSyncState],
    tombstones: dict[str, TombstoneSyncState],
    source: str,
) -> SyncState:
    next_state = SyncState(
        version=STATE_VERSION,
        events=events,
        tombstones=tombstones,
    )
    _serialize_validated_sync_state(next_state, operation="build", source=source)
    return next_state


def _parse_timestamp(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(normalized)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _apply_recovered_resource_metadata(
    previous_state: EventSyncState | None,
    result: DeliveryResult,
) -> EventSyncState | None:
    if previous_state is None or not result.recovery_succeeded:
        return None

    resource_url = result.refreshed_resource_url or previous_state.resource_url
    etag = result.refreshed_etag or previous_state.etag
    if resource_url == previous_state.resource_url and etag == previous_state.etag:
        return previous_state

    return EventSyncState(
        event_id=previous_state.event_id,
        ics_uid=previous_state.ics_uid,
        updated_at=previous_state.updated_at,
        content_hash=previous_state.content_hash,
        sequence=previous_state.sequence,
        is_deleted=previous_state.is_deleted,
        last_synced_at=previous_state.last_synced_at,
        resource_url=resource_url,
        etag=etag,
        last_delivery_status=previous_state.last_delivery_status,
        last_delivery_at=previous_state.last_delivery_at,
    )
