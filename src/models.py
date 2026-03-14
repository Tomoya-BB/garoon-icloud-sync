from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class DateRange:
    start: datetime
    end: datetime

    def to_dict(self) -> dict[str, str]:
        return {
            "start": self.start.isoformat(timespec="seconds"),
            "end": self.end.isoformat(timespec="seconds"),
        }


@dataclass(frozen=True, slots=True)
class Attendee:
    id: str | None
    code: str | None
    name: str
    type: str | None
    attendance_status: str | None


@dataclass(frozen=True, slots=True)
class Facility:
    id: str | None
    code: str | None
    name: str


@dataclass(frozen=True, slots=True)
class EventDateTime:
    date_time: str
    time_zone: str | None = None

    def as_datetime(self) -> datetime:
        return datetime.fromisoformat(self.date_time)


@dataclass(frozen=True, slots=True)
class EventRecord:
    event_id: str
    subject: str
    start: EventDateTime | None
    end: EventDateTime | None
    is_all_day: bool
    is_start_only: bool
    event_type: str | None
    event_menu: str | None
    visibility_type: str | None
    notes: str | None
    created_at: str | None
    updated_at: str | None
    original_start_time_zone: str | None
    original_end_time_zone: str | None
    repeat_id: str | None
    repeat_info: dict[str, Any] | None
    attendees: list[Attendee] = field(default_factory=list)
    facilities: list[Facility] = field(default_factory=list)
    raw: dict[str, Any] | None = None
    garoon_event_id: str | None = None

    @classmethod
    def from_garoon_dict(cls, payload: dict[str, Any]) -> "EventRecord":
        raw_attendees = payload.get("attendees")
        raw_facilities = payload.get("facilities")
        raw_event_id = str(payload.get("id", ""))
        repeat_id = _read_nested(payload, "repeatId")
        attendees = [
            _parse_attendee(attendee)
            for attendee in raw_attendees
            if isinstance(attendee, dict)
        ] if isinstance(raw_attendees, list) else []
        facilities = [
            _parse_facility(facility)
            for facility in raw_facilities
            if isinstance(facility, dict)
        ] if isinstance(raw_facilities, list) else []

        return cls(
            event_id=build_garoon_event_key(raw_event_id, repeat_id),
            subject=str(payload.get("subject", "")),
            start=_parse_event_datetime(payload.get("start")),
            end=_parse_event_datetime(payload.get("end")),
            is_all_day=bool(payload.get("isAllDay")),
            is_start_only=bool(payload.get("isStartOnly")),
            event_type=_read_nested(payload, "eventType"),
            event_menu=_read_nested(payload, "eventMenu"),
            visibility_type=_read_nested(payload, "visibilityType"),
            notes=_read_nested(payload, "notes"),
            created_at=_read_nested(payload, "createdAt"),
            updated_at=_read_nested(payload, "updatedAt"),
            original_start_time_zone=_read_nested(payload, "originalStartTimeZone"),
            original_end_time_zone=_read_nested(payload, "originalEndTimeZone"),
            repeat_id=repeat_id,
            repeat_info=payload.get("repeatInfo") if isinstance(payload.get("repeatInfo"), dict) else None,
            attendees=attendees,
            facilities=facilities,
            raw=payload,
            garoon_event_id=raw_event_id or None,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EventSnapshot:
    fetched_at: str
    range: dict[str, str]
    count: int
    events: list[dict[str, Any]]

    @classmethod
    def build(
        cls,
        fetched_at: datetime,
        date_range: DateRange,
        events: list[EventRecord],
    ) -> "EventSnapshot":
        return cls(
            fetched_at=fetched_at.isoformat(timespec="seconds"),
            range=date_range.to_dict(),
            count=len(events),
            events=[event.to_dict() for event in events],
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _read_nested(payload: dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def build_garoon_event_key(raw_event_id: str, repeat_id: Any) -> str:
    normalized_event_id = str(raw_event_id).strip()
    normalized_repeat_id = str(repeat_id).strip() if repeat_id is not None else ""
    if normalized_event_id and normalized_repeat_id:
        return f"{normalized_event_id}:{normalized_repeat_id}"
    return normalized_event_id


def _parse_event_datetime(payload: Any) -> EventDateTime | None:
    if isinstance(payload, dict):
        date_time = payload.get("dateTime")
        if isinstance(date_time, str) and date_time:
            time_zone = payload.get("timeZone")
            return EventDateTime(
                date_time=date_time,
                time_zone=str(time_zone) if isinstance(time_zone, str) and time_zone else None,
            )

    if isinstance(payload, str) and payload:
        return EventDateTime(date_time=payload)
    return None


def _parse_attendee(payload: dict[str, Any]) -> Attendee:
    source = _entity_or_self(payload)
    return Attendee(
        id=_read_nested(source, "id"),
        code=_read_nested(source, "code"),
        name=str(_read_nested(source, "name") or ""),
        type=_read_nested(source, "type"),
        attendance_status=payload.get("attendanceStatus"),
    )


def _parse_facility(payload: dict[str, Any]) -> Facility:
    source = _entity_or_self(payload)
    return Facility(
        id=_read_nested(source, "id"),
        code=_read_nested(source, "code"),
        name=str(_read_nested(source, "name") or ""),
    )


def _entity_or_self(payload: dict[str, Any]) -> dict[str, Any]:
    entity = payload.get("entity")
    if isinstance(entity, dict):
        return entity
    return payload
