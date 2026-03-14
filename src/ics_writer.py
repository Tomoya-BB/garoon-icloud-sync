from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Mapping

from src.models import EventRecord

DEFAULT_ICS_PATH = Path(__file__).resolve().parent.parent / "data" / "calendar.ics"
_START_ONLY_FALLBACK_DURATION = timedelta(minutes=30)


def build_calendar(
    events: list[EventRecord],
    generated_at: datetime | None = None,
    sequence_by_event_id: Mapping[str, int] | None = None,
    uid_by_event_id: Mapping[str, str] | None = None,
) -> str:
    timestamp = (generated_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//garoon-icloud-sync//PoC Phase 4//EN",
        "CALSCALE:GREGORIAN",
    ]

    for event in events:
        lines.extend(
            build_vevent(
                event,
                timestamp,
                sequence_by_event_id=sequence_by_event_id,
                uid_by_event_id=uid_by_event_id,
            )
        )

    lines.append("END:VCALENDAR")
    return _serialize_lines(lines)


def build_vevent(
    event: EventRecord,
    generated_at: datetime,
    sequence_by_event_id: Mapping[str, int] | None = None,
    uid_by_event_id: Mapping[str, str] | None = None,
) -> list[str]:
    lines = [
        "BEGIN:VEVENT",
        f"UID:{_build_uid(event, uid_by_event_id)}",
        f"DTSTAMP:{_format_utc_datetime(generated_at)}",
        f"SEQUENCE:{_build_sequence(event, sequence_by_event_id)}",
        f"SUMMARY:{_escape_text(_build_summary(event))}",
    ]

    last_modified = _build_last_modified(event)
    if last_modified is not None:
        lines.insert(3, f"LAST-MODIFIED:{last_modified}")

    date_range = _build_date_range(event)
    if date_range is not None:
        lines.extend(date_range)

    if event.notes:
        lines.append(f"DESCRIPTION:{_escape_text(event.notes)}")

    location = ", ".join(facility.name for facility in event.facilities if facility.name)
    if location:
        lines.append(f"LOCATION:{_escape_text(location)}")

    # TODO: Map additional Garoon fields such as organizer/attendees once the source payload is fixed.
    lines.append("END:VEVENT")
    return lines


def write_calendar(
    output_path: Path,
    events: list[EventRecord],
    generated_at: datetime | None = None,
    sequence_by_event_id: Mapping[str, int] | None = None,
    uid_by_event_id: Mapping[str, str] | None = None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = build_calendar(
        events,
        generated_at=generated_at,
        sequence_by_event_id=sequence_by_event_id,
        uid_by_event_id=uid_by_event_id,
    )
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        handle.write(content)


def build_ics_uid(event_id: str) -> str:
    raw_event_id = event_id.strip() or "missing-id"
    safe_event_id = re.sub(r"[^A-Za-z0-9._-]+", "-", raw_event_id).strip("-") or "event"
    digest = sha256(raw_event_id.encode("utf-8")).hexdigest()[:12]
    return f"garoon-event-{safe_event_id}-{digest}@garoon-icloud-sync.local"


def _build_uid(event: EventRecord, uid_by_event_id: Mapping[str, str] | None = None) -> str:
    if uid_by_event_id is None:
        return build_ics_uid(event.event_id)
    return uid_by_event_id.get(event.event_id, build_ics_uid(event.event_id))


def _build_summary(event: EventRecord) -> str:
    if event.subject.strip():
        return event.subject
    if event.event_menu and event.event_menu.strip():
        return event.event_menu
    return "(no title)"


def _build_date_range(event: EventRecord) -> list[str] | None:
    if not event.start:
        return None

    start = event.start.as_datetime()
    if event.is_all_day:
        end = event.end.as_datetime() if event.end else None
        start_date = start.date()
        end_date = _resolve_all_day_end_date(start, end)
        return [
            f"DTSTART;VALUE=DATE:{start_date.strftime('%Y%m%d')}",
            f"DTEND;VALUE=DATE:{end_date.strftime('%Y%m%d')}",
        ]

    lines = [f"DTSTART:{_format_datetime(start)}"]
    end = _resolve_timed_event_end(event, start)
    if end is not None:
        lines.append(f"DTEND:{_format_datetime(end)}")
    return lines


def _resolve_timed_event_end(event: EventRecord, start: datetime) -> datetime | None:
    if event.end is not None:
        return event.end.as_datetime()
    if event.is_start_only:
        return start + _START_ONLY_FALLBACK_DURATION
    return None


def _resolve_all_day_end_date(start: datetime, end: datetime | None) -> date:
    if end is None:
        return start.date() + timedelta(days=1)

    # Garoon all-day events in the captured payload end at 23:59:59 on the final local day.
    # ICS VALUE=DATE requires an exclusive DTEND, so non-midnight end times roll forward one day.
    if end.timetz().replace(tzinfo=None) == time(0, 0, 0):
        end_date = end.date()
    else:
        end_date = end.date() + timedelta(days=1)

    if end_date <= start.date():
        return start.date() + timedelta(days=1)
    return end_date


def _format_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        return value.strftime("%Y%m%dT%H%M%S")
    return _format_utc_datetime(value)


def _format_utc_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _build_last_modified(event: EventRecord) -> str | None:
    if not event.updated_at:
        return None
    return _format_utc_datetime(_parse_iso_datetime(event.updated_at))


def _build_sequence(event: EventRecord, sequence_by_event_id: Mapping[str, int] | None = None) -> int:
    if sequence_by_event_id is None:
        return 0
    return int(sequence_by_event_id.get(event.event_id, 0))


def _escape_text(value: str) -> str:
    escaped = value.replace("\\", "\\\\")
    escaped = escaped.replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n")
    escaped = escaped.replace(",", "\\,").replace(";", "\\;")
    return escaped


def _serialize_lines(lines: list[str]) -> str:
    return "\r\n".join(_fold_line(line) for line in lines) + "\r\n"


def _fold_line(line: str) -> str:
    if len(line.encode("utf-8")) <= 75:
        return line

    segments: list[str] = []
    remaining = line
    byte_limit = 75
    while remaining:
        chunk, remaining = _take_bytes(remaining, byte_limit)
        segments.append(chunk)
        byte_limit = 74
    return "\r\n ".join(segments)


def _take_bytes(value: str, byte_limit: int) -> tuple[str, str]:
    current_bytes = 0
    split_index = 0
    for index, char in enumerate(value):
        char_bytes = len(char.encode("utf-8"))
        if current_bytes + char_bytes > byte_limit:
            break
        current_bytes += char_bytes
        split_index = index + 1
    return value[:split_index], value[split_index:]


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(normalized)
