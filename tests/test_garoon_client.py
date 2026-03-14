from __future__ import annotations

from datetime import datetime
from typing import Any

from src.garoon_client import GaroonClient, PasswordAuthStrategy
from src.models import DateRange


class FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = ""

    def json(self) -> dict[str, Any]:
        return self._payload


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.last_request: dict[str, Any] | None = None

    def request(self, **kwargs: Any) -> FakeResponse:
        self.last_request = kwargs
        return self.response


def test_fetch_events_normalizes_response() -> None:
    payload = {
        "events": [
            {
                "id": "42",
                "subject": "Architecture review",
                "eventType": "REGULAR",
                "eventMenu": "会議",
                "visibilityType": "PUBLIC",
                "notes": "Bring the migration plan.",
                "isAllDay": False,
                "isStartOnly": False,
                "createdAt": "2026-03-01T00:00:00Z",
                "updatedAt": "2026-03-10T01:23:45Z",
                "start": {"dateTime": "2026-03-11T10:00:00+09:00", "timeZone": "Asia/Tokyo"},
                "end": {"dateTime": "2026-03-11T11:00:00+09:00", "timeZone": "Asia/Tokyo"},
                "originalStartTimeZone": "Asia/Tokyo",
                "originalEndTimeZone": "Asia/Tokyo",
                "attendees": [
                    {
                        "id": "7",
                        "code": "tomoya",
                        "name": "Tomoya",
                        "type": "USER",
                    }
                ],
                "facilities": [
                    {
                        "id": "15",
                        "code": "conf-a",
                        "name": "Conference Room A",
                    }
                ],
            }
        ]
    }
    session = FakeSession(FakeResponse(status_code=200, payload=payload))
    client = GaroonClient(
        base_url="https://example.cybozu.com/g",
        auth_strategy=PasswordAuthStrategy("user", "pass"),
        session=session,
    )
    date_range = DateRange(
        start=datetime.fromisoformat("2026-03-11T00:00:00+09:00"),
        end=datetime.fromisoformat("2026-03-11T23:59:59+09:00"),
    )

    events = client.fetch_events(date_range=date_range, target_user="7")

    assert len(events) == 1
    assert events[0].event_id == "42"
    assert events[0].subject == "Architecture review"
    assert events[0].start is not None
    assert events[0].start.date_time == "2026-03-11T10:00:00+09:00"
    assert events[0].start.time_zone == "Asia/Tokyo"
    assert events[0].updated_at == "2026-03-10T01:23:45Z"
    assert events[0].event_menu == "会議"
    assert events[0].attendees[0].name == "Tomoya"
    assert events[0].facilities[0].name == "Conference Room A"
    assert session.last_request is not None
    assert session.last_request["url"] == "https://example.cybozu.com/g/api/v1/schedule/events"
    assert session.last_request["params"]["target"] == "7"
    assert session.last_request["params"]["targetType"] == "user"
    assert session.last_request["headers"]["X-Cybozu-Authorization"] == "dXNlcjpwYXNz"
