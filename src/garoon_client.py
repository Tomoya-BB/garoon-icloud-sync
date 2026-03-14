from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from typing import Any, Protocol

import requests

from src.models import DateRange, EventRecord


class GaroonClientError(RuntimeError):
    """Base exception for Garoon client failures."""


class GaroonAuthenticationError(GaroonClientError):
    """Raised when Garoon authentication fails."""


class GaroonApiResponseError(GaroonClientError):
    """Raised when the Garoon API returns an unexpected payload."""


class AuthStrategy(Protocol):
    def build_headers(self) -> dict[str, str]:
        """Return request headers for Garoon API authentication."""


@dataclass(frozen=True, slots=True)
class PasswordAuthStrategy:
    username: str
    password: str

    def build_headers(self) -> dict[str, str]:
        token = f"{self.username}:{self.password}".encode("utf-8")
        encoded = base64.b64encode(token).decode("ascii")
        return {
            "X-Cybozu-Authorization": encoded,
            "Accept": "application/json",
        }


class GaroonClient:
    def __init__(
        self,
        base_url: str,
        auth_strategy: AuthStrategy,
        session: requests.Session | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._auth_strategy = auth_strategy
        self._session = session or requests.Session()
        self._logger = logger or logging.getLogger(__name__)

    def fetch_events(
        self,
        date_range: DateRange,
        target_user: str | None = None,
        target_calendar: str | None = None,
    ) -> list[EventRecord]:
        if target_calendar:
            # TODO: Confirm the correct Garoon API parameters for calendar-specific fetches.
            self._logger.warning(
                "GAROON_TARGET_CALENDAR is set but not used yet because the API "
                "details are still TBD."
            )

        params = {
            "rangeStart": date_range.start.isoformat(timespec="seconds"),
            "rangeEnd": date_range.end.isoformat(timespec="seconds"),
            "orderBy": "start asc",
        }
        if target_user:
            # TODO: Confirm whether the target user should be user ID, code, or login name in your tenant.
            params["target"] = target_user
            params["targetType"] = "user"

        response = self._request("GET", "/api/v1/schedule/events", params=params)
        raw_events = response.get("events")
        if not isinstance(raw_events, list):
            raise GaroonApiResponseError(
                "Garoon API response did not include an 'events' list."
            )

        events = [EventRecord.from_garoon_dict(event) for event in raw_events if isinstance(event, dict)]
        self._logger.info("Fetched %s events from Garoon.", len(events))
        return events

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        headers = self._auth_strategy.build_headers()
        self._logger.debug("Sending %s request to %s with params=%s", method, url, params)

        try:
            response = self._session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=30,
            )
        except requests.RequestException as exc:
            raise GaroonClientError(f"Failed to connect to Garoon API: {exc}") from exc

        if response.status_code in {401, 403}:
            raise GaroonAuthenticationError(
                "Garoon authentication failed. Check GAROON_USERNAME, "
                "GAROON_PASSWORD, and tenant authentication settings."
            )
        if response.status_code >= 400:
            detail = _safe_response_text(response)
            raise GaroonClientError(
                f"Garoon API returned HTTP {response.status_code}: {detail}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise GaroonApiResponseError(
                "Garoon API response was not valid JSON."
            ) from exc
        if not isinstance(payload, dict):
            raise GaroonApiResponseError(
                "Garoon API response JSON must be an object."
            )
        return payload


def _safe_response_text(response: requests.Response) -> str:
    text = response.text.strip()
    return text or "<empty response>"
