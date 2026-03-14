from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol
from urllib.parse import quote, urljoin
from xml.etree import ElementTree as ET

import requests

from src.caldav_result_helpers import summarize_state_drift_comparison
from src.ics_writer import build_calendar
from src.logger import (
    classify_exception_error_kind,
    format_structured_log_fields,
    log_structured_error,
)
from src.models import EventRecord
from src.sync_plan import SyncActionType, SyncPlan, SyncPlanAction
from src.sync_state import EventSyncState

_REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CALDAV_SYNC_RESULT_PATH = _REPO_ROOT / "data" / "caldav_sync_result.json"
DEFAULT_CALDAV_DIAGNOSTICS_DIR = _REPO_ROOT / "data" / "diagnostics"
DEFAULT_CALDAV_REPORTS_DIR = _REPO_ROOT / "data" / "reports"

_DAV_NAMESPACE = "DAV:"
_CALDAV_NAMESPACE = "urn:ietf:params:xml:ns:caldav"
_MULTISTATUS_TAG = f"{{{_DAV_NAMESPACE}}}multistatus"
_RESPONSE_TAG = f"{{{_DAV_NAMESPACE}}}response"
_HREF_TAG = f"{{{_DAV_NAMESPACE}}}href"
_DISPLAYNAME_TAG = f"{{{_DAV_NAMESPACE}}}displayname"
_CURRENT_USER_PRINCIPAL_TAG = f"{{{_DAV_NAMESPACE}}}current-user-principal"
_PRINCIPAL_URL_TAG = f"{{{_DAV_NAMESPACE}}}principal-URL"
_PRINCIPAL_TAG = f"{{{_DAV_NAMESPACE}}}principal"
_CALENDAR_HOME_SET_TAG = f"{{{_CALDAV_NAMESPACE}}}calendar-home-set"
_COLLECTION_TAG = f"{{{_DAV_NAMESPACE}}}collection"
_CALENDAR_TAG = f"{{{_CALDAV_NAMESPACE}}}calendar"
_SYNC_PLAN_EVENT_ID_PATTERN = re.compile(r"Event '([^']+)' referenced by sync_plan")
_ICS_LINE_FOLDING_PATTERN = re.compile(r"\r?\n[ \t]")
_ICS_UID_PATTERN = re.compile(r"(?im)^UID:(.+)$")


class CalDAVClientError(RuntimeError):
    """Raised when CalDAV communication fails."""


class CalDAVHTTPError(CalDAVClientError):
    def __init__(
        self,
        method: str,
        url: str,
        status_code: int,
        body: str,
        *,
        request_response_diagnostics: CalDAVRequestResponseDiagnostics | None = None,
    ) -> None:
        self.method = method
        self.url = url
        self.status_code = status_code
        self.body = body
        self.request_response_diagnostics = request_response_diagnostics
        super().__init__(
            f"{method} {url} failed with {status_code}: {body or 'empty response body'}"
        )


class CalDAVDiscoveryError(CalDAVClientError):
    def __init__(
        self,
        message: str,
        *,
        error_kind: str,
        url: str,
        status_code: int | None = None,
        root_url: str | None = None,
        principal_url: str | None = None,
        calendar_home_url: str | None = None,
        calendar_name: str | None = None,
    ) -> None:
        super().__init__(message)
        self.error_kind = error_kind
        self.url = url
        self.status_code = status_code
        self.root_url = root_url
        self.principal_url = principal_url
        self.calendar_home_url = calendar_home_url
        self.calendar_name = calendar_name


class CalDAVMutationActionError(CalDAVClientError):
    def __init__(
        self,
        message: str,
        *,
        resource_url: str | None,
        resolution_strategy: str,
        used_stored_resource_url: bool,
        uid_lookup_performed: bool,
        used_stored_etag: bool,
        status_code: int | None = None,
        conflict_kind: str | None = None,
        retryable: bool = False,
        etag_mismatch: bool = False,
        attempted_conditional_update: bool = False,
        recovery_attempted: bool = False,
        recovery_succeeded: bool = False,
        refreshed_resource_url: str | None = None,
        refreshed_etag: str | None = None,
        initial_resource_url: str | None = None,
        initial_etag: str | None = None,
        retry_attempted: bool = False,
        retry_succeeded: bool = False,
        retry_count: int = 0,
        retry_resource_url: str | None = None,
        retry_etag: str | None = None,
        create_conflict_resource_exists: bool = False,
        create_conflict_uid_match_found: bool = False,
        create_conflict_uid_lookup_attempted: bool = False,
        create_conflict_uid_lookup_candidates: int = 0,
        create_conflict_uid_lookup_method: str | None = None,
        create_conflict_remote_uid_confirmed: bool = False,
        create_conflict_state_drift_suspected: bool = False,
        create_conflict_existing_resource_url: str | None = None,
        create_conflict_selected_candidate_reason: str | None = None,
        create_conflict_selected_candidate_index: int | None = None,
        create_conflict_uid_lookup_raw_candidates: list[dict[str, Any]] | None = None,
        create_conflict_uid_query_raw_response: str | None = None,
        create_conflict_collection_scan_raw_response: str | None = None,
        create_conflict_candidate_ranking: list[dict[str, Any]] | None = None,
        request_response_diagnostics: CalDAVRequestResponseDiagnostics | None = None,
    ) -> None:
        super().__init__(message)
        self.resource_url = resource_url
        self.resolution_strategy = resolution_strategy
        self.used_stored_resource_url = used_stored_resource_url
        self.uid_lookup_performed = uid_lookup_performed
        self.used_stored_etag = used_stored_etag
        self.status_code = status_code
        self.conflict_kind = conflict_kind
        self.retryable = retryable
        self.etag_mismatch = etag_mismatch
        self.attempted_conditional_update = attempted_conditional_update
        self.recovery_attempted = recovery_attempted
        self.recovery_succeeded = recovery_succeeded
        self.refreshed_resource_url = refreshed_resource_url
        self.refreshed_etag = refreshed_etag
        self.initial_resource_url = initial_resource_url
        self.initial_etag = initial_etag
        self.retry_attempted = retry_attempted
        self.retry_succeeded = retry_succeeded
        self.retry_count = retry_count
        self.retry_resource_url = retry_resource_url
        self.retry_etag = retry_etag
        self.create_conflict_resource_exists = create_conflict_resource_exists
        self.create_conflict_uid_match_found = create_conflict_uid_match_found
        self.create_conflict_uid_lookup_attempted = create_conflict_uid_lookup_attempted
        self.create_conflict_uid_lookup_candidates = create_conflict_uid_lookup_candidates
        self.create_conflict_uid_lookup_method = create_conflict_uid_lookup_method
        self.create_conflict_remote_uid_confirmed = create_conflict_remote_uid_confirmed
        self.create_conflict_state_drift_suspected = create_conflict_state_drift_suspected
        self.create_conflict_existing_resource_url = create_conflict_existing_resource_url
        self.create_conflict_selected_candidate_reason = create_conflict_selected_candidate_reason
        self.create_conflict_selected_candidate_index = create_conflict_selected_candidate_index
        self.create_conflict_uid_lookup_raw_candidates = (
            list(create_conflict_uid_lookup_raw_candidates)
            if create_conflict_uid_lookup_raw_candidates is not None
            else []
        )
        self.create_conflict_uid_query_raw_response = create_conflict_uid_query_raw_response
        self.create_conflict_collection_scan_raw_response = create_conflict_collection_scan_raw_response
        self.create_conflict_candidate_ranking = (
            list(create_conflict_candidate_ranking)
            if create_conflict_candidate_ranking is not None
            else []
        )
        self.request_response_diagnostics = request_response_diagnostics


@dataclass(frozen=True, slots=True)
class CalDAVConnectionSettings:
    url: str
    username: str
    password: str
    calendar_name: str
    dry_run: bool = True
    timeout_seconds: float = 30.0
    diagnostic_dump_failed_ics: bool = False
    diagnostic_dump_success_ics: bool = False
    diagnostic_dump_uid_lookup_json: bool = False
    diagnostic_dir: Path | None = None


@dataclass(frozen=True, slots=True)
class PreparedCalDAVAction:
    action: SyncActionType
    event_id: str
    ics_uid: str
    sequence: int
    action_reason: str
    reappeared_from_tombstone: bool
    tombstone_deleted_at: str | None
    updated_at: str | None
    resource_name: str
    ics_payload: str
    payload_summary: dict[str, Any]
    stored_resource_url: str | None = None
    stored_etag: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["payload_bytes"] = len(self.ics_payload.encode("utf-8"))
        return payload


@dataclass(frozen=True, slots=True)
class CalDAVActionResult:
    action: str
    event_id: str
    ics_uid: str
    sequence: int
    dry_run: bool
    success: bool
    sent: bool
    action_reason: str
    resource_name: str
    resource_url: str | None
    etag: str | None
    updated_at: str | None
    delivered_at: str | None
    payload_summary: dict[str, Any]
    payload_bytes: int
    diagnostic_payload_path: str | None = None
    reappeared_from_tombstone: bool = False
    tombstone_deleted_at: str | None = None
    payload_sequence: int | None = None
    resolution_strategy: str | None = None
    used_stored_resource_url: bool = False
    uid_lookup_performed: bool = False
    used_stored_etag: bool = False
    conflict_kind: str | None = None
    retryable: bool = False
    etag_mismatch: bool = False
    attempted_conditional_update: bool = False
    recovery_attempted: bool = False
    recovery_succeeded: bool = False
    refreshed_resource_url: str | None = None
    refreshed_etag: str | None = None
    initial_resource_url: str | None = None
    initial_etag: str | None = None
    retry_attempted: bool = False
    retry_succeeded: bool = False
    retry_count: int = 0
    retry_resource_url: str | None = None
    retry_etag: str | None = None
    create_conflict_resource_exists: bool = False
    create_conflict_uid_match_found: bool = False
    create_conflict_uid_lookup_attempted: bool = False
    create_conflict_uid_lookup_candidates: int = 0
    create_conflict_uid_lookup_method: str | None = None
    create_conflict_remote_uid_confirmed: bool = False
    create_conflict_state_drift_suspected: bool = False
    create_conflict_existing_resource_url: str | None = None
    create_conflict_selected_candidate_reason: str | None = None
    create_conflict_selected_candidate_index: int | None = None
    create_conflict_uid_lookup_raw_candidates: list[dict[str, Any]] = field(default_factory=list)
    create_conflict_uid_lookup_diagnostics_path: str | None = None
    create_conflict_uid_query_raw_path: str | None = None
    create_conflict_collection_scan_raw_path: str | None = None
    create_conflict_candidate_ranking: list[dict[str, Any]] = field(default_factory=list)
    create_conflict_state_drift_report_path: str | None = None
    create_conflict_state_drift_report_status: str | None = None
    drift_report_status: str | None = None
    drift_diff_count: int | None = None
    drift_diff_fields: list[str] | None = None
    create_conflict_remote_fetch_error: str | None = None
    request_method: str | None = None
    request_url: str | None = None
    request_headers: dict[str, Any] | None = None
    response_headers: dict[str, Any] | None = None
    response_body_excerpt: str | None = None
    diagnostic_request_response_path: str | None = None
    status_code: int | None = None
    error_kind: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class IgnoredSyncPlanAction:
    action: str
    event_id: str
    ics_uid: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CalDAVSyncReport:
    generated_at: str
    dry_run: bool
    calendar_name: str
    source_url: str
    processed_count: int
    ignored_count: int
    results: list[CalDAVActionResult] = field(default_factory=list)
    ignored_actions: list[IgnoredSyncPlanAction] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return sum(1 for item in self.results if item.success)

    @property
    def failure_count(self) -> int:
        return sum(1 for item in self.results if not item.success)

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "dry_run": self.dry_run,
            "calendar_name": self.calendar_name,
            "source_url": self.source_url,
            "processed_count": self.processed_count,
            "ignored_count": self.ignored_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "results": [item.to_dict() for item in self.results],
            "ignored_actions": [item.to_dict() for item in self.ignored_actions],
        }


@dataclass(frozen=True, slots=True)
class CalDAVRequestResponseDiagnostics:
    request_method: str
    request_url: str
    request_headers: dict[str, Any]
    response_headers: dict[str, Any]
    response_body_excerpt: str | None = None


@dataclass(frozen=True, slots=True)
class CalDAVPutResult:
    status_code: int
    resource_url: str
    etag: str | None = None
    request_response_diagnostics: CalDAVRequestResponseDiagnostics | None = None


@dataclass(frozen=True, slots=True)
class CalDAVDeleteResult:
    status_code: int | None
    resource_url: str | None
    etag: str | None = None
    sent: bool = True


@dataclass(frozen=True, slots=True)
class CalDAVResourceState:
    resource_url: str
    etag: str | None = None


@dataclass(frozen=True, slots=True)
class CalDAVCalendarObject:
    resource_url: str
    etag: str | None = None
    calendar_data: str | None = None


@dataclass(frozen=True, slots=True)
class CalDAVUIDLookupCandidate:
    resource_url: str
    etag: str | None = None
    calendar_data: str | None = None
    remote_uid: str | None = None
    summary: str | None = None
    dtstart: str | None = None
    dtend: str | None = None
    found_via: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CalDAVUIDLookupQueryResult:
    candidates: list[CalDAVUIDLookupCandidate] = field(default_factory=list)
    raw_response: str | None = None


@dataclass(frozen=True, slots=True)
class CalDAVUIDLookupDiagnostics:
    attempted: bool = False
    candidate_count: int = 0
    method: str | None = None
    matched_resource_url: str | None = None
    matched_resource_etag: str | None = None
    remote_uid_confirmed: bool = False
    selected_candidate_reason: str | None = None
    selected_candidate_index: int | None = None
    candidates: list[CalDAVUIDLookupCandidate] = field(default_factory=list)
    uid_query_raw_response: str | None = None
    collection_scan_raw_response: str | None = None
    candidate_ranking: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class CalDAVActionResolution:
    resource_url: str | None
    resolution_strategy: str
    used_stored_resource_url: bool
    uid_lookup_performed: bool
    used_stored_etag: bool
    attempted_conditional_update: bool
    recovery_attempted: bool = False
    recovery_succeeded: bool = False
    refreshed_resource_url: str | None = None
    refreshed_etag: str | None = None
    initial_resource_url: str | None = None
    initial_etag: str | None = None
    retry_attempted: bool = False
    retry_succeeded: bool = False
    retry_count: int = 0
    retry_resource_url: str | None = None
    retry_etag: str | None = None


@dataclass(frozen=True, slots=True)
class CalDAVRecoveryResult:
    attempted: bool = False
    succeeded: bool = False
    refreshed_resource_url: str | None = None
    refreshed_etag: str | None = None


@dataclass(frozen=True, slots=True)
class CalDAVCreateConflictDiagnosis:
    resource_exists: bool = False
    uid_match_found: bool = False
    uid_lookup_attempted: bool = False
    uid_lookup_candidates: int = 0
    uid_lookup_method: str | None = None
    remote_uid_confirmed: bool = False
    state_drift_suspected: bool = False
    existing_resource_url: str | None = None
    selected_candidate_reason: str | None = None
    selected_candidate_index: int | None = None
    uid_lookup_raw_candidates: list[dict[str, Any]] = field(default_factory=list)
    uid_query_raw_response: str | None = None
    collection_scan_raw_response: str | None = None
    candidate_ranking: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class CalDAVCreateConflictUIDLookupArtifact:
    diagnostics_path: str | None = None
    uid_query_raw_path: str | None = None
    collection_scan_raw_path: str | None = None


@dataclass(frozen=True, slots=True)
class CalDAVComparableEventState:
    uid: str | None = None
    summary: str | None = None
    dtstart: str | None = None
    dtend: str | None = None
    has_description: bool = False
    has_location: bool = False
    sequence: str | None = None
    last_modified: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CalDAVCreateConflictStateDriftArtifact:
    path: str | None = None
    status: str | None = None
    diff_count: int | None = None
    diff_fields: list[str] | None = None
    remote_fetch_error: str | None = None


@dataclass(frozen=True, slots=True)
class CalDAVSyncFailureContext:
    action: str | None = None
    event_id: str | None = None
    ics_uid: str | None = None
    resource_url: str | None = None
    processed_count: int = 0
    remaining_count: int = 0
    total_count: int = 0
    action_index: int | None = None


class CalDAVTransport(Protocol):
    def resolve_calendar_url(self, settings: CalDAVConnectionSettings) -> str: ...

    def find_event_resource_by_uid(self, calendar_url: str, uid: str) -> CalDAVResourceState | None: ...

    def diagnose_uid_lookup(self, calendar_url: str, uid: str) -> CalDAVUIDLookupDiagnostics: ...

    def get_calendar_object(self, resource_url: str) -> CalDAVResourceState: ...

    def get_calendar_object_data(self, resource_url: str) -> CalDAVCalendarObject: ...

    def put_calendar_object(
        self,
        resource_url: str,
        ics_payload: str,
        *,
        overwrite: bool,
        etag: str | None = None,
    ) -> CalDAVPutResult: ...

    def delete_calendar_object(
        self,
        resource_url: str,
        *,
        etag: str | None = None,
    ) -> CalDAVDeleteResult: ...


class RequestsCalDAVTransport:
    def __init__(
        self,
        settings: CalDAVConnectionSettings,
        *,
        logger: logging.Logger | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self._settings = settings
        self._logger = logger or logging.getLogger(__name__)
        self._session = session or requests.Session()
        self._session.auth = (settings.username, settings.password)
        self._session.headers.update({"User-Agent": "garoon-icloud-sync/phase10"})

    def resolve_calendar_url(self, settings: CalDAVConnectionSettings) -> str:
        root_url = _ensure_trailing_slash(settings.url)
        principal_url: str | None = None
        calendar_home_url: str | None = None

        try:
            principal_url = self._discover_principal_url(root_url, settings)
            calendar_home_url = self._discover_calendar_home_url(
                principal_url,
                settings,
                root_url=root_url,
            )
            calendar_url = self._discover_calendar_collection_url(
                calendar_home_url,
                settings,
                root_url=root_url,
                principal_url=principal_url,
            )
        except CalDAVDiscoveryError as exc:
            self._log_discovery_failure(exc)
            raise

        self._logger.info(
            "caldav discovery resolved %s",
            format_structured_log_fields(
                {
                    "component": "caldav",
                    "phase": "discovery",
                    "root_url": root_url,
                    "principal_url": principal_url,
                    "calendar_home_url": calendar_home_url,
                    "calendar_url": calendar_url,
                    "calendar_name": settings.calendar_name,
                }
            ),
        )
        return calendar_url

    def find_event_resource_by_uid(self, calendar_url: str, uid: str) -> CalDAVResourceState | None:
        query_result = self._query_calendar_resources(
            calendar_url,
            uid=uid,
            include_calendar_data=False,
        )
        for item in query_result.candidates:
            return CalDAVResourceState(
                resource_url=item.resource_url,
                etag=item.etag,
            )
        return None

    def diagnose_uid_lookup(self, calendar_url: str, uid: str) -> CalDAVUIDLookupDiagnostics:
        attempted_methods: list[str] = []
        candidates_by_url: dict[str, CalDAVUIDLookupCandidate] = {}
        provisional_match: CalDAVUIDLookupCandidate | None = None
        provisional_reason: str | None = None
        last_error: CalDAVClientError | None = None
        uid_query_raw_response: str | None = None
        collection_scan_raw_response: str | None = None

        attempted_methods.append("calendar_query_uid_calendar_data")
        try:
            filtered_query_result = self._query_calendar_resources(
                calendar_url,
                uid=uid,
                include_calendar_data=True,
            )
        except CalDAVClientError as exc:
            last_error = exc
        else:
            filtered_candidates = filtered_query_result.candidates
            uid_query_raw_response = filtered_query_result.raw_response
            self._remember_uid_lookup_candidates(
                candidates_by_url,
                filtered_candidates,
                lookup_method="calendar_query_uid_calendar_data",
            )
            provisional_match = _first_candidate(filtered_candidates)
            if provisional_match is not None:
                provisional_reason = "first_candidate_from_calendar_query_uid_calendar_data"
            confirmed_match = _find_confirmed_uid_candidate(filtered_candidates, uid)
            if confirmed_match is not None:
                selected_candidates = list(candidates_by_url.values())
                return CalDAVUIDLookupDiagnostics(
                    attempted=True,
                    candidate_count=len(selected_candidates),
                    method="+".join(attempted_methods),
                    matched_resource_url=confirmed_match.resource_url,
                    matched_resource_etag=confirmed_match.etag,
                    remote_uid_confirmed=True,
                    selected_candidate_reason="confirmed_uid_match_from_calendar_query_uid_calendar_data",
                    selected_candidate_index=_find_candidate_index(
                        selected_candidates,
                        confirmed_match.resource_url,
                    ),
                    candidates=selected_candidates,
                    uid_query_raw_response=uid_query_raw_response,
                )

        attempted_methods.append("calendar_collection_scan_calendar_data")
        try:
            collection_query_result = self._query_calendar_resources(
                calendar_url,
                uid=None,
                include_calendar_data=True,
            )
        except CalDAVClientError as exc:
            if provisional_match is None and not candidates_by_url and last_error is not None:
                raise exc from last_error
            selected_candidates = list(candidates_by_url.values())
            return CalDAVUIDLookupDiagnostics(
                attempted=True,
                candidate_count=len(selected_candidates),
                method="+".join(attempted_methods),
                matched_resource_url=provisional_match.resource_url if provisional_match is not None else None,
                matched_resource_etag=provisional_match.etag if provisional_match is not None else None,
                remote_uid_confirmed=False,
                selected_candidate_reason=provisional_reason,
                selected_candidate_index=_find_candidate_index(
                    selected_candidates,
                    provisional_match.resource_url if provisional_match is not None else None,
                ),
                candidates=selected_candidates,
                uid_query_raw_response=uid_query_raw_response,
            )

        collection_candidates = collection_query_result.candidates
        collection_scan_raw_response = collection_query_result.raw_response
        self._remember_uid_lookup_candidates(
            candidates_by_url,
            collection_candidates,
            lookup_method="calendar_collection_scan_calendar_data",
        )
        confirmed_match = _find_confirmed_uid_candidate(collection_candidates, uid)
        if confirmed_match is not None:
            selected_candidates = list(candidates_by_url.values())
            return CalDAVUIDLookupDiagnostics(
                attempted=True,
                candidate_count=len(selected_candidates),
                method="+".join(attempted_methods),
                matched_resource_url=confirmed_match.resource_url,
                matched_resource_etag=confirmed_match.etag,
                remote_uid_confirmed=True,
                selected_candidate_reason="confirmed_uid_match_from_calendar_collection_scan_calendar_data",
                selected_candidate_index=_find_candidate_index(
                    selected_candidates,
                    confirmed_match.resource_url,
                ),
                candidates=selected_candidates,
                uid_query_raw_response=uid_query_raw_response,
                collection_scan_raw_response=collection_scan_raw_response,
            )

        if provisional_match is None:
            provisional_match = _first_candidate(collection_candidates)
            if provisional_match is not None:
                provisional_reason = "first_candidate_from_calendar_collection_scan_calendar_data"
        selected_candidates = list(candidates_by_url.values())
        return CalDAVUIDLookupDiagnostics(
            attempted=True,
            candidate_count=len(selected_candidates),
            method="+".join(attempted_methods),
            matched_resource_url=provisional_match.resource_url if provisional_match is not None else None,
            matched_resource_etag=provisional_match.etag if provisional_match is not None else None,
            remote_uid_confirmed=False,
            selected_candidate_reason=provisional_reason,
            selected_candidate_index=_find_candidate_index(
                selected_candidates,
                provisional_match.resource_url if provisional_match is not None else None,
            ),
            candidates=selected_candidates,
            uid_query_raw_response=uid_query_raw_response,
            collection_scan_raw_response=collection_scan_raw_response,
        )

    def get_calendar_object(self, resource_url: str) -> CalDAVResourceState:
        try:
            response = self._request(
                "HEAD",
                resource_url,
                headers={},
                data=b"",
            )
        except CalDAVHTTPError as exc:
            if exc.status_code not in {405, 501}:
                raise
            response = self._request(
                "GET",
                resource_url,
                headers={"Accept": "text/calendar"},
                data=b"",
            )

        etag = response.headers.get("ETag") or response.headers.get("Etag")
        return CalDAVResourceState(resource_url=resource_url, etag=etag)

    def get_calendar_object_data(self, resource_url: str) -> CalDAVCalendarObject:
        response = self._request(
            "GET",
            resource_url,
            headers={"Accept": "text/calendar"},
            data=b"",
        )
        etag = response.headers.get("ETag") or response.headers.get("Etag")
        return CalDAVCalendarObject(
            resource_url=resource_url,
            etag=etag,
            calendar_data=response.text,
        )

    def put_calendar_object(
        self,
        resource_url: str,
        ics_payload: str,
        *,
        overwrite: bool,
        etag: str | None = None,
    ) -> CalDAVPutResult:
        payload = ics_payload.encode("utf-8")
        headers = {
            "Content-Type": "text/calendar; charset=utf-8",
        }
        if overwrite:
            headers["If-Match"] = etag or "*"
        else:
            headers["If-None-Match"] = "*"

        response = self._request(
            "PUT",
            resource_url,
            headers=headers,
            data=payload,
        )
        etag = response.headers.get("ETag") or response.headers.get("Etag")
        return CalDAVPutResult(
            status_code=response.status_code,
            resource_url=resource_url,
            etag=etag,
            request_response_diagnostics=_build_request_response_diagnostics(
                "PUT",
                resource_url,
                headers=headers,
                data=payload,
                response_headers=response.headers,
                response_body_excerpt=_build_response_body_excerpt(response.text),
            ),
        )

    def delete_calendar_object(
        self,
        resource_url: str,
        *,
        etag: str | None = None,
    ) -> CalDAVDeleteResult:
        headers: dict[str, str] = {}
        if etag is not None:
            headers["If-Match"] = etag

        response = self._request(
            "DELETE",
            resource_url,
            headers=headers,
            data=b"",
        )
        response_etag = response.headers.get("ETag") or response.headers.get("Etag")
        return CalDAVDeleteResult(
            status_code=response.status_code,
            resource_url=resource_url,
            etag=response_etag or etag,
            sent=True,
        )

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        data: str | bytes,
    ) -> requests.Response:
        response = self._session.request(
            method=method,
            url=url,
            headers=headers,
            data=data,
            timeout=self._settings.timeout_seconds,
        )
        if 200 <= response.status_code < 300:
            return response

        body_excerpt = _build_response_body_excerpt(response.text)
        raise CalDAVHTTPError(
            method,
            url,
            response.status_code,
            body_excerpt or "empty response body",
            request_response_diagnostics=_build_request_response_diagnostics(
                method,
                url,
                headers=headers,
                data=data,
                response_headers=response.headers,
                response_body_excerpt=body_excerpt,
            ),
        )

    def _propfind(
        self,
        url: str,
        *,
        depth: int,
        data: str,
    ) -> ET.Element:
        response = self._request(
            "PROPFIND",
            url,
            headers={
                "Depth": str(depth),
                "Content-Type": "application/xml; charset=utf-8",
            },
            data=data,
        )
        return _parse_xml(response.text)

    def _query_calendar_resources(
        self,
        calendar_url: str,
        *,
        uid: str | None,
        include_calendar_data: bool,
    ) -> CalDAVUIDLookupQueryResult:
        response = self._request(
            "REPORT",
            calendar_url,
            headers={
                "Depth": "1",
                "Content-Type": "application/xml; charset=utf-8",
            },
            data=_build_calendar_query_request(
                uid=uid,
                include_calendar_data=include_calendar_data,
            ),
        )
        root = _parse_xml(response.text)

        results: list[CalDAVUIDLookupCandidate] = []
        for item in _iter_response_nodes(root):
            href = _find_response_href(item)
            if not href:
                continue
            resource_url = urljoin(_ensure_trailing_slash(calendar_url), href)
            if _is_same_calendar_collection(resource_url, calendar_url):
                continue
            calendar_data = _find_successful_propstat_text(
                item,
                f"{{{_CALDAV_NAMESPACE}}}calendar-data",
            )
            parsed_state = _extract_comparable_event_state(calendar_data)
            results.append(
                CalDAVUIDLookupCandidate(
                    resource_url=resource_url,
                    etag=_find_successful_propstat_text(item, f"{{{_DAV_NAMESPACE}}}getetag"),
                    calendar_data=calendar_data,
                    remote_uid=parsed_state.uid,
                    summary=parsed_state.summary,
                    dtstart=parsed_state.dtstart,
                    dtend=parsed_state.dtend,
                )
            )
        return CalDAVUIDLookupQueryResult(
            candidates=results,
            raw_response=response.text,
        )

    def _remember_uid_lookup_candidates(
        self,
        candidates_by_url: dict[str, CalDAVUIDLookupCandidate],
        candidates: list[CalDAVUIDLookupCandidate],
        *,
        lookup_method: str,
    ) -> None:
        for item in candidates:
            tagged_item = _tag_uid_lookup_candidate(item, lookup_method)
            existing = candidates_by_url.get(tagged_item.resource_url)
            candidates_by_url[tagged_item.resource_url] = (
                _merge_uid_lookup_candidate(existing, tagged_item)
                if existing is not None
                else tagged_item
            )

    def _discover_principal_url(
        self,
        root_url: str,
        settings: CalDAVConnectionSettings,
    ) -> str:
        try:
            root = self._propfind(
                root_url,
                depth=0,
                data=_build_principal_lookup_request(),
            )
        except CalDAVHTTPError as exc:
            raise CalDAVDiscoveryError(
                f"CalDAV discovery failed while resolving principal from {root_url}: {exc}",
                error_kind="discovery_principal_http_failed",
                url=exc.url,
                status_code=exc.status_code,
                root_url=root_url,
                calendar_name=settings.calendar_name,
            ) from exc
        except CalDAVClientError as exc:
            raise CalDAVDiscoveryError(
                f"CalDAV discovery returned an invalid principal response for {root_url}: {exc}",
                error_kind="discovery_principal_invalid_response",
                url=root_url,
                root_url=root_url,
                calendar_name=settings.calendar_name,
            ) from exc

        for item in _iter_response_nodes(root):
            principal_href = _find_nested_href(item, _CURRENT_USER_PRINCIPAL_TAG)
            if principal_href is None:
                principal_href = _find_nested_href(item, _PRINCIPAL_URL_TAG)
            if principal_href:
                return urljoin(root_url, principal_href)
            if _response_has_resource_type(item, _PRINCIPAL_TAG):
                href = _find_text(item, _HREF_TAG)
                if href:
                    return urljoin(root_url, href)

        raise CalDAVDiscoveryError(
            f"CalDAV discovery did not return a principal URL from {root_url}.",
            error_kind="discovery_principal_not_found",
            url=root_url,
            root_url=root_url,
            calendar_name=settings.calendar_name,
        )

    def _discover_calendar_home_url(
        self,
        principal_url: str,
        settings: CalDAVConnectionSettings,
        *,
        root_url: str,
    ) -> str:
        principal_url = _ensure_trailing_slash(principal_url)
        try:
            root = self._propfind(
                principal_url,
                depth=0,
                data=_build_calendar_home_lookup_request(),
            )
        except CalDAVHTTPError as exc:
            raise CalDAVDiscoveryError(
                f"CalDAV discovery failed while resolving calendar-home from {principal_url}: {exc}",
                error_kind="discovery_calendar_home_http_failed",
                url=exc.url,
                status_code=exc.status_code,
                root_url=root_url,
                principal_url=principal_url,
                calendar_name=settings.calendar_name,
            ) from exc
        except CalDAVClientError as exc:
            raise CalDAVDiscoveryError(
                f"CalDAV discovery returned an invalid calendar-home response for {principal_url}: {exc}",
                error_kind="discovery_calendar_home_invalid_response",
                url=principal_url,
                root_url=root_url,
                principal_url=principal_url,
                calendar_name=settings.calendar_name,
            ) from exc

        for item in _iter_response_nodes(root):
            calendar_home_href = _find_nested_href(item, _CALENDAR_HOME_SET_TAG)
            if calendar_home_href:
                return urljoin(principal_url, calendar_home_href)

        raise CalDAVDiscoveryError(
            f"CalDAV discovery did not return calendar-home-set from {principal_url}.",
            error_kind="discovery_calendar_home_not_found",
            url=principal_url,
            root_url=root_url,
            principal_url=principal_url,
            calendar_name=settings.calendar_name,
        )

    def _discover_calendar_collection_url(
        self,
        calendar_home_url: str,
        settings: CalDAVConnectionSettings,
        *,
        root_url: str,
        principal_url: str,
    ) -> str:
        calendar_home_url = _ensure_trailing_slash(calendar_home_url)
        try:
            root = self._propfind(
                calendar_home_url,
                depth=1,
                data=_build_calendar_lookup_request(),
            )
        except CalDAVHTTPError as exc:
            raise CalDAVDiscoveryError(
                f"CalDAV discovery failed while listing calendars below {calendar_home_url}: {exc}",
                error_kind="discovery_calendar_listing_http_failed",
                url=exc.url,
                status_code=exc.status_code,
                root_url=root_url,
                principal_url=principal_url,
                calendar_home_url=calendar_home_url,
                calendar_name=settings.calendar_name,
            ) from exc
        except CalDAVClientError as exc:
            raise CalDAVDiscoveryError(
                f"CalDAV discovery returned an invalid calendar listing for {calendar_home_url}: {exc}",
                error_kind="discovery_calendar_listing_invalid_response",
                url=calendar_home_url,
                root_url=root_url,
                principal_url=principal_url,
                calendar_home_url=calendar_home_url,
                calendar_name=settings.calendar_name,
            ) from exc

        matches: list[str] = []
        for item in _iter_response_nodes(root):
            href = _find_text(item, _HREF_TAG)
            display_name = _find_text(item, _DISPLAYNAME_TAG)
            if not href or display_name != settings.calendar_name:
                continue
            if _response_has_calendar_resource(item):
                matches.append(urljoin(calendar_home_url, href))

        if not matches:
            raise CalDAVDiscoveryError(
                (
                    f"Calendar '{settings.calendar_name}' was not found below {calendar_home_url}. "
                    "Check CALDAV_CALENDAR_NAME or account visibility."
                ),
                error_kind="discovery_calendar_not_found",
                url=calendar_home_url,
                root_url=root_url,
                principal_url=principal_url,
                calendar_home_url=calendar_home_url,
                calendar_name=settings.calendar_name,
            )
        if len(matches) > 1:
            raise CalDAVDiscoveryError(
                (
                    f"Calendar '{settings.calendar_name}' matched multiple calendar collections below "
                    f"{calendar_home_url}."
                ),
                error_kind="discovery_calendar_ambiguous",
                url=calendar_home_url,
                root_url=root_url,
                principal_url=principal_url,
                calendar_home_url=calendar_home_url,
                calendar_name=settings.calendar_name,
            )
        return matches[0]

    def _log_discovery_failure(self, exc: CalDAVDiscoveryError) -> None:
        log_structured_error(
            self._logger,
            "caldav discovery failure",
            fields={
                "component": "caldav",
                "phase": "discovery",
                "error_kind": exc.error_kind,
                "root_url": exc.root_url,
                "principal_url": exc.principal_url,
                "calendar_home_url": exc.calendar_home_url,
                "resource_url": exc.url,
                "calendar_name": exc.calendar_name,
                "status_code": exc.status_code,
                "error": str(exc),
            },
        )


class CalDAVClient:
    def __init__(
        self,
        settings: CalDAVConnectionSettings,
        *,
        logger: logging.Logger | None = None,
        transport: CalDAVTransport | None = None,
    ) -> None:
        self._settings = settings
        self._logger = logger or logging.getLogger(__name__)
        self._transport = transport or RequestsCalDAVTransport(settings, logger=self._logger)

    def sync(
        self,
        sync_plan: SyncPlan,
        events: list[EventRecord],
        *,
        generated_at: datetime | None = None,
        previous_sync_state: Mapping[str, EventSyncState] | None = None,
    ) -> CalDAVSyncReport:
        timestamp = (generated_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
        prepared_actions: list[PreparedCalDAVAction] = []
        ignored_actions: list[IgnoredSyncPlanAction] = []

        try:
            prepared_actions = build_caldav_actions(
                sync_plan,
                events,
                generated_at=generated_at,
                previous_sync_state=previous_sync_state,
            )
            ignored_actions = build_ignored_actions(sync_plan)

            results: list[CalDAVActionResult] = []
            if self._settings.dry_run:
                for index, item in enumerate(prepared_actions):
                    try:
                        self._logger.info(
                            "CalDAV dry-run action=%s event_id=%s uid=%s payload_bytes=%s",
                            item.action.value,
                            item.event_id,
                            item.ics_uid,
                            len(item.ics_payload.encode("utf-8")),
                        )
                        results.append(
                            _build_dry_run_result(
                                item,
                                diagnostic_payload_path=self._maybe_dump_diagnostic_payload(
                                    item,
                                    success=True,
                                    recorded_at=timestamp,
                                ),
                            )
                        )
                    except Exception as exc:
                        _attach_sync_failure_context(
                            exc,
                            _build_sync_failure_context_for_action(
                                item,
                                processed_count=len(results),
                                total_count=len(prepared_actions),
                                action_index=index + 1,
                            ),
                        )
                        raise
            else:
                results = self._send_actions(prepared_actions, delivered_at=timestamp)

            return CalDAVSyncReport(
                generated_at=timestamp.isoformat(timespec="seconds"),
                dry_run=self._settings.dry_run,
                calendar_name=self._settings.calendar_name,
                source_url=self._settings.url,
                processed_count=len(prepared_actions),
                ignored_count=len(ignored_actions),
                results=results,
                ignored_actions=ignored_actions,
            )
        except Exception as exc:
            _log_sync_failure(
                self._logger,
                exc,
                sync_plan=sync_plan,
                prepared_actions=prepared_actions,
            )
            raise

    def _send_actions(
        self,
        prepared_actions: list[PreparedCalDAVAction],
        *,
        delivered_at: datetime,
    ) -> list[CalDAVActionResult]:
        results: list[CalDAVActionResult] = []
        if not prepared_actions:
            return results

        calendar_url: str | None = None

        def resolve_calendar_url() -> str:
            nonlocal calendar_url
            if calendar_url is not None:
                return calendar_url
            calendar_url = self._transport.resolve_calendar_url(self._settings)
            return calendar_url

        for item in prepared_actions:
            attempted_resource_url = item.stored_resource_url
            try:
                if item.action is SyncActionType.CREATE:
                    attempted_resource_url = urljoin(
                        _ensure_trailing_slash(resolve_calendar_url()),
                        quote(item.resource_name),
                    )
                    response, resolution = self._send_create_action(
                        item,
                        attempted_resource_url=attempted_resource_url,
                        calendar_url=calendar_url,
                    )
                    sent = True
                    payload_sequence = item.sequence
                elif item.action is SyncActionType.UPDATE:
                    response, resolution = self._send_update_action(item, resolve_calendar_url)
                    sent = True
                    payload_sequence = item.sequence
                else:
                    response, resolution = self._send_delete_action(item, resolve_calendar_url)
                    sent = response.sent
                    payload_sequence = None

                request_response_diagnostics = (
                    response.request_response_diagnostics
                    if item.action is SyncActionType.CREATE
                    and isinstance(response, CalDAVPutResult)
                    else None
                )

                results.append(
                    CalDAVActionResult(
                        action=item.action.value,
                        event_id=item.event_id,
                        ics_uid=item.ics_uid,
                        sequence=item.sequence,
                        dry_run=False,
                        success=True,
                        sent=sent,
                        action_reason=item.action_reason,
                        reappeared_from_tombstone=item.reappeared_from_tombstone,
                        tombstone_deleted_at=item.tombstone_deleted_at,
                        resource_name=item.resource_name,
                        resource_url=response.resource_url,
                        etag=response.etag,
                        updated_at=item.updated_at,
                        delivered_at=_format_timestamp(delivered_at),
                        payload_summary=item.payload_summary,
                        payload_bytes=len(item.ics_payload.encode("utf-8")),
                        diagnostic_payload_path=self._maybe_dump_diagnostic_payload(
                            item,
                            success=True,
                            recorded_at=delivered_at,
                        ),
                        payload_sequence=payload_sequence,
                        resolution_strategy=resolution.resolution_strategy,
                        used_stored_resource_url=resolution.used_stored_resource_url,
                        uid_lookup_performed=resolution.uid_lookup_performed,
                        used_stored_etag=resolution.used_stored_etag,
                        attempted_conditional_update=resolution.attempted_conditional_update,
                        recovery_attempted=resolution.recovery_attempted,
                        recovery_succeeded=resolution.recovery_succeeded,
                        refreshed_resource_url=resolution.refreshed_resource_url,
                        refreshed_etag=resolution.refreshed_etag,
                        initial_resource_url=resolution.initial_resource_url,
                        initial_etag=resolution.initial_etag,
                        retry_attempted=resolution.retry_attempted,
                        retry_succeeded=resolution.retry_succeeded,
                        retry_count=resolution.retry_count,
                        retry_resource_url=resolution.retry_resource_url,
                        retry_etag=resolution.retry_etag,
                        request_method=(
                            request_response_diagnostics.request_method
                            if request_response_diagnostics is not None
                            else None
                        ),
                        request_url=(
                            request_response_diagnostics.request_url
                            if request_response_diagnostics is not None
                            else None
                        ),
                        request_headers=(
                            dict(request_response_diagnostics.request_headers)
                            if request_response_diagnostics is not None
                            else None
                        ),
                        response_headers=(
                            dict(request_response_diagnostics.response_headers)
                            if request_response_diagnostics is not None
                            else None
                        ),
                        response_body_excerpt=(
                            request_response_diagnostics.response_body_excerpt
                            if request_response_diagnostics is not None
                            else None
                        ),
                        diagnostic_request_response_path=self._maybe_dump_diagnostic_request_response(
                            item,
                            success=True,
                            recorded_at=delivered_at,
                            request_response_diagnostics=request_response_diagnostics,
                        ),
                        status_code=response.status_code,
                    )
                )
            except CalDAVClientError as exc:
                request_response_diagnostics = (
                    _extract_request_response_diagnostics(exc)
                    if item.action is SyncActionType.CREATE
                    else None
                )
                if isinstance(exc, CalDAVMutationActionError):
                    uid_lookup_artifact = self._maybe_dump_create_conflict_uid_lookup_diagnostics(
                        item,
                        exc,
                        recorded_at=delivered_at,
                    )
                    state_drift_artifact = self._maybe_write_create_conflict_state_drift_report(
                        item,
                        exc,
                        recorded_at=delivered_at,
                    )
                    results.append(
                        _build_failure_result(
                            item,
                            str(exc),
                            diagnostic_payload_path=self._maybe_dump_diagnostic_payload(
                                item,
                                success=False,
                                recorded_at=delivered_at,
                            ),
                            resolution_strategy=exc.resolution_strategy,
                            used_stored_resource_url=exc.used_stored_resource_url,
                            uid_lookup_performed=exc.uid_lookup_performed,
                            used_stored_etag=exc.used_stored_etag,
                            resource_url=exc.resource_url,
                            status_code=exc.status_code,
                            conflict_kind=exc.conflict_kind,
                            retryable=exc.retryable,
                            etag_mismatch=exc.etag_mismatch,
                            attempted_conditional_update=exc.attempted_conditional_update,
                            recovery_attempted=exc.recovery_attempted,
                            recovery_succeeded=exc.recovery_succeeded,
                            refreshed_resource_url=exc.refreshed_resource_url,
                            refreshed_etag=exc.refreshed_etag,
                            initial_resource_url=exc.initial_resource_url,
                            initial_etag=exc.initial_etag,
                            retry_attempted=exc.retry_attempted,
                            retry_succeeded=exc.retry_succeeded,
                            retry_count=exc.retry_count,
                            retry_resource_url=exc.retry_resource_url,
                            retry_etag=exc.retry_etag,
                            create_conflict_resource_exists=exc.create_conflict_resource_exists,
                            create_conflict_uid_match_found=exc.create_conflict_uid_match_found,
                            create_conflict_uid_lookup_attempted=exc.create_conflict_uid_lookup_attempted,
                            create_conflict_uid_lookup_candidates=exc.create_conflict_uid_lookup_candidates,
                            create_conflict_uid_lookup_method=exc.create_conflict_uid_lookup_method,
                            create_conflict_remote_uid_confirmed=exc.create_conflict_remote_uid_confirmed,
                            create_conflict_state_drift_suspected=exc.create_conflict_state_drift_suspected,
                            create_conflict_existing_resource_url=exc.create_conflict_existing_resource_url,
                            create_conflict_selected_candidate_reason=(
                                exc.create_conflict_selected_candidate_reason
                            ),
                            create_conflict_selected_candidate_index=(
                                exc.create_conflict_selected_candidate_index
                            ),
                            create_conflict_uid_lookup_raw_candidates=(
                                exc.create_conflict_uid_lookup_raw_candidates
                            ),
                            create_conflict_uid_lookup_diagnostics_path=uid_lookup_artifact.diagnostics_path,
                            create_conflict_uid_query_raw_path=uid_lookup_artifact.uid_query_raw_path,
                            create_conflict_collection_scan_raw_path=(
                                uid_lookup_artifact.collection_scan_raw_path
                            ),
                            create_conflict_candidate_ranking=(
                                exc.create_conflict_candidate_ranking
                            ),
                            create_conflict_state_drift_report_path=state_drift_artifact.path,
                            create_conflict_state_drift_report_status=state_drift_artifact.status,
                            drift_report_status=state_drift_artifact.status,
                            drift_diff_count=state_drift_artifact.diff_count,
                            drift_diff_fields=state_drift_artifact.diff_fields,
                            create_conflict_remote_fetch_error=state_drift_artifact.remote_fetch_error,
                            request_response_diagnostics=request_response_diagnostics,
                            diagnostic_request_response_path=self._maybe_dump_diagnostic_request_response(
                                item,
                                success=False,
                                recorded_at=delivered_at,
                                request_response_diagnostics=request_response_diagnostics,
                            ),
                            error_kind=_classify_sync_failure_error_kind(exc),
                        )
                    )
                else:
                    results.append(
                        _build_failure_result(
                            item,
                            str(exc),
                            diagnostic_payload_path=self._maybe_dump_diagnostic_payload(
                                item,
                                success=False,
                                recorded_at=delivered_at,
                            ),
                            request_response_diagnostics=request_response_diagnostics,
                            diagnostic_request_response_path=self._maybe_dump_diagnostic_request_response(
                                item,
                                success=False,
                                recorded_at=delivered_at,
                                request_response_diagnostics=request_response_diagnostics,
                            ),
                            status_code=_extract_status_code(exc),
                            error_kind=_classify_sync_failure_error_kind(exc),
                        )
                    )
            except Exception as exc:
                _attach_sync_failure_context(
                    exc,
                    _build_sync_failure_context_for_action(
                        item,
                        processed_count=len(results),
                        total_count=len(prepared_actions),
                        action_index=len(results) + 1,
                        resource_url=attempted_resource_url,
                    ),
                )
                raise
        return results

    def _send_create_action(
        self,
        item: PreparedCalDAVAction,
        *,
        attempted_resource_url: str,
        calendar_url: str | None,
    ) -> tuple[CalDAVPutResult, CalDAVActionResolution]:
        try:
            response = self._transport.put_calendar_object(
                attempted_resource_url,
                item.ics_payload,
                overwrite=False,
            )
        except CalDAVClientError as exc:
            diagnosis = self._diagnose_create_conflict(
                item,
                attempted_resource_url=attempted_resource_url,
                calendar_url=calendar_url,
                status_code=_extract_status_code(exc),
            )
            raise _build_mutation_action_error(
                exc,
                resource_url=attempted_resource_url,
                resolution_strategy="create_resource_name",
                used_stored_resource_url=False,
                uid_lookup_performed=False,
                used_stored_etag=False,
                attempted_conditional_update=False,
                create_conflict_diagnosis=diagnosis,
            ) from exc
        return response, CalDAVActionResolution(
            resource_url=response.resource_url,
            resolution_strategy="create_resource_name",
            used_stored_resource_url=False,
            uid_lookup_performed=False,
            used_stored_etag=False,
            attempted_conditional_update=False,
        )

    def _diagnose_create_conflict(
        self,
        item: PreparedCalDAVAction,
        *,
        attempted_resource_url: str,
        calendar_url: str | None,
        status_code: int | None,
    ) -> CalDAVCreateConflictDiagnosis:
        if item.action is not SyncActionType.CREATE or status_code != 412:
            return CalDAVCreateConflictDiagnosis()

        resource_exists = False
        uid_match_found = False
        uid_lookup_attempted = False
        uid_lookup_candidates = 0
        uid_lookup_method: str | None = None
        remote_uid_confirmed = False
        existing_resource_url: str | None = None
        selected_candidate_reason: str | None = None
        selected_candidate_index: int | None = None
        uid_lookup_raw_candidates: list[dict[str, Any]] = []
        uid_query_raw_response: str | None = None
        collection_scan_raw_response: str | None = None
        candidate_ranking: list[dict[str, Any]] = []

        try:
            existing_resource = self._transport.get_calendar_object(attempted_resource_url)
        except CalDAVHTTPError as exc:
            if exc.status_code not in {404, 410}:
                self._logger.warning(
                    "CalDAV create conflict resource probe failed for event_id=%s uid=%s resource_url=%s: %s",
                    item.event_id,
                    item.ics_uid,
                    attempted_resource_url,
                    exc,
                )
        except CalDAVClientError as exc:
            self._logger.warning(
                "CalDAV create conflict resource probe failed for event_id=%s uid=%s resource_url=%s: %s",
                item.event_id,
                item.ics_uid,
                attempted_resource_url,
                exc,
            )
        else:
            resource_exists = True
            existing_resource_url = existing_resource.resource_url

        if calendar_url is not None:
            try:
                uid_lookup = self._transport.diagnose_uid_lookup(calendar_url, item.ics_uid)
            except CalDAVClientError as exc:
                self._logger.warning(
                    "CalDAV create conflict UID probe failed for event_id=%s uid=%s: %s",
                    item.event_id,
                    item.ics_uid,
                    exc,
                )
            else:
                uid_lookup_attempted = uid_lookup.attempted
                uid_lookup_candidates = uid_lookup.candidate_count
                uid_lookup_method = uid_lookup.method
                remote_uid_confirmed = uid_lookup.remote_uid_confirmed
                selected_candidate_reason = uid_lookup.selected_candidate_reason
                selected_candidate_index = uid_lookup.selected_candidate_index
                uid_lookup_raw_candidates = _serialize_uid_lookup_candidates(uid_lookup.candidates)
                uid_query_raw_response = uid_lookup.uid_query_raw_response
                collection_scan_raw_response = uid_lookup.collection_scan_raw_response
                candidate_ranking = _build_uid_lookup_candidate_ranking(
                    _extract_comparable_event_state(item.ics_payload),
                    uid_lookup.candidates,
                )
                if uid_lookup.matched_resource_url is not None:
                    uid_match_found = True
                    existing_resource_url = uid_lookup.matched_resource_url

        state_drift_suspected = resource_exists or uid_match_found
        return CalDAVCreateConflictDiagnosis(
            resource_exists=resource_exists,
            uid_match_found=uid_match_found,
            uid_lookup_attempted=uid_lookup_attempted,
            uid_lookup_candidates=uid_lookup_candidates,
            uid_lookup_method=uid_lookup_method,
            remote_uid_confirmed=remote_uid_confirmed,
            state_drift_suspected=state_drift_suspected,
            existing_resource_url=existing_resource_url,
            selected_candidate_reason=selected_candidate_reason,
            selected_candidate_index=selected_candidate_index,
            uid_lookup_raw_candidates=uid_lookup_raw_candidates,
            uid_query_raw_response=uid_query_raw_response,
            collection_scan_raw_response=collection_scan_raw_response,
            candidate_ranking=candidate_ranking,
        )

    def _maybe_dump_diagnostic_payload(
        self,
        item: PreparedCalDAVAction,
        *,
        success: bool,
        recorded_at: datetime,
    ) -> str | None:
        if not self._should_dump_create_diagnostics(item, success=success):
            return None

        diagnostic_dir = self._settings.diagnostic_dir or DEFAULT_CALDAV_DIAGNOSTICS_DIR
        status_label, filename_base = self._build_create_diagnostic_filename_base(
            item,
            success=success,
            recorded_at=recorded_at,
        )
        filename = f"{filename_base}.ics"
        path = diagnostic_dir / filename

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(item.ics_payload, encoding="utf-8")
        except OSError as exc:
            self._logger.warning(
                "Failed to write CalDAV diagnostic ICS action=%s event_id=%s resource_name=%s path=%s: %s",
                item.action.value,
                item.event_id,
                item.resource_name,
                path,
                exc,
            )
            return None

        formatted_path = _format_diagnostic_path(path)
        self._logger.info(
            "Saved CalDAV diagnostic ICS action=%s event_id=%s status=%s path=%s",
            item.action.value,
            item.event_id,
            status_label,
            formatted_path,
        )
        return formatted_path

    def _maybe_dump_diagnostic_request_response(
        self,
        item: PreparedCalDAVAction,
        *,
        success: bool,
        recorded_at: datetime,
        request_response_diagnostics: CalDAVRequestResponseDiagnostics | None,
    ) -> str | None:
        if request_response_diagnostics is None:
            return None
        if not self._should_dump_create_diagnostics(item, success=success):
            return None

        diagnostic_dir = self._settings.diagnostic_dir or DEFAULT_CALDAV_DIAGNOSTICS_DIR
        status_label, filename_base = self._build_create_diagnostic_filename_base(
            item,
            success=success,
            recorded_at=recorded_at,
        )
        path = diagnostic_dir / f"{filename_base}__http.json"

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(asdict(request_response_diagnostics), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError as exc:
            self._logger.warning(
                "Failed to write CalDAV diagnostic HTTP metadata action=%s event_id=%s resource_name=%s path=%s: %s",
                item.action.value,
                item.event_id,
                item.resource_name,
                path,
                exc,
            )
            return None

        formatted_path = _format_diagnostic_path(path)
        self._logger.info(
            "Saved CalDAV diagnostic HTTP metadata action=%s event_id=%s status=%s path=%s",
            item.action.value,
            item.event_id,
            status_label,
            formatted_path,
        )
        return formatted_path

    def _maybe_dump_create_conflict_uid_lookup_diagnostics(
        self,
        item: PreparedCalDAVAction,
        exc: CalDAVMutationActionError,
        *,
        recorded_at: datetime,
    ) -> CalDAVCreateConflictUIDLookupArtifact:
        if (
            item.action is not SyncActionType.CREATE
            or exc.status_code != 412
            or not self._settings.diagnostic_dump_uid_lookup_json
        ):
            return CalDAVCreateConflictUIDLookupArtifact()

        diagnostic_dir = self._settings.diagnostic_dir or DEFAULT_CALDAV_DIAGNOSTICS_DIR
        _, filename_base = self._build_create_diagnostic_filename_base(
            item,
            success=False,
            recorded_at=recorded_at,
        )
        uid_query_raw_path = self._maybe_dump_uid_lookup_raw_response(
            diagnostic_dir / f"{filename_base}__calendar_query_uid_calendar_data.xml",
            exc.create_conflict_uid_query_raw_response,
            action=item.action.value,
            event_id=item.event_id,
            resource_name=item.resource_name,
            label="calendar_query_uid_calendar_data",
        )
        collection_scan_raw_path = self._maybe_dump_uid_lookup_raw_response(
            diagnostic_dir / f"{filename_base}__calendar_collection_scan_calendar_data.xml",
            exc.create_conflict_collection_scan_raw_response,
            action=item.action.value,
            event_id=item.event_id,
            resource_name=item.resource_name,
            label="calendar_collection_scan_calendar_data",
        )
        payload = {
            "generated_at": recorded_at.astimezone(timezone.utc).isoformat(timespec="seconds"),
            "kind": "create_conflict_uid_lookup_diagnostics",
            "action": item.action.value,
            "event_id": item.event_id,
            "ics_uid": item.ics_uid,
            "resource_name": item.resource_name,
            "attempted_resource_url": exc.resource_url,
            "existing_resource_url": exc.create_conflict_existing_resource_url,
            "selected_candidate_reason": exc.create_conflict_selected_candidate_reason,
            "selected_candidate_index": exc.create_conflict_selected_candidate_index,
            "uid_lookup_attempted": exc.create_conflict_uid_lookup_attempted,
            "uid_lookup_candidates": exc.create_conflict_uid_lookup_candidates,
            "uid_lookup_method": exc.create_conflict_uid_lookup_method,
            "remote_uid_confirmed": exc.create_conflict_remote_uid_confirmed,
            "uid_query_raw_path": uid_query_raw_path,
            "collection_scan_raw_path": collection_scan_raw_path,
            "candidate_ranking": list(exc.create_conflict_candidate_ranking),
            "candidates": list(exc.create_conflict_uid_lookup_raw_candidates),
        }
        path = diagnostic_dir / f"{filename_base}__uid_lookup_candidates.json"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError as write_exc:
            self._logger.warning(
                "Failed to write CalDAV UID lookup diagnostics action=%s event_id=%s resource_name=%s path=%s: %s",
                item.action.value,
                item.event_id,
                item.resource_name,
                path,
                write_exc,
            )
            return CalDAVCreateConflictUIDLookupArtifact(
                uid_query_raw_path=uid_query_raw_path,
                collection_scan_raw_path=collection_scan_raw_path,
            )

        formatted_path = _format_diagnostic_path(path)
        self._logger.info(
            "Saved CalDAV UID lookup diagnostics action=%s event_id=%s path=%s",
            item.action.value,
            item.event_id,
            formatted_path,
        )
        return CalDAVCreateConflictUIDLookupArtifact(
            diagnostics_path=formatted_path,
            uid_query_raw_path=uid_query_raw_path,
            collection_scan_raw_path=collection_scan_raw_path,
        )

    def _maybe_dump_uid_lookup_raw_response(
        self,
        path: Path,
        raw_response: str | None,
        *,
        action: str,
        event_id: str,
        resource_name: str,
        label: str,
    ) -> str | None:
        if raw_response is None:
            return None

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(raw_response, encoding="utf-8")
        except OSError as exc:
            self._logger.warning(
                "Failed to write CalDAV UID lookup raw response action=%s event_id=%s resource_name=%s label=%s path=%s: %s",
                action,
                event_id,
                resource_name,
                label,
                path,
                exc,
            )
            return None

        formatted_path = _format_diagnostic_path(path)
        self._logger.info(
            "Saved CalDAV UID lookup raw response action=%s event_id=%s label=%s path=%s",
            action,
            event_id,
            label,
            formatted_path,
        )
        return formatted_path

    def _maybe_write_create_conflict_state_drift_report(
        self,
        item: PreparedCalDAVAction,
        exc: CalDAVMutationActionError,
        *,
        recorded_at: datetime,
    ) -> CalDAVCreateConflictStateDriftArtifact:
        existing_resource_url = exc.create_conflict_existing_resource_url
        if (
            item.action is not SyncActionType.CREATE
            or exc.status_code != 412
            or not isinstance(existing_resource_url, str)
            or not existing_resource_url
        ):
            return CalDAVCreateConflictStateDriftArtifact()

        local_event = _extract_comparable_event_state(item.ics_payload)
        remote_event: CalDAVComparableEventState | None = None
        remote_etag: str | None = None
        remote_fetch_error: str | None = None
        report_status = "generated"

        try:
            remote_object = self._transport.get_calendar_object_data(existing_resource_url)
        except CalDAVClientError as fetch_exc:
            remote_fetch_error = str(fetch_exc)
            report_status = "remote_fetch_failed"
            self._logger.warning(
                "CalDAV create conflict drift fetch failed for event_id=%s uid=%s resource_url=%s: %s",
                item.event_id,
                item.ics_uid,
                existing_resource_url,
                fetch_exc,
            )
        else:
            remote_etag = remote_object.etag
            remote_event = _extract_comparable_event_state(remote_object.calendar_data)

        comparison = _build_state_drift_comparison(local_event, remote_event)
        diff_count, diff_fields = summarize_state_drift_comparison(comparison)
        report_payload = {
            "generated_at": recorded_at.astimezone(timezone.utc).isoformat(timespec="seconds"),
            "kind": "create_conflict_state_drift_report",
            "action": item.action.value,
            "event_id": item.event_id,
            "ics_uid": item.ics_uid,
            "sequence": item.sequence,
            "resource_name": item.resource_name,
            "attempted_resource_url": exc.resource_url,
            "existing_resource_url": existing_resource_url,
            "payload_summary": dict(item.payload_summary),
            "remote_fetch": {
                "success": remote_fetch_error is None,
                "error": remote_fetch_error,
                "etag": remote_etag,
            },
            "local_event": local_event.to_dict(),
            "remote_event": remote_event.to_dict() if remote_event is not None else None,
            "comparison": comparison,
        }

        path = DEFAULT_CALDAV_REPORTS_DIR / (
            f"{self._build_create_conflict_state_drift_report_filename_base(item, recorded_at=recorded_at)}.json"
        )
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(report_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError as write_exc:
            self._logger.warning(
                "Failed to write CalDAV state drift report action=%s event_id=%s resource_name=%s path=%s: %s",
                item.action.value,
                item.event_id,
                item.resource_name,
                path,
                write_exc,
            )
            return CalDAVCreateConflictStateDriftArtifact(
                path=None,
                status="report_write_failed",
                diff_count=diff_count,
                diff_fields=diff_fields,
                remote_fetch_error=remote_fetch_error,
            )

        formatted_path = _format_diagnostic_path(path)
        self._logger.info(
            "Saved CalDAV state drift report action=%s event_id=%s status=%s path=%s",
            item.action.value,
            item.event_id,
            report_status,
            formatted_path,
        )
        return CalDAVCreateConflictStateDriftArtifact(
            path=formatted_path,
            status=report_status,
            diff_count=diff_count,
            diff_fields=diff_fields,
            remote_fetch_error=remote_fetch_error,
        )

    def _build_create_conflict_state_drift_report_filename_base(
        self,
        item: PreparedCalDAVAction,
        *,
        recorded_at: datetime,
    ) -> str:
        timestamp = recorded_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return (
            "create_state_drift"
            f"__event_{_sanitize_diagnostic_filename_component(item.event_id, default='event')}"
            f"__resource_{_sanitize_diagnostic_filename_component(item.resource_name, default='resource')}"
            f"__seq_{item.sequence}"
            f"__{timestamp}"
        )

    def _should_dump_create_diagnostics(
        self,
        item: PreparedCalDAVAction,
        *,
        success: bool,
    ) -> bool:
        if item.action is not SyncActionType.CREATE:
            return False
        if success:
            return self._settings.diagnostic_dump_success_ics
        return self._settings.diagnostic_dump_failed_ics

    def _build_create_diagnostic_filename_base(
        self,
        item: PreparedCalDAVAction,
        *,
        success: bool,
        recorded_at: datetime,
    ) -> tuple[str, str]:
        timestamp = recorded_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        status_label = "success" if success else "failed"
        return status_label, (
            f"create_{status_label}"
            f"__event_{_sanitize_diagnostic_filename_component(item.event_id, default='event')}"
            f"__resource_{_sanitize_diagnostic_filename_component(item.resource_name, default='resource')}"
            f"__seq_{item.sequence}"
            f"__{timestamp}"
        )

    def _send_update_action(
        self,
        item: PreparedCalDAVAction,
        resolve_calendar_url: Callable[[], str],
    ) -> tuple[CalDAVPutResult, CalDAVActionResolution]:
        initial_resource_url = item.stored_resource_url
        initial_etag = item.stored_etag
        attempted_conditional_update = (
            item.stored_resource_url is not None and item.stored_etag is not None
        )
        if item.stored_resource_url:
            try:
                response = self._transport.put_calendar_object(
                    item.stored_resource_url,
                    item.ics_payload,
                    overwrite=True,
                    etag=item.stored_etag,
                )
                return response, CalDAVActionResolution(
                    resource_url=response.resource_url,
                    resolution_strategy="sync_state_resource_url",
                    used_stored_resource_url=True,
                    uid_lookup_performed=False,
                    used_stored_etag=item.stored_etag is not None,
                    attempted_conditional_update=attempted_conditional_update,
                    initial_resource_url=initial_resource_url,
                    initial_etag=initial_etag,
                )
            except CalDAVClientError as exc:
                recovery = self._recover_resource_failure(
                    item,
                    failed_resource_url=item.stored_resource_url,
                    resolve_calendar_url=resolve_calendar_url,
                    attempted_conditional_update=attempted_conditional_update,
                )
                self._logger.warning(
                    "CalDAV update via stored resource_url failed for event_id=%s uid=%s: %s",
                    item.event_id,
                    item.ics_uid,
                    exc,
                )
                if _should_retry_after_recovery(
                    exc,
                    recovery=recovery,
                    attempted_conditional_update=attempted_conditional_update,
                ):
                    self._logger.info(
                        "Retrying CalDAV update after etag recovery for event_id=%s uid=%s resource_url=%s",
                        item.event_id,
                        item.ics_uid,
                        recovery.refreshed_resource_url,
                    )
                    try:
                        response = self._transport.put_calendar_object(
                            recovery.refreshed_resource_url,
                            item.ics_payload,
                            overwrite=True,
                            etag=recovery.refreshed_etag,
                        )
                    except CalDAVClientError as retry_exc:
                        raise _build_mutation_action_error(
                            retry_exc,
                            resource_url=recovery.refreshed_resource_url,
                            resolution_strategy="sync_state_resource_url",
                            used_stored_resource_url=True,
                            uid_lookup_performed=False,
                            used_stored_etag=item.stored_etag is not None,
                            attempted_conditional_update=True,
                            recovery=recovery,
                            initial_resource_url=initial_resource_url,
                            initial_etag=initial_etag,
                            retry_attempted=True,
                            retry_succeeded=False,
                            retry_count=1,
                            retry_resource_url=recovery.refreshed_resource_url,
                            retry_etag=recovery.refreshed_etag,
                        ) from retry_exc
                    return response, CalDAVActionResolution(
                        resource_url=response.resource_url,
                        resolution_strategy="sync_state_resource_url",
                        used_stored_resource_url=True,
                        uid_lookup_performed=False,
                        used_stored_etag=item.stored_etag is not None,
                        attempted_conditional_update=attempted_conditional_update,
                        recovery_attempted=recovery.attempted,
                        recovery_succeeded=recovery.succeeded,
                        refreshed_resource_url=recovery.refreshed_resource_url,
                        refreshed_etag=recovery.refreshed_etag,
                        initial_resource_url=initial_resource_url,
                        initial_etag=initial_etag,
                        retry_attempted=True,
                        retry_succeeded=True,
                        retry_count=1,
                        retry_resource_url=recovery.refreshed_resource_url,
                        retry_etag=recovery.refreshed_etag,
                    )
                if not _should_retry_with_uid_lookup(
                    exc,
                    attempted_conditional_update=attempted_conditional_update,
                ):
                    raise _build_mutation_action_error(
                        exc,
                        resource_url=item.stored_resource_url,
                        resolution_strategy="sync_state_resource_url",
                        used_stored_resource_url=True,
                        uid_lookup_performed=False,
                        used_stored_etag=item.stored_etag is not None,
                        attempted_conditional_update=attempted_conditional_update,
                        recovery=recovery,
                        initial_resource_url=initial_resource_url,
                        initial_etag=initial_etag,
                    ) from exc
                if recovery.refreshed_resource_url is None:
                    raise CalDAVMutationActionError(
                        f"Stored resource_url update failed for UID {item.ics_uid}, and UID lookup did not find a current resource.",
                        resource_url=item.stored_resource_url,
                        resolution_strategy="sync_state_resource_url_then_uid_lookup",
                        used_stored_resource_url=True,
                        uid_lookup_performed=True,
                        used_stored_etag=item.stored_etag is not None,
                        attempted_conditional_update=attempted_conditional_update,
                        recovery_attempted=recovery.attempted,
                        recovery_succeeded=recovery.succeeded,
                        refreshed_resource_url=recovery.refreshed_resource_url,
                        refreshed_etag=recovery.refreshed_etag,
                        initial_resource_url=initial_resource_url,
                        initial_etag=initial_etag,
                    ) from exc
                try:
                    response = self._transport.put_calendar_object(
                        recovery.refreshed_resource_url,
                        item.ics_payload,
                        overwrite=True,
                    )
                except CalDAVClientError as retry_exc:
                    retry_recovery = self._recover_resource_failure(
                        item,
                        failed_resource_url=recovery.refreshed_resource_url,
                        resolve_calendar_url=resolve_calendar_url,
                        attempted_conditional_update=False,
                    )
                    raise _build_mutation_action_error(
                        retry_exc,
                        resource_url=recovery.refreshed_resource_url,
                        resolution_strategy="sync_state_resource_url_then_uid_lookup",
                        used_stored_resource_url=True,
                        uid_lookup_performed=True,
                        used_stored_etag=item.stored_etag is not None,
                        attempted_conditional_update=attempted_conditional_update,
                        recovery=_merge_recovery_results(recovery, retry_recovery),
                        initial_resource_url=initial_resource_url,
                        initial_etag=initial_etag,
                    ) from retry_exc
                return response, CalDAVActionResolution(
                    resource_url=response.resource_url,
                    resolution_strategy="sync_state_resource_url_then_uid_lookup",
                    used_stored_resource_url=True,
                    uid_lookup_performed=True,
                    used_stored_etag=item.stored_etag is not None,
                    attempted_conditional_update=attempted_conditional_update,
                    recovery_attempted=recovery.attempted,
                    recovery_succeeded=recovery.succeeded,
                    refreshed_resource_url=recovery.refreshed_resource_url,
                    refreshed_etag=recovery.refreshed_etag,
                    initial_resource_url=initial_resource_url,
                    initial_etag=initial_etag,
                )

        existing_resource = self._transport.find_event_resource_by_uid(
            resolve_calendar_url(),
            item.ics_uid,
        )
        initial_resource_url = existing_resource.resource_url if existing_resource is not None else None
        initial_etag = None
        if existing_resource is None:
            raise CalDAVMutationActionError(
                f"Existing CalDAV event with UID {item.ics_uid} was not found.",
                resource_url=None,
                resolution_strategy="uid_lookup",
                used_stored_resource_url=False,
                uid_lookup_performed=True,
                used_stored_etag=False,
                attempted_conditional_update=False,
                initial_resource_url=initial_resource_url,
                initial_etag=initial_etag,
            )
        try:
            response = self._transport.put_calendar_object(
                existing_resource.resource_url,
                item.ics_payload,
                overwrite=True,
            )
        except CalDAVClientError as exc:
            recovery = self._recover_resource_failure(
                item,
                failed_resource_url=existing_resource.resource_url,
                resolve_calendar_url=resolve_calendar_url,
                attempted_conditional_update=False,
            )
            raise _build_mutation_action_error(
                exc,
                resource_url=existing_resource.resource_url,
                resolution_strategy="uid_lookup",
                used_stored_resource_url=False,
                uid_lookup_performed=True,
                used_stored_etag=False,
                attempted_conditional_update=False,
                recovery=recovery,
                initial_resource_url=initial_resource_url,
                initial_etag=initial_etag,
            ) from exc
        return response, CalDAVActionResolution(
            resource_url=response.resource_url,
            resolution_strategy="uid_lookup",
            used_stored_resource_url=False,
            uid_lookup_performed=True,
            used_stored_etag=False,
            attempted_conditional_update=False,
            recovery_attempted=False,
            recovery_succeeded=False,
            refreshed_resource_url=None,
            refreshed_etag=None,
            initial_resource_url=initial_resource_url,
            initial_etag=initial_etag,
        )

    def _send_delete_action(
        self,
        item: PreparedCalDAVAction,
        resolve_calendar_url: Callable[[], str],
    ) -> tuple[CalDAVDeleteResult, CalDAVActionResolution]:
        initial_resource_url = item.stored_resource_url
        initial_etag = item.stored_etag

        if item.stored_resource_url is not None:
            attempted_conditional_delete = item.stored_etag is not None
            try:
                response = self._transport.delete_calendar_object(
                    item.stored_resource_url,
                    etag=item.stored_etag,
                )
                return response, CalDAVActionResolution(
                    resource_url=response.resource_url,
                    resolution_strategy="sync_state_resource_url",
                    used_stored_resource_url=True,
                    uid_lookup_performed=False,
                    used_stored_etag=item.stored_etag is not None,
                    attempted_conditional_update=attempted_conditional_delete,
                    initial_resource_url=initial_resource_url,
                    initial_etag=initial_etag,
                )
            except CalDAVClientError as exc:
                recovery = self._recover_resource_failure(
                    item,
                    failed_resource_url=item.stored_resource_url,
                    resolve_calendar_url=resolve_calendar_url,
                    attempted_conditional_update=attempted_conditional_delete,
                )
                if _should_retry_after_recovery(
                    exc,
                    recovery=recovery,
                    attempted_conditional_update=attempted_conditional_delete,
                ):
                    try:
                        response = self._transport.delete_calendar_object(
                            recovery.refreshed_resource_url,
                            etag=recovery.refreshed_etag,
                        )
                    except CalDAVClientError as retry_exc:
                        retry_recovery = self._recover_resource_failure(
                            item,
                            failed_resource_url=recovery.refreshed_resource_url,
                            resolve_calendar_url=resolve_calendar_url,
                            attempted_conditional_update=True,
                        )
                        if _can_treat_missing_resource_as_deleted(retry_exc, retry_recovery):
                            return self._build_absent_delete_result(
                                resource_url=recovery.refreshed_resource_url or item.stored_resource_url,
                                etag=recovery.refreshed_etag or item.stored_etag,
                                resolution_strategy="sync_state_resource_url",
                                used_stored_resource_url=True,
                                uid_lookup_performed=False,
                                used_stored_etag=item.stored_etag is not None,
                                attempted_conditional_update=True,
                                recovery=_merge_recovery_results(recovery, retry_recovery),
                                initial_resource_url=initial_resource_url,
                                initial_etag=initial_etag,
                                retry_attempted=True,
                                retry_succeeded=False,
                                retry_count=1,
                                retry_resource_url=recovery.refreshed_resource_url,
                                retry_etag=recovery.refreshed_etag,
                            )
                        raise _build_mutation_action_error(
                            retry_exc,
                            resource_url=recovery.refreshed_resource_url,
                            resolution_strategy="sync_state_resource_url",
                            used_stored_resource_url=True,
                            uid_lookup_performed=False,
                            used_stored_etag=item.stored_etag is not None,
                            attempted_conditional_update=True,
                            recovery=_merge_recovery_results(recovery, retry_recovery),
                            initial_resource_url=initial_resource_url,
                            initial_etag=initial_etag,
                            retry_attempted=True,
                            retry_succeeded=False,
                            retry_count=1,
                            retry_resource_url=recovery.refreshed_resource_url,
                            retry_etag=recovery.refreshed_etag,
                        ) from retry_exc
                    return response, CalDAVActionResolution(
                        resource_url=response.resource_url,
                        resolution_strategy="sync_state_resource_url",
                        used_stored_resource_url=True,
                        uid_lookup_performed=False,
                        used_stored_etag=item.stored_etag is not None,
                        attempted_conditional_update=attempted_conditional_delete,
                        recovery_attempted=recovery.attempted,
                        recovery_succeeded=recovery.succeeded,
                        refreshed_resource_url=recovery.refreshed_resource_url,
                        refreshed_etag=recovery.refreshed_etag,
                        initial_resource_url=initial_resource_url,
                        initial_etag=initial_etag,
                        retry_attempted=True,
                        retry_succeeded=True,
                        retry_count=1,
                        retry_resource_url=recovery.refreshed_resource_url,
                        retry_etag=recovery.refreshed_etag,
                    )

                if not _should_retry_with_uid_lookup(
                    exc,
                    attempted_conditional_update=attempted_conditional_delete,
                ):
                    raise _build_mutation_action_error(
                        exc,
                        resource_url=item.stored_resource_url,
                        resolution_strategy="sync_state_resource_url",
                        used_stored_resource_url=True,
                        uid_lookup_performed=False,
                        used_stored_etag=item.stored_etag is not None,
                        attempted_conditional_update=attempted_conditional_delete,
                        recovery=recovery,
                        initial_resource_url=initial_resource_url,
                        initial_etag=initial_etag,
                    ) from exc

                if recovery.refreshed_resource_url is None:
                    return self._build_absent_delete_result(
                        resource_url=item.stored_resource_url,
                        etag=item.stored_etag,
                        resolution_strategy="sync_state_resource_url_then_uid_lookup_absent",
                        used_stored_resource_url=True,
                        uid_lookup_performed=True,
                        used_stored_etag=item.stored_etag is not None,
                        attempted_conditional_update=attempted_conditional_delete,
                        recovery=recovery,
                        initial_resource_url=initial_resource_url,
                        initial_etag=initial_etag,
                    )

                try:
                    response = self._transport.delete_calendar_object(
                        recovery.refreshed_resource_url,
                        etag=recovery.refreshed_etag,
                    )
                except CalDAVClientError as retry_exc:
                    retry_recovery = self._recover_resource_failure(
                        item,
                        failed_resource_url=recovery.refreshed_resource_url,
                        resolve_calendar_url=resolve_calendar_url,
                        attempted_conditional_update=recovery.refreshed_etag is not None,
                    )
                    if _can_treat_missing_resource_as_deleted(retry_exc, retry_recovery):
                        return self._build_absent_delete_result(
                            resource_url=recovery.refreshed_resource_url,
                            etag=recovery.refreshed_etag,
                            resolution_strategy="sync_state_resource_url_then_uid_lookup",
                            used_stored_resource_url=True,
                            uid_lookup_performed=True,
                            used_stored_etag=item.stored_etag is not None,
                            attempted_conditional_update=attempted_conditional_delete,
                            recovery=_merge_recovery_results(recovery, retry_recovery),
                            initial_resource_url=initial_resource_url,
                            initial_etag=initial_etag,
                        )
                    raise _build_mutation_action_error(
                        retry_exc,
                        resource_url=recovery.refreshed_resource_url,
                        resolution_strategy="sync_state_resource_url_then_uid_lookup",
                        used_stored_resource_url=True,
                        uid_lookup_performed=True,
                        used_stored_etag=item.stored_etag is not None,
                        attempted_conditional_update=attempted_conditional_delete,
                        recovery=_merge_recovery_results(recovery, retry_recovery),
                        initial_resource_url=initial_resource_url,
                        initial_etag=initial_etag,
                    ) from retry_exc
                return response, CalDAVActionResolution(
                    resource_url=response.resource_url,
                    resolution_strategy="sync_state_resource_url_then_uid_lookup",
                    used_stored_resource_url=True,
                    uid_lookup_performed=True,
                    used_stored_etag=item.stored_etag is not None,
                    attempted_conditional_update=attempted_conditional_delete,
                    recovery_attempted=recovery.attempted,
                    recovery_succeeded=recovery.succeeded,
                    refreshed_resource_url=recovery.refreshed_resource_url,
                    refreshed_etag=recovery.refreshed_etag,
                    initial_resource_url=initial_resource_url,
                    initial_etag=initial_etag,
                )

        existing_resource = self._transport.find_event_resource_by_uid(
            resolve_calendar_url(),
            item.ics_uid,
        )
        if existing_resource is None:
            return self._build_absent_delete_result(
                resource_url=None,
                etag=None,
                resolution_strategy="uid_lookup_absent",
                used_stored_resource_url=False,
                uid_lookup_performed=True,
                used_stored_etag=False,
                attempted_conditional_update=False,
                initial_resource_url=None,
                initial_etag=None,
            )

        attempted_conditional_delete = existing_resource.etag is not None
        initial_resource_url = existing_resource.resource_url
        initial_etag = existing_resource.etag
        try:
            response = self._transport.delete_calendar_object(
                existing_resource.resource_url,
                etag=existing_resource.etag,
            )
        except CalDAVClientError as exc:
            recovery = self._recover_resource_failure(
                item,
                failed_resource_url=existing_resource.resource_url,
                resolve_calendar_url=resolve_calendar_url,
                attempted_conditional_update=attempted_conditional_delete,
            )
            if _should_retry_after_recovery(
                exc,
                recovery=recovery,
                attempted_conditional_update=attempted_conditional_delete,
            ):
                try:
                    response = self._transport.delete_calendar_object(
                        recovery.refreshed_resource_url,
                        etag=recovery.refreshed_etag,
                    )
                except CalDAVClientError as retry_exc:
                    retry_recovery = self._recover_resource_failure(
                        item,
                        failed_resource_url=recovery.refreshed_resource_url,
                        resolve_calendar_url=resolve_calendar_url,
                        attempted_conditional_update=True,
                    )
                    if _can_treat_missing_resource_as_deleted(retry_exc, retry_recovery):
                        return self._build_absent_delete_result(
                            resource_url=recovery.refreshed_resource_url,
                            etag=recovery.refreshed_etag,
                            resolution_strategy="uid_lookup",
                            used_stored_resource_url=False,
                            uid_lookup_performed=True,
                            used_stored_etag=existing_resource.etag is not None,
                            attempted_conditional_update=attempted_conditional_delete,
                            recovery=_merge_recovery_results(recovery, retry_recovery),
                            initial_resource_url=initial_resource_url,
                            initial_etag=initial_etag,
                            retry_attempted=True,
                            retry_succeeded=False,
                            retry_count=1,
                            retry_resource_url=recovery.refreshed_resource_url,
                            retry_etag=recovery.refreshed_etag,
                        )
                    raise _build_mutation_action_error(
                        retry_exc,
                        resource_url=recovery.refreshed_resource_url,
                        resolution_strategy="uid_lookup",
                        used_stored_resource_url=False,
                        uid_lookup_performed=True,
                        used_stored_etag=existing_resource.etag is not None,
                        attempted_conditional_update=True,
                        recovery=_merge_recovery_results(recovery, retry_recovery),
                        initial_resource_url=initial_resource_url,
                        initial_etag=initial_etag,
                        retry_attempted=True,
                        retry_succeeded=False,
                        retry_count=1,
                        retry_resource_url=recovery.refreshed_resource_url,
                        retry_etag=recovery.refreshed_etag,
                    ) from retry_exc
                return response, CalDAVActionResolution(
                    resource_url=response.resource_url,
                    resolution_strategy="uid_lookup",
                    used_stored_resource_url=False,
                    uid_lookup_performed=True,
                    used_stored_etag=existing_resource.etag is not None,
                    attempted_conditional_update=attempted_conditional_delete,
                    recovery_attempted=recovery.attempted,
                    recovery_succeeded=recovery.succeeded,
                    refreshed_resource_url=recovery.refreshed_resource_url,
                    refreshed_etag=recovery.refreshed_etag,
                    initial_resource_url=initial_resource_url,
                    initial_etag=initial_etag,
                    retry_attempted=True,
                    retry_succeeded=True,
                    retry_count=1,
                    retry_resource_url=recovery.refreshed_resource_url,
                    retry_etag=recovery.refreshed_etag,
                )

            if _can_treat_missing_resource_as_deleted(exc, recovery):
                return self._build_absent_delete_result(
                    resource_url=existing_resource.resource_url,
                    etag=existing_resource.etag,
                    resolution_strategy="uid_lookup",
                    used_stored_resource_url=False,
                    uid_lookup_performed=True,
                    used_stored_etag=existing_resource.etag is not None,
                    attempted_conditional_update=attempted_conditional_delete,
                    recovery=recovery,
                    initial_resource_url=initial_resource_url,
                    initial_etag=initial_etag,
                )

            raise _build_mutation_action_error(
                exc,
                resource_url=existing_resource.resource_url,
                resolution_strategy="uid_lookup",
                used_stored_resource_url=False,
                uid_lookup_performed=True,
                used_stored_etag=existing_resource.etag is not None,
                attempted_conditional_update=attempted_conditional_delete,
                recovery=recovery,
                initial_resource_url=initial_resource_url,
                initial_etag=initial_etag,
            ) from exc
        return response, CalDAVActionResolution(
            resource_url=response.resource_url,
            resolution_strategy="uid_lookup",
            used_stored_resource_url=False,
            uid_lookup_performed=True,
            used_stored_etag=existing_resource.etag is not None,
            attempted_conditional_update=attempted_conditional_delete,
            initial_resource_url=initial_resource_url,
            initial_etag=initial_etag,
        )

    def _build_absent_delete_result(
        self,
        *,
        resource_url: str | None,
        etag: str | None,
        resolution_strategy: str,
        used_stored_resource_url: bool,
        uid_lookup_performed: bool,
        used_stored_etag: bool,
        attempted_conditional_update: bool,
        recovery: CalDAVRecoveryResult | None = None,
        initial_resource_url: str | None = None,
        initial_etag: str | None = None,
        retry_attempted: bool = False,
        retry_succeeded: bool = False,
        retry_count: int = 0,
        retry_resource_url: str | None = None,
        retry_etag: str | None = None,
    ) -> tuple[CalDAVDeleteResult, CalDAVActionResolution]:
        return CalDAVDeleteResult(
            status_code=None,
            resource_url=resource_url,
            etag=etag,
            sent=False,
        ), CalDAVActionResolution(
            resource_url=resource_url,
            resolution_strategy=resolution_strategy,
            used_stored_resource_url=used_stored_resource_url,
            uid_lookup_performed=uid_lookup_performed,
            used_stored_etag=used_stored_etag,
            attempted_conditional_update=attempted_conditional_update,
            recovery_attempted=recovery.attempted if recovery is not None else False,
            recovery_succeeded=recovery.succeeded if recovery is not None else False,
            refreshed_resource_url=recovery.refreshed_resource_url if recovery is not None else None,
            refreshed_etag=recovery.refreshed_etag if recovery is not None else None,
            initial_resource_url=initial_resource_url,
            initial_etag=initial_etag,
            retry_attempted=retry_attempted,
            retry_succeeded=retry_succeeded,
            retry_count=retry_count,
            retry_resource_url=retry_resource_url,
            retry_etag=retry_etag,
        )

    def _recover_resource_failure(
        self,
        item: PreparedCalDAVAction,
        *,
        failed_resource_url: str | None,
        resolve_calendar_url: Callable[[], str],
        attempted_conditional_update: bool,
    ) -> CalDAVRecoveryResult:
        if failed_resource_url is None:
            return CalDAVRecoveryResult()

        recovery_attempted = False
        refreshed_resource_url: str | None = None
        refreshed_etag: str | None = None

        try:
            recovery_attempted = True
            current_resource = self._transport.get_calendar_object(failed_resource_url)
            refreshed_resource_url = current_resource.resource_url
            refreshed_etag = current_resource.etag
        except CalDAVClientError as exc:
            self._logger.info(
                "CalDAV read recovery via resource_url failed for event_id=%s uid=%s resource_url=%s: %s",
                item.event_id,
                item.ics_uid,
                failed_resource_url,
                exc,
            )
            if not _should_lookup_uid_for_recovery(
                _extract_status_code(exc),
                attempted_conditional_update=attempted_conditional_update,
            ):
                return CalDAVRecoveryResult(
                    attempted=recovery_attempted,
                    succeeded=False,
                )

        if refreshed_resource_url is not None and not _requires_uid_lookup_after_recovery(
            refreshed_resource_url=refreshed_resource_url,
            refreshed_etag=refreshed_etag,
            attempted_conditional_update=attempted_conditional_update,
        ):
            return CalDAVRecoveryResult(
                attempted=recovery_attempted,
                succeeded=True,
                refreshed_resource_url=refreshed_resource_url,
                refreshed_etag=refreshed_etag,
            )

        try:
            recovery_attempted = True
            current_resource = self._transport.find_event_resource_by_uid(
                resolve_calendar_url(),
                item.ics_uid,
            )
        except CalDAVClientError as exc:
            self._logger.info(
                "CalDAV read recovery via UID lookup failed for event_id=%s uid=%s: %s",
                item.event_id,
                item.ics_uid,
                exc,
            )
            return CalDAVRecoveryResult(
                attempted=recovery_attempted,
                succeeded=refreshed_resource_url is not None or refreshed_etag is not None,
                refreshed_resource_url=refreshed_resource_url,
                refreshed_etag=refreshed_etag,
            )

        if current_resource is None:
            return CalDAVRecoveryResult(
                attempted=recovery_attempted,
                succeeded=refreshed_resource_url is not None or refreshed_etag is not None,
                refreshed_resource_url=refreshed_resource_url,
                refreshed_etag=refreshed_etag,
            )

        return CalDAVRecoveryResult(
            attempted=recovery_attempted,
            succeeded=True,
            refreshed_resource_url=current_resource.resource_url,
            refreshed_etag=current_resource.etag,
        )


def build_caldav_actions(
    sync_plan: SyncPlan,
    events: list[EventRecord],
    *,
    generated_at: datetime | None = None,
    previous_sync_state: Mapping[str, EventSyncState] | None = None,
) -> list[PreparedCalDAVAction]:
    event_by_id = {event.event_id: event for event in events}
    previous_sync_state = previous_sync_state or {}
    timestamp = generated_at or datetime.now(timezone.utc)
    prepared_actions: list[PreparedCalDAVAction] = []

    for action in sync_plan.actions:
        if action.action is SyncActionType.SKIP:
            continue

        previous_event_state = previous_sync_state.get(action.event_id)
        if action.action in {SyncActionType.CREATE, SyncActionType.UPDATE}:
            event = event_by_id.get(action.event_id)
            if event is None:
                raise ValueError(f"Event '{action.event_id}' referenced by sync_plan was not found.")

            ics_payload = build_calendar(
                [event],
                generated_at=timestamp,
                sequence_by_event_id={event.event_id: action.sequence},
                uid_by_event_id={event.event_id: action.ics_uid},
            )
            payload_summary = _build_payload_summary(event)
        else:
            ics_payload = ""
            payload_summary = _build_delete_payload_summary(action, previous_event_state)

        prepared_actions.append(
            PreparedCalDAVAction(
                action=action.action,
                event_id=action.event_id,
                ics_uid=action.ics_uid,
                sequence=action.sequence,
                action_reason=action.action_reason,
                reappeared_from_tombstone=action.reappeared_from_tombstone,
                tombstone_deleted_at=action.tombstone_deleted_at,
                updated_at=action.updated_at,
                resource_name=build_caldav_resource_name(action),
                ics_payload=ics_payload,
                payload_summary=payload_summary,
                stored_resource_url=previous_event_state.resource_url if previous_event_state is not None else None,
                stored_etag=previous_event_state.etag if previous_event_state is not None else None,
            )
        )

    return prepared_actions


def build_ignored_actions(sync_plan: SyncPlan) -> list[IgnoredSyncPlanAction]:
    ignored: list[IgnoredSyncPlanAction] = []
    for action in sync_plan.actions:
        if action.action in {SyncActionType.CREATE, SyncActionType.UPDATE, SyncActionType.DELETE}:
            continue
        ignored.append(
            IgnoredSyncPlanAction(
                action=action.action.value,
                event_id=action.event_id,
                ics_uid=action.ics_uid,
                reason="no_delivery_required",
            )
        )
    return ignored


def save_caldav_sync_report(path: Path, report: CalDAVSyncReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_caldav_resource_name(action: SyncPlanAction) -> str:
    safe_uid = action.ics_uid.replace("@", "_at_")
    safe_uid = "".join(char if char.isalnum() or char in {"-", "_", "."} else "-" for char in safe_uid)
    safe_uid = safe_uid.strip("-") or action.event_id or "event"
    return f"{safe_uid}.ics"


def _build_dry_run_result(
    item: PreparedCalDAVAction,
    *,
    diagnostic_payload_path: str | None = None,
) -> CalDAVActionResult:
    return CalDAVActionResult(
        action=item.action.value,
        event_id=item.event_id,
        ics_uid=item.ics_uid,
        sequence=item.sequence,
        dry_run=True,
        success=True,
        sent=False,
        action_reason=item.action_reason,
        reappeared_from_tombstone=item.reappeared_from_tombstone,
        tombstone_deleted_at=item.tombstone_deleted_at,
        resource_name=item.resource_name,
        resource_url=None,
        etag=None,
        updated_at=item.updated_at,
        delivered_at=None,
        payload_summary=item.payload_summary,
        payload_bytes=len(item.ics_payload.encode("utf-8")),
        diagnostic_payload_path=diagnostic_payload_path,
        payload_sequence=item.sequence if item.action in {SyncActionType.CREATE, SyncActionType.UPDATE} else None,
        resolution_strategy=(
            "sync_state_resource_url"
            if item.action in {SyncActionType.UPDATE, SyncActionType.DELETE} and item.stored_resource_url
            else "uid_lookup"
            if item.action in {SyncActionType.UPDATE, SyncActionType.DELETE}
            else "create_resource_name"
        ),
        used_stored_resource_url=(
            item.action in {SyncActionType.UPDATE, SyncActionType.DELETE}
            and item.stored_resource_url is not None
        ),
        uid_lookup_performed=(
            item.action in {SyncActionType.UPDATE, SyncActionType.DELETE}
            and item.stored_resource_url is None
        ),
        used_stored_etag=(
            item.action in {SyncActionType.UPDATE, SyncActionType.DELETE}
            and item.stored_resource_url is not None
            and item.stored_etag is not None
        ),
        attempted_conditional_update=(
            item.action in {SyncActionType.UPDATE, SyncActionType.DELETE}
            and item.stored_resource_url is not None
            and item.stored_etag is not None
        ),
        retry_attempted=False,
        retry_succeeded=False,
        retry_count=0,
    )


def _build_failure_result(
    item: PreparedCalDAVAction,
    error: str,
    *,
    diagnostic_payload_path: str | None = None,
    diagnostic_request_response_path: str | None = None,
    resolution_strategy: str | None = None,
    used_stored_resource_url: bool = False,
    uid_lookup_performed: bool = False,
    used_stored_etag: bool = False,
    resource_url: str | None = None,
    status_code: int | None = None,
    conflict_kind: str | None = None,
    retryable: bool = False,
    etag_mismatch: bool = False,
    attempted_conditional_update: bool = False,
    recovery_attempted: bool = False,
    recovery_succeeded: bool = False,
    refreshed_resource_url: str | None = None,
    refreshed_etag: str | None = None,
    initial_resource_url: str | None = None,
    initial_etag: str | None = None,
    retry_attempted: bool = False,
    retry_succeeded: bool = False,
    retry_count: int = 0,
    retry_resource_url: str | None = None,
    retry_etag: str | None = None,
    create_conflict_resource_exists: bool = False,
    create_conflict_uid_match_found: bool = False,
    create_conflict_uid_lookup_attempted: bool = False,
    create_conflict_uid_lookup_candidates: int = 0,
    create_conflict_uid_lookup_method: str | None = None,
    create_conflict_remote_uid_confirmed: bool = False,
    create_conflict_state_drift_suspected: bool = False,
    create_conflict_existing_resource_url: str | None = None,
    create_conflict_selected_candidate_reason: str | None = None,
    create_conflict_selected_candidate_index: int | None = None,
    create_conflict_uid_lookup_raw_candidates: list[dict[str, Any]] | None = None,
    create_conflict_uid_lookup_diagnostics_path: str | None = None,
    create_conflict_uid_query_raw_path: str | None = None,
    create_conflict_collection_scan_raw_path: str | None = None,
    create_conflict_candidate_ranking: list[dict[str, Any]] | None = None,
    create_conflict_state_drift_report_path: str | None = None,
    create_conflict_state_drift_report_status: str | None = None,
    drift_report_status: str | None = None,
    drift_diff_count: int | None = None,
    drift_diff_fields: list[str] | None = None,
    create_conflict_remote_fetch_error: str | None = None,
    request_response_diagnostics: CalDAVRequestResponseDiagnostics | None = None,
    error_kind: str | None = None,
) -> CalDAVActionResult:
    return CalDAVActionResult(
        action=item.action.value,
        event_id=item.event_id,
        ics_uid=item.ics_uid,
        sequence=item.sequence,
        dry_run=False,
        success=False,
        sent=False,
        action_reason=item.action_reason,
        reappeared_from_tombstone=item.reappeared_from_tombstone,
        tombstone_deleted_at=item.tombstone_deleted_at,
        resource_name=item.resource_name,
        resource_url=resource_url,
        etag=None,
        updated_at=item.updated_at,
        delivered_at=None,
        payload_summary=item.payload_summary,
        payload_bytes=len(item.ics_payload.encode("utf-8")),
        diagnostic_payload_path=diagnostic_payload_path,
        payload_sequence=item.sequence if item.action in {SyncActionType.CREATE, SyncActionType.UPDATE} else None,
        resolution_strategy=resolution_strategy,
        used_stored_resource_url=used_stored_resource_url,
        uid_lookup_performed=uid_lookup_performed,
        used_stored_etag=used_stored_etag,
        conflict_kind=conflict_kind,
        retryable=retryable,
        etag_mismatch=etag_mismatch,
        attempted_conditional_update=attempted_conditional_update,
        recovery_attempted=recovery_attempted,
        recovery_succeeded=recovery_succeeded,
        refreshed_resource_url=refreshed_resource_url,
        refreshed_etag=refreshed_etag,
        initial_resource_url=initial_resource_url,
        initial_etag=initial_etag,
        retry_attempted=retry_attempted,
        retry_succeeded=retry_succeeded,
        retry_count=retry_count,
        retry_resource_url=retry_resource_url,
        retry_etag=retry_etag,
        create_conflict_resource_exists=create_conflict_resource_exists,
        create_conflict_uid_match_found=create_conflict_uid_match_found,
        create_conflict_uid_lookup_attempted=create_conflict_uid_lookup_attempted,
        create_conflict_uid_lookup_candidates=create_conflict_uid_lookup_candidates,
        create_conflict_uid_lookup_method=create_conflict_uid_lookup_method,
        create_conflict_remote_uid_confirmed=create_conflict_remote_uid_confirmed,
        create_conflict_state_drift_suspected=create_conflict_state_drift_suspected,
        create_conflict_existing_resource_url=create_conflict_existing_resource_url,
        create_conflict_selected_candidate_reason=create_conflict_selected_candidate_reason,
        create_conflict_selected_candidate_index=create_conflict_selected_candidate_index,
        create_conflict_uid_lookup_raw_candidates=list(create_conflict_uid_lookup_raw_candidates or []),
        create_conflict_uid_lookup_diagnostics_path=create_conflict_uid_lookup_diagnostics_path,
        create_conflict_uid_query_raw_path=create_conflict_uid_query_raw_path,
        create_conflict_collection_scan_raw_path=create_conflict_collection_scan_raw_path,
        create_conflict_candidate_ranking=list(create_conflict_candidate_ranking or []),
        create_conflict_state_drift_report_path=create_conflict_state_drift_report_path,
        create_conflict_state_drift_report_status=create_conflict_state_drift_report_status,
        drift_report_status=drift_report_status,
        drift_diff_count=drift_diff_count,
        drift_diff_fields=drift_diff_fields,
        create_conflict_remote_fetch_error=create_conflict_remote_fetch_error,
        request_method=(
            request_response_diagnostics.request_method
            if request_response_diagnostics is not None
            else None
        ),
        request_url=(
            request_response_diagnostics.request_url
            if request_response_diagnostics is not None
            else None
        ),
        request_headers=(
            dict(request_response_diagnostics.request_headers)
            if request_response_diagnostics is not None
            else None
        ),
        response_headers=(
            dict(request_response_diagnostics.response_headers)
            if request_response_diagnostics is not None
            else None
        ),
        response_body_excerpt=(
            request_response_diagnostics.response_body_excerpt
            if request_response_diagnostics is not None
            else None
        ),
        diagnostic_request_response_path=diagnostic_request_response_path,
        status_code=status_code,
        error_kind=error_kind,
        error=error,
    )


def _sanitize_diagnostic_filename_component(value: str, *, default: str) -> str:
    sanitized = "".join(
        char if char.isalnum() or char in {"-", "_", "."} else "-"
        for char in value
    )
    sanitized = re.sub(r"-{2,}", "-", sanitized).strip("-.")
    return sanitized or default


def _format_diagnostic_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(_REPO_ROOT))
    except ValueError:
        return str(resolved)


def _build_request_response_diagnostics(
    method: str,
    url: str,
    *,
    headers: Mapping[str, str],
    data: str | bytes,
    response_headers: Mapping[str, str] | None,
    response_body_excerpt: str | None,
) -> CalDAVRequestResponseDiagnostics:
    return CalDAVRequestResponseDiagnostics(
        request_method=method,
        request_url=url,
        request_headers={
            "If-None-Match": headers.get("If-None-Match"),
            "If-Match": headers.get("If-Match"),
            "Content-Type": headers.get("Content-Type"),
            "Content-Length": _calculate_content_length(data),
        },
        response_headers={
            "ETag": _get_header_value(response_headers, "ETag"),
            "Content-Type": _get_header_value(response_headers, "Content-Type"),
            "Content-Length": _get_header_value(response_headers, "Content-Length"),
            "Location": _get_header_value(response_headers, "Location"),
        },
        response_body_excerpt=response_body_excerpt,
    )


def _build_response_body_excerpt(body: str) -> str | None:
    normalized = body.strip().replace("\n", " ")[:240]
    return normalized or None


def _calculate_content_length(data: str | bytes) -> int:
    if isinstance(data, bytes):
        return len(data)
    return len(data.encode("utf-8"))


def _get_header_value(headers: Mapping[str, str] | None, name: str) -> str | None:
    if headers is None:
        return None
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


def _extract_request_response_diagnostics(
    exc: CalDAVClientError,
) -> CalDAVRequestResponseDiagnostics | None:
    diagnostics = getattr(exc, "request_response_diagnostics", None)
    if isinstance(diagnostics, CalDAVRequestResponseDiagnostics):
        return diagnostics
    return None


def _build_mutation_action_error(
    exc: CalDAVClientError,
    *,
    resource_url: str | None,
    resolution_strategy: str,
    used_stored_resource_url: bool,
    uid_lookup_performed: bool,
    used_stored_etag: bool,
    attempted_conditional_update: bool,
    recovery: CalDAVRecoveryResult | None = None,
    initial_resource_url: str | None = None,
    initial_etag: str | None = None,
    retry_attempted: bool = False,
    retry_succeeded: bool = False,
    retry_count: int = 0,
    retry_resource_url: str | None = None,
    retry_etag: str | None = None,
    create_conflict_diagnosis: CalDAVCreateConflictDiagnosis | None = None,
) -> CalDAVMutationActionError:
    status_code = _extract_status_code(exc)
    conflict_kind, retryable, etag_mismatch = _classify_conflict(
        status_code,
        attempted_conditional_update=attempted_conditional_update,
    )
    return CalDAVMutationActionError(
        str(exc),
        resource_url=resource_url,
        resolution_strategy=resolution_strategy,
        used_stored_resource_url=used_stored_resource_url,
        uid_lookup_performed=uid_lookup_performed,
        used_stored_etag=used_stored_etag,
        status_code=status_code,
        conflict_kind=conflict_kind,
        retryable=retryable,
        etag_mismatch=etag_mismatch,
        attempted_conditional_update=attempted_conditional_update,
        recovery_attempted=recovery.attempted if recovery is not None else False,
        recovery_succeeded=recovery.succeeded if recovery is not None else False,
        refreshed_resource_url=recovery.refreshed_resource_url if recovery is not None else None,
        refreshed_etag=recovery.refreshed_etag if recovery is not None else None,
        initial_resource_url=initial_resource_url,
        initial_etag=initial_etag,
        retry_attempted=retry_attempted,
        retry_succeeded=retry_succeeded,
        retry_count=retry_count,
        retry_resource_url=retry_resource_url,
        retry_etag=retry_etag,
        create_conflict_resource_exists=(
            create_conflict_diagnosis.resource_exists if create_conflict_diagnosis is not None else False
        ),
        create_conflict_uid_match_found=(
            create_conflict_diagnosis.uid_match_found if create_conflict_diagnosis is not None else False
        ),
        create_conflict_uid_lookup_attempted=(
            create_conflict_diagnosis.uid_lookup_attempted
            if create_conflict_diagnosis is not None
            else False
        ),
        create_conflict_uid_lookup_candidates=(
            create_conflict_diagnosis.uid_lookup_candidates
            if create_conflict_diagnosis is not None
            else 0
        ),
        create_conflict_uid_lookup_method=(
            create_conflict_diagnosis.uid_lookup_method
            if create_conflict_diagnosis is not None
            else None
        ),
        create_conflict_remote_uid_confirmed=(
            create_conflict_diagnosis.remote_uid_confirmed
            if create_conflict_diagnosis is not None
            else False
        ),
        create_conflict_state_drift_suspected=(
            create_conflict_diagnosis.state_drift_suspected
            if create_conflict_diagnosis is not None
            else False
        ),
        create_conflict_existing_resource_url=(
            create_conflict_diagnosis.existing_resource_url
            if create_conflict_diagnosis is not None
            else None
        ),
        create_conflict_selected_candidate_reason=(
            create_conflict_diagnosis.selected_candidate_reason
            if create_conflict_diagnosis is not None
            else None
        ),
        create_conflict_selected_candidate_index=(
            create_conflict_diagnosis.selected_candidate_index
            if create_conflict_diagnosis is not None
            else None
        ),
        create_conflict_uid_lookup_raw_candidates=(
            create_conflict_diagnosis.uid_lookup_raw_candidates
            if create_conflict_diagnosis is not None
            else []
        ),
        create_conflict_uid_query_raw_response=(
            create_conflict_diagnosis.uid_query_raw_response
            if create_conflict_diagnosis is not None
            else None
        ),
        create_conflict_collection_scan_raw_response=(
            create_conflict_diagnosis.collection_scan_raw_response
            if create_conflict_diagnosis is not None
            else None
        ),
        create_conflict_candidate_ranking=(
            create_conflict_diagnosis.candidate_ranking
            if create_conflict_diagnosis is not None
            else []
        ),
        request_response_diagnostics=_extract_request_response_diagnostics(exc),
    )


def _should_retry_with_uid_lookup(
    exc: CalDAVClientError,
    *,
    attempted_conditional_update: bool,
) -> bool:
    status_code = _extract_status_code(exc)
    if status_code in {404, 410}:
        return True
    return status_code == 412 and not attempted_conditional_update


def _should_retry_after_recovery(
    exc: CalDAVClientError,
    *,
    recovery: CalDAVRecoveryResult,
    attempted_conditional_update: bool,
) -> bool:
    conflict_kind, _, _ = _classify_conflict(
        _extract_status_code(exc),
        attempted_conditional_update=attempted_conditional_update,
    )
    if conflict_kind != "etag_mismatch":
        return False
    return (
        recovery.succeeded
        and recovery.refreshed_resource_url is not None
        and recovery.refreshed_etag is not None
    )


def _can_treat_missing_resource_as_deleted(
    exc: CalDAVClientError,
    recovery: CalDAVRecoveryResult | None = None,
) -> bool:
    status_code = _extract_status_code(exc)
    if status_code not in {404, 410}:
        return False
    return recovery is None or recovery.refreshed_resource_url is None


def _should_lookup_uid_for_recovery(
    status_code: int | None,
    *,
    attempted_conditional_update: bool,
) -> bool:
    if status_code in {404, 410}:
        return True
    return status_code == 412 and not attempted_conditional_update


def _requires_uid_lookup_after_recovery(
    *,
    refreshed_resource_url: str | None,
    refreshed_etag: str | None,
    attempted_conditional_update: bool,
) -> bool:
    if refreshed_resource_url is None:
        return True
    if attempted_conditional_update:
        return False
    return refreshed_etag is None


def _merge_recovery_results(
    initial: CalDAVRecoveryResult,
    follow_up: CalDAVRecoveryResult,
) -> CalDAVRecoveryResult:
    return CalDAVRecoveryResult(
        attempted=initial.attempted or follow_up.attempted,
        succeeded=follow_up.succeeded or initial.succeeded,
        refreshed_resource_url=follow_up.refreshed_resource_url or initial.refreshed_resource_url,
        refreshed_etag=follow_up.refreshed_etag or initial.refreshed_etag,
    )


def _extract_status_code(exc: BaseException) -> int | None:
    if isinstance(exc, CalDAVDiscoveryError):
        return exc.status_code
    if isinstance(exc, CalDAVHTTPError):
        return exc.status_code
    return None


def _classify_conflict(
    status_code: int | None,
    *,
    attempted_conditional_update: bool,
) -> tuple[str | None, bool, bool]:
    if status_code == 412:
        if attempted_conditional_update:
            return "etag_mismatch", True, True
        return "precondition_failed", True, False
    if status_code == 409:
        return "conflict", True, False
    return None, False, False


def _build_sync_failure_context_for_action(
    item: PreparedCalDAVAction,
    *,
    processed_count: int,
    total_count: int,
    action_index: int,
    resource_url: str | None = None,
) -> CalDAVSyncFailureContext:
    return CalDAVSyncFailureContext(
        action=item.action.value,
        event_id=item.event_id,
        ics_uid=item.ics_uid,
        resource_url=resource_url or item.stored_resource_url,
        processed_count=processed_count,
        remaining_count=max(total_count - action_index, 0),
        total_count=total_count,
        action_index=action_index,
    )


def _attach_sync_failure_context(
    exc: BaseException,
    context: CalDAVSyncFailureContext,
) -> None:
    setattr(exc, "caldav_sync_context", context)


def _extract_sync_failure_context(
    exc: BaseException,
    *,
    sync_plan: SyncPlan,
    prepared_actions: list[PreparedCalDAVAction],
) -> CalDAVSyncFailureContext:
    existing = getattr(exc, "caldav_sync_context", None)
    if isinstance(existing, CalDAVSyncFailureContext):
        return existing

    inferred = _infer_sync_failure_context_from_sync_plan(exc, sync_plan)
    if inferred is not None:
        return inferred

    return CalDAVSyncFailureContext(
        resource_url=_extract_resource_url_from_exception(exc),
        processed_count=0,
        remaining_count=len(prepared_actions),
        total_count=len(prepared_actions),
    )


def _infer_sync_failure_context_from_sync_plan(
    exc: BaseException,
    sync_plan: SyncPlan,
) -> CalDAVSyncFailureContext | None:
    match = _SYNC_PLAN_EVENT_ID_PATTERN.search(str(exc))
    if match is None:
        return None

    event_id = match.group(1)
    deliverable_actions = [
        action
        for action in sync_plan.actions
        if action.action in {SyncActionType.CREATE, SyncActionType.UPDATE, SyncActionType.DELETE}
    ]

    for index, action in enumerate(deliverable_actions, start=1):
        if action.event_id != event_id:
            continue
        total_count = len(deliverable_actions)
        return CalDAVSyncFailureContext(
            action=action.action.value,
            event_id=action.event_id,
            ics_uid=action.ics_uid,
            processed_count=0,
            remaining_count=max(total_count - 1, 0),
            total_count=total_count,
            action_index=index,
        )
    return None


def _extract_resource_url_from_exception(exc: BaseException) -> str | None:
    if isinstance(exc, CalDAVDiscoveryError):
        return exc.url
    if isinstance(exc, CalDAVMutationActionError):
        return exc.resource_url
    if isinstance(exc, CalDAVHTTPError):
        return exc.url
    return None


def _classify_sync_failure_error_kind(exc: BaseException) -> str:
    if isinstance(exc, CalDAVDiscoveryError):
        return exc.error_kind
    if isinstance(exc, CalDAVMutationActionError):
        if exc.conflict_kind is not None:
            return exc.conflict_kind
        if exc.status_code is not None:
            return "http_failed"
    if isinstance(exc, CalDAVHTTPError):
        return "http_failed"
    return classify_exception_error_kind(exc)


def _log_sync_failure(
    logger: logging.Logger,
    exc: BaseException,
    *,
    sync_plan: SyncPlan,
    prepared_actions: list[PreparedCalDAVAction],
) -> None:
    context = _extract_sync_failure_context(
        exc,
        sync_plan=sync_plan,
        prepared_actions=prepared_actions,
    )
    fields = {
        "component": "caldav",
        "phase": "sync",
        "error_kind": _classify_sync_failure_error_kind(exc),
        "action": context.action,
        "event_id": context.event_id,
        "ics_uid": context.ics_uid,
        "resource_url": context.resource_url,
        "status_code": _extract_status_code(exc),
        "processed_count": context.processed_count,
        "remaining_count": context.remaining_count,
        "total_count": context.total_count,
        "action_index": context.action_index,
        "error": str(exc),
    }
    log_structured_error(
        logger,
        "caldav sync failure",
        fields=fields,
    )


def _build_payload_summary(event: EventRecord) -> dict[str, Any]:
    location = ", ".join(facility.name for facility in event.facilities if facility.name)
    return {
        "summary": _build_event_summary(event),
        "subject": event.subject,
        "start": event.start.date_time if event.start is not None else None,
        "end": event.end.date_time if event.end is not None else None,
        "is_all_day": event.is_all_day,
        "has_description": bool(event.notes and event.notes.strip()),
        "has_location": bool(location),
    }


def _build_delete_payload_summary(
    action: SyncPlanAction,
    previous_state: EventSyncState | None,
) -> dict[str, Any]:
    return {
        "delete_target_event_id": action.event_id,
        "stored_resource_url": previous_state.resource_url if previous_state is not None else None,
        "has_stored_etag": previous_state.etag is not None if previous_state is not None else False,
    }


def _build_event_summary(event: EventRecord) -> str:
    if event.subject.strip():
        return event.subject
    if event.event_menu and event.event_menu.strip():
        return event.event_menu
    return "(no title)"


def _ensure_trailing_slash(value: str) -> str:
    return value if value.endswith("/") else f"{value}/"


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _build_calendar_lookup_request() -> str:
    return _build_propfind_request(
        "<d:displayname />",
        "<d:resourcetype />",
    )


def _build_principal_lookup_request() -> str:
    return _build_propfind_request(
        "<d:current-user-principal />",
        "<d:principal-URL />",
        "<d:resourcetype />",
    )


def _build_calendar_home_lookup_request() -> str:
    return _build_propfind_request("<c:calendar-home-set />")


def _build_propfind_request(*properties: str) -> str:
    props = "".join(properties)
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">'
        f"<d:prop>{props}</d:prop>"
        "</d:propfind>"
    )


def _build_calendar_query_request(
    *,
    uid: str | None,
    include_calendar_data: bool,
) -> str:
    props = ["<d:getetag />"]
    if include_calendar_data:
        props.append("<c:calendar-data />")
    prop_filter = ""
    if uid is not None:
        escaped_uid = _escape_xml(uid)
        prop_filter = (
            '<c:prop-filter name="UID">'
            f'<c:text-match collation="i;octet" match-type="equals">{escaped_uid}</c:text-match>'
            "</c:prop-filter>"
        )
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<c:calendar-query xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">'
        "<d:prop>"
        f"{''.join(props)}"
        "</d:prop>"
        "<c:filter>"
        '<c:comp-filter name="VCALENDAR">'
        '<c:comp-filter name="VEVENT">'
        f"{prop_filter}"
        "</c:comp-filter>"
        "</c:comp-filter>"
        "</c:filter>"
        "</c:calendar-query>"
    )


def _escape_xml(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _parse_xml(value: str) -> ET.Element:
    try:
        return ET.fromstring(value)
    except ET.ParseError as exc:
        raise CalDAVClientError("CalDAV server returned invalid XML.") from exc


def _iter_response_nodes(root: ET.Element) -> list[ET.Element]:
    if root.tag != _MULTISTATUS_TAG:
        raise CalDAVClientError("CalDAV server did not return a DAV:multistatus response.")
    return list(root.findall(_RESPONSE_TAG))


def _find_response_href(node: ET.Element) -> str | None:
    href = node.findtext(_HREF_TAG)
    if href is None:
        return None
    stripped = href.strip()
    return stripped or None


def _find_text(node: ET.Element, tag: str) -> str | None:
    for child in node.iter():
        if child.tag == tag and child.text:
            return child.text.strip()
    return None


def _find_successful_propstat_text(node: ET.Element, tag: str) -> str | None:
    for propstat in node.findall(f"{{{_DAV_NAMESPACE}}}propstat"):
        if not _propstat_is_success(propstat):
            continue
        prop = propstat.find(f"{{{_DAV_NAMESPACE}}}prop")
        if prop is None:
            continue
        text = _find_text(prop, tag)
        if text:
            return text
    return None


def _propstat_is_success(propstat: ET.Element) -> bool:
    status = propstat.findtext(f"{{{_DAV_NAMESPACE}}}status")
    if status is None:
        return True
    return bool(re.search(r"\s2\d{2}\s", status))


def _find_nested_href(node: ET.Element, tag: str) -> str | None:
    for child in node.iter():
        if child.tag != tag:
            continue
        href = _find_text(child, _HREF_TAG)
        if href:
            return href
    return None


def _response_has_resource_type(node: ET.Element, resource_type_tag: str) -> bool:
    return any(child.tag == resource_type_tag for child in node.iter())


def _response_has_calendar_resource(node: ET.Element) -> bool:
    resource_types = list(node.iterfind(f".//{_COLLECTION_TAG}"))
    if not resource_types:
        return False
    return _response_has_resource_type(node, _CALENDAR_TAG)


def _extract_uid_from_calendar_data(calendar_data: str | None) -> str | None:
    if not calendar_data:
        return None
    unfolded = _ICS_LINE_FOLDING_PATTERN.sub("", calendar_data)
    match = _ICS_UID_PATTERN.search(unfolded)
    if match is None:
        return None
    return match.group(1).strip()


def _extract_comparable_event_state(calendar_data: str | None) -> CalDAVComparableEventState:
    if not calendar_data:
        return CalDAVComparableEventState()

    unfolded = _ICS_LINE_FOLDING_PATTERN.sub("", calendar_data)
    tracked_fields: dict[str, str | None] = {}
    inside_vevent = False

    for raw_line in unfolded.splitlines():
        line = raw_line.strip()
        if line == "BEGIN:VEVENT":
            inside_vevent = True
            continue
        if line == "END:VEVENT":
            break
        if not inside_vevent or ":" not in line:
            continue

        name_part, value = line.split(":", 1)
        field_name = name_part.split(";", 1)[0].upper()
        if field_name not in {
            "UID",
            "SUMMARY",
            "DTSTART",
            "DTEND",
            "DESCRIPTION",
            "LOCATION",
            "SEQUENCE",
            "LAST-MODIFIED",
        }:
            continue
        tracked_fields.setdefault(field_name, value.strip() or None)

    return CalDAVComparableEventState(
        uid=tracked_fields.get("UID"),
        summary=tracked_fields.get("SUMMARY"),
        dtstart=tracked_fields.get("DTSTART"),
        dtend=tracked_fields.get("DTEND"),
        has_description=bool(tracked_fields.get("DESCRIPTION")),
        has_location=bool(tracked_fields.get("LOCATION")),
        sequence=tracked_fields.get("SEQUENCE"),
        last_modified=tracked_fields.get("LAST-MODIFIED"),
    )


def _build_state_drift_comparison(
    local_event: CalDAVComparableEventState,
    remote_event: CalDAVComparableEventState | None,
) -> dict[str, dict[str, Any]]:
    remote_available = remote_event is not None
    return {
        "UID": _build_state_drift_value_comparison(
            local_event.uid,
            remote_event.uid if remote_event is not None else None,
            remote_available=remote_available,
        ),
        "SUMMARY": _build_state_drift_value_comparison(
            local_event.summary,
            remote_event.summary if remote_event is not None else None,
            remote_available=remote_available,
        ),
        "DTSTART": _build_state_drift_value_comparison(
            local_event.dtstart,
            remote_event.dtstart if remote_event is not None else None,
            remote_available=remote_available,
        ),
        "DTEND": _build_state_drift_value_comparison(
            local_event.dtend,
            remote_event.dtend if remote_event is not None else None,
            remote_available=remote_available,
        ),
        "DESCRIPTION": _build_state_drift_presence_comparison(
            local_event.has_description,
            remote_event.has_description if remote_event is not None else None,
            remote_available=remote_available,
        ),
        "LOCATION": _build_state_drift_presence_comparison(
            local_event.has_location,
            remote_event.has_location if remote_event is not None else None,
            remote_available=remote_available,
        ),
        "SEQUENCE": _build_state_drift_value_comparison(
            local_event.sequence,
            remote_event.sequence if remote_event is not None else None,
            remote_available=remote_available,
        ),
        "LAST-MODIFIED": _build_state_drift_value_comparison(
            local_event.last_modified,
            remote_event.last_modified if remote_event is not None else None,
            remote_available=remote_available,
        ),
    }


def _build_state_drift_value_comparison(
    local_value: str | None,
    remote_value: str | None,
    *,
    remote_available: bool,
) -> dict[str, Any]:
    return {
        "local": local_value,
        "remote": remote_value,
        "equal": local_value == remote_value if remote_available else None,
    }


def _build_state_drift_presence_comparison(
    local_present: bool,
    remote_present: bool | None,
    *,
    remote_available: bool,
) -> dict[str, Any]:
    return {
        "local_present": local_present,
        "remote_present": remote_present,
        "equal": local_present == remote_present if remote_available else None,
    }


def _serialize_uid_lookup_candidates(
    candidates: list[CalDAVUIDLookupCandidate],
) -> list[dict[str, Any]]:
    return [
        {
            "href": item.resource_url,
            "etag": item.etag,
            "parsed_remote_uid": item.remote_uid,
            "summary": item.summary,
            "dtstart": item.dtstart,
            "dtend": item.dtend,
            "found_via": list(item.found_via),
        }
        for item in candidates
    ]


def _build_uid_lookup_candidate_ranking(
    local_event: CalDAVComparableEventState,
    candidates: list[CalDAVUIDLookupCandidate],
) -> list[dict[str, Any]]:
    ranking: list[dict[str, Any]] = []
    local_summary = _normalize_uid_lookup_summary(local_event.summary)

    for index, item in enumerate(candidates):
        remote_summary = _normalize_uid_lookup_summary(item.summary)
        summary_exact_match = bool(local_summary and remote_summary and local_summary == remote_summary)
        summary_partial_match = (
            bool(local_summary and remote_summary)
            and not summary_exact_match
            and (local_summary in remote_summary or remote_summary in local_summary)
        )
        dtstart_match = bool(local_event.dtstart and item.dtstart and local_event.dtstart == item.dtstart)
        dtend_match = bool(local_event.dtend and item.dtend and local_event.dtend == item.dtend)
        score = (
            (100 if summary_exact_match else 0)
            + (30 if dtstart_match else 0)
            + (30 if dtend_match else 0)
            + (10 if summary_partial_match else 0)
        )
        ranking.append(
            {
                "rank": 0,
                "candidate_index": index,
                "href": item.resource_url,
                "etag": item.etag,
                "parsed_remote_uid": item.remote_uid,
                "summary": item.summary,
                "dtstart": item.dtstart,
                "dtend": item.dtend,
                "found_via": list(item.found_via),
                "summary_exact_match": summary_exact_match,
                "dtstart_match": dtstart_match,
                "dtend_match": dtend_match,
                "summary_partial_match": summary_partial_match,
                "score": score,
            }
        )

    ranking.sort(key=lambda item: (-int(item["score"]), int(item["candidate_index"])))
    for rank, entry in enumerate(ranking, start=1):
        entry["rank"] = rank
    return ranking


def _normalize_uid_lookup_summary(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.split()).strip().casefold()
    return normalized or None


def _tag_uid_lookup_candidate(
    candidate: CalDAVUIDLookupCandidate,
    lookup_method: str,
) -> CalDAVUIDLookupCandidate:
    if lookup_method in candidate.found_via:
        return candidate
    return replace(candidate, found_via=(*candidate.found_via, lookup_method))


def _merge_uid_lookup_candidate(
    existing: CalDAVUIDLookupCandidate,
    new: CalDAVUIDLookupCandidate,
) -> CalDAVUIDLookupCandidate:
    found_via = tuple(dict.fromkeys((*existing.found_via, *new.found_via)))
    return CalDAVUIDLookupCandidate(
        resource_url=existing.resource_url,
        etag=existing.etag or new.etag,
        calendar_data=existing.calendar_data or new.calendar_data,
        remote_uid=existing.remote_uid or new.remote_uid,
        summary=existing.summary or new.summary,
        dtstart=existing.dtstart or new.dtstart,
        dtend=existing.dtend or new.dtend,
        found_via=found_via,
    )


def _find_candidate_index(
    candidates: list[CalDAVUIDLookupCandidate],
    resource_url: str | None,
) -> int | None:
    if resource_url is None:
        return None
    for index, item in enumerate(candidates):
        if item.resource_url == resource_url:
            return index
    return None


def _find_confirmed_uid_candidate(
    candidates: list[CalDAVUIDLookupCandidate],
    uid: str,
) -> CalDAVUIDLookupCandidate | None:
    for item in candidates:
        if item.remote_uid == uid:
            return item
    return None


def _first_candidate(candidates: list[CalDAVUIDLookupCandidate]) -> CalDAVUIDLookupCandidate | None:
    if not candidates:
        return None
    return candidates[0]


def _is_same_calendar_collection(resource_url: str, calendar_url: str) -> bool:
    return _ensure_trailing_slash(resource_url) == _ensure_trailing_slash(calendar_url)
