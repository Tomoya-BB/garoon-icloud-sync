from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any

from src.sync_state import EventDiff, EventSyncState, SyncDiffResult, SyncStatus

DEFAULT_SYNC_PLAN_PATH = Path(__file__).resolve().parent.parent / "data" / "sync_plan.json"
SYNC_PLAN_VERSION = 1


class SyncActionType(StrEnum):
    CREATE = "create"
    UPDATE = "update"
    SKIP = "skip"
    DELETE = "delete"


@dataclass(frozen=True, slots=True)
class SyncPlanAction:
    action: SyncActionType
    event_id: str
    ics_uid: str
    sequence: int
    content_hash: str
    updated_at: str | None
    action_reason: str
    reappeared_from_tombstone: bool = False
    tombstone_deleted_at: str | None = None
    summary: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SyncPlan:
    version: int = SYNC_PLAN_VERSION
    generated_at: str = ""
    actions: list[SyncPlanAction] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generated_at": self.generated_at,
            "actions": [action.to_dict() for action in self.actions],
        }


@dataclass(frozen=True, slots=True)
class SyncPlanActionSummary:
    create_count: int = 0
    update_count: int = 0
    skip_count: int = 0
    delete_count: int = 0
    total_count: int = 0


def build_sequence_by_event_id(sync_plan: SyncPlan) -> dict[str, int]:
    return {
        action.event_id: action.sequence
        for action in sync_plan.actions
        if action.action is not SyncActionType.DELETE
    }


def build_uid_by_event_id(sync_plan: SyncPlan) -> dict[str, str]:
    return {
        action.event_id: action.ics_uid
        for action in sync_plan.actions
        if action.action is not SyncActionType.DELETE
    }


def summarize_sync_plan_actions(sync_plan: SyncPlan) -> SyncPlanActionSummary:
    counts = Counter(action.action.value for action in sync_plan.actions)
    return SyncPlanActionSummary(
        create_count=counts.get(SyncActionType.CREATE.value, 0),
        update_count=counts.get(SyncActionType.UPDATE.value, 0),
        skip_count=counts.get(SyncActionType.SKIP.value, 0),
        delete_count=counts.get(SyncActionType.DELETE.value, 0),
        total_count=len(sync_plan.actions),
    )


def build_sync_plan(
    diff: SyncDiffResult,
    generated_at: datetime | None = None,
) -> SyncPlan:
    actions = [
        *[_build_event_action(item) for item in diff.new_events],
        *[_build_event_action(item) for item in diff.updated_events],
        *[_build_event_action(item) for item in diff.unchanged_events],
        *[_build_delete_candidate_action(item) for item in diff.deleted_candidates],
    ]
    return SyncPlan(
        version=SYNC_PLAN_VERSION,
        generated_at=_format_timestamp(generated_at or datetime.now(timezone.utc)),
        actions=actions,
    )


def save_sync_plan(path: Path, plan: SyncPlan) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(plan.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_sync_plan(path: Path) -> SyncPlan:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return sync_plan_from_dict(payload)


def sync_plan_from_dict(payload: Any) -> SyncPlan:
    if not isinstance(payload, dict):
        raise ValueError("sync_plan must be a JSON object")

    version = payload.get("version")
    if not isinstance(version, int) or isinstance(version, bool):
        raise ValueError("sync_plan.version must be an integer")

    generated_at = payload.get("generated_at")
    if not isinstance(generated_at, str):
        raise ValueError("sync_plan.generated_at must be a string")

    raw_actions = payload.get("actions")
    if not isinstance(raw_actions, list):
        raise ValueError("sync_plan.actions must be a list")

    return SyncPlan(
        version=version,
        generated_at=generated_at,
        actions=[
            _parse_sync_plan_action(raw_action, index=index)
            for index, raw_action in enumerate(raw_actions)
        ],
    )


def _build_event_action(diff: EventDiff) -> SyncPlanAction:
    if diff.status is SyncStatus.NEW:
        action = SyncActionType.CREATE
        reason = "reappeared_from_tombstone" if diff.reappeared_from_tombstone else "new_event"
    elif diff.status is SyncStatus.UPDATED:
        action = SyncActionType.UPDATE
        reason = _build_update_reason(diff)
    else:
        action = SyncActionType.SKIP
        reason = "no_changes"

    return SyncPlanAction(
        action=action,
        event_id=diff.next_state.event_id,
        ics_uid=diff.next_state.ics_uid,
        sequence=_resolve_action_sequence(diff),
        content_hash=diff.next_state.content_hash,
        updated_at=diff.next_state.updated_at,
        action_reason=reason,
        reappeared_from_tombstone=diff.reappeared_from_tombstone,
        tombstone_deleted_at=(
            diff.previous_tombstone.deleted_at if diff.previous_tombstone is not None else None
        ),
        summary=_build_event_summary(diff.event),
    )


def _build_delete_candidate_action(event_state: EventSyncState) -> SyncPlanAction:
    return SyncPlanAction(
        action=SyncActionType.DELETE,
        event_id=event_state.event_id,
        ics_uid=event_state.ics_uid,
        sequence=event_state.sequence,
        content_hash=event_state.content_hash,
        updated_at=event_state.updated_at,
        action_reason="missing_from_current_fetch",
    )


def _parse_sync_plan_action(payload: Any, *, index: int) -> SyncPlanAction:
    location = f"sync_plan.actions[{index}]"
    if not isinstance(payload, dict):
        raise ValueError(f"{location} must be a JSON object")

    raw_action = payload.get("action")
    if not isinstance(raw_action, str):
        raise ValueError(f"{location}.action must be a string")

    try:
        action = SyncActionType(raw_action)
    except ValueError as exc:
        raise ValueError(f"{location}.action has unsupported value: {raw_action}") from exc

    return SyncPlanAction(
        action=action,
        event_id=_require_string_field(payload, "event_id", location=location),
        ics_uid=_require_string_field(payload, "ics_uid", location=location),
        sequence=_require_int_field(payload, "sequence", location=location),
        content_hash=_require_string_field(payload, "content_hash", location=location),
        updated_at=_optional_string_field(payload.get("updated_at"), location=f"{location}.updated_at"),
        action_reason=_require_string_field(payload, "action_reason", location=location),
        reappeared_from_tombstone=_optional_bool_field(
            payload.get("reappeared_from_tombstone"),
            location=f"{location}.reappeared_from_tombstone",
        ),
        tombstone_deleted_at=_optional_string_field(
            payload.get("tombstone_deleted_at"),
            location=f"{location}.tombstone_deleted_at",
        ),
        summary=_optional_string_field(payload.get("summary"), location=f"{location}.summary"),
    )


def _build_update_reason(diff: EventDiff) -> str:
    previous_state = diff.previous_state
    if previous_state is None:
        return "new_event"

    content_changed = previous_state.content_hash != diff.next_state.content_hash
    updated_at_changed = previous_state.updated_at != diff.next_state.updated_at

    if content_changed and updated_at_changed:
        return "content_and_updated_at_changed"
    if content_changed:
        return "content_changed"
    if updated_at_changed:
        return "updated_at_changed"
    return "no_changes"


def _resolve_action_sequence(diff: EventDiff) -> int:
    if diff.status is SyncStatus.NEW:
        return 0
    if diff.status is SyncStatus.UPDATED and diff.previous_state is not None:
        return diff.previous_state.sequence + 1
    return diff.next_state.sequence


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _build_event_summary(diff_event) -> str:
    if diff_event.subject.strip():
        return diff_event.subject
    if diff_event.event_menu and diff_event.event_menu.strip():
        return diff_event.event_menu
    return "(no title)"


def _require_string_field(payload: dict[str, Any], key: str, *, location: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise ValueError(f"{location}.{key} must be a string")
    return value


def _require_int_field(payload: dict[str, Any], key: str, *, location: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{location}.{key} must be an integer")
    return value


def _optional_string_field(value: Any, *, location: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{location} must be a string or null")
    return value


def _optional_bool_field(value: Any, *, location: str) -> bool:
    if value is None:
        return False
    if not isinstance(value, bool):
        raise ValueError(f"{location} must be a boolean")
    return value
