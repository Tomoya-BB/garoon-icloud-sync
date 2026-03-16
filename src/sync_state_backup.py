from __future__ import annotations

import argparse
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence

from src.config import ConfigError, load_config
from src.sync_state import DEFAULT_SYNC_STATE_PATH, load_sync_state

DEFAULT_BACKUP_DIR_NAME = "backups"
BACKUP_TIMESTAMP_PATTERN = re.compile(r"(\d{8}-\d{6})")


@dataclass(frozen=True, slots=True)
class ManagedStateFile:
    state_path: Path
    backup_dir: Path
    label: str

    @property
    def backup_prefix(self) -> str:
        return f"{self.state_path.stem}-"


@dataclass(frozen=True, slots=True)
class RestoreResult:
    restored_backup_path: Path
    state_path: Path
    current_backup_path: Path | None


@dataclass(frozen=True, slots=True)
class PruneResult:
    kept_backups: list[Path]
    deleted_backups: list[Path]
    dry_run: bool


def _now() -> datetime:
    return datetime.now().astimezone()


class StateBackupManager:
    def __init__(self, managed_file: ManagedStateFile):
        self.managed_file = managed_file

    def backup(self, *, dry_run: bool = False, reason: str | None = None) -> Path:
        state_path = self.managed_file.state_path
        if not state_path.exists():
            raise FileNotFoundError(
                f"{self.managed_file.label} file was not found: {state_path}"
            )

        backup_path = self._build_unique_backup_path(reason=reason)
        if not dry_run:
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(state_path, backup_path)
        return backup_path

    def list_backups(self) -> list[Path]:
        backup_dir = self.managed_file.backup_dir
        if not backup_dir.exists():
            return []

        return sorted(
            (
                path
                for path in backup_dir.iterdir()
                if path.is_file()
                and path.name.startswith(self.managed_file.backup_prefix)
                and path.suffix == self.managed_file.state_path.suffix
            ),
            key=_backup_sort_key,
            reverse=True,
        )

    def restore(
        self,
        backup_name: str,
        *,
        dry_run: bool = False,
        validate: bool = False,
    ) -> RestoreResult:
        backup_path = self.resolve_backup_path(backup_name)
        if not backup_path.exists():
            raise FileNotFoundError(
                f"Backup file was not found: {backup_path}"
            )

        if validate:
            load_sync_state(backup_path, create_if_missing=False)

        current_backup_path: Path | None = None
        if self.managed_file.state_path.exists():
            current_backup_path = self._build_unique_backup_path(reason="pre-restore")

        if not dry_run:
            if current_backup_path is not None:
                current_backup_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(self.managed_file.state_path, current_backup_path)

            self.managed_file.state_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(backup_path, self.managed_file.state_path)

            if validate:
                load_sync_state(self.managed_file.state_path, create_if_missing=False)

        return RestoreResult(
            restored_backup_path=backup_path,
            state_path=self.managed_file.state_path,
            current_backup_path=current_backup_path,
        )

    def prune_backups(
        self,
        *,
        keep: int,
        dry_run: bool = False,
    ) -> PruneResult:
        if keep < 1:
            raise ValueError("--keep must be 1 or greater")

        backups = self.list_backups()
        kept_backups = backups[:keep]
        deleted_backups = backups[keep:]

        if not dry_run:
            for backup_path in deleted_backups:
                backup_path.unlink()

        return PruneResult(
            kept_backups=kept_backups,
            deleted_backups=deleted_backups,
            dry_run=dry_run,
        )

    def resolve_backup_path(self, backup_name: str) -> Path:
        requested_path = Path(backup_name).expanduser()
        if requested_path.is_absolute():
            return requested_path.resolve()
        return (self.managed_file.backup_dir / requested_path).resolve()

    def _build_unique_backup_path(self, *, reason: str | None = None) -> Path:
        timestamp = _now().strftime("%Y%m%d-%H%M%S")
        stem = self.managed_file.state_path.stem
        suffix = self.managed_file.state_path.suffix
        base_name = f"{stem}-{timestamp}{suffix}"
        if reason:
            base_name = f"{stem}-{reason}-{timestamp}{suffix}"

        candidate = self.managed_file.backup_dir / base_name
        if not candidate.exists():
            return candidate

        counter = 2
        while True:
            suffixed = self.managed_file.backup_dir / (
                f"{candidate.stem}-{counter}{candidate.suffix}"
            )
            if not suffixed.exists():
                return suffixed
            counter += 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backup and restore helper for sync_state.json.",
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--state-path",
        help="Path to the sync state file. Defaults to the active profile state path when --env-path is set.",
    )
    common.add_argument(
        "--backups-dir",
        help="Directory where backups are stored. Defaults to <state-path>/../backups.",
    )
    common.add_argument(
        "--env-path",
        default=None,
        help="Optional .env path used to resolve the data directory from OUTPUT_JSON_PATH.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    backup_parser = subparsers.add_parser("backup", parents=[common], help="Create a timestamped backup.")
    backup_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the backup destination without writing a file.",
    )

    subparsers.add_parser("list", parents=[common], help="List available backups.")

    prune_parser = subparsers.add_parser(
        "prune",
        parents=[common],
        help="Delete old backups while keeping the newest N files.",
    )
    prune_parser.add_argument(
        "--keep",
        type=_positive_int,
        required=True,
        help="Number of newest backups to keep. Must be 1 or greater.",
    )
    prune_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show which backups would be deleted without deleting them.",
    )

    restore_parser = subparsers.add_parser("restore", parents=[common], help="Restore a backup.")
    restore_parser.add_argument("backup_name", help="Backup file name from the backups directory.")
    restore_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be restored without writing files.",
    )
    restore_parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate the selected backup before and after restore.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        manager = StateBackupManager(resolve_managed_state_file(args))

        if args.command == "backup":
            return _run_backup(manager, dry_run=args.dry_run)
        if args.command == "list":
            return _run_list(manager)
        if args.command == "prune":
            return _run_prune(
                manager,
                keep=args.keep,
                dry_run=args.dry_run,
            )
        if args.command == "restore":
            return _run_restore(
                manager,
                backup_name=args.backup_name,
                dry_run=args.dry_run,
                validate=args.validate,
            )
    except ConfigError as exc:
        print(f"Failed to resolve sync state path from config: {exc}", file=sys.stderr)
        return 1
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(f"Failed to manage sync state backup: {exc}", file=sys.stderr)
        return 1

    parser.print_help()
    return 1


def resolve_managed_state_file(args: argparse.Namespace) -> ManagedStateFile:
    state_path = resolve_sync_state_path(
        state_path_arg=args.state_path,
        env_path=args.env_path,
    )
    backup_dir = resolve_backup_dir(
        state_path,
        backup_dir_arg=args.backups_dir,
    )
    return ManagedStateFile(
        state_path=state_path,
        backup_dir=backup_dir,
        label="sync_state",
    )


def resolve_sync_state_path(
    *,
    state_path_arg: str | None,
    env_path: str | None,
) -> Path:
    if state_path_arg:
        return Path(state_path_arg).expanduser().resolve()
    if env_path:
        config = load_config(env_path)
        if config.sync_state_path is not None:
            return config.sync_state_path
        return (config.output_json_path.parent / DEFAULT_SYNC_STATE_PATH.name).resolve()
    return DEFAULT_SYNC_STATE_PATH


def resolve_backup_dir(state_path: Path, *, backup_dir_arg: str | None) -> Path:
    if backup_dir_arg:
        return Path(backup_dir_arg).expanduser().resolve()
    return state_path.parent / DEFAULT_BACKUP_DIR_NAME


def _positive_int(raw_value: str) -> int:
    value = int(raw_value)
    if value < 1:
        raise argparse.ArgumentTypeError("must be 1 or greater")
    return value


def _backup_sort_key(path: Path) -> tuple[str, str]:
    matches = BACKUP_TIMESTAMP_PATTERN.findall(path.name)
    timestamp = matches[-1] if matches else ""
    return (timestamp, path.name)


def _run_backup(manager: StateBackupManager, *, dry_run: bool) -> int:
    backup_path = manager.backup(dry_run=dry_run)
    if dry_run:
        print("Dry run: would create sync_state backup")
    else:
        print("Created sync_state backup")
    print(f"source: {manager.managed_file.state_path}")
    print(f"backup: {backup_path}")
    return 0


def _run_list(manager: StateBackupManager) -> int:
    backups = manager.list_backups()
    if not backups:
        print(
            "No sync_state backups found.\n"
            f"backups_dir: {manager.managed_file.backup_dir}\n"
            "Create one with: python -m src.sync_state_backup backup"
        )
        return 0

    newest_backup = backups[0]
    oldest_backup = backups[-1]
    print(f"Available sync_state backups in {manager.managed_file.backup_dir}:")
    print(f"count: {len(backups)}")
    print(f"latest: {newest_backup.name}")
    print(f"oldest: {oldest_backup.name}")
    for backup_path in backups:
        stat = backup_path.stat()
        modified_at = datetime.fromtimestamp(stat.st_mtime).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"- {backup_path.name} | modified={modified_at} | size={stat.st_size} bytes"
        )
    return 0


def _run_prune(manager: StateBackupManager, *, keep: int, dry_run: bool) -> int:
    result = manager.prune_backups(keep=keep, dry_run=dry_run)
    total_backups = len(result.kept_backups) + len(result.deleted_backups)

    if dry_run:
        print("Dry run: would prune sync_state backups")
    else:
        print("Pruned sync_state backups")
    print(f"backups_dir: {manager.managed_file.backup_dir}")
    print(f"total_found: {total_backups}")
    print(f"keep: {keep}")
    print(f"kept_count: {len(result.kept_backups)}")
    print(f"deleted_count: {len(result.deleted_backups)}")

    if result.deleted_backups:
        label = "would_delete" if dry_run else "deleted"
        for backup_path in result.deleted_backups:
            print(f"{label}: {backup_path.name}")
    else:
        print("deleted: (none)")

    return 0


def _run_restore(
    manager: StateBackupManager,
    *,
    backup_name: str,
    dry_run: bool,
    validate: bool,
) -> int:
    result = manager.restore(
        backup_name,
        dry_run=dry_run,
        validate=validate,
    )
    if dry_run:
        print("Dry run: would restore sync_state")
    else:
        print("Restored sync_state backup")
    print(f"backup: {result.restored_backup_path}")
    print(f"destination: {result.state_path}")
    if result.current_backup_path is not None:
        print(f"previous_current_backup: {result.current_backup_path}")
    else:
        print("previous_current_backup: (none)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
