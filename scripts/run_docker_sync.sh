#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
SYNC_ENV_FILE=${SYNC_ENV_FILE:-.env}

cd "$REPO_DIR"

if [[ ! -f "$SYNC_ENV_FILE" ]]; then
    echo "Env file not found: $SYNC_ENV_FILE" >&2
    echo "Copy .env.example or a profile template and update it before running Docker sync." >&2
    exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "docker command not found. Install Docker and ensure 'docker compose' is available." >&2
    exit 1
fi

mkdir -p data data/diagnostics data/reports data/backups logs runtime

exec docker compose run --rm garoon-sync "$@"
