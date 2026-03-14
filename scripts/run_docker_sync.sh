#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$SCRIPT_DIR/.." && pwd)

cd "$REPO_DIR"

if [[ ! -f .env ]]; then
    echo ".env not found. Copy .env.example to .env and update it before running Docker sync." >&2
    exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "docker command not found. Install Docker and ensure 'docker compose' is available." >&2
    exit 1
fi

mkdir -p data data/diagnostics data/reports data/backups

exec docker compose run --rm garoon-sync "$@"
