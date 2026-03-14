#!/usr/bin/env bash

set -euo pipefail

if [[ ! -f .env ]]; then
    echo ".env not found. Copy .env.example to .env and update it before running Docker sync." >&2
    exit 1
fi

mkdir -p data data/diagnostics data/reports data/backups

exec docker compose run --rm garoon-sync "$@"
