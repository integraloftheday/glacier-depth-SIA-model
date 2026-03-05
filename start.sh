#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

HOST_UID="$(id -u)" HOST_GID="$(id -g)" docker compose up -d --build
exec docker compose exec glacier-dev bash
