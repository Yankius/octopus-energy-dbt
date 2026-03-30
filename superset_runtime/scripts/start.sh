#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

mkdir -p "${RUNTIME_DIR}/storage/postgres"
mkdir -p "${RUNTIME_DIR}/storage/redis"
mkdir -p "${RUNTIME_DIR}/storage/superset_home"

if [[ ! -f "${RUNTIME_DIR}/.env" ]]; then
  cp "${RUNTIME_DIR}/.env.example" "${RUNTIME_DIR}/.env"
  echo "Created ${RUNTIME_DIR}/.env from template. Update secrets before long-term use."
fi

cd "${RUNTIME_DIR}"
docker compose up -d --build
