#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WRITE_DISPOSITION="${1:-WRITE_TRUNCATE}"
SKIP_BIGQUERY="${2:-false}"

if [[ -x "${SCRIPT_DIR}/../.venv/bin/python" ]]; then
  PYTHON_BIN="${SCRIPT_DIR}/../.venv/bin/python"
else
  PYTHON_BIN="${PYTHON_BIN:-python3}"
fi

cd "${SCRIPT_DIR}"

if [[ ! -f "environment.env" ]]; then
  echo "Missing ${SCRIPT_DIR}/environment.env" >&2
  exit 1
fi

echo "Running agreements ingestion"
"${PYTHON_BIN}" ingest_octopus_agreements.py

echo "Running tariffs ingestion"
"${PYTHON_BIN}" ingest_octopus_tarrifs.py

echo "Running 30-minute consumption ingestion"
"${PYTHON_BIN}" octopus_usageingestion_30.py

BRIDGE_ARGS=(gcp_bridge.py --write-disposition "${WRITE_DISPOSITION}")

if [[ "${SKIP_BIGQUERY}" == "true" ]]; then
  BRIDGE_ARGS+=(--skip-bigquery)
fi

echo "Running GCP bridge"
"${PYTHON_BIN}" "${BRIDGE_ARGS[@]}"

echo "Pipeline completed successfully"
