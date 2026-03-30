#!/usr/bin/env sh
set -eu

superset db upgrade

superset fab create-admin \
  --username "${ADMIN_USERNAME:-admin}" \
  --firstname "${ADMIN_FIRSTNAME:-Superset}" \
  --lastname "${ADMIN_LASTNAME:-Admin}" \
  --email "${ADMIN_EMAIL:-admin@example.com}" \
  --password "${ADMIN_PASSWORD:-admin}" || true

superset init

if [ "${SUPERSET_LOAD_EXAMPLES:-no}" = "yes" ]; then
  superset load_examples
fi
