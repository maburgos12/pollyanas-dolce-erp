#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-${ERP_BASE_URL:-https://pollyanas-dolce-erp-production.up.railway.app}}"
TOKEN="${TOKEN:-${ERP_API_TOKEN:-}}"
USERNAME="${USERNAME:-${ERP_API_USER:-}}"
PASSWORD="${PASSWORD:-${ERP_API_PASSWORD:-}}"
TIMEOUT="${TIMEOUT:-25}"

CMD=(.venv/bin/python manage.py smoke_aliases_api --base-url "$BASE_URL" --timeout "$TIMEOUT")

if [[ -n "$TOKEN" ]]; then
  CMD+=(--token "$TOKEN")
fi
if [[ -n "$USERNAME" ]]; then
  CMD+=(--username "$USERNAME")
fi
if [[ -n "$PASSWORD" ]]; then
  CMD+=(--password "$PASSWORD")
fi

# Permite pasar flags extras, por ejemplo:
#   ./scripts/smoke_aliases_api.sh --insecure
CMD+=("$@")

exec "${CMD[@]}"
