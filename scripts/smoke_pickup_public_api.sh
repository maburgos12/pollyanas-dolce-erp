#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

BASE_URL="${BASE_URL:-${ERP_BASE_URL:-http://127.0.0.1:8000}}"
API_KEY="${API_KEY:-${ERP_PUBLIC_API_KEY:-}}"
TIMEOUT="${TIMEOUT:-15}"

CMD=(
  .venv/bin/python
  manage.py
  smoke_pickup_public_api
  --base-url
  "$BASE_URL"
  --timeout
  "$TIMEOUT"
)

if [[ -n "$API_KEY" ]]; then
  CMD+=(--api-key "$API_KEY")
fi

# Ejemplos:
#   ./scripts/smoke_pickup_public_api.sh --product-code 004499 --branch-code MATRIZ
#   ./scripts/smoke_pickup_public_api.sh --product-code 004499 --branch-code MATRIZ --mode reserve-release --confirm-live YES
#   ./scripts/smoke_pickup_public_api.sh --product-code 004499 --branch-code MATRIZ --mode full-cycle --confirm-live YES
CMD+=("$@")

exec "${CMD[@]}"
