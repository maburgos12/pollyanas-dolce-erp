#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TIMEOUT="${TIMEOUT:-30}"
ERP_BASE_URL="${ERP_BASE_URL:-https://pollyanas-dolce-erp-production.up.railway.app}"
INSECURE="${INSECURE:-false}"

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

need_cmd railway
need_cmd python3

cd "${ROOT_DIR}"

echo "Checking Railway authentication..."
if ! railway status >/dev/null 2>&1; then
  fail "Railway CLI is not authenticated on this Mac. Run 'railway login' and retry."
fi

echo "Deploying current working tree to Railway..."
railway up

echo "Waiting for ERP AI Gateway smoke validation..."
sleep 8

smoke_args=(
  "ERP_BASE_URL=${ERP_BASE_URL}"
  "TIMEOUT=${TIMEOUT}"
  "INSECURE=${INSECURE}"
)

env "${smoke_args[@]}" bash "${ROOT_DIR}/scripts/smoke_ai_gateway_remote.sh"

echo "Railway publish + gateway smoke OK."
