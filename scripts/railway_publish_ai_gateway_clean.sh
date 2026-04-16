#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ERP_BASE_URL="${ERP_BASE_URL:-https://pollyanas-dolce-erp-production.up.railway.app}"
TIMEOUT="${TIMEOUT:-30}"
INSECURE="${INSECURE:-false}"
DEPLOY_WAIT_SECONDS="${DEPLOY_WAIT_SECONDS:-420}"
DEPLOY_POLL_SECONDS="${DEPLOY_POLL_SECONDS:-10}"
STAGING_DIR="$(mktemp -d "${TMPDIR:-/tmp}/railway-clean-deploy.XXXXXX")"

cleanup() {
  rm -rf "${STAGING_DIR}"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

need_cmd railway
need_cmd rsync
need_cmd python3

cd "${ROOT_DIR}"

echo "Checking Railway authentication..."
if ! railway status >/dev/null 2>&1; then
  fail "Railway CLI is not authenticated on this Mac. Run 'railway login' and retry."
fi

echo "Applying Railway-safe web variables..."
railway variable set --skip-deploys \
  ENABLE_AUTO_SYNC_ALMACEN=0 \
  ENABLE_AUTO_MAINT_INTEGRACIONES=0 \
  ENABLE_AUTO_SYNC_POS_BRIDGE=0 \
  BOOTSTRAP_POINT_BRANCHES_ON_START=0 \
  -s pollyanas-dolce-erp

echo "Building clean staging directory..."
rsync -a --delete \
  --exclude '.git/' \
  --exclude '.github/' \
  --exclude '.venv/' \
  --exclude 'venv/' \
  --exclude '__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude '.playwright-cli/' \
  --exclude '.agent/' \
  --exclude 'logs/' \
  --exclude 'output/' \
  --exclude 'storage/' \
  --exclude 'backups/' \
  --exclude 'tmp/' \
  --exclude 'staticfiles/' \
  --exclude '_archive/' \
  --exclude 'docs/' \
  --exclude 'test_data/' \
  --exclude 'infra/' \
  --exclude 'db.sqlite3' \
  --exclude 'test_db.sqlite3' \
  --exclude '.env.local' \
  --exclude '.DS_Store' \
  ./ "${STAGING_DIR}/"

echo "Staging size:"
du -sh "${STAGING_DIR}"

echo "Deploying clean staging tree to Railway..."
railway up "${STAGING_DIR}" --path-as-root

echo "Waiting for latest Railway deployment to finish..."
SECONDS_WAITED=0
while true; do
  LATEST_STATUS="$(railway status --json > /tmp/railway_status_publish.json && python3 - <<'PY'
import json
with open('/tmp/railway_status_publish.json') as f:
    obj = json.load(f)
for env_edge in obj['environments']['edges']:
    for svc_edge in env_edge['node']['serviceInstances']['edges']:
        node = svc_edge['node']
        if node['serviceName'] == 'pollyanas-dolce-erp':
            print(node['latestDeployment']['status'])
            raise SystemExit(0)
raise SystemExit(1)
PY
)"
  echo "Latest deployment status: ${LATEST_STATUS}"
  if [ "${LATEST_STATUS}" = "SUCCESS" ]; then
    break
  fi
  if [ "${LATEST_STATUS}" = "FAILED" ]; then
    fail "Latest Railway deployment failed before gateway smoke validation."
  fi
  if [ "${SECONDS_WAITED}" -ge "${DEPLOY_WAIT_SECONDS}" ]; then
    fail "Timed out waiting for Railway deployment to finish."
  fi
  sleep "${DEPLOY_POLL_SECONDS}"
  SECONDS_WAITED=$((SECONDS_WAITED + DEPLOY_POLL_SECONDS))
done

echo "Waiting for ERP AI Gateway smoke validation..."
env \
  ERP_BASE_URL="${ERP_BASE_URL}" \
  TIMEOUT="${TIMEOUT}" \
  INSECURE="${INSECURE}" \
  bash "${ROOT_DIR}/scripts/smoke_ai_gateway_remote.sh"

echo "Clean Railway publish + gateway smoke OK."
