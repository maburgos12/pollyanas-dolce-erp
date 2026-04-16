#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-${ERP_BASE_URL:-}}"
TOKEN="${TOKEN:-${ERP_AI_GATEWAY_TOKEN:-}}"
TIMEOUT="${TIMEOUT:-20}"
INSECURE="${INSECURE:-false}"

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

info() {
  echo "INFO: $1"
}

[[ -n "${BASE_URL}" ]] || fail "Set BASE_URL or ERP_BASE_URL to the real ERP host"

base="${BASE_URL%/}"
health_url="${base}/health/"
manifest_url="${base}/api/ai-gateway/manifest/"
openapi_url="${base}/api/ai-gateway/openapi/"

curl_args=(-sS --max-time "${TIMEOUT}" -o /dev/null -w '%{http_code}')
if [[ "${INSECURE}" == "true" ]]; then
  curl_args+=(-k)
fi
if [[ -n "${TOKEN}" ]]; then
  curl_args+=(-H "Authorization: Token ${TOKEN}")
fi

status() {
  local url="$1"
  curl "${curl_args[@]}" "${url}" || true
}

health_status="$(status "${health_url}")"
manifest_status="$(status "${manifest_url}")"
openapi_status="$(status "${openapi_url}")"

info "ERP host: ${base}"
info "Health: ${health_status:-unknown}"
info "Gateway manifest: ${manifest_status:-unknown}"
info "Gateway openapi: ${openapi_status:-unknown}"

[[ "${health_status}" == "200" ]] || fail "ERP health endpoint is not healthy (${health_status})"

case "${manifest_status}" in
  200|401|403)
    ;;
  404)
    fail "Gateway manifest returned 404. The ERP host is up, but the AI Gateway is not published there yet."
    ;;
  *)
    fail "Gateway manifest returned unexpected HTTP ${manifest_status:-unknown}"
    ;;
esac

case "${openapi_status}" in
  200|401|403)
    ;;
  404)
    fail "Gateway OpenAPI returned 404. The ERP host is up, but the AI Gateway OpenAPI is not published there yet."
    ;;
  *)
    fail "Gateway OpenAPI returned unexpected HTTP ${openapi_status:-unknown}"
    ;;
esac

echo "Remote ERP AI Gateway smoke OK."
