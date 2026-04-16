#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  exec "${ROOT_DIR}/scripts/validate_ui_local.sh" "$@"
fi

"${ROOT_DIR}/scripts/reset_mcp_chrome.sh"
exec "${ROOT_DIR}/scripts/validate_ui_local.sh" "$@"
