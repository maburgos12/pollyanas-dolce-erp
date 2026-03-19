#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p "$ROOT_DIR/storage/pos_bridge/logs"

read_local_env() {
  local key="$1"
  local value="${!key:-}"
  if [ -n "$value" ]; then
    printf '%s' "$value"
    return 0
  fi
  if [ -f "$ROOT_DIR/.env" ]; then
    value="$(grep -E "^${key}=" "$ROOT_DIR/.env" | tail -n 1 | cut -d'=' -f2- || true)"
    printf '%s' "$value"
    return 0
  fi
  printf ''
}

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
if [ ! -x "$PYTHON_BIN" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  else
    PYTHON_BIN="$(command -v python)"
  fi
fi

LOOKBACK_DAYS="$(read_local_env POS_BRIDGE_SALES_CLOSE_LOOKBACK_DAYS)"
LAG_DAYS="$(read_local_env POS_BRIDGE_SALES_CLOSE_LAG_DAYS)"
BRANCH_FILTER="$(read_local_env POS_BRIDGE_SALES_CLOSE_BRANCH_FILTER)"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-3}"
LAG_DAYS="${LAG_DAYS:-1}"

CMD=("$PYTHON_BIN" manage.py run_daily_sales_sync --days "$LOOKBACK_DAYS" --lag-days "$LAG_DAYS")
if [ -n "$BRANCH_FILTER" ]; then
  CMD+=(--branch "$BRANCH_FILTER")
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] pos_bridge ventas cerradas: inicio"
"${CMD[@]}"
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] pos_bridge ventas cerradas: fin"
