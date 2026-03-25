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

BRANCH_FILTER="$(read_local_env POS_BRIDGE_INVENTORY_BRANCH_FILTER)"
LIMIT_BRANCHES="$(read_local_env POS_BRIDGE_INVENTORY_LIMIT_BRANCHES)"
INVENTORY_MODE="$(read_local_env POS_BRIDGE_INVENTORY_MODE)"

if [ -z "$INVENTORY_MODE" ]; then
  INVENTORY_MODE="realtime"
fi

if [ "$INVENTORY_MODE" = "full" ]; then
  CMD=("$PYTHON_BIN" manage.py run_inventory_sync)
  if [ -n "$BRANCH_FILTER" ]; then
    CMD+=(--branch "$BRANCH_FILTER")
  fi
  if [ -n "$LIMIT_BRANCHES" ]; then
    CMD+=(--limit-branches "$LIMIT_BRANCHES")
  fi
else
  CMD=("$PYTHON_BIN" manage.py run_realtime_inventory --force)
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] pos_bridge inventario (${INVENTORY_MODE}): inicio"
"${CMD[@]}"
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] pos_bridge inventario (${INVENTORY_MODE}): fin"
