#!/bin/bash
set -u

INTERVAL_HOURS="${AUTO_SYNC_POS_BRIDGE_INTERVAL_HOURS:-24}"
INITIAL_DELAY_SECONDS="${AUTO_SYNC_POS_BRIDGE_INITIAL_DELAY_SECONDS:-60}"
BRANCH_FILTER="${AUTO_SYNC_POS_BRIDGE_BRANCH_FILTER:-}"
LIMIT_BRANCHES="${AUTO_SYNC_POS_BRIDGE_LIMIT_BRANCHES:-}"
INCLUDE_INVENTORY="${AUTO_SYNC_POS_BRIDGE_INCLUDE_INVENTORY:-1}"
INCLUDE_SALES="${AUTO_SYNC_POS_BRIDGE_INCLUDE_SALES:-0}"
SALES_LOOKBACK_DAYS="${AUTO_SYNC_POS_BRIDGE_SALES_LOOKBACK_DAYS:-3}"
SALES_LAG_DAYS="${AUTO_SYNC_POS_BRIDGE_SALES_LAG_DAYS:-1}"
PYTHON_BIN="${PYTHON_BIN:-python}"

if ! [[ "$INTERVAL_HOURS" =~ ^[0-9]+$ ]]; then
  INTERVAL_HOURS=24
fi
if ! [[ "$INITIAL_DELAY_SECONDS" =~ ^[0-9]+$ ]]; then
  INITIAL_DELAY_SECONDS=60
fi
if ! [[ "$SALES_LOOKBACK_DAYS" =~ ^[0-9]+$ ]]; then
  SALES_LOOKBACK_DAYS=3
fi
if ! [[ "$SALES_LAG_DAYS" =~ ^[0-9]+$ ]]; then
  SALES_LAG_DAYS=1
fi
INTERVAL_SECONDS=$((INTERVAL_HOURS * 3600))
if [ "$INTERVAL_SECONDS" -lt 300 ]; then
  INTERVAL_SECONDS=300
fi

echo "[auto-sync-pos-bridge] iniciado. intervalo=${INTERVAL_HOURS}h"
if [ "$INITIAL_DELAY_SECONDS" -gt 0 ]; then
  echo "[auto-sync-pos-bridge] espera inicial=${INITIAL_DELAY_SECONDS}s"
  sleep "$INITIAL_DELAY_SECONDS"
fi

while true; do
  CMD=("$PYTHON_BIN" manage.py run_pos_bridge_scheduler --once --interval-hours "$INTERVAL_HOURS")
  if [ "$INCLUDE_INVENTORY" = "1" ]; then
    CMD+=(--run-inventory)
  fi
  if [ "$INCLUDE_SALES" = "1" ]; then
    CMD+=(--run-sales --sales-days "$SALES_LOOKBACK_DAYS" --sales-lag-days "$SALES_LAG_DAYS")
  fi
  if [ -n "$BRANCH_FILTER" ]; then
    CMD+=(--branch "$BRANCH_FILTER")
  fi
  if [ -n "$LIMIT_BRANCHES" ]; then
    CMD+=(--limit-branches "$LIMIT_BRANCHES")
  fi
  if [ "$INCLUDE_INVENTORY" != "1" ] && [ "$INCLUDE_SALES" != "1" ]; then
    echo "[auto-sync-pos-bridge] sin jobs habilitados; activando inventario por default"
    CMD+=(--run-inventory)
  fi

  if "${CMD[@]}"; then
    echo "[auto-sync-pos-bridge] sync completado OK"
  else
    echo "[auto-sync-pos-bridge] sync falló; reintentará en ${INTERVAL_HOURS}h"
  fi

  sleep "$INTERVAL_SECONDS"
done
