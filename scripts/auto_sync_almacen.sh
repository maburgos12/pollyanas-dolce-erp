#!/bin/bash
set -u

INTERVAL_HOURS="${AUTO_SYNC_INTERVAL_HOURS:-24}"
INITIAL_DELAY_SECONDS="${AUTO_SYNC_INITIAL_DELAY_SECONDS:-30}"
if ! [[ "$INTERVAL_HOURS" =~ ^[0-9]+$ ]]; then
  INTERVAL_HOURS=24
fi
if ! [[ "$INITIAL_DELAY_SECONDS" =~ ^[0-9]+$ ]]; then
  INITIAL_DELAY_SECONDS=30
fi
INTERVAL_SECONDS=$((INTERVAL_HOURS * 3600))
if [ "$INTERVAL_SECONDS" -lt 300 ]; then
  INTERVAL_SECONDS=300
fi

echo "[auto-sync] iniciado. intervalo=${INTERVAL_HOURS}h"
if [ "$INITIAL_DELAY_SECONDS" -gt 0 ]; then
  echo "[auto-sync] espera inicial=${INITIAL_DELAY_SECONDS}s (arranque estable)"
  sleep "$INITIAL_DELAY_SECONDS"
fi

while true; do
  START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  echo "[auto-sync] ejecutando sync_almacen_drive en ${START_TS}"
  if python manage.py sync_almacen_drive --create-missing-insumos; then
    echo "[auto-sync] sync completado OK"
  else
    echo "[auto-sync] sync falló; reintentará en ${INTERVAL_HOURS}h"
  fi
  sleep "$INTERVAL_SECONDS"
done
