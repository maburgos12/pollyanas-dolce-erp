#!/bin/bash
set -u

INTERVAL_HOURS="${AUTO_SYNC_INTERVAL_HOURS:-24}"
if ! [[ "$INTERVAL_HOURS" =~ ^[0-9]+$ ]]; then
  INTERVAL_HOURS=24
fi
INTERVAL_SECONDS=$((INTERVAL_HOURS * 3600))
if [ "$INTERVAL_SECONDS" -lt 300 ]; then
  INTERVAL_SECONDS=300
fi

echo "[auto-sync] iniciado. intervalo=${INTERVAL_HOURS}h"

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
