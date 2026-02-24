#!/bin/bash
set -u

INTERVAL_HOURS="${AUTO_MAINT_INTEGRACIONES_INTERVAL_HOURS:-24}"
INITIAL_DELAY_SECONDS="${AUTO_MAINT_INTEGRACIONES_INITIAL_DELAY_SECONDS:-45}"
IDLE_DAYS="${AUTO_MAINT_INTEGRACIONES_IDLE_DAYS:-30}"
IDLE_LIMIT="${AUTO_MAINT_INTEGRACIONES_IDLE_LIMIT:-100}"
RETAIN_DAYS="${AUTO_MAINT_INTEGRACIONES_RETAIN_DAYS:-90}"
MAX_DELETE="${AUTO_MAINT_INTEGRACIONES_MAX_DELETE:-5000}"
DRY_RUN="${AUTO_MAINT_INTEGRACIONES_DRY_RUN:-1}"
ACTOR_USERNAME="${AUTO_MAINT_INTEGRACIONES_ACTOR_USERNAME:-}"
CONFIRM_LIVE="${AUTO_MAINT_INTEGRACIONES_CONFIRM_LIVE:-}"

if ! [[ "$INTERVAL_HOURS" =~ ^[0-9]+$ ]]; then
  INTERVAL_HOURS=24
fi
if ! [[ "$INITIAL_DELAY_SECONDS" =~ ^[0-9]+$ ]]; then
  INITIAL_DELAY_SECONDS=45
fi
INTERVAL_SECONDS=$((INTERVAL_HOURS * 3600))
if [ "$INTERVAL_SECONDS" -lt 300 ]; then
  INTERVAL_SECONDS=300
fi

echo "[auto-maint-integraciones] iniciado. intervalo=${INTERVAL_HOURS}h dry_run=${DRY_RUN}"
if [ "$INITIAL_DELAY_SECONDS" -gt 0 ]; then
  echo "[auto-maint-integraciones] espera inicial=${INITIAL_DELAY_SECONDS}s"
  sleep "$INITIAL_DELAY_SECONDS"
fi

while true; do
  START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  echo "[auto-maint-integraciones] ejecutando mantenimiento en ${START_TS}"

  CMD=(python manage.py run_integraciones_maintenance
    --idle-days "$IDLE_DAYS"
    --idle-limit "$IDLE_LIMIT"
    --retain-days "$RETAIN_DAYS"
    --max-delete "$MAX_DELETE"
  )

  if [ "$DRY_RUN" = "1" ]; then
    CMD+=(--dry-run)
  else
    CMD+=(--confirm-live "${CONFIRM_LIVE:-YES}")
  fi

  if [ -n "$ACTOR_USERNAME" ]; then
    CMD+=(--actor-username "$ACTOR_USERNAME")
  fi

  if "${CMD[@]}"; then
    echo "[auto-maint-integraciones] mantenimiento OK"
  else
    echo "[auto-maint-integraciones] mantenimiento falló; reintentará en ${INTERVAL_HOURS}h"
  fi

  sleep "$INTERVAL_SECONDS"
done
