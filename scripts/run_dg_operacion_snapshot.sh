#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p "$ROOT_DIR/storage/dg_reports" "$ROOT_DIR/storage/dg_reports/logs"

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

export DJANGO_SETTINGS_MODULE="${DJANGO_SETTINGS_MODULE:-config.settings}"

DG_GROUP_BY="$(read_local_env DG_OPERACION_GROUP_BY)"
DG_START_DATE="$(read_local_env DG_OPERACION_START_DATE)"
DG_END_DATE="$(read_local_env DG_OPERACION_END_DATE)"
DG_FECHA_OPERACION="$(read_local_env DG_OPERACION_FECHA_OPERACION)"
DG_OUTPUT_DIR="$(read_local_env DG_OPERACION_OUTPUT_DIR)"
DG_EXPORT_FORMATS="$(read_local_env DG_OPERACION_EXPORT_FORMATS)"

DG_GROUP_BY="${DG_GROUP_BY:-day}"
DG_OUTPUT_DIR="${DG_OUTPUT_DIR:-storage/dg_reports}"
DG_EXPORT_FORMATS="${DG_EXPORT_FORMATS:-json,xlsx}"

IFS=',' read -r -a FORMATS <<< "$DG_EXPORT_FORMATS"

echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] dg_operacion snapshot: inicio"
for format in "${FORMATS[@]}"; do
  format="$(echo "$format" | tr '[:upper:]' '[:lower:]' | xargs)"
  if [ -z "$format" ]; then
    continue
  fi
  CMD=(
    "$PYTHON_BIN" manage.py generar_snapshot_dg_operacion
    --format "$format"
    --output-dir "$DG_OUTPUT_DIR"
    --dg-group-by "$DG_GROUP_BY"
  )
  if [ -n "$DG_START_DATE" ]; then
    CMD+=(--dg-start-date "$DG_START_DATE")
  fi
  if [ -n "$DG_END_DATE" ]; then
    CMD+=(--dg-end-date "$DG_END_DATE")
  fi
  if [ -n "$DG_FECHA_OPERACION" ]; then
    CMD+=(--fecha-operacion "$DG_FECHA_OPERACION")
  fi
  "${CMD[@]}"
done
echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] dg_operacion snapshot: fin"
