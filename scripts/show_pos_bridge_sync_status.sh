#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
if [ ! -x "$PYTHON_BIN" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  else
    PYTHON_BIN="$(command -v python)"
  fi
fi

echo "== Launchd ventas =="
launchctl print "gui/$(id -u)/com.pollyanasdolce.pos-bridge-sales-close" 2>/dev/null | sed -n '1,40p' || echo "No instalado"
echo
echo "== Launchd inventario =="
launchctl print "gui/$(id -u)/com.pollyanasdolce.pos-bridge-inventory" 2>/dev/null | sed -n '1,40p' || echo "No instalado"
echo
echo "== Ultimos jobs pos_bridge =="
DJANGO_SETTINGS_MODULE=config.settings "$PYTHON_BIN" manage.py shell -c "from pos_bridge.models import PointSyncJob; import json; jobs=list(PointSyncJob.objects.order_by('-id')[:6].values('id','job_type','status','started_at','finished_at','error_message')); print(json.dumps(jobs, ensure_ascii=False, indent=2, default=str))"
