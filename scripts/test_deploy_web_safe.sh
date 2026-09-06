#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/deploy_web_safe.sh"

requires_process_restart $'logistica/models.py\nlogistica/migrations/0036_example.py'
requires_process_restart $'api/logistica_views.py'
requires_process_restart $'requirements.txt'
requires_compose_recreate $'docker-compose.yml\npos_bridge/tasks/celery_tasks.py'
if requires_compose_recreate 'pos_bridge/tasks/celery_tasks.py'; then
  echo "Python-only changes need restart, not compose recreation" >&2
  exit 1
fi

if requires_process_restart $'logistica/templates/logistica/ruta_detail.html\nlogistica/static/logistica/pwa/sw.js'; then
  echo "HTML/JS-only changes must not require a process restart" >&2
  exit 1
fi

echo "deploy-web-safe-tests-ok"
