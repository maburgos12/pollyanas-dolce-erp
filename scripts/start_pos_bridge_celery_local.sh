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

echo "Desinstalando launchd legado de pos_bridge..."
./scripts/uninstall_pos_bridge_sales_close_launchd.sh >/dev/null 2>&1 || true
./scripts/uninstall_pos_bridge_inventory_launchd.sh >/dev/null 2>&1 || true

echo "Levantando Postgres y Redis locales..."
docker compose up -d db redis

echo "Esperando Postgres..."
for _ in $(seq 1 60); do
  if docker compose exec -T db pg_isready -U "${DB_USER:-postgres}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! docker compose exec -T db pg_isready -U "${DB_USER:-postgres}" >/dev/null 2>&1; then
  echo "Postgres no respondió a tiempo."
  exit 1
fi

echo "Esperando Redis..."
for _ in $(seq 1 30); do
  if docker compose exec -T redis redis-cli ping >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! docker compose exec -T redis redis-cli ping >/dev/null 2>&1; then
  echo "Redis no respondió a tiempo."
  exit 1
fi

echo "Aplicando migraciones en Postgres..."
docker compose run --rm web python manage.py migrate

echo "Registrando schedules de django-celery-beat en Postgres..."
docker compose run --rm web python manage.py setup_celery_schedules

echo "Levantando web, worker y beat..."
docker compose up -d web worker beat

echo "Stack local Celery listo."
echo "Web local queda disponible por Docker en el puerto configurado y todo el stack usa Postgres."
echo "Usa ./scripts/show_pos_bridge_sync_status.sh para validar estado."
