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

STAMP="$(date +%Y%m%d_%H%M%S)"
EXPORT_PATH="$ROOT_DIR/tmp/sqlite_to_postgres_${STAMP}.json"
SQLITE_BACKUP="$ROOT_DIR/backups/db.sqlite3.pre_postgres_migration_${STAMP}"

mkdir -p "$ROOT_DIR/tmp" "$ROOT_DIR/backups"

echo "Deteniendo stack Docker para congelar escrituras..."
docker compose stop web beat worker redis db >/dev/null 2>&1 || true

if [ -f "$ROOT_DIR/db.sqlite3" ]; then
  cp "$ROOT_DIR/db.sqlite3" "$SQLITE_BACKUP"
  echo "Respaldo SQLite: $SQLITE_BACKUP"
fi

echo "Exportando datos desde SQLite..."
DATABASE_URL="" DB_HOST="db" DJANGO_SETTINGS_MODULE=config.settings \
  "$PYTHON_BIN" manage.py dumpdata \
  --natural-foreign \
  --natural-primary \
  --exclude contenttypes \
  --exclude auth.permission \
  --exclude sessions \
  --exclude admin.logentry \
  --exclude django_celery_beat \
  > "$EXPORT_PATH"

echo "Levantando Postgres..."
docker compose up -d db

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

echo "Aplicando migraciones sobre Postgres..."
docker compose run --rm web python manage.py migrate

echo "Limpiando Postgres antes de cargar datos..."
docker compose run --rm web python manage.py flush --noinput

echo "Cargando datos exportados a Postgres..."
docker compose run --rm -T web python manage.py loaddata "/app/tmp/$(basename "$EXPORT_PATH")"

echo "Recreando schedules de Celery..."
docker compose run --rm web python manage.py setup_celery_schedules

echo "Migración completada."
echo "Dump JSON: $EXPORT_PATH"
