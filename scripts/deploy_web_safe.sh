#!/bin/bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/pastelerias-erp}"
COMPOSE_FILE="${COMPOSE_FILE:-$APP_DIR/docker-compose.yml}"
COMPOSE=(docker compose -f "$COMPOSE_FILE")

cd "$APP_DIR"

git pull origin main
"${COMPOSE[@]}" exec -T web python manage.py migrate --noinput
"${COMPOSE[@]}" exec -T web python manage.py check
"${COMPOSE[@]}" exec -T web python manage.py collectstatic --noinput
"${COMPOSE[@]}" exec -T web sh -lc 'kill -HUP 1'

for _ in {1..20}; do
  if curl -fsS http://127.0.0.1:8011/login/ >/dev/null; then
    echo "web-ready"
    exit 0
  fi
  sleep 1
done

echo "web did not become ready after HUP reload" >&2
exit 1
