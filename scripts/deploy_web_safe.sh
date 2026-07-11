#!/bin/bash
set -euo pipefail

requires_process_restart() {
  local changed_files="${1:-}"
  grep -Eq '(^|/)[^/]+\.py$|(^|/)(requirements[^/]*\.txt|pyproject\.toml|poetry\.lock|Dockerfile[^/]*|docker-compose[^/]*\.ya?ml)$' <<<"$changed_files"
}

main() {
  local app_dir="${APP_DIR:-/opt/pastelerias-erp}"
  local compose_file="${COMPOSE_FILE:-$app_dir/docker-compose.yml}"
  local old_head new_head changed_files
  local -a compose=(docker compose -f "$compose_file")

  cd "$app_dir"
  old_head="$(git rev-parse HEAD)"
  git pull origin main
  new_head="$(git rev-parse HEAD)"
  changed_files="$(git diff --name-only "$old_head" "$new_head")"

  "${compose[@]}" exec -T web python manage.py migrate --noinput
  "${compose[@]}" exec -T web python manage.py check
  "${compose[@]}" exec -T web python manage.py collectstatic --noinput

  if requires_process_restart "$changed_files"; then
    # Gunicorn runs with preload, so HUP forks workers from the master's stale
    # Python memory. Celery processes also retain imported task code.
    "${compose[@]}" restart worker beat
    "${compose[@]}" restart web
  else
    "${compose[@]}" exec -T web sh -lc 'kill -HUP 1'
  fi

  for _ in {1..20}; do
    if curl -fsS http://127.0.0.1:8011/login/ >/dev/null; then
      echo "web-ready"
      return 0
    fi
    sleep 1
  done

  echo "web did not become ready after deploy reload" >&2
  return 1
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
