#!/bin/bash
set -euo pipefail

requires_process_restart() {
  local changed_files="${1:-}"
  grep -Eq '(^|/)[^/]+\.py$|(^|/)(requirements[^/]*\.txt|pyproject\.toml|poetry\.lock|Dockerfile[^/]*|docker-compose[^/]*\.ya?ml)$' <<<"$changed_files"
}

requires_compose_recreate() {
  grep -Eq '(^|/)docker-compose[^/]*\.ya?ml$' <<<"${1:-}"
}

# Una dependencia nueva sólo existe si se reconstruye la imagen: `restart`
# reusa la que ya está y el import falla en producción con ModuleNotFoundError.
requires_image_rebuild() {
  grep -Eq '(^|/)(requirements[^/]*\.txt|pyproject\.toml|poetry\.lock|Dockerfile[^/]*)$' <<<"${1:-}"
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

  # Una dependencia nueva se instala reconstruyendo la imagen, y eso tiene que
  # pasar ANTES de migrar y de `check`: el contenedor viejo no puede importar el
  # código nuevo, así que `check` abortaría el deploy a medias.
  local imagen_reconstruida=0
  if requires_image_rebuild "$changed_files"; then
    imagen_reconstruida=1
    "${compose[@]}" up -d --build --force-recreate worker beat worker_recetas recetas_watchdog web
  fi

  "${compose[@]}" exec -T web python manage.py migrate --noinput
  "${compose[@]}" exec -T web python manage.py check
  "${compose[@]}" exec -T web python manage.py collectstatic --noinput

  if [[ "$imagen_reconstruida" == "1" ]]; then
    : # La imagen nueva ya está corriendo; recrear otra vez sólo alarga el corte.
  elif requires_compose_recreate "$changed_files"; then
    # restart does not apply changed commands or create new services.
    "${compose[@]}" up -d --no-deps --build --force-recreate worker beat worker_recetas recetas_watchdog web
  elif requires_process_restart "$changed_files"; then
    # Gunicorn runs with preload, so HUP forks workers from the master's stale
    # Python memory. Celery processes also retain imported task code.
    "${compose[@]}" restart worker beat worker_recetas recetas_watchdog
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
