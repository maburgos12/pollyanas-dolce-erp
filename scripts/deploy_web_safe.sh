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

# Docker Compose asigna una imagen distinta a cada servicio aun cuando todos
# comparten el mismo Dockerfile. Construirlos en una sola llamada puede dejar
# unas imágenes con la caché anterior y otras con las dependencias nuevas.
# La construcción secuencial garantiza que cada runtime reciba requirements.txt.
build_application_images() {
  local compose_file="$1"
  local service
  for service in web worker worker_notificaciones worker_sat worker_recetas worker_logistica_point recetas_watchdog beat; do
    COMPOSE_PARALLEL_LIMIT=1 docker compose -f "$compose_file" build "$service"
  done
}

# Called under the common deployment lock. Fetching main proves membership;
# pulling the immutable SHA prevents a later main update from changing release.
# Checkout files must remain readable by the app despite the publisher umask 0077.
update_checkout() {
  local expected_sha="${1:-}" expected_base="${2:-}"
  [[ "$(git branch --show-current)" == main && -z "$(git status --porcelain)" ]] || {
    echo "deployment checkout must be clean main" >&2; return 1;
  }
  if [[ -n "$expected_sha" || -n "$expected_base" ]]; then
    [[ "$expected_sha" =~ ^[0-9a-f]{40}$ && "$expected_base" =~ ^[0-9a-f]{40}$ ]] || {
      echo "invalid expected revision" >&2; return 1;
    }
    [[ "$(git rev-parse HEAD)" == "$expected_base" ]] || {
      echo "production revision changed" >&2; return 1;
    }
    git fetch origin main || return 1
    git merge-base --is-ancestor "$expected_base" "$expected_sha" || return 1
    git merge-base --is-ancestor "$expected_sha" FETCH_HEAD || return 1
    (umask 022; git pull --ff-only origin "$expected_sha") || return 1
    [[ "$(git rev-parse HEAD)" == "$expected_sha" ]] || return 1
  else
    (umask 022; git pull --ff-only origin main)
  fi
}

verify_processes() {
  local compose_file="$1" minimum_started="$2" service identifier
  local -a identifiers=()
  for service in web worker worker_notificaciones worker_sat worker_recetas worker_logistica_point recetas_watchdog beat; do
    identifier="$(docker compose -f "$compose_file" ps -q "$service")" || return 1
    [[ -n "$identifier" && "$identifier" != *$'\n'* ]] || return 1
    identifiers+=("$identifier")
  done
  docker inspect --format '{{.State.Running}} {{.State.StartedAt}}' "${identifiers[@]}" |
    python3 -c 'import datetime,sys
rows=sys.stdin.read().splitlines()
minimum=int(sys.argv[1])
assert len(rows)==8, "missing application processes"
for row in rows:
 running,started=row.split()
 assert running=="true", "application process stopped"
 stamp=datetime.datetime.fromisoformat(started.replace("Z", "+00:00")).timestamp()
 assert stamp>=minimum, "application process did not restart"
' "$minimum_started"
}

# Gunicorn preload took 29s in production; bound both startup and each probe.
wait_for_web() {
  local deadline remaining probe_timeout
  deadline=$(( $(date +%s) + 120 ))
  while true; do
    remaining=$(( deadline - $(date +%s) ))
    (( remaining > 0 )) || break
    probe_timeout=5
    (( remaining >= probe_timeout )) || probe_timeout="$remaining"
    if curl --connect-timeout 2 --max-time "$probe_timeout" -fsS http://127.0.0.1:8011/login/ >/dev/null; then
      return 0
    fi
    (( $(date +%s) < deadline )) || break
    sleep 1
  done
  echo "web did not become ready within 120 seconds" >&2
  return 1
}

main() {
  local app_dir="${APP_DIR:-/opt/pastelerias-erp}"
  local compose_file="${COMPOSE_FILE:-$app_dir/docker-compose.yml}"
  local old_head new_head changed_files expected_sha="" expected_base=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --expected-sha) [[ $# -ge 2 && "$2" =~ ^[0-9a-f]{40}$ ]] || return 2; expected_sha="$2"; shift 2 ;;
      --expected-base) [[ $# -ge 2 && "$2" =~ ^[0-9a-f]{40}$ ]] || return 2; expected_base="$2"; shift 2 ;;
      *) echo "unknown deployment argument" >&2; return 2 ;;
    esac
  done
  # All invocations, including manual deployments, share this lock.
  exec 9>"${ERP_DEPLOY_LOCK:-/run/lock/erp-deploy.lock}"
  flock -n 9 || { echo "another deployment is running" >&2; return 1; }
  local -a compose=(docker compose -f "$compose_file")

  cd "$app_dir" || return 1
  old_head="$(git rev-parse HEAD)"
  update_checkout "$expected_sha" "$expected_base" || return 1
  new_head="$(git rev-parse HEAD)"
  changed_files="$(git diff --name-only "$old_head" "$new_head")"

  # Una dependencia nueva se instala reconstruyendo la imagen, y eso tiene que
  # pasar ANTES de migrar y de `check`: el contenedor viejo no puede importar el
  # código nuevo, así que `check` abortaría el deploy a medias.
  local imagen_reconstruida=0 deployment_started
  deployment_started="$(date +%s)"
  if requires_image_rebuild "$changed_files"; then
    imagen_reconstruida=1
    build_application_images "$compose_file"
    "${compose[@]}" up -d --force-recreate worker worker_notificaciones worker_sat beat worker_recetas worker_logistica_point recetas_watchdog web
  fi

  "${compose[@]}" exec -T web python manage.py migrate --noinput
  "${compose[@]}" exec -T web python manage.py check
  "${compose[@]}" exec -T web python manage.py collectstatic --noinput

  if [[ "$imagen_reconstruida" == "1" ]]; then
    # La imagen nueva ya corría antes de collectstatic, así que el manifiesto de
    # estáticos que tiene en memoria es el viejo: sin este reinicio, cualquier
    # plantilla que use un estático nuevo revienta con "Missing staticfiles
    # manifest entry". No se recrea el contenedor otra vez, sólo se reinicia.
    "${compose[@]}" restart worker worker_notificaciones worker_sat beat worker_recetas worker_logistica_point recetas_watchdog
    "${compose[@]}" restart web
  elif requires_compose_recreate "$changed_files"; then
    # restart does not apply changed commands or create new services.
    "${compose[@]}" up -d --no-deps --build --force-recreate worker worker_notificaciones worker_sat beat worker_recetas worker_logistica_point recetas_watchdog web
  elif requires_process_restart "$changed_files"; then
    # Gunicorn runs with preload, so HUP forks workers from the master's stale
    # Python memory. Celery processes also retain imported task code.
    "${compose[@]}" restart worker worker_notificaciones worker_sat beat worker_recetas worker_logistica_point recetas_watchdog
    "${compose[@]}" restart web
  else
    "${compose[@]}" exec -T web sh -lc 'kill -HUP 1'
  fi

  local minimum_started=0
  if [[ "$imagen_reconstruida" == 1 ]] || requires_process_restart "$changed_files" || requires_compose_recreate "$changed_files"; then
    minimum_started="$deployment_started"
  fi
  verify_processes "$compose_file" "$minimum_started" || return 1

  wait_for_web || return 1
  echo "web-ready revision=$new_head"

}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
