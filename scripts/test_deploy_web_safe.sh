#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/deploy_web_safe.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
docker_calls="$tmp_dir/docker-calls.txt"

docker() {
  printf '%s\n' "$*" >>"$docker_calls"
}

build_application_images "/tmp/docker-compose-test.yml"
cat >"$tmp_dir/expected-docker-calls.txt" <<'EOF'
compose -f /tmp/docker-compose-test.yml build web
compose -f /tmp/docker-compose-test.yml build worker
compose -f /tmp/docker-compose-test.yml build worker_notificaciones
compose -f /tmp/docker-compose-test.yml build worker_sat
compose -f /tmp/docker-compose-test.yml build worker_recetas
compose -f /tmp/docker-compose-test.yml build worker_logistica_point
compose -f /tmp/docker-compose-test.yml build recetas_watchdog
compose -f /tmp/docker-compose-test.yml build beat
EOF
diff -u "$tmp_dir/expected-docker-calls.txt" "$docker_calls"

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

# Una dependencia nueva exige reconstruir la imagen: con `restart` el
# contenedor reusa la imagen vieja y el import revienta en producción.
requires_image_rebuild $'requirements.txt'
requires_image_rebuild $'Dockerfile'
requires_image_rebuild $'pyproject.toml'
requires_image_rebuild $'activos/services_pasaporte.py\nrequirements.txt'

for solo_codigo in \
  'logistica/models.py' \
  'docker-compose.yml' \
  'templates/operacion/activo_pasaporte.html' \
  'static/operacion/sw.js'
do
  if requires_image_rebuild "$solo_codigo"; then
    echo "«$solo_codigo» no cambia dependencias; reconstruir la imagen sólo alarga el corte" >&2
    exit 1
  fi
done

echo "deploy-web-safe-tests-ok"

# Real isolated Git history: main advances after the approved release.
# Pinned deployment must stop at that release, not deploy the later commit.
git init --bare --quiet "$tmp_dir/origin.git"
git init --quiet -b main "$tmp_dir/author"
git -C "$tmp_dir/author" config user.email test@example.invalid
git -C "$tmp_dir/author" config user.name Test
printf 'base\n' > "$tmp_dir/author/version"
git -C "$tmp_dir/author" add version
git -C "$tmp_dir/author" commit --quiet -m base
base=$(git -C "$tmp_dir/author" rev-parse HEAD)
git -C "$tmp_dir/author" remote add origin "$tmp_dir/origin.git"
git -C "$tmp_dir/author" push --quiet origin main
git clone --quiet -b main "$tmp_dir/origin.git" "$tmp_dir/production"
printf 'approved\n' > "$tmp_dir/author/version"
git -C "$tmp_dir/author" commit --quiet -am approved
approved=$(git -C "$tmp_dir/author" rev-parse HEAD)
printf 'unapproved\n' > "$tmp_dir/author/version"
git -C "$tmp_dir/author" commit --quiet -am unapproved
git -C "$tmp_dir/author" push --quiet origin main
(
  cd "$tmp_dir/production"
  if update_checkout invalid "$base"; then exit 1; fi
  if update_checkout "$approved" ''; then exit 1; fi
  touch foreign-file
  if update_checkout "$approved" "$base"; then exit 1; fi
  rm foreign-file
  if update_checkout "$approved" "$approved"; then exit 1; fi
  test "$(git rev-parse HEAD)" = "$base"
  update_checkout "$approved" "$base"
  test "$(git rev-parse HEAD)" = "$approved"
  test "$(cat version)" = approved
  # A repeated attempt using the old production base is rejected.
  if update_checkout "$approved" "$base"; then exit 1; fi
)
echo "deploy-exact-revision-tests-ok"

# Run main with Docker captured: rejection must happen before any application call.
# macOS has no util-linux flock; mock its decision here, Ubuntu also tests a real lock.
: > "$docker_calls"
(
  export APP_DIR="$tmp_dir/production" ERP_DEPLOY_LOCK="$tmp_dir/deploy.lock"
  flock() { return 1; }
  if main --expected-sha "$approved" --expected-base "$approved"; then exit 1; fi
  flock() { return 0; }
  if main --expected-sha invalid --expected-base "$approved"; then exit 1; fi
  if main --expected-sha "$approved" --expected-base "$base"; then exit 1; fi
  touch "$tmp_dir/production/foreign-file"
  if main; then exit 1; fi
  rm "$tmp_dir/production/foreign-file"
  test ! -s "$docker_calls"
)
if command -v flock >/dev/null 2>&1; then
  (
    exec 8>"$tmp_dir/real.lock"
    flock -n 8
    if (export APP_DIR="$tmp_dir/production" ERP_DEPLOY_LOCK="$tmp_dir/real.lock";
        main --expected-sha "$approved" --expected-base "$approved"); then exit 1; fi
    test ! -s "$docker_calls"
  )
fi
echo "deploy-preflight-before-docker-tests-ok"

(
  docker() {
    if [[ "$1" == compose ]]; then
      echo "container-${*: -1}"
    elif [[ "$1" == inspect ]]; then
      for _ in {1..8}; do echo "${process_running:-true} 2026-01-01T00:00:00Z"; done
    else return 1
    fi
  }
  verify_processes /tmp/compose.yml 1767225600
  if verify_processes /tmp/compose.yml 1767225601 2>/dev/null; then
    echo 'stale process accepted' >&2; exit 1
  fi
  process_running=false
  if verify_processes /tmp/compose.yml 0 2>/dev/null; then
    echo 'stopped process accepted' >&2; exit 1
  fi
)
echo "deploy-process-verification-tests-ok"
