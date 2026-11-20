#!/bin/bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
fixture="$(mktemp -d)"
container="erp-rollout-test-$$"
cleanup() {
  if docker inspect "$container" >/dev/null 2>&1; then
    test "$(docker inspect --format '{{index .Config.Labels "codex.task"}}' "$container")" = erp-deploy-continuidad
    docker stop "$container" >/dev/null
  fi
  find "$fixture" -maxdepth 1 -type f -delete
  rmdir "$fixture"
}
trap cleanup EXIT
cat > "$fixture/nginx.conf" <<'NGINX'
pid /tmp/router.pid;
events {}
http {
  include /fixture/route.conf;
  server {
    listen 18080;
    location / {
      proxy_pass http://erp_web;
      proxy_next_upstream error timeout http_502 http_503 http_504;
      proxy_hide_header X-ERP-Backend;
      add_header X-ERP-Backend $upstream_addr;
    }
  }
}
NGINX
for slot in 18001 18002; do
  cat > "$fixture/backend-$slot.conf" <<NGINX
pid /tmp/backend-$slot.pid;
events {}
http { server { listen $slot; location / { return 200 "$slot"; } } }
NGINX
done
printf 'upstream erp_web { server 127.0.0.1:18001; }\n' > "$fixture/route.conf"
docker run --rm -d --name "$container" --label codex.task=erp-deploy-continuidad \
  -v "$fixture:/fixture" -v "$root/scripts:/scripts:ro" nginx:stable-alpine@sha256:583b7f2fce1cdb8da5ad5c39ebd3c33b98b87d493190e548bdee1fa8c619c311 sleep 3600 >/dev/null
run() { docker exec "$container" "$@"; }
run nginx -c /fixture/backend-18001.conf
run nginx -c /fixture/nginx.conf
probe() { run curl -fsS --max-time 2 http://127.0.0.1:18080/login/; }
promote() {
  run env ERP_NGINX_CONFIG=/fixture/nginx.conf ERP_WEB_ROUTE=/fixture/route.conf \
    ERP_WEB_PROBE=http://127.0.0.1:18080/login/ ERP_DEPLOY_LOCK=/tmp/erp-deploy.lock \
    ERP_WEB_READY_SECONDS=3 sh /scripts/promote_erp_web.sh 18001 18002
}
test "$(probe)" = 18001
if promote; then
  echo 'An unavailable candidate was accepted' >&2
  exit 1
fi
test "$(probe)" = 18001
test "$(cat "$fixture/route.conf")" = 'upstream erp_web { server 127.0.0.1:18001; }'
run nginx -c /fixture/backend-18002.conf
cat > "$fixture/curl" <<'SH'
#!/bin/sh
set -e
result=$(/usr/bin/curl "$@")
case "$*" in
  *http://127.0.0.1:18002/login/) nginx -s stop -c /fixture/backend-18002.conf >/dev/null ;;
esac
printf '%s\n' "$result"
SH
chmod +x "$fixture/curl"
if run env PATH=/fixture:/usr/bin:/usr/sbin:/bin:/sbin ERP_NGINX_CONFIG=/fixture/nginx.conf \
    ERP_WEB_ROUTE=/fixture/route.conf ERP_WEB_PROBE=http://127.0.0.1:18080/login/ \
    ERP_DEPLOY_LOCK=/tmp/erp-deploy.lock ERP_WEB_READY_SECONDS=3 \
    sh /scripts/promote_erp_web.sh 18001 18002; then
  echo 'A candidate that failed at cutover was accepted' >&2
  exit 1
fi
test "$(probe)" = 18001
test "$(cat "$fixture/route.conf")" = 'upstream erp_web { server 127.0.0.1:18001; }'
run nginx -c /fixture/backend-18002.conf
run sh -c 'for i in $(seq 1 120); do curl -fsS --max-time 2 http://127.0.0.1:18080/login/ || exit 1; sleep 0.03; done' > "$fixture/traffic" &
traffic_pid=$!
promote
wait "$traffic_pid"
test "$(wc -c < "$fixture/traffic" | tr -d " ")" = 600
test "$(probe)" = 18002
run nginx -s stop -c /fixture/backend-18002.conf
for i in $(seq 1 15); do test "$(probe)" = 18001; done
echo 'NGINX: candidato caído antes y después del cambio; tráfico continuo durante el cambio.'
