#!/bin/sh
# Switch only an already staged, schema-compatible web release.
set -eu

previous=${1:?previous loopback port required}
candidate=${2:?candidate loopback port required}
route=${ERP_WEB_ROUTE:-/etc/nginx/conf.d/erp-web-upstream.conf}
probe=${ERP_WEB_PROBE:-https://erp.pollyanasdolce.com/login/}
host=${ERP_WEB_HOST:-erp.pollyanasdolce.com}
config=${ERP_NGINX_CONFIG:-/etc/nginx/nginx.conf}
ready_seconds=${ERP_WEB_READY_SECONDS:-120}

for port in "$previous" "$candidate"; do
  case "$port" in ''|*[!0-9]*|0*) exit 2;; esac
  [ "$port" -ge 1024 ] && [ "$port" -le 65535 ] || exit 2
done
[ "$previous" != "$candidate" ] && [ "$#" -eq 2 ] || exit 2
case "$ready_seconds" in ''|*[!0-9]*|0*) exit 2;; esac
[ "$ready_seconds" -le 120 ] || exit 2
exec 9>"${ERP_DEPLOY_LOCK:-/run/lock/erp-deploy.lock}"
flock -n 9 || { echo 'another ERP deployment is running' >&2; exit 1; }
[ -f "$route" ] && [ ! -L "$route" ] || { echo 'managed nginx route is missing' >&2; exit 1; }

direct_ready() {
  status=$(curl -sS --connect-timeout 1 --max-time 3 -o /dev/null -w '%{http_code}' \
    -H "Host: $host" -H 'X-Forwarded-Proto: https' "http://127.0.0.1:$1/login/") || return 1
  [ "$status" = 200 ]
}

through_proxy() {
  headers=$(curl -fsS --connect-timeout 2 --max-time 5 -D - -o /dev/null "$probe") || return 1
  headers=$(printf '%s\n' "$headers" | tr -d '\r')
  printf '%s\n' "$headers" | grep -Eq '^HTTP/[0-9.]+ 200( |$)' || return 1
  printf '%s\n' "$headers" | grep -iFxq "X-ERP-Backend: 127.0.0.1:$1"
}

wait_until() {
  deadline=$(( $(date +%s) + ready_seconds ))
  until "$@"; do
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 1
  done
}

through_proxy "$previous" || { echo 'the previous slot is not serving traffic' >&2; exit 1; }
direct_ready "$previous" || exit 1
wait_until direct_ready "$candidate" || { echo 'candidate failed readiness; previous slot retained' >&2; exit 1; }

stage=$(mktemp -d "$route.transaction.XXXXXX")
backup=$stage/previous
next=$stage/next
rollback=0
cleanup() {
  result=$?
  trap - EXIT
  if [ "$rollback" -eq 1 ]; then
    if ! cp -p "$backup" "$next" || ! mv -f "$next" "$route" || ! nginx -t -c "$config" || ! nginx -s reload -c "$config" ||
       ! wait_until through_proxy "$previous"; then
      echo "could not confirm rollback; previous configuration preserved at $backup" >&2
      exit 1
    fi
  fi
  [ ! -e "$backup" ] || unlink "$backup"
  [ ! -e "$next" ] || unlink "$next"
  rmdir "$stage"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
cp -p "$route" "$backup"
printf 'upstream erp_web {\n  server 127.0.0.1:%s;\n  server 127.0.0.1:%s backup;\n}\n' \
  "$candidate" "$previous" > "$next"
chmod 644 "$next"
rollback=1
mv -f "$next" "$route"
nginx -t -c "$config"
nginx -s reload -c "$config"
wait_until through_proxy "$candidate" || { echo 'candidate not confirmed through nginx' >&2; exit 1; }
rollback=0
printf 'erp-web-active=%s fallback=%s\n' "$candidate" "$previous"
