#!/usr/bin/env bash
set -euo pipefail
[[ $EUID == 0 && $# == 4 && "$2" =~ ^[0-9a-f]{40}$ \
  && "$3" =~ ^sha256:[0-9a-f]{64}$ && "$4" =~ ^sha256:[0-9a-f]{64}$ ]] || {
  echo 'Usage(root): install.sh CONTROL_REPO REVIEWED_SHA TOOLS_IMAGE_ID PG16_IMAGE_ID' >&2; exit 2;
}
repo="$1" revision="$2" tools_image="$3" pg_image="$4"
[[ "$(git -C "$repo" remote get-url origin)" == https://github.com/maburgos12/pollyanas-dolce-erp.git ]]
[[ "$(git -C "$repo" rev-parse "$revision^{commit}")" == "$revision" ]]
[[ -f /etc/erp-autofix/app.pem && -f /etc/erp-autofix/github-app.json ]]
[[ "$(id -Gn erp-codex)" == erp-codex ]]
docker image inspect "$tools_image" "$pg_image" >/dev/null
install -d -o root -g root -m 0755 /opt/erp-autofix /opt/erp-autofix/releases
install -d -o root -g root -m 0700 /etc/erp-autofix /var/lib/erp-autofix /var/lib/erp-autofix/jobs
install -d -o root -g root -m 0711 /var/lib/erp-codex/jobs
release="/opt/erp-autofix/releases/$revision"
[[ ! -e "$release" ]] || { echo 'Release exists: inspect before re-installing' >&2; exit 1; }
install -d -o root -g root -m 0755 "$release"
git -C "$repo" archive "$revision" ops/erp-autofix scripts/task_factory.py \
  infra/systemd/erp-autofix.service infra/systemd/erp-autofix.timer | tar -x -C "$release"
chown -R root:root "$release"
chmod -R go-w "$release"
python3 - "$tools_image" "$pg_image" <<'PY'
import json,sys,os
from pathlib import Path
p=Path('/etc/erp-autofix/config.json')
p.write_text(json.dumps(dict(tools_image=sys.argv[1],pg_image=sys.argv[2])))
p.chmod(0o600)
PY
systemd-analyze verify "$release"/infra/systemd/erp-autofix.service "$release"/infra/systemd/erp-autofix.timer
ln -sfn "$release" /opt/erp-autofix/current
install -o root -g root -m 0644 "$release"/infra/systemd/erp-autofix.service \
  "$release"/infra/systemd/erp-autofix.timer /etc/systemd/system/
systemctl daemon-reload
# Dispatcher runs only after reviewed installation; the existing observer/monitor are untouched.
systemctl enable --now erp-autofix.timer
systemctl is-active erp-autofix.timer
printf 'Installed draft-only dispatcher %s\n' "$revision"
