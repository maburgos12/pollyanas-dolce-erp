#!/usr/bin/env bash
set -euo pipefail

# Install only a reviewed Git revision; never change the ERP checkout or its services.
[[ $EUID == 0 && $# == 2 && "$2" =~ ^[0-9a-f]{40}$ ]] || {
  echo "Usage (root): bash install.sh REPO REVIEWED_SHA" >&2; exit 2;
}
repo="$1" revision="$2"
[[ "$(git -C "$repo" rev-parse "$revision^{commit}")" == "$revision" ]]
getent passwd erp-observer >/dev/null || useradd --system --user-group \
  --home-dir /var/lib/erp-observer --shell /usr/sbin/nologin erp-observer
[[ "$(id -Gn erp-observer)" == erp-observer ]] || { echo "Unexpected observer groups" >&2; exit 1; }
install -d -o root -g root -m 0755 /opt/erp-observer /opt/erp-observer/releases
install -d -o root -g erp-observer -m 2750 /var/lib/erp-observer-source
release="/opt/erp-observer/releases/$revision"
if [[ ! -d "$release" ]]; then
  install -d -o root -g root -m 0755 "$release"
  git -C "$repo" archive "$revision" ops/erp-observer infra/systemd/erp-observer.service \
    infra/systemd/erp-observer-collect.service infra/systemd/erp-observer-collect.timer | tar -x -C "$release"
  chown -R root:root "$release"
  chmod -R go-w "$release"
fi
systemd-analyze verify "$release"/infra/systemd/erp-observer*.service "$release"/infra/systemd/erp-observer*.timer
ln -sfn "$release" /opt/erp-observer/current
install -o root -g root -m 0644 "$release"/infra/systemd/erp-observer*.service \
  "$release"/infra/systemd/erp-observer*.timer /etc/systemd/system/
systemctl daemon-reload
systemctl start erp-observer-collect.service
systemctl enable erp-observer.service erp-observer-collect.timer
systemctl restart erp-observer.service erp-observer-collect.timer
systemctl is-active erp-observer.service erp-observer-collect.timer
printf 'Installed observation revision %s\n' "$revision"
