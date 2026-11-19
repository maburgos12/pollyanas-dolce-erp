#!/bin/bash
set -euo pipefail
umask 077
test "$(id -u)" = 0
repo=/opt/pastelerias-erp
nas_home=/share/homes/DolcePasteleria/.erp-backup-verifier
nas_python=/share/CACHEDEV1_DATA/.qpkg/HybridBackup/CloudConnector3/python/bin/python3
rotator=/usr/local/sbin/erp-rotate-verified-backups
wrapper='#!/bin/sh
exec /usr/bin/python3 /opt/pastelerias-erp/scripts/rotate_verified_backups.py "$@"'
test -x "$rotator"
# Refuse to overwrite an operator's unreviewed replacement.
if ! cmp -s "$rotator" <(printf '%s\n' "$wrapper"); then
  test "$(sha256sum "$rotator" | cut -d ' ' -f 1)" = e8cba9307cf6486aa2429004a0f81e62bdbd472f8afe6b6117f9da6a28de1e2f
fi
/usr/bin/python3 -m py_compile "$repo/scripts/rotate_verified_backups.py" "$repo/scripts/vps_backup_health.py"
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes qnap-nasdolce-archive \
  "test -d $nas_home/runtime && test -O $nas_home && chmod 700 $nas_home"
# Deploy only the reviewed revision already delivered through Git to this VPS.
ssh qnap-nasdolce-archive "umask 077; cat > $nas_home/nas_restore_daily.py.next && mv $nas_home/nas_restore_daily.py.next $nas_home/nas_restore_daily.py" < "$repo/scripts/nas_restore_daily.py"
ssh qnap-nasdolce-archive "$nas_python $nas_home/nas_restore_daily.py --prepare-runtime"
install -m 644 "$repo/scripts/ops/erp-backup-health.service" /etc/systemd/system/
install -m 644 "$repo/scripts/ops/erp-backup-health.timer" /etc/systemd/system/
systemd-analyze verify /etc/systemd/system/erp-backup-health.service /etc/systemd/system/erp-backup-health.timer
printf '%s\n' "$wrapper" > "$rotator.next"
chmod 755 "$rotator.next"
mv "$rotator.next" "$rotator"
systemctl daemon-reload
# Enable the timer only after the first successful service run has been inspected.
echo 'Installed; run python3 scripts/vps_backup_health.py --verify-only and inspect health-latest.json before enabling the timer. The service verifies recovery without cleanup; the existing cron rotator remains separate.'
