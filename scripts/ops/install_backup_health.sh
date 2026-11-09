#!/bin/bash
set -euo pipefail
umask 077
test "$(id -u)" = 0
repo=/opt/pastelerias-erp
nas_home=/share/homes/DolcePasteleria/.erp-backup-verifier
nas_python=/share/CACHEDEV1_DATA/.qpkg/HybridBackup/CloudConnector3/python/bin/python3
test -x /usr/local/sbin/erp-rotate-verified-backups
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes qnap-nasdolce-archive \
  "test -d $nas_home/runtime && test -O $nas_home && chmod 700 $nas_home"
# Deploy only the reviewed revision already delivered through Git to this VPS.
ssh qnap-nasdolce-archive "umask 077; cat > $nas_home/nas_restore_daily.py.next && mv $nas_home/nas_restore_daily.py.next $nas_home/nas_restore_daily.py" < "$repo/scripts/nas_restore_daily.py"
ssh qnap-nasdolce-archive "$nas_python $nas_home/nas_restore_daily.py --prepare-runtime"
install -m 644 "$repo/scripts/ops/erp-backup-health.service" /etc/systemd/system/
install -m 644 "$repo/scripts/ops/erp-backup-health.timer" /etc/systemd/system/
systemd-analyze verify /etc/systemd/system/erp-backup-health.service /etc/systemd/system/erp-backup-health.timer
systemctl daemon-reload
# Enable the timer only after the first successful service run has been inspected.
echo 'Installed; run systemctl start erp-backup-health.service, inspect health-latest.json, then enable --now the timer.'
