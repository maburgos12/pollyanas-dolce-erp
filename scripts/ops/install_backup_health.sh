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
original_sha=e8cba9307cf6486aa2429004a0f81e62bdbd472f8afe6b6117f9da6a28de1e2f
baseline="$rotator.baseline.$original_sha"
# Keep the known original outside Git so rollback can restore its Python entrypoint.
if ! cmp -s "$rotator" <(printf '%s\n' "$wrapper"); then
  test "$(sha256sum "$rotator" | cut -d ' ' -f 1)" = "$original_sha"
  if ! test -e "$baseline" && ! test -L "$baseline"; then
    (set -o noclobber; cat "$rotator" > "$baseline")
    chmod 700 "$baseline"
  fi
fi
test -f "$baseline" && ! test -L "$baseline" && test -O "$baseline"
/usr/bin/python3 -c 'import os, sys; assert os.stat(sys.argv[1]).st_mode & 0o777 == 0o700' "$baseline"
test "$(sha256sum "$baseline" | cut -d ' ' -f 1)" = "$original_sha"
/usr/bin/python3 -m py_compile "$repo/scripts/rotate_verified_backups.py" "$repo/scripts/vps_backup_health.py"
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes qnap-nasdolce-archive \
  "test -d $nas_home/runtime && test -O $nas_home && chmod 700 $nas_home"
# Deploy only the reviewed revision already delivered through Git to this VPS.
ssh qnap-nasdolce-archive "umask 077; cat > $nas_home/nas_restore_daily.py.next && chmod 600 $nas_home/nas_restore_daily.py.next && mv $nas_home/nas_restore_daily.py.next $nas_home/nas_restore_daily.py" < "$repo/scripts/nas_restore_daily.py"
ssh qnap-nasdolce-archive "$nas_python $nas_home/nas_restore_daily.py --prepare-runtime"
install -m 644 "$repo/scripts/ops/erp-backup-health.service" /etc/systemd/system/
install -m 644 "$repo/scripts/ops/erp-backup-health.timer" /etc/systemd/system/
systemd-analyze verify /etc/systemd/system/erp-backup-health.service /etc/systemd/system/erp-backup-health.timer
printf '%s\n' "$wrapper" > "$rotator.next"
chmod 700 "$rotator.next"
mv "$rotator.next" "$rotator"
systemctl daemon-reload
# Enable the timer only after the first successful service run has been inspected.
echo 'Installed; verify the restricted NAS service key and encrypted SMB runtime using --verify-only --nas-side --smb-direct before enabling the timer. The scheduled service retires older verified VPS copies only, never NAS copies or Docker data.'
