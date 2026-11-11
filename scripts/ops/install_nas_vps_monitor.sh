#!/bin/sh
# Run once with sudo on QNAP, from a reviewed Git release staged on the NAS.
set -eu
test "$(id -u)" = 0 || { echo 'Requires sudo on the NAS'; exit 1; }
SOURCE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
BASE=/share/CACHEDEV1_DATA/.erp-vps-monitor
PYTHON=/share/CACHEDEV1_DATA/.qpkg/HybridBackup/CloudConnector3/python/bin/python3
test -x "$PYTHON"
test -f "$SOURCE/nas_vps_monitor.py"
test -f "$SOURCE/telegram.json"
test ! -e "$BASE" || { echo 'Existing installation: inspect before updating'; exit 1; }
umask 077
mkdir "$BASE"
chmod 755 "$BASE"
cp "$SOURCE/nas_vps_monitor.py" "$BASE/nas_vps_monitor.py"
chmod 644 "$BASE/nas_vps_monitor.py"
cp "$SOURCE/telegram.json" "$BASE/telegram.json"
chmod 600 "$BASE/telegram.json"
mkdir "$BASE/state"
chown 1000:100 "$BASE/telegram.json" "$BASE/state"
# The monitor always runs without administrator privileges, even from root cron.
/usr/bin/sudo -n -u DolcePasteleria "$PYTHON" -I "$BASE/nas_vps_monitor.py"
cp -p /etc/config/crontab "$BASE/crontab.before"
chmod 600 "$BASE/crontab.before"
cp /etc/config/crontab "$BASE/crontab.next"
printf '\n* * * * * /usr/bin/sudo -n -u DolcePasteleria %s -I %s/nas_vps_monitor.py --entry-point cron >/dev/null 2>&1 # erp-external-vps-monitor\n' "$PYTHON" "$BASE" >> "$BASE/crontab.next"
cmp -s /etc/config/crontab "$BASE/crontab.before" || { echo 'Cron changed concurrently; installation stopped'; exit 1; }
cat "$BASE/crontab.next" > /etc/config/crontab
if ! /usr/bin/crontab -u admin /etc/config/crontab; then
    cat "$BASE/crontab.before" > /etc/config/crontab
    echo 'Cron load failed; original configuration restored'
    exit 1
fi
/etc/init.d/crond.sh restart
/usr/bin/crontab -u admin -l | grep -F '# erp-external-vps-monitor'
echo 'Installed. Verify state/latest.json after the next minute.'
