#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="${POS_BRIDGE_INVENTORY_LABEL:-com.pollyanasdolce.pos-bridge-inventory}"
HOUR="${POS_BRIDGE_INVENTORY_HOUR:-2}"
MINUTE="${POS_BRIDGE_INVENTORY_MINUTE:-15}"
REALTIME_MINUTES="${POS_BRIDGE_REALTIME_INTERVAL_MINUTES:-5}"
LAUNCH_AGENTS_DIR="${HOME}/Library/LaunchAgents"
PLIST_PATH="${LAUNCH_AGENTS_DIR}/${LABEL}.plist"
STDOUT_PATH="${ROOT_DIR}/storage/pos_bridge/logs/launchd_pos_bridge_inventory.log"
STDERR_PATH="${ROOT_DIR}/storage/pos_bridge/logs/launchd_pos_bridge_inventory.error.log"
RUNNER_PATH="${ROOT_DIR}/scripts/run_pos_bridge_inventory_sync.sh"

mkdir -p "${LAUNCH_AGENTS_DIR}"
mkdir -p "${ROOT_DIR}/storage/pos_bridge/logs"
chmod 755 "${RUNNER_PATH}"
xattr -d com.apple.provenance "${RUNNER_PATH}" >/dev/null 2>&1 || true
xattr -d com.apple.quarantine "${RUNNER_PATH}" >/dev/null 2>&1 || true

cat > "${PLIST_PATH}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${RUNNER_PATH}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${ROOT_DIR}</string>
  <key>RunAtLoad</key>
  <false/>
PLIST

if [ -n "${REALTIME_MINUTES}" ] && [[ "${REALTIME_MINUTES}" =~ ^[0-9]+$ ]] && [ "${REALTIME_MINUTES}" -ge 5 ]; then
cat >> "${PLIST_PATH}" <<PLIST
  <key>StartInterval</key>
  <integer>$((REALTIME_MINUTES * 60))</integer>
PLIST
else
cat >> "${PLIST_PATH}" <<PLIST
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>${HOUR}</integer>
    <key>Minute</key>
    <integer>${MINUTE}</integer>
  </dict>
PLIST
fi

cat >> "${PLIST_PATH}" <<PLIST
  <key>StandardOutPath</key>
  <string>${STDOUT_PATH}</string>
  <key>StandardErrorPath</key>
  <string>${STDERR_PATH}</string>
</dict>
</plist>
PLIST

launchctl bootout "gui/$(id -u)" "${PLIST_PATH}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_PATH}"
launchctl enable "gui/$(id -u)/${LABEL}"

echo "Instalado launchd: ${LABEL}"
if [ -n "${REALTIME_MINUTES}" ] && [[ "${REALTIME_MINUTES}" =~ ^[0-9]+$ ]] && [ "${REALTIME_MINUTES}" -ge 5 ]; then
  echo "Intervalo programado: cada ${REALTIME_MINUTES} minutos"
else
  echo "Hora programada: $(printf '%02d:%02d' "${HOUR}" "${MINUTE}")"
fi
echo "Plist: ${PLIST_PATH}"
echo "Stdout: ${STDOUT_PATH}"
echo "Stderr: ${STDERR_PATH}"
