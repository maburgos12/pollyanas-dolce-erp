#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="${POS_BRIDGE_SALES_CLOSE_LABEL:-com.pollyanasdolce.pos-bridge-sales-close}"
HOUR="${POS_BRIDGE_SALES_CLOSE_HOUR:-1}"
MINUTE="${POS_BRIDGE_SALES_CLOSE_MINUTE:-30}"
LAUNCH_AGENTS_DIR="${HOME}/Library/LaunchAgents"
PLIST_PATH="${LAUNCH_AGENTS_DIR}/${LABEL}.plist"
STDOUT_PATH="${ROOT_DIR}/storage/pos_bridge/logs/launchd_pos_bridge_sales_close.log"
STDERR_PATH="${ROOT_DIR}/storage/pos_bridge/logs/launchd_pos_bridge_sales_close.error.log"
RUNNER_PATH="${ROOT_DIR}/scripts/run_pos_bridge_daily_sales_close.sh"

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
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>${HOUR}</integer>
    <key>Minute</key>
    <integer>${MINUTE}</integer>
  </dict>
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
echo "Hora programada: $(printf '%02d:%02d' "${HOUR}" "${MINUTE}")"
echo "Plist: ${PLIST_PATH}"
echo "Stdout: ${STDOUT_PATH}"
echo "Stderr: ${STDERR_PATH}"
