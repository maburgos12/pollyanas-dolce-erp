#!/bin/bash
set -euo pipefail

LABEL="${POS_BRIDGE_SALES_CLOSE_LABEL:-com.pollyanasdolce.pos-bridge-sales-close}"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"

launchctl bootout "gui/$(id -u)" "${PLIST_PATH}" >/dev/null 2>&1 || true
rm -f "${PLIST_PATH}"

echo "Desinstalado launchd: ${LABEL}"
echo "Plist eliminado: ${PLIST_PATH}"
