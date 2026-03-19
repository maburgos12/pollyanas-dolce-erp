#!/bin/bash
set -euo pipefail

LABEL="${POS_BRIDGE_INVENTORY_LABEL:-com.pollyanasdolce.pos-bridge-inventory}"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"

launchctl bootout "gui/$(id -u)" "${PLIST_PATH}" >/dev/null 2>&1 || true
rm -f "${PLIST_PATH}"

echo "Desinstalado launchd: ${LABEL}"
echo "Plist eliminado: ${PLIST_PATH}"
