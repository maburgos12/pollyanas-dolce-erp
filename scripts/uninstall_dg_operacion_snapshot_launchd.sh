#!/bin/bash
set -euo pipefail

LABEL="${DG_OPERACION_SNAPSHOT_LABEL:-com.pollyanasdolce.dg-operacion-snapshot}"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"

launchctl bootout "gui/$(id -u)" "${PLIST_PATH}" >/dev/null 2>&1 || true
rm -f "${PLIST_PATH}"

echo "Desinstalado launchd: ${LABEL}"
echo "Plist eliminado: ${PLIST_PATH}"
