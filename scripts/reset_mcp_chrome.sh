#!/usr/bin/env bash
set -euo pipefail

PROFILE_DIR="${HOME}/Library/Caches/ms-playwright/mcp-chrome"
HARD_RESET="0"

usage() {
  cat <<'EOF'
Uso:
  ./scripts/reset_mcp_chrome.sh [--hard]

Qué hace:
  - Mata procesos colgados de playwright-mcp y Chrome ligados al perfil MCP.
  - Limpia locks Singleton del perfil persistente del MCP.
  - Con --hard, además elimina por completo el perfil mcp-chrome.

Notas:
  - Diseñado para macOS.
  - No toca el perfil normal de Chrome del usuario.
EOF
}

for arg in "$@"; do
  case "$arg" in
    --hard)
      HARD_RESET="1"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Argumento no reconocido: $arg" >&2
      usage
      exit 1
      ;;
  esac
done

echo "[reset_mcp_chrome] Cerrando procesos ligados al MCP..."
pkill -f "playwright-mcp" || true
pkill -f "run-cli-server --daemon-session" || true
pkill -f "Google Chrome.*ms-playwright/mcp-chrome" || true

if [[ -d "$PROFILE_DIR" ]]; then
  echo "[reset_mcp_chrome] Limpiando locks Singleton en $PROFILE_DIR"
  rm -f \
    "$PROFILE_DIR/SingletonLock" \
    "$PROFILE_DIR/SingletonSocket" \
    "$PROFILE_DIR/SingletonCookie"
fi

if [[ "$HARD_RESET" == "1" ]]; then
  echo "[reset_mcp_chrome] Eliminando perfil persistente completo..."
  rm -rf "$PROFILE_DIR"
fi

echo "[reset_mcp_chrome] Estado final:"
if lsof -iTCP -sTCP:LISTEN | rg -q "playwright|Chrome"; then
  echo "  Quedan procesos de navegador activos. Revisa manualmente si estorban." >&2
else
  echo "  Sin listeners residuales detectados para este flujo."
fi

echo "[reset_mcp_chrome] Listo."
