#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "No existe ${PYTHON_BIN}. Activa o crea el virtualenv antes de usar Point browser pull." >&2
  exit 1
fi

exec "$PYTHON_BIN" "${ROOT_DIR}/scripts/point_browser_pull.py" "$@"
